using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NsqSharp.Core;
using NsqSharp.Utils;
using NsqSharp.Utils.Channels;
using NsqSharp.Utils.Loggers;

namespace NsqSharp
{
    // https://github.com/nsqio/go-nsq/blob/master/producer.go

    /// <summary>
    /// IConn interface
    /// </summary>
    internal interface INsqConnection
    {
        /// <summary>
        /// SetLogger assigns the logger to use as well as a level.
        ///
        /// The format parameter is expected to be a printf compatible string with
        /// a single {0} argument.  This is useful if you want to provide additional
        /// context to the log messages that the connection will print, the default
        /// is '({0})'.
        /// </summary>
        void SetLogger(ILogger l, string format);

        /// <summary>
        /// Connect dials and bootstraps the nsqd connection
        /// (including IDENTIFY) and returns the IdentifyResponse
        /// </summary>
        IdentifyResponse Connect(Action<INsqCommandWritter> initialHandshake);

        /// <summary>
        /// Close idempotently initiates connection close
        /// </summary>
        void Close();

        /// <summary>
        /// WriteCommand is a thread safe method to write a Command
        /// to this connection, and flush.
        /// </summary>
        void WriteCommand(Command command);
    }

    /// <summary>
    /// <para>Producer is a high-level type to publish to NSQ.</para>
    ///
    /// <para>A Producer instance is 1:1 with a destination nsqd
    /// and will lazily connect to that instance (and re-connect)
    /// when Publish commands are executed.</para>
    /// <seealso cref="Publish(string, string)"/>
    /// <seealso cref="Publish(string, byte[])"/>
    /// <seealso cref="Stop"/>
    /// </summary>
    public sealed partial class Producer
    {
        private static long _instCount;

        internal long _id;
        private readonly string _addr;
        private INsqConnection _conn;
        private readonly Config _config;

        private readonly ILogger _logger;

        private readonly Channel<byte[]> _responseChan;
        private readonly Channel<byte[]> _errorChan;

        private readonly Channel<ProducerResponse> _transactionChan;
        private readonly Queue<ProducerResponse> _transactions = new();
        private int _state;

        private int _concurrentProducers;
        private int _stopFlag;

        private CancellationTokenSource _exitChanTokenSource = new();
        private CancellationToken exitToken => _exitChanTokenSource.Token;
        private readonly SemaphoreSlim _guard = new SemaphoreSlim(1, 1);

    }

    /// <summary>
    /// ProducerResponse is returned by the async publish methods
    /// to retrieve metadata about the command after the
    /// response is received.
    /// </summary>
    public record ProducerResponse
    {
        internal Command _cmd { get; init; } = Command.Nop();
        internal TaskCompletionSource<ProducerResponse>? _doneChan { get; init; }

        /// <summary>
        /// the error (or nil) of the publish command
        /// </summary>
        public Exception? Error { get; init; }

        /// <summary>
        /// the slice of variadic arguments passed to PublishAsync or MultiPublishAsync
        /// </summary>
        public object[] Args { get; init; } = [];

        internal void SetAsFinished()
        {
            if(this.Error != null)
            {
                _doneChan?.TrySetException(this.Error);
            }
            else
            {
                _doneChan?.TrySetResult(this);
            }
        }
    }

    public sealed partial class Producer : IConnDelegate
    {
        /// <summary>
        /// Initializes a new instance of the Producer class.
        /// </summary>
        /// <param name="nsqdAddress">The nsqd address.</param>
        public Producer(string nsqdAddress)
            : this(nsqdAddress, new Config())
        {
        }

        /// <summary>
        /// Initializes a new instance of the Producer class.
        /// </summary>
        /// <param name="nsqdAddress">The nsqd address.</param>
        /// <param name="config">The config. After Config is passed in the values are
        /// no longer mutable (they are copied).</param>
        public Producer(string nsqdAddress, Config config)
            : this(nsqdAddress, new ConsoleLogger(LogLevel.Info), config, null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the Producer class.
        /// </summary>
        /// <param name="nsqdAddress">The nsqd address.</param>
        /// <param name="logger">
        /// The logger. Default = <see cref="ConsoleLogger"/>(<see cref="E:LogLevel.Info"/>).
        /// </param>
        public Producer(string nsqdAddress, ILogger logger)
            : this(nsqdAddress, logger, new Config(), null)
        {
        }

        /// <summary>
        /// Initializes a new instance of the Producer class.
        /// </summary>
        /// <param name="nsqdAddress">The nsqd address.</param>
        /// <param name="logger">
        /// The logger. Default = <see cref="ConsoleLogger"/>(<see cref="E:LogLevel.Info"/>).
        /// </param>
        /// <param name="config">The config. Values are copied, changing the properties on <paramref name="config"/>
        /// after the constructor is called will have no effect on the <see cref="Producer"/>.</param>
        public Producer(string nsqdAddress, ILogger logger, Config config)
            : this(nsqdAddress, logger, config, null)
        {
        }

        private Producer(string addr, ILogger logger, Config config, object? _)
        {
            if (string.IsNullOrEmpty(addr))
                throw new ArgumentNullException(nameof(addr));
            ArgumentNullException.ThrowIfNull(logger, nameof(logger));
            ArgumentNullException.ThrowIfNull(config, nameof(config));
                

            _id = Interlocked.Increment(ref _instCount);

            config.Validate();

            _addr = addr;
            _config = config.Clone();

            _logger = logger;

            _transactionChan = Channel.CreateUnbounded<ProducerResponse>();
            _responseChan = Channel.CreateUnbounded<byte[]>();
            _errorChan = Channel.CreateUnbounded<byte[]>();

            _conn = new EmptyNsqConnectionHandler();
        }

        /// <summary>Returns the address of the Producer.</summary>
        /// <returns>The address of the Producer.</returns>
        public override string ToString()
        {
            return _addr;
        }

        /// <summary>
        /// <para>Stop initiates a graceful stop of the Producer (permanent).</para>
        ///
        /// <para>NOTE: this blocks until completion</para>
        /// </summary>
        public void Stop()
        {
            if (Interlocked.CompareExchange(ref _stopFlag, value: 1, comparand: 0) != 0)
            {
                // already closed
                return;
            }
            log(LogLevel.Info, "stopping");
            CloseTcpConnection();
            _logger.Flush();
        }

        /// <summary>
        ///     <para>Publishes a message <paramref name="body"/> to the specified <paramref name="topic"/>
        ///     but does not wait for the response from nsqd.</para>
        ///     
        ///     <para>When the Producer eventually receives the response from nsqd, the Task will return a
        ///     <see cref="ProducerResponse"/> instance with the supplied <paramref name="args"/> and the response error if
        ///     present.</para>
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="body">The message body.</param>
        /// <param name="args">A variable-length parameters list containing arguments. These arguments will be returned on
        ///     <see cref="ProducerResponse.Args"/>.
        /// </param>
        /// <returns>A Task&lt;ProducerResponse&gt; which can be awaited.</returns>
        public async Task<ProducerResponse> PublishAsync(string topic, byte[] body, params object[] args)
        {
            await CheckAndTryConnectAsync();
            var doneChan = new TaskCompletionSource<ProducerResponse>();
            SendCommandToChannel(Command.Publish(topic, body), 
                doneChan, 
                args);
            return await doneChan.Task;
        }

        /// <summary>
        ///     <para>Publishes a string <paramref name="value"/> message to the specified <paramref name="topic"/>
        ///     but does not wait for the response from nsqd.</para>
        ///     
        ///     <para>When the Producer eventually receives the response from nsqd, the Task will return a
        ///     <see cref="ProducerResponse"/> instance with the supplied <paramref name="args"/> and the response error if
        ///     present.</para>
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="value">The message body.</param>
        /// <param name="args">A variable-length parameters list containing arguments. These arguments will be returned on
        ///     <see cref="ProducerResponse.Args"/>.
        /// </param>
        /// <returns>A Task&lt;ProducerResponse&gt; which can be awaited.</returns>
        public Task<ProducerResponse> PublishAsync(string topic, string value, params object[] args)
        {
            return PublishAsync(topic, Encoding.UTF8.GetBytes(value), args);
        }

        /// <summary>
        ///     <para>Publishes a collection of message <paramref name="bodies"/> to the specified <paramref name="topic"/>
        ///     but does not wait for the response from nsqd.</para>
        ///     
        ///     <para>When the Producer eventually receives the response from nsqd, the Task will return a
        ///     <see cref="ProducerResponse"/> instance with the supplied <paramref name="args"/> and the response error if
        ///     present.</para>
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="bodies">The collection of message bodies.</param>
        /// <param name="args">A variable-length parameters list containing arguments. These arguments will be returned on
        ///     <see cref="ProducerResponse.Args"/>.
        /// </param>
        /// <returns>A Task&lt;ProducerResponse&gt; which can be awaited.</returns>
        public async Task<ProducerResponse> MultiPublishAsync(string topic, IEnumerable<byte[]> bodies, params object[] args)
        {
            await CheckAndTryConnectAsync();
            var doneChan = new TaskCompletionSource<ProducerResponse>();
            SendCommandToChannel(Command.MultiPublish(topic, bodies), 
                doneChan, 
                args);
            return await doneChan.Task;
        }

        /// <summary>
        ///     Synchronously publishes a message <paramref name="body"/> to the specified <paramref name="topic"/>, throwing
        ///     an exception if publish failed.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="body">The message body.</param>
        public void Publish(string topic, byte[] body, bool fireAndForgot = true)
        {
            if(!fireAndForgot)
                SendCommandWait(Command.Publish(topic, body));
            else
            {
                SendCommandNoWait(Command.Publish(topic, body));
            }
        }

        /// <summary>
        ///     Synchronously publishes string <paramref name="value"/> message to the specified <paramref name="topic"/>,
        ///     throwing an exception if publish failed.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="value">The message body.</param>
        public void Publish(string topic, string value, bool fireAndForgot = true)
        {
            Publish(topic, Encoding.UTF8.GetBytes(value), fireAndForgot);
        }

        /// <summary>
        ///     Synchronously publishes a collection of message <paramref name="bodies"/> to the specified
        ///     <paramref name="topic"/>, throwing an exception if publish failed.
        /// </summary>
        /// <param name="topic">The topic to publish to.</param>
        /// <param name="bodies">The collection of message bodies.</param>
        public void MultiPublish(string topic, IEnumerable<byte[]> bodies, bool fireAndForgot = true)
        {
            var cmd = Command.MultiPublish(topic, bodies);
            if (!fireAndForgot)
                SendCommandWait(cmd);
            else
            {
                SendCommandNoWait(cmd);
            }
        }
        private void CheckAndTryConnectSync()
        {
            if (_state != (int)State.Connected)
            {
                var task = Connect();
                task.ConfigureAwait(false).GetAwaiter().GetResult();
            }
        }

        private async Task CheckAndTryConnectAsync()
        {
            if (_state != (int)State.Connected)
                await Connect();
        }

        private void SendCommandWait(Command cmd)
        {
            var doneChan = new TaskCompletionSource<ProducerResponse>();
            try
            {
                CheckAndTryConnectSync();
                SendCommandToChannel(cmd, doneChan);
            }
            catch (Exception ex)
            {
                doneChan.TrySetException(ex);
                throw;
            }

            doneChan.Task.Wait();
        }

        private void SendCommandNoWait(Command cmd)
        {
            CheckAndTryConnectSync();
            SendCommandToChannel(cmd, null);
        }

        private void SendCommandToChannel(Command cmd, 
            TaskCompletionSource<ProducerResponse>? doneChan, 
            params object[] args)
        {
            ProducerResponse? t;
            Interlocked.Increment(ref _concurrentProducers);

            t = new ProducerResponse
            {
                _cmd = cmd,
                _doneChan = doneChan,
                Args = args
            };

            _transactionChan.Writer.TryWrite(t);

            Interlocked.Decrement(ref _concurrentProducers);
        }

        /// <summary>
        ///     Connects to nsqd. Calling this method is optional; otherwise, Connect will be lazy invoked when Publish is
        ///     called.
        /// </summary>
        /// <exception cref="ErrStopped">Thrown if the Producer has been stopped.</exception>
        /// <exception cref="ErrNotConnected">Thrown if the Producer is currently waiting to close and reconnect.</exception>
        public async Task Connect()
        {
            try
            {
                await _guard.WaitAsync();
                if (_stopFlag == 1)
                    throw new ErrStopped();

                switch (_state)
                {
                    case (int)State.Init:
                        break;
                    case (int)State.Connected:
                        return;
                    default:
                        throw new ErrNotConnected();
                }

                log(LogLevel.Info, string.Format("{0} connecting to nsqd", _addr));

                var builder = new NsqConnectionBuilder(_addr, _config, this);
                builder.SetLogger(_logger, string.Format("P{0} ({{0}})", _id));
                try
                {
                    await builder.Dial();
                    builder.HandShake(_ => { });
                    _conn = builder.GetRuntimeConnection();
                }
                catch (Exception ex)
                {
                    log(LogLevel.Error, string.Format("({0}) error connecting to nsqd - {1}", _addr, ex.Message));
                    throw;
                }

                _state = (int)State.Connected;
                log(LogLevel.Info, string.Format("{0} connected to nsqd", _addr));
                _ = StartProducer(this.exitToken); // Background process
            }
            finally
            {
                _guard.Release();
            }
        }

        private void CloseTcpConnection()
        {
            // no need to lock, user must ensure stop and connect are not called concurrently
            const int newValue = (int)State.Disconnected;
            const int comparand = (int)State.Connected;
            if (Interlocked.CompareExchange(ref _state, newValue, comparand) != comparand)
            {
                return;
            }
            _exitChanTokenSource.Cancel(); // notify stop for sending to nsqd
            _exitChanTokenSource = new CancellationTokenSource(); // recreate for next connection context

            _conn.Close();
            //wait
            _state = (int)State.Init;
        }

        private async Task StartProducer(CancellationToken exitToken)
        {
            try
            {
                var select = Select
                .CaseReceive(_transactionChan, t =>
                {
                    _transactions.Enqueue(t);
                    if(this._state != (int)State.Connected)
                        return;
                    _conn.WriteCommand(t._cmd); // no need to handle send failure, OnIOError will be called
                })
                .CaseReceive(_responseChan, data =>
                    popTransaction(FrameType.Response, data)
                )
                .CaseReceive(_errorChan, data =>
                    popTransaction(FrameType.Error, data)
                );

                while (!exitToken.IsCancellationRequested)
                {
                    await select.ExecuteAsync(exitToken);
                }
            }
            finally
            {
                transactionCleanup();
                log(LogLevel.Info, "exiting producer router");
            }
        }

        private void popTransaction(FrameType frameType, byte[] data)
        {
            var t = _transactions.Dequeue();
            if (frameType == FrameType.Error)
            {
                t = t with { Error = new ErrProtocol(Encoding.UTF8.GetString(data)) };
            }
            t.SetAsFinished();
        }

        private void transactionCleanup()
        {
            // clean up transactions we can easily account for
            foreach (var t in _transactions)
            {
                var t1 = t with { Error = new ErrNotConnected() };
                t1.SetAsFinished();
            }
            _transactions.Clear();

            while(_transactionChan.Reader.TryRead(out var t2))
            {
                var t3 = t2 with { Error = new ErrNotConnected() }; 
                t3.SetAsFinished();
            }
        }

        private void log(LogLevel lvl, string line)
        {
            // TODO: proper width formatting
            _logger.Output(lvl, string.Format("P{0} {1}", _id, line));
        }

        void IConnDelegate.OnResponse(NsqContext c, byte[] data)
        {
            _responseChan.Writer.TryWrite(data);
        }

        void IConnDelegate.OnError(NsqContext c, byte[] data)
        {
            _errorChan.Writer.TryWrite(data);
        }

        void IConnDelegate.OnIOError(NsqContext c, Exception err)
        {
            CloseTcpConnection();
        }

        void IConnDelegate.OnClose(NsqContext c)
        {
            _transactionChan.Writer.Complete();
            _errorChan.Writer.Complete();
            _responseChan.Writer.Complete();
            _exitChanTokenSource.Cancel();
        }

        void IConnDelegate.OnMessage(NsqContext c, Message m) { }
        void IConnDelegate.OnMessageFinished(NsqContext c, Message m) { }
        void IConnDelegate.OnMessageRequeued(NsqContext c, Message m) { }
        void IConnDelegate.OnBackoff(NsqContext c) { }
        void IConnDelegate.OnContinue(NsqContext c) { }
        void IConnDelegate.OnResume(NsqContext c) { }
        void IConnDelegate.OnHeartbeat(NsqContext c) { }
    }
}
