using System;
using System.Buffers;
using System.IO;
using System.Linq;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Json;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using CommunityToolkit.HighPerformance;
using NsqSharp.Utils;
using NsqSharp.Utils.Channels;
using NsqSharp.Utils.Extensions;

namespace NsqSharp.Core
{
    // https://github.com/nsqio/go-nsq/blob/master/conn.go

    /// <summary>
    /// IdentifyResponse represents the metadata
    /// returned from an IDENTIFY command to nsqd
    /// </summary>
    [DataContract]
    public class IdentifyResponse
    {
        /// <summary>Max RDY count</summary>
        [DataMember(Name = "max_rdy_count")]
        public long MaxRdyCount { get; set; }
        /// <summary>Use TLSv1</summary>
        [DataMember(Name = "tls_v1")]
        public bool TLSv1 { get; set; }
        /// <summary>Use Deflate compression</summary>
        [DataMember(Name = "deflate")]
        public bool Deflate { get; set; }
        /// <summary>Use Snappy compression</summary>
        [DataMember(Name = "snappy")]
        public bool Snappy { get; set; }
        /// <summary>Auth required</summary>
        [DataMember(Name = "auth_required")]
        public bool AuthRequired { get; set; }
    }

    /// <summary>
    /// AuthResponse represents the metadata
    /// returned from an AUTH command to nsqd
    /// </summary>
    [DataContract]
    public class AuthResponse
    {
        /// <summary>Identity</summary>
        [DataMember(Name = "identity")]
        public string Identity { get; set; }
        /// <summary>Identity URL</summary>
        [DataMember(Name = "identity_url")]
        public string IdentityUrl { get; set; }
        /// <summary>Permission Count</summary>
        [DataMember(Name = "permission_count")]
        public long PermissionCount { get; set; }
    }

    internal class msgResponse
    {
        public Message msg { get; set; }
        public Command cmd { get; set; }
        public bool success { get; set; }
        public bool backoff { get; set; }
    }

    internal interface INsqContexet
    {
        void SetLogger(ILogger l, string format);

        void OnContextIOError(Exception ex);
    }

    internal interface INsqCommandWritter
    {
        void WriteCommand(Command cmd);

        ValueTask WriteCommandAsync(Command cmd, CancellationToken token);
    }

    internal class NsqConnectionBuilder
    {
        private readonly string _addr;
        private readonly Config _config;
        private readonly IConnDelegate _delegate;
        private ILogger? logger;
        private string logFmt = "{0}";
        private CoreNsqConnectionHandler? Handler;

        private NsqContext? ConnectionContext;
        private ITcpConn? Connection;
        private bool IsHandshaked = false;
        private IdentifyResponse? LastResult;
        public NsqConnectionBuilder(string addr, Config config, IConnDelegate connDelegate)
        {
            _addr = addr;
            _config = config;
            _delegate = connDelegate;
        }

        public void SetLogger(ILogger l, string logFormat)
        {
            logger = l;
            this.logFmt = logFormat;
        }

        public async Task DialAsync(CancellationToken token = default)
        {
            var ctx = new NsqContext(_addr, _config, _delegate);
            if(this.logger !=null && !string.IsNullOrWhiteSpace(this.logFmt))
            {
                ctx.SetLogger(this.logger, this.logFmt);
            }
            var conn = await Net.DialTimeoutAsync("tcp", _addr, _config.DialTimeout, token);
            var _conn = (ITcpConn)conn;

            ConnectionContext = ctx;
            Connection = _conn;
            Handler = new CoreNsqConnectionHandler(ctx, _conn, this.logger ?? null);
        }

        public async Task<IdentifyResponse?> HandShakeAsync(Func<INsqCommandWritter, CancellationToken, Task> initialHandshake, 
            CancellationToken token)
        {
            try
            {
                if (IsHandshaked)
                    return LastResult; // already handshaked
                if (ConnectionContext == null)
                    throw new Exception("Call Dial() before HandShake()");
                if (Connection == null)
                    throw new Exception("Call Dial() before HandShake()");
                if (Handler == null)
                    throw new Exception("Call Dial() before HandShake()");
                IsHandshaked = true;
                LastResult = await ConnectionContext.Handshake(Connection, Handler, initialHandshake, token);
                return LastResult;
            }
            catch
            {
                Handler?.Close();
                throw;
            }
           
        }

        public NsqContext GetNsqContext()
        {
            if (ConnectionContext == null)
                throw new Exception("Does not connected");
            return ConnectionContext;
        }

        public INsqConnection GetRuntimeConnection()
        {
            if (ConnectionContext == null || LastResult == null || Handler == null)
                throw new Exception("Does not connected or handshaked");
            return new RunTimeNsqConnectionHandler(Handler.ConnectionCancelContext, Handler, ConnectionContext.CommandWriter);
        }
    }

    /// <summary>
    /// Conn represents a connection to nsqd
    ///
    /// Conn exposes a set of callbacks for the
    /// various events that occur on a connection
    /// </summary>
    internal partial class NsqContext : INsqContexet
    {
        private static readonly byte[] HEARTBEAT_BYTES = Encoding.UTF8.GetBytes("_heartbeat_");

        internal long _messagesInFlight;
        private long _maxRdyCount;
        private long _rdyCount;
        private long _lastRdyCount;
        private long _lastMsgTimestamp;

        private readonly Config _config;

        private readonly string _addr;

        private readonly IConnDelegate _delegate;

        private ILogger? _logger;
        private string _logFmt = "{0}";

        private readonly Channel<Command> _cmdChan;
        private readonly Channel<(Command, TaskCompletionSource<bool>)> _cmdTraceChan;
        private readonly Channel<msgResponse> _consumerMsgResponseChan;

        private readonly Once _stopper = new();
        private CancellationTokenSource ConnectionCancelContext = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="NsqContext"/> class.
        /// </summary>
        public NsqContext(string addr, Config config, IConnDelegate connDelegate)
        {
            if (string.IsNullOrEmpty(addr))
                throw new ArgumentNullException(nameof(addr));

            _addr = addr;

            _config = config.Clone();
            _delegate = connDelegate;

            _maxRdyCount = 2500;
            _lastMsgTimestamp = DateTime.Now.UnixNano();

            _cmdChan = Channel.CreateUnbounded<Command>();
            _cmdTraceChan = Channel.CreateUnbounded<(Command, TaskCompletionSource<bool>)>();
            _consumerMsgResponseChan = Channel.CreateUnbounded<msgResponse>();
        }

        public ChannelWriter<Command> CommandWriter => _cmdChan.Writer;

        /// <summary>
        /// SetLogger assigns the logger to use as well as a level.
        ///
        /// The format parameter is expected to be a printf compatible string with
        /// a single {0} argument.  This is useful if you want to provide additional
        /// context to the log messages that the connection will print, the default
        /// is '({0})'.
        /// </summary>
        public void SetLogger(ILogger l, string format)
        {
            _logger = l;
            _logFmt = format;
            if (string.IsNullOrWhiteSpace(_logFmt))
            {
                _logFmt = "({0})";
            }
        }

        /// <summary>
        /// handshake the nsqd connection
        /// (including IDENTIFY) and returns the IdentifyResponse
        /// </summary>
        internal async Task<IdentifyResponse?> Handshake(
            ITcpConn _conn,
            CoreNsqConnectionHandler handler,
            Func<INsqCommandWritter, CancellationToken, Task> initialHandshake,
            CancellationToken token)
        {
            _conn.ReadTimeout = _config.ReadTimeout;
            _conn.WriteTimeout = _config.WriteTimeout;
            var cts = ConnectionCancelContext = handler.ConnectionCancelContext;
            try
            {
                await handler.WriteAsync(Protocol.MagicV2.AsMemory(0, Protocol.MagicV2.Length), token);
            }
            catch (Exception ex)
            {
                cts.Cancel();
                throw new Exception(string.Format("[{0}] failed to write magic - {1}", _addr, ex.Message), ex);
            }

            IdentifyResponse? resp;
            try
            {
                resp = await NsqConnectionHandshake.IdentifyAsync(this._config, handler, handler, this._logger, _conn, token);
                if(resp != null)
                    this._maxRdyCount = resp.MaxRdyCount;
            }
            catch (ErrIdentify ex)
            {
                if (_addr.Contains(":4151"))
                {
                    throw new ErrIdentify("Error connecting to nsqd. It looks like you tried to connect to the HTTP port " +
                        "(4151), use the TCP port (4150) instead.", ex);
                }
                else if (_addr.Contains(":4160") || _addr.Contains(":4161"))
                {
                    throw new ErrIdentify("Error connecting to nsqd. It looks like you tried to connect to nsqlookupd. " +
                        "Producers must connect to nsqd over TCP (4150). Consumers can connect to nsqd over TCP (4150) using " +
                        "Consumer.ConnectToNsqd or to nsqlookupd (4161) using Consumer.ConnectToNsqLookupd.", ex);
                }
                cts.Cancel();
                throw;
            }

            try
            {
                if (resp != null && resp.AuthRequired)
                {
                    if (string.IsNullOrEmpty(_config.AuthSecret))
                    {
                        WriteLog(LogLevel.Error, "Auth Required");
                        throw new Exception("Auth Required");
                    }
                    await NsqConnectionHandshake.AuthAsync(handler, _config.AuthSecret, _logger, token);
                }
                await initialHandshake(handler, token);
            }
            catch
            {
                cts.Cancel();
                throw;
            }

            var readLoopCtx = new RunningLoopContext(handler.ConnectionCancelContext);
            var writeLoopCtx = new RunningLoopContext(handler.ConnectionCancelContext);
            _ = RunReadLoop(handler, readLoopCtx);
            _ = RunWriteLoop(handler, writeLoopCtx);
            return resp;
        }

        /// <summary>
        /// Close idempotently initiates connection close
        /// </summary>
        public void Close()
        {
            ConnectionCancelContext.Cancel();
        }

        /// <summary>
        /// IsClosing indicates whether or not the
        /// connection is currently in the processing of
        /// gracefully closing
        /// </summary>
        public bool IsClosing => ConnectionCancelContext.Token.IsCancellationRequested;

        /// <summary>
        /// RDY returns the current RDY count
        /// </summary>
        public long RDY
        {
            get { return _rdyCount; }
        }

        /// <summary>
        /// LastRDY returns the previously set RDY count
        /// </summary>
        public long LastRDY
        {
            get { return _lastRdyCount; }
        }

        private readonly object _rdyLocker = new();
        /// <summary>
        /// SetRDY stores the specified RDY count
        /// </summary>
        public void SetRDY(long rdy)
        {
            lock (_rdyLocker) // atmoic rdy set
            {
                _rdyCount = rdy;
                _lastRdyCount = rdy;
                WriteCommandToChannel(Command.Ready(rdy));
            }
        }

        private static readonly DateTime _epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private const long Second = 1_000_000_000;
        private static DateTime Unix(long sec, long nsec)
        {
            long ticks = sec * Second / 100 + nsec / 100;
            return _epoch.AddTicks(ticks);
        }

        /// <summary>
        /// MaxRDY returns the nsqd negotiated maximum
        /// RDY count that it will accept for this connection
        /// </summary>
        public long MaxRDY
        {
            get { return _maxRdyCount; }
        }

        /// <summary>
        /// LastMessageTime returns a time.Time representing
        /// the time at which the last message was received
        /// </summary>
        public DateTime LastMessageTime
        {
            get { return Unix(0, _lastMsgTimestamp); }
        }

        /// <summary>
        /// RemoteAddr returns the configured destination nsqd address
        /// </summary>
        public string RemoteAddr()
        {
            //return _conn.RemoteAddr(); // TODO
            return _addr;
        }

        /// <summary>
        /// String returns the fully-qualified address
        /// </summary>
        public override string ToString()
        {
            return _addr;
        }

        public bool WriteCommandToChannel(Command cmd)
        {
            return _cmdChan.Writer.TryWrite(cmd);
        }

        public async Task<bool> WriteCommandTask(Command cmd, CancellationToken token = default)
        {
            var tcs = new TaskCompletionSource<bool>();
            token.Register(() => tcs.TrySetCanceled());
            if (this.ConnectionCancelContext.IsCancellationRequested)
            {
                return false; // cannot sent message due to context exit
            }
            else if (!_cmdTraceChan.Writer.TryWrite((cmd,tcs)))
            {
                return false; // the channel cannot accept more messages
            }
            return await tcs.Task;
        }

        public void OnContextIOError(Exception ex)
        {
            this._delegate.OnIOError(this, ex);
        }
    }

    internal class CoreNsqConnectionHandler : IReader, IWriter, INsqConnection, INsqCommandWritter
    {
        private readonly ITcpConn _conn;

        private readonly ILogger? _logger;

        private readonly INsqContexet NsqConnectionInstance;

        /// <summary>
        /// Cancel if the io stream is closed
        /// </summary>
        public CancellationTokenSource ConnectionCancelContext { get; } = new();

        /// <summary>
        /// Initializes a new instance of the <see cref="NsqContext"/> class.
        /// </summary>
        public CoreNsqConnectionHandler(INsqContexet nsqContext,
            ITcpConn conn, 
            ILogger? logger)
        {
            NsqConnectionInstance = nsqContext;
            _conn = conn;
            _logger = logger;
            ConnectionCancelContext.Token.Register(() =>
            {
                conn.Close();
            });
        }

        public IdentifyResponse Connect(Action<INsqCommandWritter> initialHandshake) =>
            throw new NotImplementedException();

        public void SetLogger(ILogger l, string format) => this.NsqConnectionInstance.SetLogger(l, format);

        public void Close()
        {
            ConnectionCancelContext.Cancel();
        }

        public ValueTask ReadExactlyAsync(Memory<byte> memory, CancellationToken cancellationToken = default)
        {
            return _conn.ReadExactlyAsync(memory, cancellationToken);
        }

        public ValueTask WriteAsync(ReadOnlyMemory<byte> memory, CancellationToken cancellationToken = default)
        {
            return _conn.WriteAsync(memory, cancellationToken);
        }

        public async ValueTask WriteCommandAsync(Command cmd, CancellationToken token)
        {
            int size = cmd.GetByteCount();
            byte[] _bigBuf = ArrayPool<byte>.Shared.Rent(size);
            try
            {
                await this.WriteAsync(
                    cmd.SerializeToMemory(_bigBuf.AsMemory(0, size)), 
                    token
                );
            }
            catch (Exception ex)
            {
                object msg = (ex is ConnectionClosedException) ? ex.Message : (object)ex;
                _logger?.Output(LogLevel.Error, string.Format("Conn.WriteCommand IO error - {0}", msg));
                this.NsqConnectionInstance.OnContextIOError(ex);
                throw;
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(_bigBuf);
            }
        }

        public void WriteCommand(Command cmd)
        {
            throw new NotImplementedException();
        }
    }

    internal class RunningLoopContext
    {
        public CancellationTokenSource LoopTokenSource { get; }

        private readonly CancellationTokenSource ConnectionTokenSource;

        public RunningLoopContext(CancellationTokenSource connectionTokenSource)
        {
            ConnectionTokenSource = connectionTokenSource;
            LoopTokenSource = CancellationTokenSource.CreateLinkedTokenSource(connectionTokenSource.Token);
        }

        public void SetIoClosing()
        {
            this.ConnectionTokenSource.Cancel();
        }
    }

    internal class RunTimeNsqConnectionHandler : INsqConnection, INsqCommandWritter
    {
        private readonly INsqConnection Handler;
        private readonly ChannelWriter<Command> Writer;
        private readonly CancellationTokenSource Context;
        public RunTimeNsqConnectionHandler(CancellationTokenSource ctx, INsqConnection handler, ChannelWriter<Command> writer)
        {
            Writer = writer;
            Handler = handler;
            Context = ctx;
        }

        public void Close()
        {
            this.Context.Cancel();
        }

        public IdentifyResponse Connect(Action<INsqCommandWritter> initialHandshake)
        {
            return Handler.Connect(initialHandshake);
        }

        public void SetLogger(ILogger l, string format)
        {
            Handler.SetLogger(l, format);
        }

        public void WriteCommand(Command command)
        {
            Writer.TryWrite(command);
        }

        public ValueTask WriteCommandAsync(Command cmd, CancellationToken token)
        {
            Writer.TryWrite(cmd);
            return ValueTask.CompletedTask;
        }
    }

    internal class EmptyNsqConnectionHandler : INsqConnection
    {
        public void Close()
        {
            throw new NotImplementedException();
        }

        public IdentifyResponse Connect(Action<INsqCommandWritter> initialHandshake)
        {
            throw new NotImplementedException();
        }

        public void SetLogger(ILogger l, string format)
        {
            throw new NotImplementedException();
        }

        public void WriteCommand(Command command)
        {
            throw new NotImplementedException();
        }
    }

    internal static class NsqConnectionHandshake
    {
        public static async Task<IdentifyResponse?> IdentifyAsync(Config _config, 
            INsqCommandWritter commandWriter, 
            IReader reader, 
            ILogger? logger,
            IConn originalInstance,
            CancellationToken token)
        {
            var ci = new IdentifyRequest
            {
                client_id = _config.ClientID,
                hostname = _config.Hostname,
                user_agent = _config.UserAgent,
                short_id = _config.ClientID, // deprecated
                long_id = _config.Hostname,  // deprecated
                tls_v1 = (_config.TlsConfig != null),
                deflate = false, //_config.Deflate; // TODO: Deflate
                deflate_level = 6, //_config.DeflateLevel; // TODO: Deflate
                snappy = false, //_config.Snappy; // TODO: Snappy
                feature_negotiation = true
            };
            if (_config.HeartbeatInterval <= TimeSpan.Zero)
            {
                ci.heartbeat_interval = -1;
            }
            else
            {
                ci.heartbeat_interval = (int)_config.HeartbeatInterval.TotalMilliseconds;
            }
            ci.sample_rate = _config.SampleRate;
            ci.output_buffer_size = _config.OutputBufferSize;
            if (_config.OutputBufferTimeout <= TimeSpan.Zero)
            {
                ci.output_buffer_timeout = -1;
            }
            else
            {
                ci.output_buffer_timeout = (int)_config.OutputBufferTimeout.TotalMilliseconds;
            }
            ci.msg_timeout = (int)_config.MessageTimeout.TotalMilliseconds;

            try
            {
                
                await commandWriter.WriteCommandAsync(Command.Identify(ci), token);
                using var buffer = new NsqBufferContext();
                await buffer.ReadUnpackedResponseAsync(reader, token);

                FrameType frameType = buffer.FrameType;
                string json = Encoding.UTF8.GetString(buffer.Body.Span);

                if (frameType == FrameType.Error)
                {
                    throw new ErrIdentify(json);
                }

                // check to see if the server was able to respond w/ capabilities
                // i.e. it was a JSON response
                if (buffer.Body.Span[0] != '{')
                {
                    return null;
                }

                string respJson = Encoding.UTF8.GetString(buffer.Body.Span);
                logger?.Output(LogLevel.Debug, string.Format("IDENTIFY response: {0}", respJson));

                IdentifyResponse? resp;
                var serializer = new DataContractJsonSerializer(typeof(IdentifyResponse));
                using (var memoryStream = buffer.Body.AsStream())
                {
                    resp = (IdentifyResponse?)serializer.ReadObject(memoryStream);
                }
                if(resp == null)
                    throw new Exception("Failed to deserialize IdentifyResponse");

                if (resp.TLSv1)
                {
                    logger?.Output(LogLevel.Info, "upgrading to TLS");
                    await UpgradeTlsAsync(_config.TlsConfig, reader, originalInstance, token);
                }

                // TODO: Deflate
                /*if resp.Deflate {
                    c.log(LogLevelInfo, "upgrading to Deflate")
                    err := c.upgradeDeflate(c.config.DeflateLevel)
                    if err != nil {
                        return nil, ErrIdentify{err.Error()}
                    }
                }*/

                // TODO: Snappy
                /*if resp.Snappy {
                    c.log(LogLevelInfo, "upgrading to Snappy")
                    err := c.upgradeSnappy()
                    if err != nil {
                        return nil, ErrIdentify{err.Error()}
                    }
                }*/

                // now that connection is bootstrapped, enable read buffering
                // (and write buffering if it's not already capable of Flush())

                // TODO: Determine if TcpClient or Socket should be used, and what needs to be done about buffering
                /*c.r = bufio.NewReader(c.r)
                if _, ok := c.w.(flusher); !ok {
                    c.w = bufio.NewWriter(c.w)
                }*/

                return resp;
            }
            catch (ErrIdentify)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new ErrIdentify(ex.Message, ex);
            }
        }

        private static async Task UpgradeTlsAsync(TlsConfig? tlsConfig, IReader reader, IConn originalInstance, CancellationToken token)
        {
            ArgumentNullException.ThrowIfNull(tlsConfig, nameof(tlsConfig));

            if(originalInstance is not TcpConn _conn)
            {
                throw new ArgumentException("originalInstance must be of type TcpConn to upgrade TLS");
            }
            await _conn.UpgradeTlsAsync(tlsConfig);

            using var buffer = new NsqBufferContext();
            await buffer.ReadUnpackedResponseAsync(reader, token);
            
            if (buffer.FrameType != FrameType.Response || !buffer.Body.Span.SequenceEqual(Encoding.UTF8.GetBytes("OK")))
                throw new Exception("invalid response from TLS upgrade");
        }

        public static async Task AuthAsync(CoreNsqConnectionHandler handler, string secret, ILogger? logger, CancellationToken token)
        {
            handler.WriteCommand(Command.Auth(secret));
            using var buffer = new NsqBufferContext();
            await buffer.ReadUnpackedResponseAsync(handler, token);
            
            FrameType frameType = buffer.FrameType;
            string json = Encoding.UTF8.GetString(buffer.Body.Span);

            if (frameType == FrameType.Error)
            {
                throw new Exception(string.Format("Error authenticating {0}", json));
            }

            AuthResponse? resp;
            var serializer = new DataContractJsonSerializer(typeof(AuthResponse));
            using (var memoryStream = buffer.Body.AsStream())
            {
                resp = (AuthResponse?)serializer.ReadObject(memoryStream);
            }
            if (resp == null)
                throw new Exception("Failed to deserialize AuthResponse");

            logger?.Output(LogLevel.Info, string.Format("Auth accepted. Identity: {0} {1} Permissions: {2}",
                resp.Identity, resp.IdentityUrl, resp.PermissionCount));
        }
    }

    internal partial class NsqContext
    {

        /*private void upgradeDeflate()
        {
            // TODO
        }

        private void upgradeSnappy()
        {
            // TODO
        }*/
        private async Task RunReadLoop(CoreNsqConnectionHandler handler, RunningLoopContext readLoopCtx)
        {
            try
            {
                var cts = readLoopCtx.LoopTokenSource;
                var token = readLoopCtx.LoopTokenSource.Token;
                while (!token.IsCancellationRequested)
                {
                    using var buffer = new NsqBufferContext();
                    try
                    {
                        await buffer.ReadUnpackedResponseAsync(handler, token);
                    }
                    catch (Exception ex)
                    {
                        // TODO: determine equivalent exception type from .NET runtime
                        // if !strings.Contains(err.Error(), "use of closed network connection")
                        if (!token.IsCancellationRequested)
                        {
                            WriteLog(LogLevel.Error, string.Format("IO error on ReadUnpackedResponse - {0}", ex));
                            _delegate.OnIOError(this, ex);
                        }
                        cts.Cancel();
                        break;
                    }

                    FrameType frameType = buffer.FrameType;
                    if (frameType == FrameType.Response && HEARTBEAT_BYTES.AsSpan().SequenceEqual(buffer.Body.Span))
                    {
                        _delegate.OnHeartbeat(this);
                        this.WriteCommandToChannel(Command.Nop());
                        continue;
                    }

                    switch (frameType)
                    {
                        case FrameType.Response:
                            _delegate.OnResponse(this, buffer.Body);
                            break;
                        case FrameType.Message:
                            Message msg;
                            try
                            {
                                msg = Message.DecodeMessage(buffer.Body);
                            }
                            catch (Exception ex)
                            {
                                WriteLog(LogLevel.Error, string.Format("IO error on DecodeMessage - {0}", ex));
                                _delegate.OnIOError(this, ex);
                                cts.Cancel();
                                break;
                            }
                            msg.Delegate = new ConnMessageDelegate(context: this);
                            msg.NsqdAddress = ToString();

                            Interlocked.Decrement(ref _rdyCount);
                            Interlocked.Increment(ref _messagesInFlight);
                            _lastMsgTimestamp = DateTime.Now.UnixNano();

                            _delegate.OnMessage(this, msg);
                            break;
                        case FrameType.Error:
                            string errMsg = Encoding.UTF8.GetString(buffer.Body.Span);
                            WriteLog(LogLevel.Error, string.Format("protocol error - {0}", errMsg));
                            _delegate.OnError(this, buffer.Body);
                            break;
                        default:
                            // TODO: what would 'err' be in this case?
                            // https://github.com/nsqio/go-nsq/blob/v1.0.3/conn.go#L518
                            var unknownFrameTypeEx = new Exception(string.Format("unknown frame type {0}", frameType));
                            WriteLog(LogLevel.Error, string.Format("IO error - {0}", unknownFrameTypeEx.Message));
                            _delegate.OnIOError(this, unknownFrameTypeEx);
                            break;
                    }
                }
            }
            finally
            {
                //exit:
                var messagesInFlight = _messagesInFlight;
                if (messagesInFlight == 0)
                {
                    // if we exited readLoop with no messages in flight
                    // we need to explicitly trigger the close because
                    // writeLoop won't
                    Dispose();
                }
                else
                {
                    WriteLog(LogLevel.Warning, string.Format("delaying close, {0} outstanding messages", messagesInFlight));
                }
                WriteLog(LogLevel.Info, "readLoop exiting");
            }
        }

        private async Task RunWriteLoop(CoreNsqConnectionHandler handler, RunningLoopContext writeLoopCtx)
        {
            var token = writeLoopCtx.LoopTokenSource.Token;
            var select =
            Select
                .CaseReceive(_cmdChan, cmd =>
                {
                    try
                    {
                        handler.WriteCommand(cmd);
                    }
                    catch (Exception ex)
                    {
                        object msg = (ex is ConnectionClosedException) ? ex.Message : (object)ex;
                        WriteLog(LogLevel.Error, string.Format("Conn.writeLoop, cmdChan: error sending command {0} - {1}", cmd, msg));
                        Dispose();
                    }
                    // TODO: Create PR to remove unnecessary continue in go-nsq
                    // https://github.com/nsqio/go-nsq/blob/v1.0.3/conn.go#L552
                })
                .CaseReceive(_cmdTraceChan, (pair) =>
                {
                    var (cmd, tcs) = pair;
                    try
                    {
                        handler.WriteCommand(cmd);
                        tcs.TrySetResult(true);
                    }
                    catch (Exception ex)
                    {
                        object msg = (ex is ConnectionClosedException) ? ex.Message : (object)ex;
                        WriteLog(LogLevel.Error, string.Format("Conn.writeLoop, cmdChan: error sending command {0} - {1}", cmd, msg));
                        tcs.TrySetException(ex);
                        Dispose();
                    }
                })
                .CaseReceive(_consumerMsgResponseChan, resp =>
                {
                    // Decrement this here so it is correct even if we can't respond to nsqd
                    var msgsInFlight = Interlocked.Decrement(ref _messagesInFlight);

                    if (resp.success)
                    {
                        WriteLog(LogLevel.Debug, string.Format("FIN {0}", resp.msg.Id));
                        _delegate.OnMessageFinished(this, resp.msg);
                        _delegate.OnResume(this);
                    }
                    else
                    {
                        WriteLog(LogLevel.Debug, string.Format("REQ {0}", resp.msg.Id));
                        _delegate.OnMessageRequeued(this, resp.msg);
                        if (resp.backoff)
                        {
                            _delegate.OnBackoff(this);
                        }
                        else
                        {
                            _delegate.OnContinue(this);
                        }
                    }

                    WriteCommandToChannel(resp.cmd);
                });
            
            // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
            while (!token.IsCancellationRequested)
            {
                await select.ExecuteAsync(token);
            }

            WriteLog(LogLevel.Info, "writeLoop exiting");
        }

        private void Dispose()
        {
            // a "clean" connection close is orchestrated as follows:
            //
            //     1. CLOSE cmd sent to nsqd
            //     2. CLOSE_WAIT response received from nsqd
            //     3. set c.closeFlag
            //     4. readLoop() exits
            //         a. if messages-in-flight > 0 delay close()
            //             i. writeLoop() continues receiving on c.msgResponseChan chan
            //                 x. when messages-in-flight == 0 call close()
            //         b. else call close() immediately
            //     5. c.exitChan close
            //         a. writeLoop() exits
            //             i. c.drainReady close
            //     6a. launch cleanup() goroutine (we're racing with intraprocess
            //        routed messages, see comments below)
            //         a. wait on c.drainReady
            //         b. loop and receive on c.msgResponseChan chan
            //            until messages-in-flight == 0
            //            i. ensure that readLoop has exited
            //     6b. launch waitForCleanup() goroutine
            //         b. wait on waitgroup (covers readLoop() and writeLoop()
            //            and cleanup goroutine)
            //         c. underlying TCP connection close
            //         d. trigger Delegate OnClose()
            //

            _stopper.Do(() =>
            {
                WriteLog(LogLevel.Info, "clean close complete");
                _delegate.OnClose(this);
            });
        }
        internal void OnMessageFinish(Message m)
        {
            _consumerMsgResponseChan.Writer.TryWrite(new msgResponse { msg = m, cmd = Command.Finish(m.ID), success = true, backoff = true });
        }

        internal TimeSpan OnMessageRequeue(Message m, TimeSpan? delay, bool backoff)
        {
            if (delay == null || delay <= TimeSpan.Zero)
            {
                // linear delay
                delay = TimeSpan.FromTicks(_config.DefaultRequeueDelay.Ticks * m.Attempts);
                // bound the requeueDelay to configured max
                if (delay > _config.MaxRequeueDelay)
                {
                    delay = _config.MaxRequeueDelay;
                }
            }

            _consumerMsgResponseChan.Writer.TryWrite(new msgResponse
            {
                msg = m,
                cmd = Command.ReQueue(m.ID, delay.Value),
                success = false,
                backoff = backoff
            });

            return delay.Value;
        }

        internal void OnMessageTouch(Message m)
        {
            _cmdChan.Writer.TryWrite(Command.Touch(m.ID));
        }

        private void WriteLog(LogLevel lvl, string line)
        {
            // TODO: Review format string
            _logger?.Output(lvl, string.Format("{0} {1}",
                string.Format(_logFmt, ToString()), line));
        }
    }

    /// <summary>
    /// Identify request.
    /// </summary>
    [DataContract]
    public class IdentifyRequest
    {
        /// <summary>client_id</summary>
        [DataMember(Name = "client_id")]
        public string client_id { get; set; }
        /// <summary>hostname</summary>
        [DataMember(Name = "hostname")]
        public string hostname { get; set; }
        /// <summary>user_agent</summary>
        [DataMember(Name = "user_agent")]
        public string user_agent { get; set; }
        /// <summary>short_id (deprecated)</summary>
        [DataMember(Name = "short_id")]
        public string short_id { get; set; }
        /// <summary>long_id (deprecated)</summary>
        [DataMember(Name = "long_id")]
        public string long_id { get; set; }
        /// <summary>tls_v1</summary>
        [DataMember(Name = "tls_v1")]
        public bool tls_v1 { get; set; }
        /// <summary>deflate</summary>
        [DataMember(Name = "deflate")]
        public bool deflate { get; set; }
        /// <summary>deflate_level</summary>
        [DataMember(Name = "deflate_level")]
        public int deflate_level { get; set; }
        /// <summary>snappy</summary>
        [DataMember(Name = "snappy")]
        public bool snappy { get; set; }
        /// <summary>feature_negotiation</summary>
        [DataMember(Name = "feature_negotiation")]
        public bool feature_negotiation { get; set; }
        /// <summary>heartbeat_interval</summary>
        [DataMember(Name = "heartbeat_interval")]
        public int heartbeat_interval { get; set; }
        /// <summary>sample_rate</summary>
        [DataMember(Name = "sample_rate")]
        public int sample_rate { get; set; }
        /// <summary>output_buffer_size</summary>
        [DataMember(Name = "output_buffer_size")]
        public long output_buffer_size { get; set; }
        /// <summary>output_buffer_timeout</summary>
        [DataMember(Name = "output_buffer_timeout")]
        public int output_buffer_timeout { get; set; }
        /// <summary>msg_timeout</summary>
        [DataMember(Name = "msg_timeout")]
        public int msg_timeout { get; set; }
    }
}
