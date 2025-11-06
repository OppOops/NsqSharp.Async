using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Net;
using System.Security.Cryptography;
using System.Text;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using NsqSharp.Api;
using NsqSharp.Core;
using NsqSharp.Utils;
using NsqSharp.Utils.Channels;
using NsqSharp.Utils.Extensions;
using NsqSharp.Utils.Loggers;

namespace NsqSharp
{
    /// <summary>
    ///     <para>Message processing logging interface for <see cref="Consumer" /> when failure (when the attempts exceed configuration).</para>
    ///     <para>When an exception is thrown the <see cref="Consumer"/> will automatically handle REQ'ing the message.</para>
    /// </summary>

    /// <summary>
    ///     <para>Message processing interface for <see cref="Consumer" />.</para>
    ///     <para>When the <see cref="HandleMessage"/> method returns the <see cref="Consumer"/> will automatically handle
    ///     FIN'ing the message.</para>
    ///     <para>When an exception is thrown the <see cref="Consumer"/> will automatically handle REQ'ing the message.</para>
    /// </summary>
    /// <seealso cref="Consumer.AddHandler"/>
    public interface IHandler
    {
        /// <summary>
        ///     Called when a <see cref="Message"/> has exceeded the <see cref="Consumer"/> specified
        ///     <see cref="Config.MaxAttempts"/>.
        /// </summary>
        /// <param name="message">The failed message.</param>
        void LogFailedMessage(IMessage message);
        bool RunAsAsync { get; }

        /// <summary>Handles a message.</summary>
        /// <param name="message">The message.</param>
        void HandleMessage(IMessage message);

        Task HandleMessageAsync(IMessage message, CancellationToken token);
    }

    /// <summary>
    ///     <see cref="IDiscoveryFilter" /> is accepted by <see cref="Consumer.SetBehaviorDelegate"/>
    ///     for filtering the nsqd addresses returned from nsqlookupd.
    /// </summary>
    public interface IDiscoveryFilter
    {
        /// <summary>Filters a list of nsqd addresses.</summary>
        /// <param name="nsqds">nsqd addresses returned by nsqlookupd.</param>
        /// <returns>The filtered list of nsqd addresses to use.</returns>
        IEnumerable<string> Filter(IEnumerable<string> nsqds);
    }

    /// <summary>
    ///     <see cref="ConsumerStats" /> represents a snapshot of the state of a <see cref="Consumer"/>'s connections and the
    ///     messages it has seen.
    /// </summary>
    public class ConsumerStats
    {
        /// <summary>The number of messages received.</summary>
        /// <value>The number of messages received.</value>
        public long MessagesReceived { get; internal set; }

        /// <summary>The number of messages finished.</summary>
        /// <value>The number of messages finished.</value>
        public long MessagesFinished { get; internal set; }

        /// <summary>The number of messages requeued.</summary>
        /// <value>The number of messages requeued.</value>
        public long MessagesReQueued { get; internal set; }

        /// <summary>The number of nsqd connections.</summary>
        /// <value>The number of nsqd connections.</value>
        public int Connections { get; internal set; }
    }

    internal enum BackoffSignal
    {
        BackoffFlag,
        ContinueFlag,
        ResumeFlag
    }

    /// <summary>
    ///     <para><see cref="Consumer"/> is a high-level type to consume messages from NSQ.</para>
    ///     
    ///     <para>A <see cref="Consumer"/> instance is supplied an <see cref="IHandler"/> instance to
    ///     <see cref="AddHandler"/>. The supplied instance will be executed concurrently to process the stream of
    ///     messages consumed from the specified topic/channel.</para>
    ///     
    ///     <para>If configured, it will poll nsqlookupd instances and handle connection (and reconnection) to any discovered
    ///     nsqds. See <see cref="ConnectToNsqLookupdAsync"/>.</para>
    /// </summary>
    /// <example>
    ///     <code>
    ///     
    ///     using System;
    ///     using System.Text;
    ///     using NsqSharp;
    ///     
    ///     class Program
    ///     {
    ///         static void Main()  
    ///         {
    ///             // To test, run:
    ///             // nsqd.exe
    ///             // to_nsq.exe -topic=test-topic-name -nsqd-tcp-address=127.0.0.1:4150
    ///
    ///             // Create a new Consumer for each topic/channel
    ///             var consumer = new Consumer("test-topic-name", "channel-name");
    ///             consumer.AddHandler(new MessageHandler());
    ///             consumer.ConnectToNsqd("127.0.0.1:4150"); // nsqd tcp address/port
    ///             //consumer.ConnectToNsqLookupd("127.0.0.1:4161"); // nsqlookupd http address/port
    ///     
    ///             Console.WriteLine("Listening for messages. Press enter to stop...");
    ///             Console.ReadLine();
    ///     
    ///             consumer.Stop();
    ///         }
    ///     }
    ///     
    ///     public class MessageHandler : IHandler
    ///     {
    ///         // Handles a message.
    ///         public void HandleMessage(IMessage message)
    ///         {
    ///             string msg = Encoding.UTF8.GetString(message.Body);
    ///             Console.WriteLine(msg);
    ///         }
    ///     
    ///         // Called when a message has exceeded the specified MaxAttempts.
    ///         public void LogFailedMessage(IMessage message)
    ///         {
    ///             // Log failed messages
    ///         }
    ///     }
    ///     </code>
    /// </example>
    /// <seealso cref="AddHandler"/>
    /// <seealso cref="ConnectToNsqdAsync"/>
    /// <seealso cref="ConnectToNsqLookupdAsync"/>
    /// <seealso cref="Stop()"/>
    public sealed partial class Consumer : IConnDelegate
    {
        private static readonly byte[] CLOSE_WAIT_BYTES = Encoding.UTF8.GetBytes("CLOSE_WAIT");

        private static long _instCount;

        private long _messagesReceived;
        private long _messagesFinished;
        private long _messagesRequeued;
        private long _totalRdyCount;
        private long _backoffDuration;
        private int _backoffCounter;
        private int _maxInFlight;
        private long _perConnMaxInFlightOverride;

        private readonly ReaderWriterLockSlim _mtx = new();

        private readonly ILogger _logger;

        private IDiscoveryFilter? _behaviorDelegate;

        private readonly long _id;
        private readonly string _topic;
        private readonly string _channel;
        private readonly Config _config;


        private int _needRdyRedistributed;

        private readonly ReaderWriterLockSlim _backoffMtx = new(); // TODO: Dispose

        private readonly Channel<Message> _incomingMessages;

        private readonly ReaderWriterLockSlim _rdyRetryMtx = new(); // TODO: Dispose
        private readonly ConcurrentDictionary<string, CancellationTokenSource> _rdyRetryTimers;

        private readonly ConcurrentDictionary<string, CancellationTokenSource> _pendingConnections;
        private readonly ConcurrentDictionary<string, NsqContext> _connections;

        private readonly List<string> _nsqdTCPAddrs = [];

        // used at connection close to force a possible reconnect
        private readonly Channel<int> _lookupdRecheckChan;
        private readonly List<string> _lookupdHTTPAddrs = [];
        private int _lookupdQueryIndex;

        private int _runningHandlers;
        private int _stopFlag;
        private int _connectedFlag;
        private readonly Once _stopHandler = new Once();
        private readonly Once _exitHandler = new Once();

        // read from this channel to block until consumer is cleanly stopped
        private readonly CancellationTokenSource ConsumerStopContext = new();

        /// <summary>
        ///     <para>Creates a new instance of <see cref="Consumer"/> for the specified <paramref name="topic"/> and
        ///     <paramref name="channel"/>.</para>
        ///
        ///     <para>Uses the default <see cref="Config"/> and <see cref="ConsoleLogger"/> with log level
        ///     <see cref="F:LogLevel.Info"/>.</para>
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when one or more required arguments are null.</exception>
        /// <exception cref="ArgumentException">Thrown when the <paramref name="topic"/> or <paramref name="channel"/>
        ///     exceed the maximum length or contain invalid characters. Topic and channel names must be greater than 0 and
        ///     less than or equal to 64 characters longer and must match the pattern "^[\.a-zA-Z0-9_-]+(#ephemeral)?$".
        /// </exception>
        /// <remarks>
        ///     <para>Uses <see cref="ConsoleLogger"/> with <see cref="F:LogLevel.Info"/> to log messages.</para>
        ///     <para>Uses the default <see cref="Config"/> to configure this <see cref="Consumer"/>.</para>
        /// </remarks>
        /// <param name="topic">The topic name.</param>
        /// <param name="channel">The channel name.</param>
        public Consumer(string topic, string channel)
            : this(topic, channel, new ConsoleLogger(LogLevel.Info))
        {
        }

        /// <summary>
        ///     <para>Creates a new instance of <see cref="Consumer"/> for the specified <paramref name="topic"/> and
        ///     <paramref name="channel"/>, using the specified <paramref name="logger"/>.</para>
        ///     <para>Uses the default <see cref="Config"/>.</para>
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when one or more required arguments are null.</exception>
        /// <exception cref="ArgumentException">Thrown when the <paramref name="topic"/> or <paramref name="channel"/>
        ///     exceed the maximum length or contain invalid characters. Topic and channel names must be greater than 0 and
        ///     less than or equal to 64 characters longer and must match the pattern "^[\.a-zA-Z0-9_-]+(#ephemeral)?$".
        /// </exception>
        /// <remarks>Uses the default <see cref="Config"/> to configure this <see cref="Consumer"/>.</remarks>
        /// <param name="topic">The topic name.</param>
        /// <param name="channel">The channel name.</param>
        /// <param name="logger">The <see cref="ILogger"/> instance.</param>
        public Consumer(string topic, string channel, ILogger logger)
            : this(topic, channel, logger, new Config())
        {
        }

        /// <summary>
        ///     <para>Creates a new instance of <see cref="Consumer"/> for the specified <paramref name="topic"/> and
        ///     <paramref name="channel"/>, using the specified <paramref name="config"/>.</para>
        ///     <para>Uses <see cref="ConsoleLogger"/> with log level <see cref="F:LogLevel.Info"/>.</para>
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when one or more required arguments are null.</exception>
        /// <exception cref="ArgumentException">Thrown when the <paramref name="topic"/> or <paramref name="channel"/>
        ///     exceed the maximum length or contain invalid characters. Topic and channel names must be greater than 0 and
        ///     less than or equal to 64 characters longer and must match the pattern "^[\.a-zA-Z0-9_-]+(#ephemeral)?$".
        /// </exception>
        /// <remarks>Uses <see cref="ConsoleLogger"/> with <see cref="F:LogLevel.Info"/> to log messages.</remarks>
        /// <param name="topic">The topic name.</param>
        /// <param name="channel">The channel name.</param>
        /// <param name="config">The <see cref="Config"/> settings. After config is passed in the values are no longer mutable
        ///     (they are copied).
        /// </param>
        public Consumer(string topic, string channel, Config config)
            : this(topic, channel, new ConsoleLogger(LogLevel.Info), config)
        {
        }

        /// <summary>
        ///     <para>Creates a new instance of <see cref="Consumer"/> for the specified <paramref name="topic"/> and
        ///     <paramref name="channel"/>, using the specified <paramref name="logger"/> and <paramref name="config"/>.</para>
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when one or more required arguments are null.</exception>
        /// <exception cref="ArgumentException">Thrown when the <paramref name="topic"/> or
        ///     <paramref name="channel"/> exceed the maximum length or contain invalid characters. Topic and channel names
        ///     must be greater than 0 and less than or equal to 64 characters longer and must match the pattern "^[\.a-zA-Z0-
        ///     9_-]+(#ephemeral)?$".
        /// </exception>
        /// <param name="topic">The topic name.</param>
        /// <param name="channel">The channel name.</param>
        /// <param name="logger">The <see cref="ILogger"/> instance.</param>
        /// <param name="config">The <see cref="Config"/> settings. After config is passed in the values are no longer mutable
        ///     (they are copied).
        /// </param>
        public Consumer(string topic, string channel, ILogger logger, Config config)
        {
            if (string.IsNullOrEmpty(topic))
                throw new ArgumentNullException(nameof(topic));
            if (string.IsNullOrEmpty(channel))
                throw new ArgumentNullException(nameof(channel));
            config.Validate();

            if (!Protocol.IsValidTopicName(topic))
                throw new ArgumentException("invalid topic name", nameof(topic));

            if (!Protocol.IsValidChannelName(channel))
                throw new ArgumentException("invalid channel name", nameof(channel));

            _id = Interlocked.Increment(ref _instCount);

            _topic = topic;
            _channel = channel;
            _config = config.Clone();
            _logger = logger;

            _maxInFlight = config.MaxInFlight;

            _incomingMessages = Channel.CreateUnbounded<Message>();

            _rdyRetryTimers = new();
            _pendingConnections = new();
            _connections = new();

            _lookupdRecheckChan = Channel.CreateBounded<int>(1);

            _ = RunRdyLoop(this.ConsumerStopContext.Token);
        }

        /// <summary>
        ///     Retrieves the current connection and message <see cref="ConsumerStats"/> for this <see cref="Consumer"/>.
        /// </summary>
        /// <returns>Messages received, messages finished, messages requeued, and number of nsqd connections.</returns>
        public ConsumerStats GetStats()
        {
            return new ConsumerStats
            {
                MessagesReceived = _messagesReceived,
                MessagesFinished = _messagesFinished,
                MessagesReQueued = _messagesRequeued,
                Connections = GetCurrentConnections().Count
            };
        }

        private List<NsqContext> GetCurrentConnections()
        {
            _mtx.EnterReadLock();
            try
            {
                return [.. _connections.Values];
            }
            finally
            {
                _mtx.ExitReadLock();
            }
        }

        /// <summary>
        ///     <see cref="SetBehaviorDelegate" /> takes an <see cref="IDiscoveryFilter"/>
        ///     that can filter the list of nsqd addresses returned by nsqlookupd.
        /// </summary>
        /// <param name="discoveryFilter">The discovery filter.</param>
        /// <seealso cref="ConnectToNsqLookupdAsync"/>
        public void SetBehaviorDelegate(IDiscoveryFilter discoveryFilter)
        {
            // TODO: can go-nsq take a DiscoveryFilter instead of interface{} ?
            _behaviorDelegate = discoveryFilter;
        }

        /// <summary>
        /// perConnMaxInFlight calculates the per-connection max-in-flight count.
        ///
        /// This may change dynamically based on the number of connections to nsqd the Consumer
        /// is responsible for.
        /// </summary>
        private long perConnMaxInFlight()
        {
            if (_perConnMaxInFlightOverride == 0)
            {
                long b = getMaxInFlight();
                int connCount = GetCurrentConnections().Count;
                long s = (connCount == 0 ? 1 : b / connCount);
                return Math.Min(Math.Max(1, s), b);
            }
            else
            {
                return _perConnMaxInFlightOverride;
            }
        }

        /// <summary>
        ///     Indicates whether any connections for this <see cref="Consumer"/> are blocked on processing before being able
        ///     to receive more messages (ie. RDY count of 0 and not exiting).
        /// </summary>
        /// <value><c>true</c> if this <see cref="Consumer"/> instance is starved; otherwise, <c>false</c>.</value>
        public bool IsStarved
        {
            get
            {
                foreach (var conn in GetCurrentConnections())
                {
                    // TODO: if in backoff, would IsStarved return true? what's the impact?
                    // TODO: go-nsq PR, use conn.LastRDY() which does the atomic load for us
                    long threshold = (long)(conn.LastRDY * 0.85);
                    long inFlight = conn._messagesInFlight;
                    if (inFlight >= threshold && inFlight > 0)
                    {
                        return true;
                    }
                }
                return false;
            }
        }

        private int getMaxInFlight()
        {
            return _maxInFlight;
        }

        /// <summary>
        ///     <para>Sets a new maximum number of messages this <see cref="Consumer"/> instance will allow in-flight, and
        ///     updates all existing connections as appropriate.</para>
        ///     
        ///     <para>For example, <see cref="ChangeMaxInFlight"/>(0) would pause message flow.</para>
        ///     
        ///     <para>If already connected, it updates the reader RDY state for each connection.</para>
        /// </summary>
        /// <param name="maxInFlight">The maximum number of message to allow in flight.</param>
        public void ChangeMaxInFlight(int maxInFlight)
        {
            if (getMaxInFlight() == maxInFlight)
                return;

            _maxInFlight = maxInFlight;

            foreach (var c in GetCurrentConnections())
            {
                maybeUpdateRDY(c);
            }
        }

        /// <summary>
        ///     <para>Adds nsqlookupd addresses to the list for this <see cref="Consumer"/> instance.</para>
        ///     <para>If it is the first to be added, it initiates an HTTP request to discover nsqd
        ///     producers for the configured topic.</para>
        ///     
        ///     <para>A new thread is created to handle continual polling.</para>
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="addresses"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="addresses"/> is empty.
        /// </exception>
        /// <param name="addresses">The nsqlookupd address(es) to add.</param>
        /// <seealso cref="ConnectToNsqdAsync"/>
        public async Task ConnectToNsqLookupdAsync(IEnumerable<string> addressList, CancellationToken token = default)
        {
            var addresses = addressList.ToArray();
            if (addresses.Length == 0)
                throw new ArgumentException("addresses.Length = 0", nameof(addresses));
            addresses = [.. addresses.Distinct()];
            if (_lookupdHTTPAddrs.Count > 0)
                throw new InvalidOperationException("cannot modify lookupd address at runtime");
            if (_stopFlag == 1)
                throw new Exception("consumer stopped");
            if (_runningHandlers == 0)
                throw new Exception("no handlers");

            foreach (var address in addresses)
            {
                if(!ValidatedLookupdAddress(address, out string? error))
                {
                    throw new ArgumentException($"invalid lookupd address '{address}': {error}", nameof(addresses));
                }
            }

            try
            {
                _mtx.EnterWriteLock();
                _lookupdHTTPAddrs.AddRange(addresses);
            }
            finally
            {
                _mtx.ExitWriteLock();
            }
            await queryLookupdAsync(token);
            _connectedFlag = 1;
            _ = RunLookupdLoop(this.ConsumerStopContext.Token);
        }

        private static bool ValidatedLookupdAddress(string address, out string? error)
        {
            error = null;
            if (string.IsNullOrEmpty(address))
            {
                error = "empty address";
                return false;
            }
            if (!address.Contains(':'))
            {
                error = "address missing port";
                return false;
            }
            if (address.Contains('/'))
            {
                // TODO: verify this is the kind of validation we want
                try
                {
                    _ = new Uri(address, UriKind.Absolute);
                }
                catch(Exception ex)
                {
                    error = $"invalid address format: {ex.Message}";
                    return false;
                }
            }
            return true;
        }

        /// <summary>
        /// poll all known lookup servers every LookupdPollInterval
        /// </summary>
        private async Task RunLookupdLoop(CancellationToken token)
        {
            // add some jitter so that multiple consumers discovering the same topic,
            // when restarted at the same time, dont all connect at once.
            var jitter = new TimeSpan((long)(Random.Shared.NextDouble() * _config.LookupdPollJitter * _config.LookupdPollInterval.Ticks));
            await Task.Delay(jitter, token);

            var ticker = new Ticker(_config.LookupdPollInterval, token);
            var select = Select
                        .CaseReceive(ticker.C, (o, token) => queryLookupdAsync(token))
                        .CaseReceive(_lookupdRecheckChan, (o, token) => queryLookupdAsync(token));
            while (!token.IsCancellationRequested)
            {
                await select.ExecuteAsync(token);
            }
            log(LogLevel.Info, "exiting lookupdLoop");
        }

        /// <summary>
        /// return the next lookupd endpoint to query
        /// keeping track of which one was last used
        /// </summary>
        private string nextLookupdEndpoint()
        {
            string addr;
            int num;

            _mtx.EnterReadLock();
            try
            {
                if (_lookupdQueryIndex >= _lookupdHTTPAddrs.Count)
                {
                    _lookupdQueryIndex = 0;
                }
                addr = _lookupdHTTPAddrs[_lookupdQueryIndex];
                num = _lookupdHTTPAddrs.Count;
            }
            finally
            {
                _mtx.ExitReadLock();
            }

            _lookupdQueryIndex = (_lookupdQueryIndex + 1) % num;

            return addr;
        }

        private async Task queryLookupdAsync(CancellationToken token)
        {
            string endpoint = nextLookupdEndpoint();

            log(LogLevel.Debug, string.Format("querying nsqlookupd {0}", endpoint));

            int timeoutMilliseconds = (int)_config.DialTimeout.TotalMilliseconds;
            if (timeoutMilliseconds < 2000)
                timeoutMilliseconds = 2000;

            TopicProducerInformation[] producers;
            try
            {
                var nsqLookupdClient = new NsqLookupdHttpClient(endpoint, TimeSpan.FromMilliseconds(timeoutMilliseconds));
                producers = nsqLookupdClient.Lookup(_topic).Producers;
            }
            catch (Exception ex)
            {
                var webException = ex as WebException;
                if (webException != null)
                {
                    var httpWebResponse = webException.Response as HttpWebResponse;
                    if (httpWebResponse != null && httpWebResponse.StatusCode == HttpStatusCode.NotFound)
                    {
                        log(LogLevel.Warning, string.Format("404 querying nsqlookupd ({0}) for topic {1}", endpoint, _topic));
                        if (endpoint.Contains(":4151"))
                        {
                            log(LogLevel.Error, string.Format("404 querying nsqlookupd ({0}) - *** {1} ***  - {2}",
                                endpoint, "This endpoint looks like an nsqd address. Try connecting to port 4161.", ex));
                        }

                        return;
                    }
                }

                if (endpoint.Contains(":4150") || endpoint.Contains(":4151"))
                {
                    log(LogLevel.Error, string.Format("error querying nsqlookupd ({0}) - *** {1} ***  - {2}",
                        endpoint, "This endpoint looks like an nsqd address. Try connecting to port 4161.", ex));
                }
                else if (endpoint.Contains(":4160"))
                {
                    log(LogLevel.Error, string.Format("error querying nsqlookupd ({0}) - *** {1} *** - {2}",
                        endpoint, "This endpoint looks like an nsqlookupd TCP port. Try connecting to HTTP port 4161.", ex));
                }

                log(LogLevel.Error, string.Format("error querying nsqlookupd ({0}) - {1}", endpoint, ex));
                return;
            }

            // {
            //     "channels": [],
            //     "producers": [
            //         {
            //             "broadcast_address": "jehiah-air.local",
            //             "http_port": 4151,
            //             "tcp_port": 4150
            //         }
            //     ],
            //     "timestamp": 1340152173
            // }
            var nsqAddrs = new Collection<string>();
            foreach (var producer in producers)
            {
                var broadcastAddress = producer.BroadcastAddress;
                var port = producer.TcpPort;
                var joined = string.Format("{0}:{1}", broadcastAddress, port);
                nsqAddrs.Add(joined);
            }

            var behaviorDelegate = _behaviorDelegate;
            if (behaviorDelegate != null)
            {
                nsqAddrs = new Collection<string>([.. behaviorDelegate.Filter(nsqAddrs)]);
            }

            if (_stopFlag == 1)
                return;

            foreach (var addr in nsqAddrs)
            {
                try
                {
                    await ConnectToNsqdAsync([addr], token);
                }
                catch (Exception ex)
                {
                    log(LogLevel.Error, string.Format("({0}) error connecting to nsqd - {1}", addr, ex));
                }
            }
        }

        /// <summary>
        ///     <para>Adds nsqd addresses to directly connect to for this <see cref="Consumer" /> instance.</para>
        ///     
        ///     <para>It is recommended to use <see cref="ConnectToNsqLookupdAsync"/> so that topics are discovered automatically.
        ///     This method is useful when you want to connect to a single, local instance.</para>
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="addresses"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">Thrown when <paramref name="addresses"/> is empty.</exception>
        /// <param name="addresses">The nsqd address(es) to add.</param>
        /// <seealso cref="DisconnectFromNsqd"/>
        /// <seealso cref="ConnectToNsqLookupdAsync"/>
        public async Task ConnectToNsqdAsync(IEnumerable<string> addressList, CancellationToken token = default)
        {
            var addresses = addressList.ToArray();
            if (addresses.Length == 0)
                throw new ArgumentException("addresses.Length = 0", nameof(addresses));

            foreach (string address in addresses)
            {
                await connectToNsqd(address, token);
            }
        }

        private async Task connectToNsqd(string addr, CancellationToken token)
        {
            if (string.IsNullOrEmpty(addr))
                throw new ArgumentNullException(nameof(addr));

            if (_stopFlag == 1)
            {
                throw new Exception("consumer stopped");
            }

            if (_runningHandlers == 0)
            {
                throw new Exception("no handlers");
            }

            _connectedFlag = 1;

            var cts = CancellationTokenSource.CreateLinkedTokenSource(token);
            _mtx.EnterWriteLock();
            try
            {
                bool pendingOk = _pendingConnections.ContainsKey(addr);
                bool ok = _connections.ContainsKey(addr);
                if (pendingOk || ok)
                {
                    cts.Dispose();
                    return;
                }
                _pendingConnections[addr] = cts;
                if (!_nsqdTCPAddrs.Contains(addr))
                    _nsqdTCPAddrs.Add(addr);
            }
            finally
            {
                _mtx.ExitWriteLock();
            }

            log(LogLevel.Info, string.Format("({0}) connecting to nsqd", addr));
            var nsqConnectionBuilder = new NsqConnectionBuilder(addr, _config, this);
            // TODO: Check log format
            nsqConnectionBuilder.SetLogger(_logger, string.Format("C{0} [{1}/{2}] ({{0}})", _id, _topic, _channel));
            var cleanupConnection = new Action(() =>
            {
                _mtx.EnterWriteLock();
                try
                {
                    _pendingConnections.TryRemove(addr, out _);
                }
                finally
                {
                    _mtx.ExitWriteLock();
                    cts.Cancel();
                }
            });

            IdentifyResponse? resp;
            NsqContext nsqConnectionContext;
            try
            {
                await nsqConnectionBuilder.DialAsync(cts.Token);
                resp = await nsqConnectionBuilder.HandShakeAsync(async(cmdWriter, token) =>
                {
                    await cmdWriter.WriteCommandAsync(Command.Subscribe(_topic, _channel), token);
                }, token);
                nsqConnectionContext = nsqConnectionBuilder.GetNsqContext();
            }
            catch (Exception)
            {
                cleanupConnection();
                throw;
            }

            if (resp != null)
            {
                if (resp.MaxRdyCount < getMaxInFlight())
                {
                    log(LogLevel.Warning, string.Format(
                        "({0}) max RDY count {1} < consumer max in flight {2}, truncation possible",
                        nsqConnectionContext, resp.MaxRdyCount, getMaxInFlight()));
                }
            }


            _mtx.EnterWriteLock();
            try
            {
                _pendingConnections.TryRemove(addr, out _);
                _connections[addr] = nsqConnectionContext;
            }
            finally
            {
                _mtx.ExitWriteLock();
            }

            log(LogLevel.Info, string.Format("({0}) connected to nsqd", addr));

            // pre-emptive signal to existing connections to lower their RDY count
            _perConnMaxInFlightOverride = 0;
            foreach (var c in GetCurrentConnections())
            {
                maybeUpdateRDY(c);
            }
        }

        /// <summary>
        ///     Closes the connection to and removes the specified <paramref name="nsqdAddress" /> from the list.
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="nsqdAddress"/> is <c>null</c>.</exception>
        /// <exception cref="ErrNotConnected">Thrown when the specified <paramref name="nsqdAddress"/> is not in the list of
        ///     active connections.
        /// </exception>
        /// <param name="nsqdAddress">The nsqd address to disconnect from.</param>
        /// <seealso cref="ConnectToNsqdAsync"/>
        public void DisconnectFromNsqd(string nsqdAddress)
        {
            if (string.IsNullOrEmpty(nsqdAddress))
                throw new ArgumentNullException(nameof(nsqdAddress));

            _mtx.EnterWriteLock();
            try
            {
                int idx = _nsqdTCPAddrs.IndexOf(nsqdAddress);
                if (idx == -1)
                    throw new ErrNotConnected();

                _nsqdTCPAddrs.RemoveAt(idx);

                // TODO: PR go-nsq remove from connections/pendingConnections
                if (_connections.TryRemove(nsqdAddress, out var conn))
                {
                    conn.Close();
                }
                else if (_pendingConnections.TryRemove(nsqdAddress, out var pendingConn))
                {
                    pendingConn.Cancel();
                }
            }
            finally
            {
                _mtx.ExitWriteLock();
            }
        }

        void IConnDelegate.OnMessage(NsqContext c, Message msg)
        {
            Interlocked.Decrement(ref _totalRdyCount);
            Interlocked.Increment(ref _messagesReceived);
            _incomingMessages.Writer.TryWrite(msg);
            maybeUpdateRDY(c);
        }

        void IConnDelegate.OnMessageFinished(NsqContext c, Message msg)
        {
            Interlocked.Increment(ref _messagesFinished);
        }

        void IConnDelegate.OnMessageRequeued(NsqContext c, Message msg)
        {
            Interlocked.Increment(ref _messagesRequeued);
        }

        void IConnDelegate.OnBackoff(NsqContext c)
        {
            startStopContinueBackoff(BackoffSignal.BackoffFlag);
        }

        void IConnDelegate.OnContinue(NsqContext c)
        {
            startStopContinueBackoff(BackoffSignal.ContinueFlag);
        }

        void IConnDelegate.OnResume(NsqContext c)
        {
            startStopContinueBackoff(BackoffSignal.ResumeFlag);
        }

        void IConnDelegate.OnResponse(NsqContext c, ReadOnlyMemory<byte> data)
        {
            if (CLOSE_WAIT_BYTES.AsSpan().SequenceEqual(data.Span))
            {
                // server is ready for us to close (it ack'd our StartClose)
                // we can assume we will not receive any more messages over this channel
                // (but we can still write back responses)
                log(LogLevel.Info, string.Format("({0}) received CLOSE_WAIT from nsqd", c));
                c.Close();
            }
        }

        void IConnDelegate.OnError(NsqContext c, ReadOnlyMemory<byte> data) { }

        void IConnDelegate.OnHeartbeat(NsqContext c) { }

        void IConnDelegate.OnIOError(NsqContext c, Exception err)
        {
            c.Close();
        }

        void IConnDelegate.OnClose(NsqContext c)
        {
            bool hasRDYRetryTimer = false;

            string connAddr = c.ToString();

            // remove this connections RDY count from the consumer's total
            long rdyCount = c.RDY;
            Interlocked.Add(ref _totalRdyCount, rdyCount * -1);

            _rdyRetryMtx.EnterWriteLock();
            try
            {
                if (_rdyRetryTimers.TryRemove(connAddr, out var cts))
                {
                    // stop any pending retry of an old RDY update
                    cts.Cancel();
                    hasRDYRetryTimer = true;
                }
            }
            finally
            {
                _rdyRetryMtx.ExitWriteLock();
            }

            int left;

            _mtx.EnterWriteLock();
            try
            {
                _connections.TryRemove(connAddr, out _);
                left = _connections.Count;
            }
            finally
            {
                _mtx.ExitWriteLock();
            }

            var connsAlivelogLevel = (_stopFlag == 1 ? LogLevel.Info : LogLevel.Warning);
            log(connsAlivelogLevel, string.Format("there are {0} connections left alive", left));

            if ((hasRDYRetryTimer || rdyCount > 0) &&
                (left == getMaxInFlight() || inBackoff()))
            {
                // we're toggling out of (normal) redistribution cases and this conn
                // had a RDY count...
                //
                // trigger RDY redistribution to make sure this RDY is moved
                // to a new connection
                _needRdyRedistributed = 1;
            }

            if (_stopFlag == 1)
            {
                if (left == 0)
                {
                    StopAcceptIncomingMessage();
                }
                return;
            }

            int numLookupd;
            bool reconnect;

            _mtx.EnterReadLock();
            try
            {
                numLookupd = _lookupdHTTPAddrs.Count;
                reconnect = _nsqdTCPAddrs.Contains(connAddr);
            }
            finally
            {
                _mtx.ExitReadLock();
            }

            if (numLookupd > 0)
            {
                // trigger a poll of the lookupd
                _lookupdRecheckChan.Writer.TryWrite(1);
            }
            else if (reconnect)
            {
                // there are no lookupd and we still have this nsqd TCP address in our list...
                // try to reconnect after a bit
                Task.Run(async() =>
                {
                    var token = ConsumerStopContext.Token;
                    while (!token.IsCancellationRequested)
                    {
                        // TODO: PR go-nsq: do they need .Seconds() on their r.log string?
                        // https://github.com/nsqio/go-nsq/blob/667c739c212e55a5ddde2a33d4be2b9376d2c7e5/consumer.go#L731
                        log(LogLevel.Info, string.Format("({0}) re-connecting in {1:0.0000} seconds...", connAddr,
                            _config.LookupdPollInterval.TotalSeconds));
                        await Task.Delay(_config.LookupdPollInterval, token);
                        if (_stopFlag == 1)
                        {
                            break;
                        }
                        _mtx.EnterReadLock();
                        reconnect = _nsqdTCPAddrs.Contains(connAddr);
                        _mtx.ExitReadLock();
                        if (!reconnect)
                        {
                            log(LogLevel.Warning, string.Format("({0}) skipped reconnect after removal...", connAddr));
                            return;
                        }
                        try
                        {
                            await ConnectToNsqdAsync([connAddr], token);
                        }
                        catch (Exception ex)
                        {
                            log(LogLevel.Error, string.Format("({0}) error connecting to nsqd - {1}", connAddr, ex));
                            continue;
                            // TODO: PR go-nsq if we get DialTimeout this loop stops. check other exceptions.
                        }
                        break;
                    }
                });
            }
        }

        private void startStopContinueBackoff(BackoffSignal signal)
        {
            // prevent many async failures/successes from immediately resulting in
            // max backoff/normal rate (by ensuring that we dont continually incr/decr
            // the counter during a backoff period)
            lock (_backoffMtx)
            {
                if (inBackoffTimeout())
                {
                    return;
                }

                // update backoff state
                var backoffUpdated = false;
                var backoffCounter = _backoffCounter;
                switch (signal)
                {
                    case BackoffSignal.ResumeFlag:
                        if (backoffCounter > 0)
                        {
                            backoffCounter--;
                            backoffUpdated = true;
                        }
                        break;
                    case BackoffSignal.BackoffFlag:
                        bool increaseBackoffLevel = (backoffCounter == 0);
                        if (!increaseBackoffLevel)
                        {
                            increaseBackoffLevel = _config.BackoffStrategy.Calculate(_config, backoffCounter)
                                .IncreaseBackoffLevel;
                        }
                        if (increaseBackoffLevel)
                        {
                            backoffCounter++;
                            backoffUpdated = true;
                        }
                        break;
                }
                _backoffCounter = backoffCounter;

                if (backoffCounter == 0 && backoffUpdated)
                {
                    // exit backoff
                    var count = perConnMaxInFlight();
                    log(LogLevel.Warning, string.Format("exiting backoff, return all to RDY {0}", count));
                    foreach (var c in GetCurrentConnections())
                    {
                        updateRDY(c, count);
                    }
                }
                else if (backoffCounter > 0)
                {
                    // start or continue backoff
                    var backoffDuration = _config.BackoffStrategy.Calculate(_config, backoffCounter).Duration;

                    if (backoffDuration > _config.MaxBackoffDuration)
                    {
                        backoffDuration = _config.MaxBackoffDuration;
                    }

                    log(LogLevel.Warning,
                        string.Format("backing off for {0:0.000} seconds (backoff level {1}), setting all to RDY 0",
                            backoffDuration.TotalSeconds, backoffCounter
                        ));

                    // send RDY 0 immediately (to *all* connections)
                    foreach (var c in GetCurrentConnections())
                    {
                        updateRDY(c, 0);
                    }

                    backoff(backoffDuration);
                }
            }
        }

        private void backoff(TimeSpan d)
        {
            _backoffDuration = d.Nanoseconds();
            Task.Run(async () => { await Task.Delay(d); resume(); });
        }

        private void resume()
        {
            if (_stopFlag == 1)
            {
                _backoffDuration = 0;
                return;
            }

            // pick a random connection to test the waters
            var connections = GetCurrentConnections();
            if (connections.Count == 0)
            {
                log(LogLevel.Warning, "no connection available to resume");
                log(LogLevel.Warning, string.Format("backing off for {0:0.0000} seconds", 1));
                backoff(TimeSpan.FromSeconds(1));
                return;
            }
            var idx = Random.Shared.Next(connections.Count);
            var choice = connections[idx];

            log(LogLevel.Warning,
                string.Format("({0}) backoff timeout expired, sending RDY 1",
                choice));

            // while in backoff only ever let 1 message at a time through
            var err = updateRDY(choice, 1);
            if (err != null)
            {
                log(LogLevel.Warning, string.Format("({0}) error resuming RDY - {1}", choice, err.Message));
                log(LogLevel.Warning, string.Format("backing off for {0:0.0000} seconds", 1));
                backoff(TimeSpan.FromSeconds(1));
                return;
            }

            _backoffDuration = 0;
        }

        private bool inBackoff()
        {
            return _backoffCounter > 0;
        }

        private bool inBackoffTimeout()
        {
            return _backoffDuration > 0;
        }

        private void maybeUpdateRDY(NsqContext conn)
        {
            var isInBackoff = inBackoff();
            var isInBackoffTimeout = inBackoffTimeout();
            if (isInBackoff || isInBackoffTimeout)
            {
                log(LogLevel.Debug, string.Format("({0}) skip sending RDY inBackoff:{1} || inBackoffTimeout:{2}",
                    conn, isInBackoff, isInBackoffTimeout));
                return;
            }

            long remain = conn.RDY;
            long lastRdyCount = conn.LastRDY;
            long count = perConnMaxInFlight();

            // refill when at 1, or at 25%, or if connections have changed and we're imbalanced
            if (remain <= 1 || remain < (lastRdyCount / 4) || (count > 0 && count < remain))
            {
                log(LogLevel.Debug, string.Format("({0}) sending RDY {1} ({2} remain from last RDY {3})",
                    conn, count, remain, lastRdyCount));
                updateRDY(conn, count);
            }
            else
            {
                log(LogLevel.Debug, string.Format("({0}) skip sending RDY {1} ({2} remain out of last RDY {3})",
                    conn, count, remain, lastRdyCount));
            }
        }

        private async Task RunRdyLoop(CancellationToken token)
        {
            while (!token.IsCancellationRequested)
            {
                await Task.Delay(_config.RDYRedistributeInterval, token);
                redistributeRDY();
            }
        }

        private Exception? updateRDY(NsqContext c, long count)
        {
            try
            {
                if (c.IsClosing)
                {
                    return new ErrClosing();
                }

                // never exceed the nsqd's configured max RDY count
                if (count > c.MaxRDY)
                    count = c.MaxRDY;

                string connAddr = c.ToString();

                // stop any pending retry of an old RDY update
                _rdyRetryMtx.EnterWriteLock();
                try
                {
                    if (_rdyRetryTimers.TryRemove(connAddr, out var cts))
                        cts.Cancel();
                }
                finally
                {
                    _rdyRetryMtx.ExitWriteLock();
                }

                // never exceed our global max in flight. truncate if possible.
                // this could help a new connection get partial max-in-flight
                long rdyCount = c.RDY;
                long maxPossibleRdy = getMaxInFlight() - _totalRdyCount + rdyCount;
                if (maxPossibleRdy > 0 && maxPossibleRdy < count)
                {
                    count = maxPossibleRdy;
                }
                else if (maxPossibleRdy <= 0 && count > 0)
                {
                    // TODO: PR go-nsq: add "else" for clarity
                    if (rdyCount == 0)
                    {
                        // we wanted to exit a zero RDY count but we couldn't send it...
                        // in order to prevent eternal starvation we reschedule this attempt
                        // (if any other RDY update succeeds this timer will be stopped)
                        _rdyRetryMtx.EnterWriteLock();
                        try
                        {
                            var cts = new CancellationTokenSource();
                            Task.Run(async () =>
                            {
                                await Task.Delay(TimeSpan.FromSeconds(5), cts.Token);
                                updateRDY(c, count);
                            });
                            _rdyRetryTimers[connAddr] = cts;
                        }
                        finally
                        {
                            _rdyRetryMtx.ExitWriteLock();
                        }
                    }
                    throw new ErrOverMaxInFlight();
                }

                sendRDY(c, count);
            }
            catch (Exception ex)
            {
                // NOTE: errors intentionally not rethrown
                log(ex is ErrClosing ? LogLevel.Warning : LogLevel.Error,
                    string.Format("({0}) error in updateRDY {1} - {2}", c, count, ex));
                return ex;
            }

            return null;
        }

        private void sendRDY(NsqContext c, long count)
        {
            if (count == 0 && c.LastRDY == 0)
            {
                // no need to send. It's already that RDY count
                return;
            }

            Interlocked.Add(ref _totalRdyCount, -c.RDY + count);
            c.SetRDY(count);
        }

        private void redistributeRDY()
        {
            if (inBackoffTimeout())
            {
                _perConnMaxInFlightOverride = 0;
                return;
            }

            // if an external heuristic set needRDYRedistributed we want to wait
            // until we can actually redistribute to proceed
            var connections = GetCurrentConnections();
            if (connections.Count == 0)
            {
                _perConnMaxInFlightOverride = 0;
                return;
            }

            int maxInFlight = getMaxInFlight();
            if (connections.Count > maxInFlight)
            {
                log(LogLevel.Debug, string.Format("redistributing RDY state ({0} conns > {1} max_in_flight)",
                    connections.Count, maxInFlight));
                _needRdyRedistributed = 1;
            }
            else if (connections.Count > 1)
            {
                if (inBackoff())
                {
                    log(LogLevel.Debug, string.Format("redistributing RDY state (in backoff and {0} conns > 1)",
                        connections.Count));
                    _needRdyRedistributed = 1;
                }
                else if (_config.RDYRedistributeOnIdle && maxInFlight > 0)
                {
                    redistributeRDYForIdleConnections(connections, maxInFlight);
                    return;
                }
            }

            _perConnMaxInFlightOverride = 0;

            if (Interlocked.CompareExchange(ref _needRdyRedistributed, value: 0, comparand: 1) != 1)
            {
                return;
            }

            var possibleConns = new List<NsqContext>();
            foreach (var c in connections)
            {
                var lastMsgDuration = DateTime.Now.Subtract(c.LastMessageTime);
                long rdyCount = c.RDY;
                log(LogLevel.Debug, string.Format("({0}) rdy: {1} (last message received {2})",
                    c, rdyCount, lastMsgDuration));
                if (rdyCount > 0 && lastMsgDuration > _config.LowRdyIdleTimeout)
                {
                    log(LogLevel.Debug, string.Format("({0}) idle connection, giving up RDY", c));
                    updateRDY(c, 0);
                }
                possibleConns.Add(c);
            }

            long availableMaxInFlight = maxInFlight - _totalRdyCount;
            if (inBackoff())
            {
                availableMaxInFlight = 1 - _totalRdyCount;
            }

            while (possibleConns.Count > 0 && availableMaxInFlight > 0)
            {
                availableMaxInFlight--;
                int i = Random.Shared.Next(possibleConns.Count);
                var c = possibleConns[i];
                // delete
                possibleConns.Remove(c);
                log(LogLevel.Debug, string.Format("({0}) redistributing RDY", c));
                updateRDY(c, 1);
            }
        }

        private void redistributeRDYForIdleConnections(List<NsqContext> connections, int maxInFlight)
        {
            var activeConns = new List<NsqContext>();
            var idleConns = new List<NsqContext>();

            // get idle and active connections
            // idle = RDY > 0 and last message received > LowRdyIdleTimeout
            // active = all other connections
            // if an idle connection exists or an active connection with RDY=0, we're going to try to redistribute
            foreach (var c in connections)
            {
                var lastMsgDuration = DateTime.Now.Subtract(c.LastMessageTime);
                long rdyCount = c.RDY;
                if (rdyCount > 0 && lastMsgDuration > _config.LowRdyIdleTimeout)
                {
                    idleConns.Add(c);
                    _needRdyRedistributed = 1;
                }
                else
                {
                    activeConns.Add(c);
                    if (rdyCount == 0)
                    {
                        _needRdyRedistributed = 1;
                    }
                }
            }

            if (Interlocked.CompareExchange(ref _needRdyRedistributed, value: 0, comparand: 1) != 1)
            {
                return;
            }

            // if we're in backoff let redistributeRDY handle this scenario
            if (inBackoff())
            {
                return;
            }

            // everything's idle with a RDY count > 0, let it be
            if (activeConns.Count == 0)
            {
                return;
            }

            // set the RDY count to 0 for idle connections
            foreach (var c in idleConns)
            {
                var lastMsgDuration = DateTime.Now.Subtract(c.LastMessageTime);
                long rdyCount = c.RDY;
                log(LogLevel.Debug, string.Format("({0}) rdy: {1} (last message received {2})",
                    c, rdyCount, lastMsgDuration));
                log(LogLevel.Debug, string.Format("({0}) idle connection, giving up RDY", c));
                updateRDY(c, 0);
            }

            long perConnMaxInFlight = maxInFlight / activeConns.Count;
            _perConnMaxInFlightOverride = perConnMaxInFlight;

            // update all active connections to the new perConnMaxInFlight
            foreach (var c in activeConns.OrderByDescending(p => p.RDY))
            {
                log(LogLevel.Debug, string.Format("({0}) redistributing RDY to {1}", c, perConnMaxInFlight));
                updateRDY(c, perConnMaxInFlight);
            }
        }

        /// <summary>
        ///     Synchronously initiates a graceful stop of the <see cref="Consumer" /> (permanent) and waits for the stop to
        ///     complete.
        /// </summary>
        public void Stop()
        {
            StopAsync().ConfigureAwait(false).GetAwaiter().GetResult();
        }

        /// <summary>Asynchronously initiates a graceful stop of the <see cref="Consumer" /> (permanent).</summary>
        /// <returns>A <see cref="Task"/> which can be awaited for the stop to complete.</returns>
        public async Task StopAsync()
        {
            log(LogLevel.Info, "stopping...");
            var connections = GetCurrentConnections();
            if (connections.Count == 0)
            {
                StopAcceptIncomingMessage();
                return;
            }
            var cts = new CancellationTokenSource(TimeSpan.FromSeconds(30));
            var cmd = Command.StartClose();
            var all = Task.WhenAll(
                connections.Select(c => c.WriteCommandTask(cmd, cts.Token))
            );
            await all;
            _stopFlag = 1;
        }

        private void StopAcceptIncomingMessage()
        {
            _stopHandler.Do(() =>
            {
                log(LogLevel.Info, "stopping handlers");
                _incomingMessages.Writer.Complete();
            });
        }

        /// <summary>
        ///     <para>Sets the <see cref="IHandler" /> instance to handle for messages received by this
        ///     <see cref="Consumer"/>.</para>
        ///     
        ///     <para>This method throws if called after connecting to nsqd or nsqlookupd.</para>
        /// </summary>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="handler"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="threads"/> is less than 1.
        /// </exception>
        /// <exception cref="Exception">
        ///     Thrown when <see cref="ConnectToNsqdAsync"/> or <see cref="ConnectToNsqLookupdAsync"/> has been called before invoking
        ///     <see cref="AddHandler"/>.
        /// </exception>
        /// <param name="handler">The handler for the topic/channel of this <see cref="Consumer"/> instance.</param>
        /// <param name="threads">The number of threads used to handle incoming messages for this
        ///     <see cref="Consumer" /> (default = 1).
        /// </param>
        public void AddHandler(IHandler handler, int threads = 1, CancellationToken token = default)
        {
            if (threads <= 0)
                throw new ArgumentOutOfRangeException(nameof(threads), threads, "threads must be > 0");

            addConcurrentHandlers(handler, threads, token);
        }

        /// <summary>
        /// AddConcurrentHandlers sets the Handler for messages received by this Consumer.  It
        /// takes a second argument which indicates the number of goroutines to spawn for
        /// message handling.
        ///
        /// This panics if called after connecting to nsqd or nsqlookupd
        ///
        /// (see Handler or HandlerFunc for details on implementing this interface)
        /// </summary>
        private void addConcurrentHandlers(IHandler handler, int concurrency, CancellationToken token)
        {
            if (concurrency <= 0)
                throw new ArgumentOutOfRangeException(nameof(concurrency), concurrency, "concurrency must be > 0");

            if (_connectedFlag == 1)
            {
                throw new Exception("already connected");
            }

            Interlocked.Add(ref _runningHandlers, concurrency);
            for (int i = 0; i < concurrency; i++)
            {
                _ = handlerLoop(handler, token);
            }
        }

        private async Task handlerLoop(IHandler handler, CancellationToken token)
        {
            log(LogLevel.Debug, "starting Handler");
            var cts = CancellationTokenSource.CreateLinkedTokenSource(ConsumerStopContext.Token, token);
            token = cts.Token;
            while (!token.IsCancellationRequested)
            {
                var ok = await _incomingMessages.Reader.WaitToReadAsync();
                if (!ok)
                {
                    break;
                }
                if (!_incomingMessages.Reader.TryRead(out var message))
                {
                    continue;
                }
                
                if (message == null)
                    continue;

                if (shouldFailMessage(message, handler))
                {
                    message.Finish();
                    continue;
                }

                try
                {
                    message.MaxAttempts = _config.MaxAttempts;
                    if(handler.RunAsAsync)
                        await handler.HandleMessageAsync(message, token);
                    else
                        handler.HandleMessage(message);
                }
                catch (Exception ex)
                {
                    log(LogLevel.Error, string.Format("Handler returned error for msg {0} - {1}", message.Id, ex));
                    if (!message.IsAutoResponseDisabled)
                        message.ReQueue();
                    continue;
                }

                if (!message.IsAutoResponseDisabled)
                    message.Finish();
            }

            //exit:
            log(LogLevel.Debug, "stopping Handler");
            if (Interlocked.Decrement(ref _runningHandlers) == 0)
            {
                Dispose();
            }
        }

        private bool shouldFailMessage(Message message, IHandler handler)
        {
            if (_config.MaxAttempts > 0 && message.Attempts > _config.MaxAttempts)
            {
                log(LogLevel.Warning, string.Format("msg {0} attempted {1} times, giving up",
                    message.Id, message.Attempts));

                try
                {
                    handler.LogFailedMessage(message);
                }
                catch (Exception ex)
                {
                    log(LogLevel.Error, string.Format("LogFailedMessage returned error for msg {0} - {1}",
                        message.Id, ex));
                }

                return true;
            }
            return false;
        }

        private void Dispose()
        {
            _exitHandler.Do(() =>
            {
                this.ConsumerStopContext.Cancel();
                _logger.Flush();
            });
        }

        private void log(LogLevel lvl, string msg)
        {
            // TODO: proper width formatting
            _logger.Output(lvl, string.Format("C{0} [{1}/{2}] {3}", _id, _topic, _channel, msg));
        }
    }
}
