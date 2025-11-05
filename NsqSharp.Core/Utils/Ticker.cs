using System;
using System.Threading;
using System.Threading.Channels;
using NsqSharp.Utils.Channels;

namespace NsqSharp.Utils
{
    /// <summary>
    /// A Ticker holds a channel that delivers `ticks' of a clock at intervals. http://golang.org/pkg/time/#Ticker
    /// </summary>
    public class Ticker
    {
        private readonly Channel<DateTime> _tickerChan;
        private readonly CancellationTokenSource TimerContext;

        /// <summary>
        /// Initializes a new instance of the Ticker class.
        /// </summary>
        /// <param name="duration">The interval between ticks on the channel.</param>
        public Ticker(TimeSpan duration, CancellationToken token = default)
        {
            if (duration <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(duration), "duration must be > 0");

            TimerContext = CancellationTokenSource.CreateLinkedTokenSource(token);

            var started = DateTime.Now;
            _tickerChan = Channel.CreateUnbounded<DateTime>();

            Task.Run(async () =>
            {
                while (!TimerContext.Token.IsCancellationRequested)
                {
                    await Task.Delay(duration, TimerContext.Token);
                    if (!TimerContext.Token.IsCancellationRequested)
                    {
                        _tickerChan.Writer.TryWrite(DateTime.UtcNow);
                    }
                }
            }, token).ContinueWith(_=> 
            { 
                _tickerChan.Writer.TryComplete();
            }, token);
        }

        /// <summary>
        /// Stop turns off a ticker. After Stop, no more ticks will be sent. Stop does not close the channel,
        /// to prevent a read from the channel succeeding incorrectly. See <see cref="Close"/>.
        /// </summary>
        public void Stop()
        {
            TimerContext.Cancel();
            _tickerChan.Writer.TryComplete();
        }

        public ChannelReader<DateTime> C
        {
            get { return _tickerChan.Reader; }
        }

    }
}
