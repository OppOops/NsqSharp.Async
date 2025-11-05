using NsqSharp.Utils;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace NsqSharp
{
	public sealed partial class Consumer
	{
		public void AddAsyncHandler(IAsyncHandler handler, int threads = 1)
		{
            if (threads <= 0)
				throw new ArgumentOutOfRangeException(nameof(threads), threads, "threads must be > 0");

			AddAsyncConcurrentHandlers(handler, threads);
		}

		private void AddAsyncConcurrentHandlers(IAsyncHandler handler, int concurrency)
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
				_ = HandlerAsyncLoop(handler);
			}
		}

		private async Task HandlerAsyncLoop(IAsyncHandler handler)
		{

		}
	}
}
