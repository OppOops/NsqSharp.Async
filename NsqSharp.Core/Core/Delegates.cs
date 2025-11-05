using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("NsqSharp.Core.Tests")]
namespace NsqSharp.Core
{
    // https://github.com/nsqio/go-nsq/blob/master/delegates.go

    /// <summary>
    /// MessageDelegate is an interface of methods that are used as
    /// callbacks in Message
    /// </summary>
    internal interface IMessageDelegate
    {
        /// <summary>
        /// OnFinish is called when the Finish() method
        /// is triggered on the Message
        /// </summary>
        void OnFinish(Message m);

        /// <summary>
        /// OnRequeue is called when the Requeue() method
        /// is triggered on the Message
        /// </summary>
        TimeSpan OnRequeue(Message m, TimeSpan? delay, bool backoff);

        /// <summary>
        /// OnTouch is called when the Touch() method
        /// is triggered on the Message
        /// </summary>
        void OnTouch(Message m);
    }

    internal interface IAsyncMessageDelegate
    {
    }
    
    internal class ConnMessageDelegate : IMessageDelegate
    {
        public NsqContext Context { get; }

        public void OnFinish(Message m) { Context.OnMessageFinish(m); }
        public TimeSpan OnRequeue(Message m, TimeSpan? delay, bool backoff)
        {
            return Context.OnMessageRequeue(m, delay, backoff);
        }
        public void OnTouch(Message m) { Context.OnMessageTouch(m); }

        public ConnMessageDelegate(NsqContext context)
        {
            Context = context;
        }
    }

    internal class ConnAsyncMessageDelegate : IAsyncMessageDelegate
    {

    }


    /// <summary>
    /// ConnDelegate is an interface of methods that are used as
    /// callbacks in Conn
    /// </summary>
    internal interface IConnDelegate
    {
        /// <summary>
        /// OnResponse is called when the connection
        /// receives a FrameTypeResponse from nsqd
        /// </summary>
        void OnResponse(NsqContext c, byte[] data);

        /// <summary>
        /// OnError is called when the connection
        /// receives a FrameTypeError from nsqd
        /// </summary>
        void OnError(NsqContext c, byte[] data);

        /// <summary>
        /// OnMessage is called when the connection
        /// receives a FrameTypeMessage from nsqd
        /// </summary>
        void OnMessage(NsqContext c, Message m);

        /// <summary>
        /// OnMessageFinished is called when the connection
        /// handles a FIN command from a message handler
        /// </summary>
        void OnMessageFinished(NsqContext c, Message m);

        /// <summary>
        /// OnMessageRequeued is called when the connection
        /// handles a REQ command from a message handler
        /// </summary>
        void OnMessageRequeued(NsqContext c, Message m);

        /// <summary>
        /// OnBackoff is called when the connection triggers a backoff state
        /// </summary>
        void OnBackoff(NsqContext c);

        /// <summary>
        /// OnContinue is called when the connection finishes a message without adjusting backoff state
        /// </summary>
        void OnContinue(NsqContext c);

        /// <summary>
        /// OnResume is called when the connection triggers a resume state
        /// </summary>
        void OnResume(NsqContext c);

        /// <summary>
        /// OnIOError is called when the connection experiences
        /// a low-level TCP transport error
        /// </summary>
        void OnIOError(NsqContext c, Exception err);

        /// <summary>
        /// OnHeartbeat is called when the connection
        /// receives a heartbeat from nsqd
        /// </summary>
        void OnHeartbeat(NsqContext c);

        /// <summary>
        /// OnClose is called when the connection
        /// closes, after all cleanup
        /// </summary>
        void OnClose(NsqContext c);
    }
}
