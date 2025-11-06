using System;
using System.Buffers;
using System.Text;
using System.Text.RegularExpressions;
using NsqSharp.Utils;

namespace NsqSharp.Core
{
    // https://github.com/nsqio/go-nsq/blob/master/protocol.go

    /// <summary>
    /// Protocol
    /// </summary>
    public static partial class Protocol
    {
        /// <summary>
        /// MagicV1 is the initial identifier sent when connecting for V1 clients
        /// </summary>
        public static readonly byte[] MagicV1 = Encoding.UTF8.GetBytes("  V1");

        /// <summary>
        /// MagicV2 is the initial identifier sent when connecting for V2 clients
        /// </summary>
        public static readonly byte[] MagicV2 = Encoding.UTF8.GetBytes("  V2");
    }

    /// <summary>
    /// Frame types
    /// </summary>
    public enum FrameType
    {
        /// <summary>Response</summary>
        Response = 0,
        /// <summary>Error</summary>
        Error = 1,
        /// <summary>Message</summary>
        Message = 2
    }

    public static partial class Protocol
    {
        private static readonly Regex _validTopicChannelNameRegex =
            new Regex(@"^[\.a-zA-Z0-9_-]+(#ephemeral)?$", RegexOptions.Compiled);

        /// <summary>
        /// IsValidTopicName checks a topic name for correctness
        /// </summary>
        /// <param name="name">The topic name to check</param>
        /// <returns><c>true</c> if the topic name is valid; otherwise, <c>false</c></returns>
        public static bool IsValidTopicName(string name)
        {
            return isValidName(name);
        }

        /// <summary>
        /// IsValidChannelName checks a channel name for correctness
        /// </summary>
        /// <param name="name">The channel name to check</param>
        /// <returns><c>true</c> if the channel name is valid; otherwise, <c>false</c></returns>
        public static bool IsValidChannelName(string name)
        {
            return isValidName(name);
        }

        internal static bool isValidName(string name)
        {
            if (string.IsNullOrEmpty(name) || name.Length > 64)
            {
                return false;
            }

            return _validTopicChannelNameRegex.IsMatch(name);
        }
    }

    public class NsqBufferContext : IDisposable
    {
        public ReadOnlyMemory<byte> Body => BodyBuffer.AsMemory(Offset, BodyLength);

        private int Offset = 0;
        private int BodyLength = 0;
        private byte[] BodyBuffer = [];
        public FrameType FrameType { get; private set; }
        private bool HasRent = false;


        private async ValueTask ReadResponseAsync(IReader r, CancellationToken token)
        {
            // message size
            int msgSize = BodyLength = await Binary.ReadInt32Async(r, Binary.BigEndian);
            byte[] data = BodyBuffer = ArrayPool<byte>.Shared.Rent(msgSize);
            await r.ReadExactlyAsync(data.AsMemory(0, msgSize), token);
        }

        private void UnpackResponse()
        {
            if (BodyLength < 4)
                throw new ArgumentException("length of response is too small", nameof(BodyLength));

            FrameType = (FrameType)Binary.BigEndian.Int32(Body);
            Offset = 4;
            BodyLength -= 4;
        }

        public async ValueTask ReadUnpackedResponseAsync(IReader r, CancellationToken token = default)
        {
            await ReadResponseAsync(r, token);
            UnpackResponse();
            HasRent = true;
        }

        public void Dispose()
        {
            if (HasRent)
            {
                ArrayPool<byte>.Shared.Return(BodyBuffer);
                HasRent = false;
            }
        }
    }
}
