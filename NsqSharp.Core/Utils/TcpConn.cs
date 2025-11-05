using NsqSharp.Utils.Extensions;
using System;
using System.IO;
using System.Net.Security;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Tasks;

[assembly: InternalsVisibleTo("NsqSharp.Core.Tests")]
namespace NsqSharp.Utils
{
    internal class TcpConn : ITcpConn
    {
        // TODO: Might be better to use Sockets than TcpClient http://angrez.blogspot.com/2007/02/flush-socket-in-net-or-c.html
        // https://msdn.microsoft.com/en-us/library/system.net.sockets.socket.setsocketoption.aspx

        private readonly TcpClient _tcpClient;
        private readonly object _readLocker = new();
        private readonly object _writeLocker = new();
        private readonly object _closeLocker = new();
        private readonly string _hostname;
        private Stream _networkStream;
        private bool _isClosed;

        TcpConn(TcpClient connection, string hostname)
        {
            _tcpClient = connection;
            _networkStream = _tcpClient.GetStream();
            _hostname = hostname;
        }

        public static async Task<TcpConn> ConnectAsync(string hostName, int port, CancellationToken token = default)
        {
            var c = new TcpClient();
            await c.ConnectAsync(hostName, port, token);
            return new TcpConn(c, hostName);
        }

        public void UpgradeTLS(TlsConfig configTLS)
        {
            lock (_readLocker)
            {
                lock (_writeLocker)
                {
                    const bool leaveInnerStreamOpen = false;

                    var enabledSslProtocols = configTLS.GetEnabledSslProtocols();

                    string? errorMessage = null;

                    var sslStream = new SslStream(
                        _networkStream,
                        leaveInnerStreamOpen,
                        (sender, certificate, chain, sslPolicyErrors) =>
                            ValidateCertificates(chain, sslPolicyErrors, configTLS, out errorMessage)
                    );

                    try
                    {
                        sslStream.AuthenticateAsClient(_hostname, new X509Certificate2Collection(), enabledSslProtocols, configTLS.CheckCertificateRevocation);
                    }
                    catch (Exception ex)
                    {
                        throw new Exception(string.Format("{0} - {1}", ex.Message, errorMessage), ex);
                    }

                    _networkStream = sslStream;
                }
            }
        }

        private static bool ValidateCertificates(X509Chain chain, SslPolicyErrors sslPolicyErrors, TlsConfig tlsConfig, out string errorMessage)
        {
            errorMessage = string.Empty;

            if ((sslPolicyErrors & SslPolicyErrors.RemoteCertificateNotAvailable) == SslPolicyErrors.RemoteCertificateNotAvailable)
            {
                errorMessage = chain.ChainStatus.GetErrors();
                return false;
            }

            if (tlsConfig.InsecureSkipVerify || sslPolicyErrors == SslPolicyErrors.None)
            {
                return true;
            }
            else
            {
                errorMessage = chain.ChainStatus.GetErrors();
                return false;
            }
        }

        public TimeSpan ReadTimeout
        {
            get { return TimeSpan.FromMilliseconds(_tcpClient.ReceiveTimeout); }
            set { _tcpClient.ReceiveTimeout = (int)value.TotalMilliseconds; }
        }

        public TimeSpan WriteTimeout
        {
            get { return TimeSpan.FromMilliseconds(_tcpClient.SendTimeout); }
            set { _tcpClient.SendTimeout = (int)value.TotalMilliseconds; }
        }

        public int Read(byte[] b)
        {
            if (_isClosed)
                throw new ConnectionClosedException();
            lock (_readLocker)
            {
                int byteLength = b.Length;

                int total = _networkStream.Read(b, 0, byteLength);
                if (total == byteLength || total == 0)
                    return total;

                while (total < byteLength)
                {
                    int n = _networkStream.Read(b, total, byteLength - total);
                    if (n == 0)
                        return total;
                    total += n;
                }
                return total;
            }
        }

        public int Write(byte[] b, int offset, int length)
        {
            if (_isClosed)
                throw new ConnectionClosedException();
            lock (_writeLocker)
            {
                _networkStream.Write(b, offset, length);
                return length;
            }
        }

        public void Close()
        {
            if (_isClosed)
                return;

            lock (_writeLocker)
            {
                lock (_closeLocker)
                {
                    if (_isClosed)
                        return;
                    _isClosed = true;
                }
                try
                {
                    _networkStream.Flush();

                    ReadTimeout = TimeSpan.FromMilliseconds(10);
                    WriteTimeout = TimeSpan.FromMilliseconds(10);

                    _networkStream.Close();
                    _tcpClient.Close();
                }
                catch (SocketException)
                {
                }
                catch (IOException)
                {
                }
                catch (ObjectDisposedException)
                {
                }
                
            }
        }

        public void Flush()
        {
            if (_isClosed)
                throw new ConnectionClosedException();

            lock (_writeLocker)
            {
                _networkStream.Flush();
            }
        }
    }
}
