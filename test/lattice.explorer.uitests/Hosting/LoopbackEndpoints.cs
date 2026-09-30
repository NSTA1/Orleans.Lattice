using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The loopback plumbing every in-process head needs: free ports, chosen by the
/// operating system, and a throwaway TLS certificate for the browser-facing port.
/// </summary>
/// <remarks>
/// <para>
/// The browser port is HTTPS because the web head's sign-in cookie is always
/// <c>Secure</c>. Chromium and Firefox accept a secure cookie over
/// <c>http://127.0.0.1</c>, but WebKit does not, and the pilot and isolation fixtures
/// run in WebKit too. A certificate generated per run, with the browsers told to
/// ignore its issuer, keeps the head on the exact cookie path a real deployment uses
/// rather than weakening it for the test.
/// </para>
/// <para>
/// A port is reserved by binding port 0 and releasing it. Another process could in
/// principle take it in the moment between, which would fail the head's start with a
/// clear bind error rather than a wrong result.
/// </para>
/// <para>
/// Once released, a port is free again, so the operating system may hand the same one
/// to the next reservation. A world reserves several ports before binding any of them,
/// and the suite starts more than one world, so every port handed out is remembered
/// for the life of the process and never handed out twice.
/// </para>
/// </remarks>
internal static class LoopbackEndpoints
{
    private static readonly HashSet<int> Issued = [];

    /// <summary>A free loopback TCP port, never one this process has already been given.</summary>
    public static int ReservePort()
    {
        while (true)
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var port = ((IPEndPoint)listener.LocalEndpoint).Port;
            listener.Stop();

            lock (Issued)
            {
                if (Issued.Add(port))
                {
                    return port;
                }
            }
        }
    }

    /// <summary>
    /// A self-signed server certificate for <c>localhost</c> and <c>127.0.0.1</c>,
    /// valid for a week, with an exportable private key Kestrel can use on every
    /// operating system.
    /// </summary>
    public static X509Certificate2 CreateServerCertificate()
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest("CN=localhost", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);

        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], critical: false));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: false));

        var now = DateTimeOffset.UtcNow;
        using var ephemeral = request.CreateSelfSigned(now.AddDays(-1), now.AddDays(7));

        // Round-trip through PKCS#12 so the key is persisted rather than ephemeral:
        // SChannel on Windows refuses an ephemeral key for a server handshake.
        return X509CertificateLoader.LoadPkcs12(ephemeral.Export(X509ContentType.Pfx), password: null);
    }
}
