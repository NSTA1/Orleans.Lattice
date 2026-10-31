using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>
/// The estate's run-scoped public key infrastructure: a certificate authority generated for
/// one estate and discarded with it, and the server certificates it issues to every
/// gateway, silo and Explorer head, each valid for <c>localhost</c> and <c>127.0.0.1</c>.
/// </summary>
internal sealed class JourneyCertificates : IDisposable
{
    /// <summary>The password every issued PKCS#12 file is written with. It protects nothing: the files live for one run.</summary>
    public const string PfxPassword = "journey-estate";

    private readonly X509Certificate2 _authority;
    private readonly string _directory;

    private JourneyCertificates(X509Certificate2 authority, string directory)
    {
        _authority = authority;
        _directory = directory;
        AuthorityPemFile = Path.Combine(directory, "ca.pem");
        File.WriteAllText(AuthorityPemFile, authority.ExportCertificatePem());
    }

    /// <summary>The authority's certificate, as PEM: what every client of the estate trusts.</summary>
    public string AuthorityPemFile { get; }

    /// <summary>The authority's public certificate.</summary>
    public X509Certificate2 Authority => _authority;

    /// <summary>Creates a fresh authority whose files live in <paramref name="directory"/>.</summary>
    /// <param name="directory">The estate's private working directory.</param>
    /// <param name="estate">The estate's name, recorded in the authority's subject.</param>
    public static JourneyCertificates Create(string directory, string estate)
    {
        Directory.CreateDirectory(directory);
        // The key stays alive for the authority's lifetime: it signs every certificate issued.
        var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN=Journey estate {estate} CA", key, HashAlgorithmName.SHA256,         RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(certificateAuthority: true, hasPathLengthConstraint: true, pathLengthConstraint: 0, critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, critical: true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, critical: false));
        var authority = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(2));
        return new JourneyCertificates(authority, directory);
    }

    /// <summary>
    /// Issues a server certificate for <paramref name="name"/> and writes it as a PKCS#12 file
    /// protected by <see cref="PfxPassword"/>.
    /// </summary>
    /// <param name="name">A file-safe name for the host, such as <c>west-gateway</c>.</param>
    /// <returns>The PKCS#12 file.</returns>
    public string IssueServerPfx(string name)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={name}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var names = new SubjectAlternativeNameBuilder();
        names.AddDnsName("localhost");
        names.AddIpAddress(IPAddress.Loopback);
        request.CertificateExtensions.Add(names.Build());
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, critical: true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, critical: true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid("1.3.6.1.5.5.7.3.1")], critical: false));

        var serial = new byte[16];
        RandomNumberGenerator.Fill(serial);
        serial[0] &= 0x7F;
        using var issued = request.Create(_authority, DateTimeOffset.UtcNow.AddMinutes(-5), DateTimeOffset.UtcNow.AddDays(1), serial);
        using var withKey = issued.CopyWithPrivateKey(key);

        var path = Path.Combine(_directory, name + ".pfx");
        File.WriteAllBytes(path, withKey.Export(X509ContentType.Pkcs12, PfxPassword));
        return path;
    }

    /// <inheritdoc />
    public void Dispose() => _authority.Dispose();
}
