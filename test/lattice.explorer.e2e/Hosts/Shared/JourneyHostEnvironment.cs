using System.Net.Security;
using System.Security.Cryptography.X509Certificates;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosting;

/// <summary>
/// The environment contract between the journey harness and the journey hosts. Every
/// host is configured by environment variables only, as a container is: the harness
/// writes these, and each host reads them. The ASP.NET configuration keys a host shares
/// with its reference-architecture counterpart (<c>Storage__ConnectionString</c>,
/// <c>Cluster__Id</c>, <c>Auth__BootstrapAdministrators</c> and the rest) are set under
/// their reference names; the names below are the ones only the journey estate has.
/// </summary>
internal static class JourneyHostEnvironment
{
    /// <summary>The PEM file holding the run-scoped certificate authority every host trusts.</summary>
    public const string CaCertificateFile = "LATTICE_E2E_CA_CERT";

    /// <summary>The PKCS#12 file holding this host's server certificate, issued by the run-scoped CA.</summary>
    public const string TlsPfxFile = "LATTICE_E2E_TLS_PFX";

    /// <summary>The password of <see cref="TlsPfxFile"/>.</summary>
    public const string TlsPfxPassword = "LATTICE_E2E_TLS_PFX_PASSWORD";

    /// <summary>The JSON identity directory (<c>JourneyDirectoryDocument</c>) the silo validates sign-ins against.</summary>
    public const string DirectoryFile = "LATTICE_E2E_DIRECTORY";

    /// <summary>Set to <c>1</c> to enable the journey-host-only fault and diagnostic seams.</summary>
    public const string Faults = "LATTICE_E2E_FAULTS";

    /// <summary>The loopback port a host serves its harness control surface on (gateway and line feed).</summary>
    public const string ControlPort = "LATTICE_E2E_CONTROL_PORT";

    /// <summary>The value <see cref="Faults"/> carries when the fault seams are enabled.</summary>
    public const string Enabled = "1";

    /// <summary>Whether the fault and diagnostic seams are enabled in this process.</summary>
    public static bool FaultsEnabled =>
        string.Equals(Environment.GetEnvironmentVariable(Faults), Enabled, StringComparison.Ordinal);

    /// <summary>Loads this host's server certificate, or <see langword="null"/> when none is configured.</summary>
    public static X509Certificate2? LoadServerCertificate()
    {
        var path = Environment.GetEnvironmentVariable(TlsPfxFile);
        if (string.IsNullOrWhiteSpace(path))
        {
            return null;
        }

        return X509CertificateLoader.LoadPkcs12FromFile(
            path,
            Environment.GetEnvironmentVariable(TlsPfxPassword),
            X509KeyStorageFlags.Exportable);
    }

    /// <summary>Loads the run-scoped certificate authority, or <see langword="null"/> when none is configured.</summary>
    public static X509Certificate2? LoadCertificateAuthority()
    {
        var path = Environment.GetEnvironmentVariable(CaCertificateFile);
        if (string.IsNullOrWhiteSpace(path))
        {
            return null;
        }

        // A public certificate alone: PEM with no key, so it is decoded rather than loaded
        // through CreateFromPemFile, which expects the private key beside it.
        var pem = File.ReadAllText(path);
        var fields = System.Security.Cryptography.PemEncoding.Find(pem);
        return X509CertificateLoader.LoadCertificate(Convert.FromBase64String(pem[fields.Base64Data]));
    }

    /// <summary>
    /// A server-certificate check that accepts exactly the chains rooted in
    /// <paramref name="authority"/>, with the host name checked as usual. It is what a
    /// service configured to trust a private PKI does; nothing here accepts an arbitrary
    /// certificate.
    /// </summary>
    /// <param name="authority">The trusted root.</param>
    public static RemoteCertificateValidationCallback TrustOnly(X509Certificate2 authority)
    {
        ArgumentNullException.ThrowIfNull(authority);
        return (_, certificate, _, errors) =>
        {
            if (certificate is null || (errors & SslPolicyErrors.RemoteCertificateNameMismatch) != 0)
            {
                return false;
            }

            using var chain = new X509Chain();
            chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
            chain.ChainPolicy.CustomTrustStore.Add(authority);
            chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
            if (certificate is X509Certificate2 typed)
            {
                return chain.Build(typed);
            }

            using var leaf = X509CertificateLoader.LoadCertificate(certificate.GetRawCertData());
            return chain.Build(leaf);
        };
    }
}
