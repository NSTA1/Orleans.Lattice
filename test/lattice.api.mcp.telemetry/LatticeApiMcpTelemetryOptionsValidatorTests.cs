using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="LatticeApiMcpTelemetryOptionsValidator"/>: the default
/// options validate, and each misconfiguration (missing or relative backend
/// address, an auth mode without its credential, non-positive guardrails, and a
/// deny-all posture without an allow-list) is rejected.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpTelemetryOptionsValidatorTests
{
    private static readonly LatticeApiMcpTelemetryOptionsValidator Validator = new();

    private static LatticeApiMcpTelemetryOptions Valid() => new()
    {
        BackendAddress = new Uri("https://prometheus.internal:9090/"),
    };

    private static bool IsValid(LatticeApiMcpTelemetryOptions options)
        => Validator.Validate(name: null, options).Succeeded;

    [Test]
    public void A_backend_address_and_defaults_validate()
        => Assert.That(IsValid(Valid()), Is.True);

    [Test]
    public void A_missing_backend_address_is_rejected()
    {
        var options = Valid();
        options.BackendAddress = null;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void A_relative_backend_address_is_rejected()
    {
        var options = Valid();
        options.BackendAddress = new Uri("api/v1/query", UriKind.Relative);
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void A_non_positive_request_timeout_is_rejected()
    {
        var options = Valid();
        options.RequestTimeout = TimeSpan.Zero;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void A_non_positive_max_range_is_rejected()
    {
        var options = Valid();
        options.MaxRange = TimeSpan.FromSeconds(-1);
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void A_non_positive_max_step_is_rejected()
    {
        var options = Valid();
        options.MaxStep = TimeSpan.Zero;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void Bearer_mode_without_a_token_is_rejected()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.Bearer;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void Bearer_mode_with_a_token_validates()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.Bearer;
        options.Credential = new LatticeTelemetryBackendCredential { BearerToken = "token" };
        Assert.That(IsValid(options), Is.True);
    }

    [Test]
    public void Basic_mode_without_a_username_is_rejected()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.Basic;
        options.Credential = new LatticeTelemetryBackendCredential { BasicPassword = "secret" };
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void Mutual_tls_mode_without_a_certificate_is_rejected()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.MutualTls;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void Mutual_tls_mode_with_a_certificate_validates()
    {
        using var certificate = SelfSignedCertificate();
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.MutualTls;
        options.Credential = new LatticeTelemetryBackendCredential { ClientCertificate = certificate };
        Assert.That(IsValid(options), Is.True);
    }

    [Test]
    public void Dynamic_bearer_mode_needs_no_static_credential_to_validate()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer;
        Assert.That(IsValid(options), Is.True);
    }

    [Test]
    public void Deny_all_without_an_allow_list_is_rejected()
    {
        var options = Valid();
        options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void Deny_all_with_a_blank_allow_list_entry_is_rejected()
    {
        var options = Valid();
        options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
        options.AllowedMetrics.Add("  ");
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void Deny_all_with_an_allow_list_validates()
    {
        var options = Valid();
        options.MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed;
        options.AllowedMetrics.Add("lattice_wal_*");
        Assert.That(IsValid(options), Is.True);
    }

    [Test]
    public void Validate_rejects_a_null_options_instance()
        => Assert.Throws<ArgumentNullException>(() => Validator.Validate(name: null, options: null!));

    private static X509Certificate2 SelfSignedCertificate()
    {
        using var rsa = RSA.Create(2048);
        var request = new CertificateRequest(
            "CN=lattice-telemetry-test", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        return request.CreateSelfSigned(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddDays(1));
    }
}
