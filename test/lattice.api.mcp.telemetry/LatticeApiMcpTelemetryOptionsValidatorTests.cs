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
    public void Basic_mode_with_a_username_validates()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.Basic;
        options.Credential = new LatticeTelemetryBackendCredential
        {
            BasicUsername = "prometheus",
            BasicPassword = "secret",
        };
        Assert.That(IsValid(options), Is.True);
    }

    [Test]
    public void None_mode_needs_no_credential_to_validate()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.None;
        options.Credential = null;
        Assert.That(IsValid(options), Is.True);
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
    public void Basic_mode_with_no_credential_at_all_is_rejected()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.Basic;
        options.Credential = null;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void Bearer_mode_with_no_credential_at_all_is_rejected()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.Bearer;
        options.Credential = null;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void Mutual_tls_mode_with_a_credential_holding_no_certificate_is_rejected()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.MutualTls;
        options.Credential = new LatticeTelemetryBackendCredential();
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void An_undefined_auth_mode_is_rejected()
    {
        var options = Valid();
        options.AuthMode = (LatticeTelemetryBackendAuthMode)int.MaxValue;

        var result = Validator.Validate(name: null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.False);
            Assert.That(
                result.Failures,
                Has.Some.Contains(nameof(LatticeApiMcpTelemetryOptions.AuthMode)),
                "An undefined auth mode must be named in the failure so an operator can find it.");
        });
    }

    [Test]
    public void An_undefined_metric_access_mode_is_rejected()
    {
        var options = Valid();
        options.MetricAccess = (LatticeTelemetryMetricAccessMode)int.MaxValue;

        var result = Validator.Validate(name: null, options);

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.False);
            Assert.That(
                result.Failures,
                Has.Some.Contains(nameof(LatticeApiMcpTelemetryOptions.MetricAccess)));
        });
    }

    [Test]
    public void An_undefined_auth_mode_is_not_also_reported_as_a_missing_credential()
    {
        // The credential check is an else-arm of the defined-enum check: an
        // undefined mode has no credential contract to violate, so reporting one
        // would be noise on top of the real fault.
        var options = Valid();
        options.AuthMode = (LatticeTelemetryBackendAuthMode)int.MaxValue;

        var result = Validator.Validate(name: null, options);

        Assert.That(
            result.Failures,
            Has.None.Contains(nameof(LatticeApiMcpTelemetryOptions.Credential)));
    }

    [Test]
    public void An_undefined_metric_access_mode_is_not_also_reported_as_a_missing_allow_list()
    {
        var options = Valid();
        options.MetricAccess = (LatticeTelemetryMetricAccessMode)int.MaxValue;

        var result = Validator.Validate(name: null, options);

        Assert.That(
            result.Failures,
            Has.None.Contains(nameof(LatticeApiMcpTelemetryOptions.AllowedMetrics)));
    }

    [Test]
    public void Every_independent_misconfiguration_is_reported_in_one_pass()
    {
        // The validator accumulates rather than failing at the first fault, so an
        // operator fixes a wholly broken configuration in a single edit.
        var options = new LatticeApiMcpTelemetryOptions
        {
            BackendAddress = null,
            AuthMode = LatticeTelemetryBackendAuthMode.Bearer,
            RequestTimeout = TimeSpan.Zero,
            MaxRange = TimeSpan.Zero,
            MaxStep = TimeSpan.Zero,
            MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed,
        };

        var result = Validator.Validate(name: null, options);
        var failures = result.Failures?.ToArray() ?? [];

        Assert.Multiple(() =>
        {
            Assert.That(result.Succeeded, Is.False);
            Assert.That(failures, Has.Exactly(1).Contains(nameof(LatticeApiMcpTelemetryOptions.BackendAddress)));
            Assert.That(failures, Has.Exactly(1).Contains(nameof(LatticeTelemetryBackendCredential.BearerToken)));
            Assert.That(failures, Has.Exactly(1).Contains(nameof(LatticeApiMcpTelemetryOptions.RequestTimeout)));
            Assert.That(failures, Has.Exactly(1).Contains(nameof(LatticeApiMcpTelemetryOptions.MaxRange)));
            Assert.That(failures, Has.Exactly(1).Contains(nameof(LatticeApiMcpTelemetryOptions.MaxStep)));
            Assert.That(failures, Has.Exactly(1).Contains(nameof(LatticeApiMcpTelemetryOptions.AllowedMetrics)));
            Assert.That(failures, Has.Length.EqualTo(6), "Exactly the six independent faults, with no duplicates.");
        });
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
