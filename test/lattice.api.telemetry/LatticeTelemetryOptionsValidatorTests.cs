using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Tests for <see cref="LatticeTelemetryOptionsValidator"/>: the default options
/// validate, and each misconfiguration (missing or relative backend address, an
/// auth mode without its credential, an undefined enum value, non-positive
/// guardrails, and a deny-all posture without a usable allow-list) is rejected.
/// The failure text is asserted for the address and guardrail rules because a
/// host reads it verbatim from the startup exception.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryOptionsValidatorTests
{
    private static readonly LatticeTelemetryOptionsValidator Validator = new();

    private static LatticeTelemetryOptions Valid() => new()
    {
        BackendAddress = new Uri("https://prometheus.internal:9090/"),
    };

    private static bool IsValid(LatticeTelemetryOptions options)
        => Validator.Validate(name: null, options).Succeeded;

    private static IEnumerable<string> Failures(LatticeTelemetryOptions options)
        => Validator.Validate(name: null, options).Failures ?? [];

    [Test]
    public void A_backend_address_and_defaults_validate()
        => Assert.That(IsValid(Valid()), Is.True);

    [Test]
    public void A_missing_backend_address_is_rejected_by_name()
    {
        var options = Valid();
        options.BackendAddress = null;

        Assert.Multiple(() =>
        {
            Assert.That(IsValid(options), Is.False);
            Assert.That(Failures(options), Does.Contain("BackendAddress must be supplied."));
        });
    }

    [Test]
    public void A_relative_backend_address_is_rejected()
    {
        var options = Valid();
        options.BackendAddress = new Uri("api/v1/query", UriKind.Relative);

        Assert.Multiple(() =>
        {
            Assert.That(IsValid(options), Is.False);
            Assert.That(Failures(options), Does.Contain("BackendAddress must be an absolute URI."));
        });
    }

    [Test]
    public void An_undefined_auth_mode_is_rejected()
    {
        var options = Valid();
        options.AuthMode = (LatticeTelemetryBackendAuthMode)int.MaxValue;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void An_undefined_metric_access_mode_is_rejected()
    {
        var options = Valid();
        options.MetricAccess = (LatticeTelemetryMetricAccessMode)int.MaxValue;
        Assert.That(IsValid(options), Is.False);
    }

    [Test]
    public void A_non_positive_request_timeout_is_rejected()
    {
        var options = Valid();
        options.RequestTimeout = TimeSpan.Zero;

        Assert.Multiple(() =>
        {
            Assert.That(IsValid(options), Is.False);
            Assert.That(Failures(options), Does.Contain("RequestTimeout must be strictly positive."));
        });
    }

    private static IEnumerable<TimeSpan> TimeoutsAboveTheHttpClientCeiling()
    {
        yield return TimeSpan.FromMilliseconds(int.MaxValue) + TimeSpan.FromMilliseconds(1);
        yield return TimeSpan.FromDays(30);

        // The usual attempt to say "no timeout".
        yield return TimeSpan.MaxValue;
    }

    [TestCaseSource(nameof(TimeoutsAboveTheHttpClientCeiling))]
    public void A_request_timeout_the_backend_http_client_cannot_represent_is_rejected(TimeSpan timeout)
    {
        // HttpClient.Timeout refuses any finite value above int.MaxValue milliseconds, so a
        // timeout above that ceiling would validate and then fail every resolution of the
        // backend client instead.
        var options = Valid();
        options.RequestTimeout = timeout;

        using var client = new HttpClient();

        Assert.Multiple(() =>
        {
            Assert.That(IsValid(options), Is.False);
            Assert.That(
                Failures(options),
                Has.Some.StartsWith("RequestTimeout must be at most "));
            Assert.That(
                () => client.Timeout = timeout,
                Throws.InstanceOf<ArgumentOutOfRangeException>(),
                "anti-vacuity: the rejected value is one HttpClient itself refuses");
        });
    }

    [Test]
    public void The_largest_admitted_request_timeout_is_one_the_backend_http_client_accepts()
    {
        var options = Valid();
        options.RequestTimeout = TimeSpan.FromMilliseconds(int.MaxValue);

        using var client = new HttpClient();

        Assert.Multiple(() =>
        {
            Assert.That(IsValid(options), Is.True);
            Assert.That(() => client.Timeout = options.RequestTimeout, Throws.Nothing);
        });
    }

    [Test]
    public void A_non_positive_max_range_is_rejected()
    {
        var options = Valid();
        options.MaxRange = TimeSpan.FromSeconds(-1);

        Assert.Multiple(() =>
        {
            Assert.That(IsValid(options), Is.False);
            Assert.That(Failures(options), Does.Contain("MaxRange must be strictly positive."));
        });
    }

    [Test]
    public void A_non_positive_max_step_is_rejected()
    {
        var options = Valid();
        options.MaxStep = TimeSpan.Zero;

        Assert.Multiple(() =>
        {
            Assert.That(IsValid(options), Is.False);
            Assert.That(Failures(options), Does.Contain("MaxStep must be strictly positive."));
        });
    }

    [Test]
    public void Every_violation_is_reported_together_rather_than_first_only()
    {
        var options = new LatticeTelemetryOptions
        {
            RequestTimeout = TimeSpan.Zero,
            MaxRange = TimeSpan.Zero,
            MaxStep = TimeSpan.Zero,
        };

        Assert.That(Failures(options).Count(), Is.EqualTo(4));
    }

    [Test]
    public void None_mode_needs_no_credential_to_validate()
    {
        var options = Valid();
        options.AuthMode = LatticeTelemetryBackendAuthMode.None;
        Assert.That(IsValid(options), Is.True);
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
            BasicUsername = "svc",
            BasicPassword = "secret",
        };
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
    public void A_derived_binding_options_type_is_validated_by_the_same_rules()
    {
        var options = new DerivedOptions();
        Assert.That(IsValid(options), Is.False, "A derived options type must not bypass the base rules.");

        options.BackendAddress = new Uri("https://prometheus.internal:9090/");
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

    private sealed class DerivedOptions : LatticeTelemetryOptions;
}
