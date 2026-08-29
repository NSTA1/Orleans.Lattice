namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Tests for the defaults, round-trip, and exact guardrail values of
/// <see cref="LatticeTelemetryOptions"/>: read-all metric access, a 30-second
/// request timeout, a 24-hour range and 1-hour step budget, no backend auth, and
/// a mutable allow-list. The concrete budgets are asserted here (rather than
/// merely "positive") because a binding's guardrail rejection message quotes
/// them.
/// </summary>
[TestFixture]
public sealed class LatticeTelemetryOptionsTests
{
    [Test]
    public void Defaults_are_read_all_with_no_backend_auth_and_sane_guardrails()
    {
        var options = new LatticeTelemetryOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.BackendAddress, Is.Null);
            Assert.That(options.AuthMode, Is.EqualTo(LatticeTelemetryBackendAuthMode.None));
            Assert.That(options.Credential, Is.Null);
            Assert.That(options.RequestTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.MaxRange, Is.EqualTo(TimeSpan.FromHours(24)));
            Assert.That(options.MaxStep, Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(options.MetricAccess, Is.EqualTo(LatticeTelemetryMetricAccessMode.ReadAll));
            Assert.That(options.AllowedMetrics, Is.Empty);
        });
    }

    [Test]
    public void Properties_round_trip_assigned_values()
    {
        var address = new Uri("https://prometheus.internal:9090/");
        var credential = new LatticeTelemetryBackendCredential { BearerToken = "token" };
        var options = new LatticeTelemetryOptions
        {
            BackendAddress = address,
            AuthMode = LatticeTelemetryBackendAuthMode.Bearer,
            Credential = credential,
            RequestTimeout = TimeSpan.FromSeconds(5),
            MaxRange = TimeSpan.FromHours(6),
            MaxStep = TimeSpan.FromMinutes(15),
            MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed,
        };
        options.AllowedMetrics.Add("lattice_wal_*");

        Assert.Multiple(() =>
        {
            Assert.That(options.BackendAddress, Is.SameAs(address));
            Assert.That(options.AuthMode, Is.EqualTo(LatticeTelemetryBackendAuthMode.Bearer));
            Assert.That(options.Credential, Is.SameAs(credential));
            Assert.That(options.RequestTimeout, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.MaxRange, Is.EqualTo(TimeSpan.FromHours(6)));
            Assert.That(options.MaxStep, Is.EqualTo(TimeSpan.FromMinutes(15)));
            Assert.That(options.MetricAccess, Is.EqualTo(LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed));
            Assert.That(options.AllowedMetrics, Does.Contain("lattice_wal_*"));
        });
    }

    [Test]
    public void The_options_type_is_open_for_a_binding_to_derive_from()
    {
        // A transport binding presents its own named options type over this
        // surface, so the type must not be sealed. Locking it down again would
        // silently force a binding to fork the settings instead.
        Assert.That(typeof(LatticeTelemetryOptions).IsSealed, Is.False);
    }

    [Test]
    public void The_allow_list_is_shared_by_reference_with_a_derived_options_type()
    {
        var derived = new DerivedOptions();
        derived.AllowedMetrics.Add("orleans_lattice_*");

        LatticeTelemetryOptions upcast = derived;
        Assert.That(upcast.AllowedMetrics, Is.SameAs(derived.AllowedMetrics));
    }

    private sealed class DerivedOptions : LatticeTelemetryOptions;
}
