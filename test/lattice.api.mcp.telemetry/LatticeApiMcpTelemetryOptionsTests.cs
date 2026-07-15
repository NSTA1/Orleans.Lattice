namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Tests for the defaults and round-trip of
/// <see cref="LatticeApiMcpTelemetryOptions"/>: read-all metric access, a sane
/// request timeout, positive range guardrails, no backend auth, and a mutable
/// allow-list.
/// </summary>
[TestFixture]
public sealed class LatticeApiMcpTelemetryOptionsTests
{
    [Test]
    public void Defaults_are_read_all_with_no_backend_auth_and_sane_guardrails()
    {
        var options = new LatticeApiMcpTelemetryOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.BackendAddress, Is.Null);
            Assert.That(options.AuthMode, Is.EqualTo(LatticeTelemetryBackendAuthMode.None));
            Assert.That(options.Credential, Is.Null);
            Assert.That(options.RequestTimeout, Is.EqualTo(TimeSpan.FromSeconds(30)));
            Assert.That(options.MaxRange, Is.GreaterThan(TimeSpan.Zero));
            Assert.That(options.MaxStep, Is.GreaterThan(TimeSpan.Zero));
            Assert.That(options.MetricAccess, Is.EqualTo(LatticeTelemetryMetricAccessMode.ReadAll));
            Assert.That(options.AllowedMetrics, Is.Empty);
        });
    }

    [Test]
    public void Properties_round_trip_assigned_values()
    {
        var address = new Uri("https://prometheus.internal:9090/");
        var credential = new LatticeTelemetryBackendCredential { BearerToken = "token" };
        var options = new LatticeApiMcpTelemetryOptions
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
}
