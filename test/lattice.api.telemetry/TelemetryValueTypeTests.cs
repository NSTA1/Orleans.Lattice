using System.Text.Json;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Tests for the backend envelope value types
/// (<see cref="PrometheusQueryResponse"/> and
/// <see cref="PrometheusMetadataResponse"/>), the scan result
/// <see cref="PromQlMetricReferences"/>, and the backend credential holder
/// <see cref="LatticeTelemetryBackendCredential"/>: each carries exactly what a
/// binding reads back off it, and the two auth-mode / access-mode enumerations
/// declare the members a host configures by name.
/// </summary>
[TestFixture]
public sealed class TelemetryValueTypeTests
{
    [Test]
    public void A_query_envelope_carries_its_status_and_data()
    {
        using var document = JsonDocument.Parse("{\"resultType\":\"vector\",\"result\":[]}");
        var envelope = new PrometheusQueryResponse("success", document.RootElement);

        Assert.Multiple(() =>
        {
            Assert.That(envelope.Status, Is.EqualTo("success"));
            Assert.That(envelope.Data.GetProperty("resultType").GetString(), Is.EqualTo("vector"));
        });
    }

    [Test]
    public void A_metadata_envelope_carries_its_status_and_data()
    {
        using var document = JsonDocument.Parse("{\"up\":[{\"type\":\"gauge\"}]}");
        var envelope = new PrometheusMetadataResponse("success", document.RootElement);

        Assert.Multiple(() =>
        {
            Assert.That(envelope.Status, Is.EqualTo("success"));
            Assert.That(envelope.Data.GetProperty("up").GetArrayLength(), Is.EqualTo(1));
        });
    }

    [Test]
    public void A_default_envelope_carries_an_undefined_data_payload()
    {
        var envelope = default(PrometheusQueryResponse);

        Assert.Multiple(() =>
        {
            Assert.That(envelope.Status, Is.Null);
            Assert.That(envelope.Data.ValueKind, Is.EqualTo(JsonValueKind.Undefined));
        });
    }

    [Test]
    public void Metric_references_default_the_unconstrained_selector_flag_to_false()
    {
        var references = new PromQlMetricReferences
        {
            Names = ["up"],
            HasUnresolvableNameMatcher = false,
        };

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.EqualTo(new[] { "up" }));
            Assert.That(references.HasUnresolvableNameMatcher, Is.False);
            Assert.That(references.HasUnconstrainedSelector, Is.False);
        });
    }

    [Test]
    public void Metric_references_carry_both_fail_closed_flags_when_set()
    {
        var references = new PromQlMetricReferences
        {
            Names = [],
            HasUnresolvableNameMatcher = true,
            HasUnconstrainedSelector = true,
        };

        Assert.Multiple(() =>
        {
            Assert.That(references.Names, Is.Empty);
            Assert.That(references.HasUnresolvableNameMatcher, Is.True);
            Assert.That(references.HasUnconstrainedSelector, Is.True);
        });
    }

    [Test]
    public void The_backend_credential_defaults_every_member_to_null()
    {
        var credential = new LatticeTelemetryBackendCredential();

        Assert.Multiple(() =>
        {
            Assert.That(credential.BearerToken, Is.Null);
            Assert.That(credential.BasicUsername, Is.Null);
            Assert.That(credential.BasicPassword, Is.Null);
            Assert.That(credential.ClientCertificate, Is.Null);
        });
    }

    [Test]
    public void The_backend_credential_round_trips_its_members()
    {
        var credential = new LatticeTelemetryBackendCredential
        {
            BearerToken = "token",
            BasicUsername = "svc",
            BasicPassword = "secret",
        };

        Assert.Multiple(() =>
        {
            Assert.That(credential.BearerToken, Is.EqualTo("token"));
            Assert.That(credential.BasicUsername, Is.EqualTo("svc"));
            Assert.That(credential.BasicPassword, Is.EqualTo("secret"));
        });
    }

    [Test]
    public void The_auth_modes_are_exactly_the_five_a_host_may_configure()
        => Assert.That(
            Enum.GetValues<LatticeTelemetryBackendAuthMode>(),
            Is.EqualTo(new[]
            {
                LatticeTelemetryBackendAuthMode.None,
                LatticeTelemetryBackendAuthMode.Bearer,
                LatticeTelemetryBackendAuthMode.Basic,
                LatticeTelemetryBackendAuthMode.MutualTls,
                LatticeTelemetryBackendAuthMode.DynamicBearer,
            }));

    [Test]
    public void The_default_auth_mode_is_the_unauthenticated_one()
        => Assert.That(
            default(LatticeTelemetryBackendAuthMode),
            Is.EqualTo(LatticeTelemetryBackendAuthMode.None));

    [Test]
    public void The_metric_access_modes_are_exactly_read_all_and_deny_all()
        => Assert.That(
            Enum.GetValues<LatticeTelemetryMetricAccessMode>(),
            Is.EqualTo(new[]
            {
                LatticeTelemetryMetricAccessMode.ReadAll,
                LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed,
            }));
}
