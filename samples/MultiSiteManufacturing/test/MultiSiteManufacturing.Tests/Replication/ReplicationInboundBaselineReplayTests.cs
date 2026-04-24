using Microsoft.Extensions.Logging.Abstractions;
using MultiSiteManufacturing.Host.Baseline;
using MultiSiteManufacturing.Host.Domain;
using MultiSiteManufacturing.Host.Lattice;
using MultiSiteManufacturing.Host.Replication;
using MultiSiteManufacturing.Tests.Federation;
using static MultiSiteManufacturing.Tests.Federation.FactFixtures;

namespace MultiSiteManufacturing.Tests.Replication;

/// <summary>
/// Unit tests for <see cref="ReplicationInboundEndpoint.TryReplayToBaselineAsync"/>:
/// the helper that decodes a replicated <c>mfg-facts</c> payload and
/// feeds it into the peer's <see cref="BaselineFactBackend"/>. The
/// helper must be robust to malformed payloads because a single bad
/// entry must not abort an in-progress replication batch.
/// </summary>
[TestFixture]
public sealed class ReplicationInboundBaselineReplayTests
{
    private FederationTestClusterFixture _fixture = null!;
    private BaselineFactBackend _baseline = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new FederationTestClusterFixture();
        await _fixture.InitializeAsync();
        _baseline = _fixture.NewBaselineBackend();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    [Test]
    public async Task Valid_payload_is_decoded_and_emitted_to_baseline()
    {
        var serial = new PartSerialNumber("HPT-BLD-S1-2028-99001");
        var fact = Nc(serial, tick: 1, ncNumber: "NC-R-1", NcSeverity.Minor, ProcessSite.ToulouseNdtLab);
        var payload = FactJsonCodec.Encode(fact);

        await ReplicationInboundEndpoint.TryReplayToBaselineAsync(
            _baseline, payload, sourceCluster: "us", key: "replog-key",
            NullLogger.Instance, CancellationToken.None);

        var facts = await _baseline.GetFactsAsync(serial);
        var state = await _baseline.GetStateAsync(serial);

        Assert.Multiple(() =>
        {
            Assert.That(facts, Has.Count.EqualTo(1));
            Assert.That(facts[0].FactId, Is.EqualTo(fact.FactId));
            Assert.That(state, Is.EqualTo(ComplianceState.FlaggedForReview));
        });
    }

    [Test]
    public void Malformed_payload_is_swallowed_without_throwing()
    {
        // A payload that fails to decode must never abort the caller;
        // the batch continues past the bad entry so the rest of the
        // facts still apply.
        var garbage = new byte[] { 0x7b, 0x99, 0x00 }; // "{" + invalid UTF-8

        Assert.DoesNotThrowAsync(() => ReplicationInboundEndpoint.TryReplayToBaselineAsync(
            _baseline, garbage, sourceCluster: "us", key: "bad-key",
            NullLogger.Instance, CancellationToken.None));
    }
}
