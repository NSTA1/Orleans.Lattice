namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Pins the <em>default interface implementations</em> on
/// <see cref="IReplicationDigestProbeTransport"/>. They are the compatibility
/// contract for a transport that predates a later probe: an implementer supplies
/// only the required <see cref="IReplicationDigestProbeTransport.ProbeDigestAsync"/>
/// and inherits a conservative, read-only answer for everything added since.
/// <para>
/// The shipped <c>NoOpReplicationDigestProbeTransport</c> overrides several of
/// these, so its fixture exercises the overrides rather than the defaults. This
/// fixture uses a minimal implementer that overrides nothing, which is the only
/// way the inherited bodies run - and is exactly the shape a third-party or
/// rolling-upgrade transport presents.
/// </para>
/// </summary>
[TestFixture]
public class ReplicationDigestProbeTransportDefaultsTests
{
    /// <summary>
    /// A transport implementing only the one required member, so every other
    /// probe resolves to the interface's default implementation.
    /// </summary>
    private sealed class MinimalProbeTransport : IReplicationDigestProbeTransport
    {
        public Task<DigestProbeResponse> ProbeDigestAsync(
            string targetClusterId, DigestProbeRequest request, CancellationToken cancellationToken)
            => Task.FromResult(new DigestProbeResponse { DigestAvailable = false });
    }

    private static readonly IReplicationDigestProbeTransport Transport = new MinimalProbeTransport();

    [Test]
    public async Task Default_ProbeMerkleWalkAsync_reports_the_peer_unavailable()
    {
        var response = await Transport.ProbeMerkleWalkAsync(
            "site-b",
            new MerkleWalkProbeRequest { TreeName = "orders", ShardIndex = 0, Depth = 0 },
            CancellationToken.None);

        Assert.That(response.Available, Is.False,
            "without a transport that can compute a remote key-range subtree digest, the localisation pass "
            + "must abort cleanly rather than assume the peer agrees");
    }

    [Test]
    public async Task Default_GetPeerHighWaterMarkAsync_returns_the_conservative_zero_cursor()
    {
        var cursor = await Transport.GetPeerHighWaterMarkAsync(
            "site-b", "orders", "cluster-a", CancellationToken.None);

        Assert.That(cursor, Is.EqualTo(HybridLogicalClock.Zero),
            "the safe default re-ships every in-range retained entry and leans on receiver-side dedup; "
            + "returning anything higher could silently skip entries the peer never applied");
    }

    [Test]
    public async Task Default_ExchangeContentManifestAsync_reports_the_exchange_unsupported()
    {
        var response = await Transport.ExchangeContentManifestAsync(
            "site-b",
            new ContentManifestRequest { TreeName = "orders", OriginClusterId = "cluster-a" },
            CancellationToken.None);

        Assert.That(response.ExchangeSupported, Is.False,
            "an unsupported exchange must make the sender treat every entry as missing and ship the batch verbatim");
    }

    [Test]
    public async Task Default_PullCompressionDictionaryAsync_reports_the_pull_unsupported()
    {
        var response = await Transport.PullCompressionDictionaryAsync(
            "site-b",
            new CompressionDictionaryPullRequest { DictionaryId = 7u },
            CancellationToken.None);

        Assert.That(response.ExchangeSupported, Is.False,
            "an unsupported pull must leave the dictionary uninstalled for a later tick, never install unverified bytes");
    }

    [Test]
    public async Task The_required_member_is_still_the_implementers_own()
    {
        var response = await Transport.ProbeDigestAsync(
            "site-b",
            new DigestProbeRequest { TreeName = "orders", ShardIndex = 0 },
            CancellationToken.None);

        Assert.That(response.DigestAvailable, Is.False,
            "ProbeDigestAsync has no default implementation; the minimal transport must supply it");
    }
}
