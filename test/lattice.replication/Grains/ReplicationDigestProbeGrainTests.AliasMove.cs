using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage of the alias re-checks around the single routing snapshot a probe
/// pass reads its local digests through: a copy an alias cutover left retained
/// still answers those reads with its frozen digest, so a pass spanning the move
/// must neither record its mismatches nor advance the cadence.
/// </summary>
public partial class ReplicationDigestProbeGrainTests
{
    private static Task<RoutingInfo> Routing(string physicalTreeId, int shardCount = 1)
        => Task.FromResult(new RoutingInfo(physicalTreeId, ShardMap.CreateDefault(shardCount, shardCount)));

    private static void StubPeerDigest(IReplicationDigestProbeTransport transport, byte[] hash)
        => transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse { DigestAvailable = true, Digest = Digest(hash) }));

    [Test]
    public async Task ProcessNextPhaseAsync_does_not_advance_cadence_when_the_alias_moved_during_the_pass()
    {
        var (grain, state, _, transport, shardCounts, digests) = CreateProbeGrain();
        shardCounts.GetShardRoutingAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Routing("phys"), Routing("phys-restored"), Routing("phys"));
        digests.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest([1, 2, 3])));
        StubPeerDigest(transport, [1, 2, 3]);

        await grain.ProcessNextPhaseAsync();
        var afterMovedPass = state.State.LastProbeTicks;
        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(afterMovedPass, Is.EqualTo(0L), "a pass that spanned an alias move must re-run");
            Assert.That(state.State.LastProbeTicks, Is.GreaterThan(0L), "a pass on stable routing advances");
        });
    }

    [Test]
    public async Task ProcessNextPhaseAsync_discards_a_mismatch_read_from_a_copy_the_alias_left()
    {
        var (grain, state, _, transport, shardCounts, digests) = CreateProbeGrain(shardCount: 2);
        shardCounts.GetShardRoutingAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Routing("phys", shardCount: 2), Routing("phys-restored", shardCount: 2));
        digests.GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest([1, 2, 3])));
        StubPeerDigest(transport, [9, 9, 9]);

        using var compared = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeComparedName);
        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        await grain.ProcessNextPhaseAsync();

        await digests.DidNotReceive().GetLeafProjectionDigestAsync(1, Arg.Any<CancellationToken>());
        await shardCounts.Received(2).GetShardRoutingAsync(Tree, Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(compared.Measurements, Is.Empty);
            Assert.That(mismatch.Measurements, Is.Empty);
            Assert.That(state.State.LastProbeTicks, Is.EqualTo(0L));
        });
    }

    [Test]
    public async Task ProcessNextPhaseAsync_rechecks_the_alias_once_per_pass_however_many_shards_mismatch()
    {
        var (grain, state, _, transport, shardCounts, digests) = CreateProbeGrain(shardCount: 3);
        shardCounts.GetShardRoutingAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Routing("phys", shardCount: 3));
        digests.GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest([1, 2, 3])));
        StubPeerDigest(transport, [9, 9, 9]);

        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        await grain.ProcessNextPhaseAsync();

        // Pass start, first mismatch, pass end.
        await shardCounts.Received(3).GetShardRoutingAsync(Tree, Arg.Any<CancellationToken>());
        Assert.Multiple(() =>
        {
            Assert.That(mismatch.Measurements, Has.Count.EqualTo(3));
            Assert.That(state.State.LastProbeTicks, Is.GreaterThan(0L));
        });
    }

    [Test]
    public async Task ProcessNextPhaseAsync_does_not_advance_cadence_when_the_end_of_pass_recheck_faults()
    {
        var (grain, state, _, transport, shardCounts, digests) = CreateProbeGrain();
        shardCounts.GetShardRoutingAsync(Tree, Arg.Any<CancellationToken>())
            .Returns(Routing("phys"), Task.FromException<RoutingInfo>(new TimeoutException("registry unavailable")));
        digests.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest([1, 2, 3])));
        StubPeerDigest(transport, [1, 2, 3]);

        using var compared = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeComparedName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(compared.Measurements, Has.Count.EqualTo(1));
            Assert.That(state.State.LastProbeTicks, Is.EqualTo(0L));
        });
    }
}
