using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage of the best-effort fault arms of the digest-probe phase pump - the
/// catch blocks that keep a single failing dependency from disturbing the
/// detection cadence. Each test drives one seam to throw and asserts the
/// documented recovery: a shard-count failure leaves the cadence un-advanced so
/// the next tick retries; a transient per-shard local-digest read is skipped and
/// the pass continues; a peer probe failure is logged and the next peer/cadence
/// proceeds; a cadence-stamp persist failure is rolled back in memory; and a
/// Merkle-walk localisation failure is swallowed. None of these propagate.
/// </summary>
public partial class ReplicationDigestProbeGrainTests
{
    [Test]
    public async Task ProcessNextPhaseAsync_does_not_advance_cadence_when_the_shard_count_cannot_be_resolved()
    {
        var (grain, state, lattice, _, shardCounts) = CreateProbeGrain();
        shardCounts.GetShardCountAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<int>(new InvalidOperationException("shard count unavailable")));

        await grain.ProcessNextPhaseAsync();

        // The pass bailed before reading any digest, and the cadence stamp is
        // left at zero so the very next phase tick retries.
        await lattice.DidNotReceive().GetLeafProjectionDigestAsync(Arg.Any<int>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.LastProbeTicks, Is.EqualTo(0L));
    }

    [Test]
    public async Task ProcessNextPhaseAsync_skips_a_shard_whose_local_digest_read_faults_transiently()
    {
        var (grain, state, lattice, transport, _) = CreateProbeGrain();
        // A non-InvalidOperationException is a transient read failure (the
        // permanent-latch path is InvalidOperationException, covered elsewhere).
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromException<LeafProjectionDigest>(new TimeoutException("digest read timed out")));

        await grain.ProcessNextPhaseAsync();

        // The shard is skipped (no peer probe for it) but the pass completes and
        // advances the cadence, since the failure is not the permanent latch.
        await transport.DidNotReceive().ProbeDigestAsync(
            Arg.Any<string>(), Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>());
        Assert.That(state.State.LastProbeTicks, Is.GreaterThan(0L));
    }

    [Test]
    public async Task ProcessNextPhaseAsync_logs_and_continues_when_a_peer_probe_faults()
    {
        var (grain, state, lattice, transport, _) = CreateProbeGrain();
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromException<DigestProbeResponse>(new TimeoutException("peer unreachable")));

        using var compared = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeComparedName);

        await grain.ProcessNextPhaseAsync();

        // The comparison never ran (the probe threw first), yet the pass still
        // completes normally and advances the cadence.
        Assert.That(compared.Measurements, Is.Empty);
        Assert.That(state.State.LastProbeTicks, Is.GreaterThan(0L));
    }

    [Test]
    public async Task AdvanceCadence_restores_the_previous_stamp_when_persisting_it_faults()
    {
        // Empty peer set routes straight to AdvanceCadenceAsync; the persist then
        // faults, so the in-memory cadence stamp must be rolled back to its prior
        // value (zero) rather than skipping a whole interval.
        var (grain, state, _, _, _) = CreateProbeGrain(peers: Array.Empty<string>());
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(state.State.LastProbeTicks, Is.EqualTo(0L));
            Assert.That(state.WriteCount, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task ProcessNextPhaseAsync_swallows_a_merkle_walk_localisation_fault()
    {
        // A mismatch triggers the (opted-in) Merkle-walk localisation, whose
        // physical-tree-id resolution faults. The best-effort localise stage must
        // swallow it and let the detect-stage cadence advance regardless.
        var (grain, state, lattice, transport, _) = CreateProbeGrain(merkleWalkEnabled: true);
        lattice.GetLeafProjectionDigestAsync(0, Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(Digest(new byte[] { 1, 2, 3 })));
        transport.ProbeDigestAsync("site-b", Arg.Any<DigestProbeRequest>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new DigestProbeResponse
            {
                DigestAvailable = true,
                Digest = Digest(new byte[] { 9, 9, 9 }),
            }));
        lattice.GetRoutingAsync(Arg.Any<CancellationToken>())
            .Returns(new ValueTask<RoutingInfo>(
                Task.FromException<RoutingInfo>(new InvalidOperationException("routing unavailable"))));

        using var mismatch = new MeterCollector<long>(
            LatticeReplicationMetrics.MeterName, LatticeReplicationMetrics.DigestProbeMismatchName);

        await grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            // Detection still fired (the mismatch was recorded) and the pass
            // advanced its cadence despite the localise fault.
            Assert.That(mismatch.Measurements, Is.Not.Empty);
            Assert.That(state.State.LastProbeTicks, Is.GreaterThan(0L));
        });
    }
}
