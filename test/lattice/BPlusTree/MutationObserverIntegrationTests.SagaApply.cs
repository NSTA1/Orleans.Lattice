using NUnit.Framework;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration tests asserting that the atomic batched apply
/// seam — installs a batch of remote mutations through the per-tree
/// saga grain — surfaces every per-key emit with the source-side
/// <see cref="LatticeMutation.OriginClusterId"/> /
/// <see cref="LatticeMutation.VectorClock"/> /
/// <see cref="LatticeMutation.TransactionId"/> stamps shared across the
/// batch. The receiver-side observer is the only place the in-process
/// origin-filter and the outbound-ship loop see the saga-wide stamps,
/// so a regression here would silently re-route remote writes back to
/// the authoring cluster.
/// </summary>
public sealed partial class MutationObserverIntegrationTests
{
    [Test]
    public async Task ApplyManyAtomicAsync_emits_per_key_mutations_with_origin_cluster_id()
    {
        const string treeId = "obs-e2e-saga-apply-origin";
        await _fixture.CreateTreeAsync(treeId);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeId);

        var hlcA = new HybridLogicalClock { WallClockTicks = 10_000, Counter = 0 };
        var hlcB = new HybridLogicalClock { WallClockTicks = 10_001, Counter = 0 };

        var result = await apply.ApplyManyAtomicAsync(
            new[]
            {
                new AtomicApplyEntry
                {
                    Key = "a", Value = new byte[] { 1 }, Timestamp = hlcA,
                    ExpiresAtTicks = 0, VectorClock = null, IsTombstone = false,
                },
                new AtomicApplyEntry
                {
                    Key = "b", Value = new byte[] { 2 }, Timestamp = hlcB,
                    ExpiresAtTicks = 0, VectorClock = null, IsTombstone = false,
                },
            },
            transactionId: Guid.NewGuid(),
            originClusterId: "remote-site",
            sourceVectorClock: null);

        Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));

        var mA = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.TreeId == treeId && m.Key == "a");
        var mB = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.TreeId == treeId && m.Key == "b");

        Assert.Multiple(() =>
        {
            Assert.That(mA.OriginClusterId, Is.EqualTo("remote-site"));
            Assert.That(mB.OriginClusterId, Is.EqualTo("remote-site"));
        });
    }

    [Test]
    public async Task ApplyManyAtomicAsync_emits_per_key_mutations_with_per_entry_vector_clock()
    {
        const string treeId = "obs-e2e-saga-apply-vc";
        await _fixture.CreateTreeAsync(treeId);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeId);

        var vcA = new VersionVector();
        vcA.Tick("remote-site");

        var vcB = new VersionVector();
        vcB.Tick("remote-site");
        vcB.Tick("remote-site");

        var result = await apply.ApplyManyAtomicAsync(
            new[]
            {
                new AtomicApplyEntry
                {
                    Key = "a", Value = new byte[] { 1 },
                    Timestamp = new HybridLogicalClock { WallClockTicks = 11_000 },
                    ExpiresAtTicks = 0, VectorClock = vcA, IsTombstone = false,
                },
                new AtomicApplyEntry
                {
                    Key = "b", Value = new byte[] { 2 },
                    Timestamp = new HybridLogicalClock { WallClockTicks = 11_001 },
                    ExpiresAtTicks = 0, VectorClock = vcB, IsTombstone = false,
                },
            },
            transactionId: Guid.NewGuid(),
            originClusterId: "remote-site",
            sourceVectorClock: null);

        Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));

        var mA = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.TreeId == treeId && m.Key == "a"
            && m.VectorClock != null);
        var mB = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.TreeId == treeId && m.Key == "b"
            && m.VectorClock != null);

        Assert.Multiple(() =>
        {
            Assert.That(mA.VectorClock!.GetClock("remote-site"),
                Is.EqualTo(vcA.GetClock("remote-site")));
            Assert.That(mB.VectorClock!.GetClock("remote-site"),
                Is.EqualTo(vcB.GetClock("remote-site")));
        });
    }

    [Test]
    public async Task ApplyManyAtomicAsync_per_key_mutations_share_TransactionId_across_batch()
    {
        const string treeId = "obs-e2e-saga-apply-tx";
        await _fixture.CreateTreeAsync(treeId);
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeId);

        var result = await apply.ApplyManyAtomicAsync(
            new[]
            {
                new AtomicApplyEntry
                {
                    Key = "a", Value = new byte[] { 1 },
                    Timestamp = new HybridLogicalClock { WallClockTicks = 12_000 },
                    ExpiresAtTicks = 0, VectorClock = null, IsTombstone = false,
                },
                new AtomicApplyEntry
                {
                    Key = "b", Value = new byte[] { 2 },
                    Timestamp = new HybridLogicalClock { WallClockTicks = 12_001 },
                    ExpiresAtTicks = 0, VectorClock = null, IsTombstone = false,
                },
                new AtomicApplyEntry
                {
                    Key = "c", Value = new byte[] { 3 },
                    Timestamp = new HybridLogicalClock { WallClockTicks = 12_002 },
                    ExpiresAtTicks = 0, VectorClock = null, IsTombstone = false,
                },
            },
            transactionId: Guid.NewGuid(),
            originClusterId: "remote-site",
            sourceVectorClock: null);

        Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));

        var mA = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.TreeId == treeId && m.Key == "a");
        var mB = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.TreeId == treeId && m.Key == "b");
        var mC = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.TreeId == treeId && m.Key == "c");

        // The saga's transaction id is shared across every per-key emit
        // — observers see one logical batch identifier, not three
        // independent per-key ids.
        Assert.Multiple(() =>
        {
            Assert.That(mA.TransactionId, Is.Not.EqualTo(Guid.Empty));
            Assert.That(mB.TransactionId, Is.EqualTo(mA.TransactionId));
            Assert.That(mC.TransactionId, Is.EqualTo(mA.TransactionId));
        });
    }
}
