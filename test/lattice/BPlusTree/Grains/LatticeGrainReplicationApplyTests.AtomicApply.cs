using Orleans.Lattice;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Tests for the atomic batched apply seam — installs a batch of remote
/// mutations through the per-tree saga grain so the entire batch either
/// commits or rolls back as a unit. The receiver must preserve each
/// entry's source <see cref="HybridLogicalClock"/> /
/// <see cref="VersionVector"/> / origin-cluster id verbatim, exactly as
/// the per-key seams do.
/// </summary>
public partial class LatticeGrainReplicationApplyTests
{
    [Test]
    public async Task ApplyManyAtomicAsync_commits_all_entries_when_every_step_succeeds()
    {
        const string tree = "rapply-atomic-commit";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        var entries = new[]
        {
            new AtomicApplyEntry
            {
                Key = "a",
                Value = new byte[] { 1 },
                Timestamp = Hlc(1_000),
                ExpiresAtTicks = 0,
                VectorClock = null,
                IsTombstone = false,
            },
            new AtomicApplyEntry
            {
                Key = "b",
                Value = new byte[] { 2 },
                Timestamp = Hlc(1_001),
                ExpiresAtTicks = 0,
                VectorClock = null,
                IsTombstone = false,
            },
        };

        var result = await apply.ApplyManyAtomicAsync(
            entries,
            transactionId: Guid.NewGuid(),
            originClusterId: "site-x",
            sourceVectorClock: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));
            Assert.That(result.AppliedCount, Is.EqualTo(2));
            Assert.That(result.FailureReason, Is.Null);
            Assert.That(lattice.GetAsync("a").Result, Is.EqualTo(new byte[] { 1 }));
            Assert.That(lattice.GetAsync("b").Result, Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task ApplyManyAtomicAsync_preserves_source_hlc_on_each_entry()
    {
        const string tree = "rapply-atomic-hlc";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        var hlcA = Hlc(42_000, 3);
        var hlcB = Hlc(42_001, 0);

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
            originClusterId: "site-x",
            sourceVectorClock: null);

        Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));

        var versA = await lattice.GetWithVersionAsync("a");
        var versB = await lattice.GetWithVersionAsync("b");
        Assert.Multiple(() =>
        {
            Assert.That(versA.Version, Is.EqualTo(hlcA));
            Assert.That(versB.Version, Is.EqualTo(hlcB));
        });
    }

    [Test]
    public async Task ApplyManyAtomicAsync_preserves_per_entry_vector_clock_and_origin()
    {
        const string tree = "rapply-atomic-vc";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        var vcA = new VersionVector();
        vcA.Tick("site-x");
        vcA.Tick("site-y");

        var vcB = new VersionVector();
        vcB.Tick("site-x");
        vcB.Tick("site-x");

        var result = await apply.ApplyManyAtomicAsync(
            new[]
            {
                new AtomicApplyEntry
                {
                    Key = "a", Value = new byte[] { 1 }, Timestamp = Hlc(2_000),
                    ExpiresAtTicks = 0, VectorClock = vcA, IsTombstone = false,
                },
                new AtomicApplyEntry
                {
                    Key = "b", Value = new byte[] { 2 }, Timestamp = Hlc(2_001),
                    ExpiresAtTicks = 0, VectorClock = vcB, IsTombstone = false,
                },
            },
            transactionId: Guid.NewGuid(),
            originClusterId: "site-x",
            sourceVectorClock: null);

        Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));

        var rawA = await ReadRawEntryAsync(tree, "a");
        var rawB = await ReadRawEntryAsync(tree, "b");
        Assert.Multiple(() =>
        {
            Assert.That(rawA.HasValue, Is.True);
            Assert.That(rawA!.Value.OriginClusterId, Is.EqualTo("site-x"));
            Assert.That(rawA.Value.VectorClock, Is.Not.Null);
            Assert.That(rawA.Value.VectorClock!.GetClock("site-x"), Is.EqualTo(vcA.GetClock("site-x")));
            Assert.That(rawA.Value.VectorClock!.GetClock("site-y"), Is.EqualTo(vcA.GetClock("site-y")));

            Assert.That(rawB.HasValue, Is.True);
            Assert.That(rawB!.Value.OriginClusterId, Is.EqualTo("site-x"));
            Assert.That(rawB.Value.VectorClock!.GetClock("site-x"), Is.EqualTo(vcB.GetClock("site-x")));
        });
    }

    [Test]
    public async Task ApplyManyAtomicAsync_tombstone_entries_remove_seeded_values()
    {
        const string tree = "rapply-atomic-tombstone";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        // Seed a value the tombstone will overwrite.
        await lattice.SetAsync("k", new byte[] { 9 });
        var seed = await lattice.GetWithVersionAsync("k");
        var tombstoneHlc = seed.Version with { WallClockTicks = seed.Version.WallClockTicks + 5_000 };

        var result = await apply.ApplyManyAtomicAsync(
            new[]
            {
                new AtomicApplyEntry
                {
                    Key = "k", Value = null, Timestamp = tombstoneHlc,
                    ExpiresAtTicks = 0, VectorClock = null, IsTombstone = true,
                },
                new AtomicApplyEntry
                {
                    Key = "fresh", Value = new byte[] { 7 }, Timestamp = Hlc(3_000),
                    ExpiresAtTicks = 0, VectorClock = null, IsTombstone = false,
                },
            },
            transactionId: Guid.NewGuid(),
            originClusterId: "site-x",
            sourceVectorClock: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));
            Assert.That(result.AppliedCount, Is.EqualTo(2));
            Assert.That(lattice.GetAsync("k").Result, Is.Null);
            Assert.That(lattice.GetAsync("fresh").Result, Is.EqualTo(new byte[] { 7 }));
        });
    }

    [Test]
    public async Task ApplyManyAtomicAsync_idempotent_retry_returns_same_outcome_without_duplicate_writes()
    {
        const string tree = "rapply-atomic-idempotent";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);
        var lattice = _fixture.Cluster.Client.GetGrain<ILattice>(tree);

        var transactionId = Guid.NewGuid();
        var entries = new[]
        {
            new AtomicApplyEntry
            {
                Key = "x", Value = new byte[] { 11 }, Timestamp = Hlc(4_000),
                ExpiresAtTicks = 0, VectorClock = null, IsTombstone = false,
            },
        };

        var first = await apply.ApplyManyAtomicAsync(entries, transactionId, "site-x", sourceVectorClock: null);
        var second = await apply.ApplyManyAtomicAsync(entries, transactionId, "site-x", sourceVectorClock: null);

        Assert.Multiple(() =>
        {
            Assert.That(first.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));
            Assert.That(second.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));
            Assert.That(second.AppliedCount, Is.EqualTo(first.AppliedCount));
            Assert.That(lattice.GetAsync("x").Result, Is.EqualTo(new byte[] { 11 }));
        });
    }

    [Test]
    public async Task ApplyManyAtomicAsync_empty_batch_returns_committed_with_zero_count()
    {
        const string tree = "rapply-atomic-empty";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        var result = await apply.ApplyManyAtomicAsync(
            Array.Empty<AtomicApplyEntry>(),
            transactionId: Guid.NewGuid(),
            originClusterId: "site-x",
            sourceVectorClock: null);

        Assert.Multiple(() =>
        {
            Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));
            Assert.That(result.AppliedCount, Is.EqualTo(0));
            Assert.That(result.FailureReason, Is.Null);
        });
    }

    [Test]
    public void ApplyManyAtomicAsync_empty_origin_cluster_id_throws()
    {
        const string tree = "rapply-atomic-empty-origin";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        Assert.That(
            async () => await apply.ApplyManyAtomicAsync(
                new[]
                {
                    new AtomicApplyEntry
                    {
                        Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(1),
                        ExpiresAtTicks = 0, VectorClock = null, IsTombstone = false,
                    },
                },
                transactionId: Guid.NewGuid(),
                originClusterId: "",
                sourceVectorClock: null),
            Throws.ArgumentException);
    }

    [Test]
    public void ApplyManyAtomicAsync_empty_transaction_id_throws()
    {
        const string tree = "rapply-atomic-empty-tx";
        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(tree);

        Assert.That(
            async () => await apply.ApplyManyAtomicAsync(
                new[]
                {
                    new AtomicApplyEntry
                    {
                        Key = "k", Value = new byte[] { 1 }, Timestamp = Hlc(1),
                        ExpiresAtTicks = 0, VectorClock = null, IsTombstone = false,
                    },
                },
                transactionId: Guid.Empty,
                originClusterId: "site-x",
                sourceVectorClock: null),
            Throws.ArgumentException);
    }
}
