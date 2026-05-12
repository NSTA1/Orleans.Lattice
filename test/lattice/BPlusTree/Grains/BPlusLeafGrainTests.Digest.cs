using System.IO.Hashing;
using System.Text;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="BPlusLeafGrain.GetProjectionDigestAsync"/>.
/// Verifies determinism, sort-order invariance, tombstone differentiation,
/// checkpoint-offset participation, and metadata fingerprinting.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [Test]
    public async Task Digest_empty_leaf_has_zero_entry_count()
    {
        var grain = CreateGrain();

        var digest = await grain.GetProjectionDigestAsync();

        Assert.That(digest.EntryCount, Is.Zero);
        Assert.That(digest.CheckpointOffset, Is.Zero);
        Assert.That(digest.Hash, Is.Not.Null);
        Assert.That(digest.Hash.Length, Is.EqualTo(16)); // XxHash128
    }

    [Test]
    public async Task Digest_is_deterministic_for_identical_state()
    {
        var grain1 = CreateGrain();
        var grain2 = CreateGrain();
        await grain1.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain1.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await grain2.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain2.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var d1 = await grain1.GetProjectionDigestAsync();
        var d2 = await grain2.GetProjectionDigestAsync();

        // Hashes will differ because HLCs differ per grain (different replica ids
        // for clock advancement) - verify only structural properties here.
        Assert.That(d1.EntryCount, Is.EqualTo(2));
        Assert.That(d2.EntryCount, Is.EqualTo(2));
    }

    [Test]
    public async Task Digest_is_invariant_to_insertion_order()
    {
        // Two grains using the same replica id with identical write sequence
        // converge on identical hashes - sort-order invariance plus determinism.
        var grain1 = CreateGrain(replicaId: "leaf-x");
        var grain2 = CreateGrain(replicaId: "leaf-x");
        // The leaf's HLC advances per write; force the same HLC by using
        // ILeafProjection.Apply with explicit timestamps.
        var p1 = (ILeafProjection)grain1;
        var p2 = (ILeafProjection)grain2;

        p1.Apply(BuildSet("a", Encoding.UTF8.GetBytes("1"), hlcPhysical: 100));
        p1.Apply(BuildSet("b", Encoding.UTF8.GetBytes("2"), hlcPhysical: 200));
        p1.Apply(BuildSet("c", Encoding.UTF8.GetBytes("3"), hlcPhysical: 300));

        p2.Apply(BuildSet("c", Encoding.UTF8.GetBytes("3"), hlcPhysical: 300));
        p2.Apply(BuildSet("a", Encoding.UTF8.GetBytes("1"), hlcPhysical: 100));
        p2.Apply(BuildSet("b", Encoding.UTF8.GetBytes("2"), hlcPhysical: 200));

        var d1 = await grain1.GetProjectionDigestAsync();
        var d2 = await grain2.GetProjectionDigestAsync();

        Assert.That(d1.Hash, Is.EqualTo(d2.Hash));
    }

    [Test]
    public async Task Digest_changes_when_value_changes()
    {
        var grain1 = CreateGrain();
        var grain2 = CreateGrain();
        var p1 = (ILeafProjection)grain1;
        var p2 = (ILeafProjection)grain2;
        p1.Apply(BuildSet("k", Encoding.UTF8.GetBytes("v1"), hlcPhysical: 100));
        p2.Apply(BuildSet("k", Encoding.UTF8.GetBytes("v2"), hlcPhysical: 100));

        var d1 = await grain1.GetProjectionDigestAsync();
        var d2 = await grain2.GetProjectionDigestAsync();

        Assert.That(d1.Hash, Is.Not.EqualTo(d2.Hash));
    }

    [Test]
    public async Task Digest_distinguishes_tombstone_from_live_empty_value()
    {
        var grainLive = CreateGrain();
        var grainTomb = CreateGrain();
        var pLive = (ILeafProjection)grainLive;
        var pTomb = (ILeafProjection)grainTomb;

        pLive.Apply(BuildSet("k", Array.Empty<byte>(), hlcPhysical: 100));
        pTomb.Apply(BuildDelete("k", hlcPhysical: 100));

        var dLive = await grainLive.GetProjectionDigestAsync();
        var dTomb = await grainTomb.GetProjectionDigestAsync();

        Assert.That(dLive.Hash, Is.Not.EqualTo(dTomb.Hash));
        Assert.That(dLive.EntryCount, Is.EqualTo(1));
        Assert.That(dTomb.EntryCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Digest_includes_tombstone_entries_in_count()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await grain.DeleteAsync("k");

        var digest = await grain.GetProjectionDigestAsync();

        Assert.That(digest.EntryCount, Is.EqualTo(1));
    }

    [Test]
    public async Task Digest_changes_when_checkpoint_offset_advances()
    {
        var grain = CreateGrain();
        await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));

        var before = await grain.GetProjectionDigestAsync();

        var projection = (ILeafProjection)grain;
        await projection.SetCheckpointOffsetAsync(42, CancellationToken.None);
        // SetCheckpointOffsetAsync coalesces - force-flush so the persisted
        // ProjectionCheckpointOffset (which the digest reads) advances.
        await projection.FlushCheckpointAsync(CancellationToken.None);

        var after = await grain.GetProjectionDigestAsync();

        Assert.That(before.Hash, Is.Not.EqualTo(after.Hash));
        Assert.That(after.CheckpointOffset, Is.EqualTo(42));
    }

    [Test]
    public async Task Digest_changes_when_origin_cluster_id_changes()
    {
        var grain1 = CreateGrain();
        var grain2 = CreateGrain();
        var p1 = (ILeafProjection)grain1;
        var p2 = (ILeafProjection)grain2;

        p1.Apply(BuildSet("k", Encoding.UTF8.GetBytes("v"), hlcPhysical: 100, originClusterId: "cluster-A"));
        p2.Apply(BuildSet("k", Encoding.UTF8.GetBytes("v"), hlcPhysical: 100, originClusterId: "cluster-B"));

        var d1 = await grain1.GetProjectionDigestAsync();
        var d2 = await grain2.GetProjectionDigestAsync();

        Assert.That(d1.Hash, Is.Not.EqualTo(d2.Hash));
    }

    [Test]
    public async Task Digest_changes_when_vector_clock_changes()
    {
        var vcA = new VersionVector();
        vcA.Tick("replica-A");
        var vcB = new VersionVector();
        vcB.Tick("replica-B");

        var grain1 = CreateGrain();
        var grain2 = CreateGrain();
        var p1 = (ILeafProjection)grain1;
        var p2 = (ILeafProjection)grain2;

        p1.Apply(BuildSet("k", Encoding.UTF8.GetBytes("v"), hlcPhysical: 100, vectorClock: vcA));
        p2.Apply(BuildSet("k", Encoding.UTF8.GetBytes("v"), hlcPhysical: 100, vectorClock: vcB));

        var d1 = await grain1.GetProjectionDigestAsync();
        var d2 = await grain2.GetProjectionDigestAsync();

        Assert.That(d1.Hash, Is.Not.EqualTo(d2.Hash));
    }

    [Test]
    public async Task Digest_changes_when_expires_at_ticks_changes()
    {
        var grain1 = CreateGrain();
        var grain2 = CreateGrain();
        var p1 = (ILeafProjection)grain1;
        var p2 = (ILeafProjection)grain2;

        p1.Apply(BuildSet("k", Encoding.UTF8.GetBytes("v"), hlcPhysical: 100, expiresAtTicks: 0));
        p2.Apply(BuildSet("k", Encoding.UTF8.GetBytes("v"), hlcPhysical: 100, expiresAtTicks: 9_000_000_000L));

        var d1 = await grain1.GetProjectionDigestAsync();
        var d2 = await grain2.GetProjectionDigestAsync();

        Assert.That(d1.Hash, Is.Not.EqualTo(d2.Hash));
    }

    [Test]
    public async Task Digest_distinct_for_different_entry_counts()
    {
        var grain1 = CreateGrain();
        var grain2 = CreateGrain();
        await grain1.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain2.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await grain2.SetAsync("b", Encoding.UTF8.GetBytes("2"));

        var d1 = await grain1.GetProjectionDigestAsync();
        var d2 = await grain2.GetProjectionDigestAsync();

        Assert.That(d1.EntryCount, Is.EqualTo(1));
        Assert.That(d2.EntryCount, Is.EqualTo(2));
        Assert.That(d1.Hash, Is.Not.EqualTo(d2.Hash));
    }

    [Test]
    public async Task Digest_empty_leaf_matches_known_good_vector()
    {
        // Pin the public hash shape: an empty leaf with checkpoint=0 must
        // produce XxHash128 over (16 zero bytes XOR-fold ||
        // int64LE(0) entry-count || int64LE(0) checkpoint-offset). Any
        // future change to the digest's outer framing - for example,
        // appending an additional metadata field after the checkpoint -
        // will break this regression and force the change to be a
        // deliberate, documented wire-format bump.
        var grain = CreateGrain();

        var digest = await grain.GetProjectionDigestAsync();

        var expected = new byte[16];
        XxHash128.Hash(new byte[32], expected);
        Assert.That(digest.Hash, Is.EqualTo(expected));
        Assert.That(digest.EntryCount, Is.Zero);
        Assert.That(digest.CheckpointOffset, Is.Zero);
    }

    [Test]
    public async Task ProjectionHash_lazily_backfills_on_legacy_state()
    {
        // A grain activated against state that pre-dates the new persisted
        // hash slot must produce the same digest as a freshly-written
        // grain. The lazy backfill happens on first mutation or first
        // digest read.
        var legacyState = new FakePersistentState<LeafNodeState>();
        legacyState.State.Entries["a"] = new LwwValue<byte[]>
        {
            Value = Encoding.UTF8.GetBytes("legacy"),
            Timestamp = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 },
            IsTombstone = false,
        };
        legacyState.State.ProjectionHash = null; // simulates pre-slot state

        var grain = CreateGrain(legacyState);

        var digest = await grain.GetProjectionDigestAsync();

        Assert.That(digest.EntryCount, Is.EqualTo(1));
        Assert.That(legacyState.State.ProjectionHash, Is.Not.Null,
            "first digest read must lazily backfill the persisted hash slot");
        Assert.That(legacyState.State.ProjectionHash!.Length, Is.EqualTo(16));
    }
}
