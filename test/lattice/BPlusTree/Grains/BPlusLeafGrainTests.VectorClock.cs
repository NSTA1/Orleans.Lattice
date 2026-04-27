using System.Text;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests covering grain-side stamping of
/// <see cref="LwwValue{T}.VectorClock"/> from the ambient
/// <see cref="LatticeVectorClockContext"/> on every authoring write path.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearVectorClockContext()
    {
        // Every test on this logical thread must start with a clean slate so
        // ambient context from a previous test cannot leak into assertions.
        LatticeVectorClockContext.Current = null;
    }

    private static VersionVector NewVc(string replicaId)
    {
        var vc = new VersionVector();
        vc.Tick(replicaId);
        return vc;
    }

    [Test]
    public async Task SetAsync_stamps_VectorClock_from_ambient_context_onto_persisted_entry()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);
        var vc = NewVc("east");

        using (LatticeVectorClockContext.With(vc))
        {
            await grain.SetAsync("k", [1]);
        }

        Assert.That(state.State.Entries["k"].VectorClock, Is.SameAs(vc));
    }

    [Test]
    public async Task SetAsync_stamps_null_vector_clock_when_context_unset()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);

        await grain.SetAsync("k", [1]);

        Assert.That(state.State.Entries["k"].VectorClock, Is.Null);
    }

    [Test]
    public async Task SetAsync_with_ttl_stamps_VectorClock()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);
        var expiresAt = DateTimeOffset.UtcNow.AddMinutes(5).UtcTicks;
        var vc = NewVc("west");

        using (LatticeVectorClockContext.With(vc))
        {
            await grain.SetAsync("k", [1], expiresAt);
        }

        var entry = state.State.Entries["k"];
        Assert.That(entry.VectorClock, Is.SameAs(vc));
        Assert.That(entry.ExpiresAtTicks, Is.EqualTo(expiresAt));
    }

    [Test]
    public async Task DeleteAsync_stamps_VectorClock_on_tombstone()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);
        await grain.SetAsync("k", [1]);
        var vc = NewVc("peer-a");

        using (LatticeVectorClockContext.With(vc))
        {
            await grain.DeleteAsync("k");
        }

        var tomb = state.State.Entries["k"];
        Assert.That(tomb.IsTombstone, Is.True);
        Assert.That(tomb.VectorClock, Is.SameAs(vc));
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_VectorClock_on_all_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);
        await grain.SetAsync("a1", [1]);
        await grain.SetAsync("a2", [2]);
        await grain.SetAsync("a3", [3]);
        var vc = NewVc("peer-b");

        using (LatticeVectorClockContext.With(vc))
        {
            await grain.DeleteRangeAsync("a", "b");
        }

        foreach (var k in new[] { "a1", "a2", "a3" })
        {
            Assert.That(state.State.Entries[k].IsTombstone, Is.True, k);
            Assert.That(state.State.Entries[k].VectorClock, Is.SameAs(vc), k);
        }
    }

    [Test]
    public async Task SetAsync_publishes_LatticeMutation_carrying_VectorClock()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer, treeId: "tree-vc");
        var vc = NewVc("cluster-e2e");

        using (LatticeVectorClockContext.With(vc))
        {
            await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        }

        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(observer.Mutations[0].VectorClock, Is.SameAs(vc));
    }

    [Test]
    public async Task DeleteAsync_publishes_tombstone_LatticeMutation_carrying_VectorClock()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer, treeId: "tree-vc");

        await grain.SetAsync("k", [1]);
        var before = observer.Mutations.Count;
        var vc = NewVc("peer-c");

        using (LatticeVectorClockContext.With(vc))
        {
            await grain.DeleteAsync("k");
        }

        Assert.That(observer.Mutations, Has.Count.EqualTo(before + 1));
        Assert.That(observer.Mutations[^1].Kind, Is.EqualTo(MutationKind.Delete));
        Assert.That(observer.Mutations[^1].VectorClock, Is.SameAs(vc));
    }

    [Test]
    public async Task MergeEntriesAsync_preserves_VectorClock_of_winning_value()
    {
        // Merge paths operate on LwwValue<byte[]> directly, so VectorClock
        // must flow through verbatim with no re-stamping or context lookup.
        // This is the zero-cost passthrough invariant — replication
        // shadow-forward, snapshot/restore drain, and continuous-merge all
        // depend on it.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);
        var vc = NewVc("incoming-peer");

        var hlc = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };
        var incoming = LwwValue<byte[]>.Create([42], hlc) with { VectorClock = vc };

        await grain.MergeEntriesAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = incoming,
        });

        var stored = state.State.Entries["k"];
        Assert.That(stored.VectorClock, Is.SameAs(vc),
            "merge must not strip or re-capture the incoming VectorClock");
        Assert.That(stored.Value, Is.EqualTo(new byte[] { 42 }));
    }

    [Test]
    public async Task MergeManyAsync_preserves_VectorClock_of_winning_value()
    {
        // Mirrors MergeEntriesAsync but exercises the bulk-write entry point
        // used by ShardRootGrain.MergeManyAsync's leaf-grouped fan-out.
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);
        var vc = NewVc("incoming-peer-bulk");

        var hlc = new HybridLogicalClock { WallClockTicks = DateTimeOffset.UtcNow.UtcTicks, Counter = 0 };
        var incoming = LwwValue<byte[]>.Create([7, 8], hlc) with { VectorClock = vc };

        await grain.MergeManyAsync(new Dictionary<string, LwwValue<byte[]>>
        {
            ["k"] = incoming,
        });

        var stored = state.State.Entries["k"];
        Assert.That(stored.VectorClock, Is.SameAs(vc));
        Assert.That(stored.Value, Is.EqualTo(new byte[] { 7, 8 }));
    }
}
