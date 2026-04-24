using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests covering grain-side stamping of
/// <see cref="LwwValue{T}.OriginClusterId"/> from the ambient
/// <see cref="LatticeOriginContext"/> on every authoring write path.
/// </summary>
public partial class BPlusLeafGrainTests
{
    [TearDown]
    public void ClearOriginContext()
    {
        // Every test on this logical thread must start with a clean slate so
        // ambient context from a previous test cannot leak into assertions.
        LatticeOriginContext.Current = null;
    }

    [Test]
    public async Task SetAsync_stamps_OriginClusterId_from_ambient_context_onto_persisted_entry()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);

        using (LatticeOriginContext.With("cluster-east"))
        {
            await grain.SetAsync("k", [1]);
        }

        Assert.That(state.State.Entries["k"].OriginClusterId, Is.EqualTo("cluster-east"));
    }

    [Test]
    public async Task SetAsync_stamps_null_origin_when_context_unset()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);

        await grain.SetAsync("k", [1]);

        Assert.That(state.State.Entries["k"].OriginClusterId, Is.Null);
    }

    [Test]
    public async Task SetAsync_with_ttl_stamps_OriginClusterId()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);
        var expiresAt = DateTimeOffset.UtcNow.AddMinutes(5).UtcTicks;

        using (LatticeOriginContext.With("cluster-west"))
        {
            await grain.SetAsync("k", [1], expiresAt);
        }

        var entry = state.State.Entries["k"];
        Assert.That(entry.OriginClusterId, Is.EqualTo("cluster-west"));
        Assert.That(entry.ExpiresAtTicks, Is.EqualTo(expiresAt));
    }

    [Test]
    public async Task DeleteAsync_stamps_OriginClusterId_on_tombstone()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);
        await grain.SetAsync("k", [1]);

        using (LatticeOriginContext.With("peer-a"))
        {
            await grain.DeleteAsync("k");
        }

        var tomb = state.State.Entries["k"];
        Assert.That(tomb.IsTombstone, Is.True);
        Assert.That(tomb.OriginClusterId, Is.EqualTo("peer-a"));
    }

    [Test]
    public async Task DeleteRangeAsync_stamps_OriginClusterId_on_all_tombstones()
    {
        var state = new FakePersistentState<LeafNodeState>();
        var grain = CreateGrain(state: state);
        await grain.SetAsync("a1", [1]);
        await grain.SetAsync("a2", [2]);
        await grain.SetAsync("a3", [3]);

        using (LatticeOriginContext.With("peer-b"))
        {
            await grain.DeleteRangeAsync("a", "b");
        }

        foreach (var k in new[] { "a1", "a2", "a3" })
        {
            Assert.That(state.State.Entries[k].IsTombstone, Is.True, k);
            Assert.That(state.State.Entries[k].OriginClusterId, Is.EqualTo("peer-b"), k);
        }
    }

    [Test]
    public async Task SetAsync_publishes_LatticeMutation_carrying_OriginClusterId()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer, treeId: "tree-origin");

        using (LatticeOriginContext.With("cluster-e2e"))
        {
            await grain.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        }

        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(observer.Mutations[0].OriginClusterId, Is.EqualTo("cluster-e2e"));
    }

    [Test]
    public async Task DeleteAsync_publishes_tombstone_LatticeMutation_carrying_OriginClusterId()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer, treeId: "tree-origin");

        // Set without origin so the Delete is the only mutation we care about.
        await grain.SetAsync("k", [1]);
        var before = observer.Mutations.Count;

        using (LatticeOriginContext.With("peer-c"))
        {
            await grain.DeleteAsync("k");
        }

        Assert.That(observer.Mutations, Has.Count.EqualTo(before + 1));
        Assert.That(observer.Mutations[^1].Kind, Is.EqualTo(MutationKind.Delete));
        Assert.That(observer.Mutations[^1].OriginClusterId, Is.EqualTo("peer-c"));
    }
}
