using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the <see cref="IMutationObserver"/> hook invoked from the
/// single-key write paths inside <see cref="BPlusLeafGrain"/>.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static BPlusLeafGrain CreateGrainWithObserver(
        RecordingMutationObserver observer,
        FakePersistentState<LeafNodeState>? state = null,
        string treeId = "tree-x")
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("leaf", "replica-a"));
        state ??= new FakePersistentState<LeafNodeState>();
        var factory = Substitute.For<IGrainFactory>();
        var resolver = TestOptionsResolver.Create(
            maxLeafKeys: 128,
            shardCount: 1,
            factory: factory);
        var grain = new BPlusLeafGrain(context, state, factory, resolver, TestMutationObservers.With(observer));
        // Set the tree id so the observer sees a stable value instead of an empty string.
        grain.SetTreeIdAsync(treeId).GetAwaiter().GetResult();
        return grain;
    }

    [Test]
    public async Task SetAsync_publishes_single_set_mutation_with_value_and_timestamp()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer, treeId: "tree-set");

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));

        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        var m = observer.Mutations[0];
        Assert.That(m.Kind, Is.EqualTo(MutationKind.Set));
        Assert.That(m.TreeId, Is.EqualTo("tree-set"));
        Assert.That(m.Key, Is.EqualTo("k1"));
        Assert.That(m.IsTombstone, Is.False);
        Assert.That(m.Value, Is.EqualTo(Encoding.UTF8.GetBytes("v1")));
        Assert.That(m.ExpiresAtTicks, Is.Zero);
        Assert.That(m.Timestamp, Is.Not.EqualTo(default(Orleans.Lattice.Primitives.HybridLogicalClock)));
    }

    [Test]
    public async Task SetAsync_with_ttl_publishes_mutation_carrying_expires_at_ticks()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer);

        var expiresAt = DateTimeOffset.UtcNow.AddMinutes(5).UtcTicks;
        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"), expiresAt);

        Assert.That(observer.Mutations, Has.Count.EqualTo(1));
        Assert.That(observer.Mutations[0].ExpiresAtTicks, Is.EqualTo(expiresAt));
    }

    [Test]
    public async Task DeleteAsync_publishes_delete_mutation_when_key_present()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer, treeId: "tree-del");

        await grain.SetAsync("k1", Encoding.UTF8.GetBytes("v1"));
        observer.Mutations.ToList().Clear(); // irrelevant — Mutations returns a copy

        // Capture mutation count after the set, then delete.
        var countAfterSet = observer.Mutations.Count;
        await grain.DeleteAsync("k1");

        Assert.That(observer.Mutations, Has.Count.EqualTo(countAfterSet + 1));
        var m = observer.Mutations[^1];
        Assert.That(m.Kind, Is.EqualTo(MutationKind.Delete));
        Assert.That(m.Key, Is.EqualTo("k1"));
        Assert.That(m.IsTombstone, Is.True);
        Assert.That(m.Value, Is.Null);
    }

    [Test]
    public async Task DeleteAsync_does_not_publish_when_key_absent()
    {
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer);

        await grain.DeleteAsync("missing");

        Assert.That(observer.Mutations, Is.Empty);
    }

    [Test]
    public async Task DeleteRangeAsync_on_leaf_does_not_publish_per_key_events()
    {
        // Range deletes are emitted at the shard level (single DeleteRange
        // event) rather than the leaf (which would produce one per key).
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer);

        await grain.SetAsync("a", [1]);
        await grain.SetAsync("b", [2]);
        await grain.SetAsync("c", [3]);
        var mutationsBeforeRange = observer.Mutations.Count;

        await grain.DeleteRangeAsync("a", "z");

        Assert.That(observer.Mutations.Count, Is.EqualTo(mutationsBeforeRange));
    }

    [Test]
    public async Task MergeEntriesAsync_does_not_publish_mutations()
    {
        // Merge paths are internal coordination (split drain, replication
        // apply) — the replication package's observer intercepts originating
        // writes, not re-applications.
        var observer = new RecordingMutationObserver();
        var grain = CreateGrainWithObserver(observer);

        var entries = new Dictionary<string, Orleans.Lattice.Primitives.LwwValue<byte[]>>
        {
            ["k"] = Orleans.Lattice.Primitives.LwwValue<byte[]>.Create([1],
                Orleans.Lattice.Primitives.HybridLogicalClock.Tick(default)),
        };
        await grain.MergeEntriesAsync(entries);

        Assert.That(observer.Mutations, Is.Empty);
    }

    [Test]
    public async Task SetAsync_with_no_observers_registered_is_zero_cost_and_succeeds()
    {
        // Uses the default CreateGrain helper which wires TestMutationObservers.NoObservers.
        var grain = CreateGrain();
        var result = await grain.SetAsync("k", [1]);
        Assert.That(result, Is.Null);
    }
}
