using System.Text;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration tests asserting that the typed CRDT accessors
/// (<see cref="OrSetAccessor"/>, <see cref="PnCounterAccessor"/>,
/// <see cref="VersionVectorAccessor"/>) stamp the pre-merge author's
/// delta onto every committed mutation via
/// <see cref="LatticeDeltaContext"/>, so observers see
/// <see cref="LatticeMutation.DeltaKind"/> /
/// <see cref="LatticeMutation.DeltaPayload"/> populated.
/// </summary>
public sealed partial class MutationObserverIntegrationTests
{
    [Test]
    public async Task OrSetAccessor_AddAsync_stamps_or_set_add_delta_on_mutation()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-ors-add");
        var element = Encoding.UTF8.GetBytes("alice");

        await tree.OrSet("members").AddAsync(element, replicaId: "r1");

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "members"
            && m.TreeId == "obs-e2e-crdt-ors-add"
            && m.DeltaKind == CrdtDeltaKinds.OrSetAdd);

        Assert.That(m.DeltaPayload, Is.Not.Null);
        var delta = JsonLatticeSerializer<CrdtDeltaPayloads.OrSetAddDelta>.Default
            .Deserialize(m.DeltaPayload!);
        Assert.That(delta.Element, Is.EqualTo(element));
        Assert.That(delta.ReplicaId, Is.EqualTo("r1"));
        Assert.That(delta.Counter, Is.EqualTo(1));
    }

    [Test]
    public async Task OrSetAccessor_RemoveAsync_stamps_or_set_remove_delta_with_observed_dots()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-ors-rm");
        var element = Encoding.UTF8.GetBytes("bob");
        await tree.OrSet("members").AddAsync(element, replicaId: "r1");
        MutationObserverClusterFixture.Drain();

        await tree.OrSet("members").RemoveAsync(element);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "members"
            && m.TreeId == "obs-e2e-crdt-ors-rm"
            && m.DeltaKind == CrdtDeltaKinds.OrSetRemove);

        Assert.That(m.DeltaPayload, Is.Not.Null);
        var delta = JsonLatticeSerializer<CrdtDeltaPayloads.OrSetRemoveDelta>.Default
            .Deserialize(m.DeltaPayload!);
        Assert.That(delta.Element, Is.EqualTo(element));
        Assert.That(delta.ObservedDots, Has.Length.EqualTo(1));
        Assert.That(delta.ObservedDots[0].ReplicaId, Is.EqualTo("r1"));
        Assert.That(delta.ObservedDots[0].Counter, Is.EqualTo(1));
    }

    [Test]
    public async Task OrSetAccessor_RemoveAsync_stamps_empty_observed_dots_when_element_absent()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-ors-rm-absent");
        var element = Encoding.UTF8.GetBytes("ghost");

        await tree.OrSet("members").RemoveAsync(element);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "members"
            && m.TreeId == "obs-e2e-crdt-ors-rm-absent"
            && m.DeltaKind == CrdtDeltaKinds.OrSetRemove);

        Assert.That(m.DeltaPayload, Is.Not.Null);
        var delta = JsonLatticeSerializer<CrdtDeltaPayloads.OrSetRemoveDelta>.Default
            .Deserialize(m.DeltaPayload!);
        Assert.That(delta.Element, Is.EqualTo(element));
        Assert.That(delta.ObservedDots, Is.Empty);
    }

    [Test]
    public async Task OrSetAccessor_MergeAsync_stamps_or_set_merge_delta_with_other_state()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-ors-mrg");
        var other = new OrSet();
        other.Add(Encoding.UTF8.GetBytes("x"), "r2", 7);

        await tree.OrSet("members").MergeAsync(other);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "members"
            && m.TreeId == "obs-e2e-crdt-ors-mrg"
            && m.DeltaKind == CrdtDeltaKinds.OrSetMerge);

        Assert.That(m.DeltaPayload, Is.Not.Null);
        var delta = JsonLatticeSerializer<CrdtDeltaPayloads.OrSetMergeDelta>.Default
            .Deserialize(m.DeltaPayload!);
        Assert.That(delta.Adds, Has.Count.EqualTo(1));
        var key = Convert.ToBase64String(Encoding.UTF8.GetBytes("x"));
        Assert.That(delta.Adds.ContainsKey(key), Is.True);
        Assert.That(delta.Adds[key][0].ReplicaId, Is.EqualTo("r2"));
        Assert.That(delta.Adds[key][0].Counter, Is.EqualTo(7));
    }

    [Test]
    public async Task PnCounterAccessor_IncrementAsync_stamps_pn_counter_increment_delta()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-pnc-inc");

        await tree.PnCounter("hits").IncrementAsync("r1", 5);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "hits"
            && m.TreeId == "obs-e2e-crdt-pnc-inc"
            && m.DeltaKind == CrdtDeltaKinds.PnCounterIncrement);

        Assert.That(m.DeltaPayload, Is.Not.Null);
        var delta = JsonLatticeSerializer<CrdtDeltaPayloads.PnCounterIncrementDelta>.Default
            .Deserialize(m.DeltaPayload!);
        Assert.That(delta.ReplicaId, Is.EqualTo("r1"));
        Assert.That(delta.Amount, Is.EqualTo(5));
    }

    [Test]
    public async Task PnCounterAccessor_DecrementAsync_stamps_pn_counter_decrement_delta()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-pnc-dec");

        await tree.PnCounter("hits").DecrementAsync("r2", 3);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "hits"
            && m.TreeId == "obs-e2e-crdt-pnc-dec"
            && m.DeltaKind == CrdtDeltaKinds.PnCounterDecrement);

        Assert.That(m.DeltaPayload, Is.Not.Null);
        var delta = JsonLatticeSerializer<CrdtDeltaPayloads.PnCounterDecrementDelta>.Default
            .Deserialize(m.DeltaPayload!);
        Assert.That(delta.ReplicaId, Is.EqualTo("r2"));
        Assert.That(delta.Amount, Is.EqualTo(3));
    }

    [Test]
    public async Task PnCounterAccessor_MergeAsync_stamps_pn_counter_merge_delta_with_other_state()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-pnc-mrg");
        var other = new PnCounter();
        other.Increment("r1", 4);
        other.Decrement("r2", 1);

        await tree.PnCounter("hits").MergeAsync(other);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "hits"
            && m.TreeId == "obs-e2e-crdt-pnc-mrg"
            && m.DeltaKind == CrdtDeltaKinds.PnCounterMerge);

        Assert.That(m.DeltaPayload, Is.Not.Null);
        var delta = JsonLatticeSerializer<CrdtDeltaPayloads.PnCounterMergeDelta>.Default
            .Deserialize(m.DeltaPayload!);
        Assert.That(delta.Increments["r1"], Is.EqualTo(4));
        Assert.That(delta.Decrements["r2"], Is.EqualTo(1));
    }

    [Test]
    public async Task VersionVectorAccessor_TickAsync_stamps_version_vector_tick_delta()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-vvc-tick");

        await tree.VersionVector("vec").TickAsync("r1");

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "vec"
            && m.TreeId == "obs-e2e-crdt-vvc-tick"
            && m.DeltaKind == CrdtDeltaKinds.VersionVectorTick);

        Assert.That(m.DeltaPayload, Is.Not.Null);
        var delta = JsonLatticeSerializer<CrdtDeltaPayloads.VersionVectorTickDelta>.Default
            .Deserialize(m.DeltaPayload!);
        Assert.That(delta.ReplicaId, Is.EqualTo("r1"));
        Assert.That(delta.WallClockTicks, Is.GreaterThan(0));
    }

    [Test]
    public async Task VersionVectorAccessor_MergeAsync_stamps_version_vector_merge_delta_with_other_state()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-vvc-mrg");
        var other = new VersionVector();
        other.Tick("r1");
        other.Tick("r2");

        await tree.VersionVector("vec").MergeAsync(other);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "vec"
            && m.TreeId == "obs-e2e-crdt-vvc-mrg"
            && m.DeltaKind == CrdtDeltaKinds.VersionVectorMerge);

        Assert.That(m.DeltaPayload, Is.Not.Null);
        var delta = JsonLatticeSerializer<CrdtDeltaPayloads.VersionVectorMergeDelta>.Default
            .Deserialize(m.DeltaPayload!);
        Assert.That(delta.Entries, Has.Count.EqualTo(2));
        Assert.That(delta.Entries.ContainsKey("r1"), Is.True);
        Assert.That(delta.Entries.ContainsKey("r2"), Is.True);
    }

    [Test]
    public async Task Plain_SetAsync_leaves_delta_slots_null_when_no_producer_stamps_context()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-plain-set");

        await tree.SetAsync("k", [1]);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "k"
            && m.TreeId == "obs-e2e-crdt-plain-set");

        Assert.That(m.DeltaKind, Is.Null);
        Assert.That(m.DeltaPayload, Is.Null);
    }
}
