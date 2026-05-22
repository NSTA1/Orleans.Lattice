using System.Text;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration tests asserting that the typed CRDT accessors
/// (<see cref="OrSetAccessor"/>, <see cref="PnCounterAccessor"/>,
/// <see cref="VersionVectorAccessor"/>) stamp the pre-merge author's
/// delta onto every committed mutation via <see cref="LatticeDeltaContext"/>,
/// so observers see <see cref="LatticeMutation.Delta"/> populated with
/// the Orleans-serialised public typed-delta DTO.
/// </summary>
public sealed partial class MutationObserverIntegrationTests
{
    [Test]
    public async Task OrSetAccessor_AddAsync_stamps_or_set_delta_carrying_added_dot()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-ors-add");
        var element = Encoding.UTF8.GetBytes("alice");

        await tree.OrSet("members").AddAsync(element, replicaId: "r1");

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "members"
            && m.TreeId == "obs-e2e-crdt-ors-add"
            && m.Delta is not null);

        var delta = JsonLatticeSerializer<OrSetDelta>.Default.Deserialize(m.Delta!);
        Assert.That(delta.Removes, Is.Empty);
        Assert.That(delta.Adds, Has.Count.EqualTo(1));
        var dot = delta.Adds[0];
        Assert.That(dot.Element, Is.EqualTo(element));
        Assert.That(dot.ReplicaId, Is.EqualTo("r1"));
        Assert.That(dot.Counter, Is.EqualTo(1));
    }

    [Test]
    public async Task OrSetAccessor_RemoveAsync_stamps_or_set_delta_carrying_observed_dots()
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
            && m.Delta is not null
            && JsonLatticeSerializer<OrSetDelta>.Default.Deserialize(m.Delta!).Removes.Count > 0);

        var delta = JsonLatticeSerializer<OrSetDelta>.Default.Deserialize(m.Delta!);
        Assert.That(delta.Adds, Is.Empty);
        Assert.That(delta.Removes, Has.Count.EqualTo(1));
        Assert.That(delta.Removes[0].ReplicaId, Is.EqualTo("r1"));
        Assert.That(delta.Removes[0].Counter, Is.EqualTo(1));
        Assert.That(delta.Removes[0].Element, Is.EqualTo(element));
    }

    [Test]
    public async Task OrSetAccessor_RemoveAsync_stamps_empty_delta_when_element_absent()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-ors-rm-absent");
        var element = Encoding.UTF8.GetBytes("ghost");

        await tree.OrSet("members").RemoveAsync(element);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "members"
            && m.TreeId == "obs-e2e-crdt-ors-rm-absent"
            && m.Delta is not null);

        var delta = JsonLatticeSerializer<OrSetDelta>.Default.Deserialize(m.Delta!);
        Assert.That(delta.Adds, Is.Empty);
        Assert.That(delta.Removes, Is.Empty);
    }

    [Test]
    public async Task OrSetAccessor_MergeAsync_stamps_or_set_delta_carrying_other_state()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-ors-mrg");
        var other = new OrSet();
        other.Add(Encoding.UTF8.GetBytes("x"), "r2", 7);

        await tree.OrSet("members").MergeAsync(other);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "members"
            && m.TreeId == "obs-e2e-crdt-ors-mrg"
            && m.Delta is not null
            && JsonLatticeSerializer<OrSetDelta>.Default.Deserialize(m.Delta!).Adds.Count > 0);

        var delta = JsonLatticeSerializer<OrSetDelta>.Default.Deserialize(m.Delta!);
        Assert.That(delta.Adds, Has.Count.EqualTo(1));
        Assert.That(delta.Adds[0].ReplicaId, Is.EqualTo("r2"));
        Assert.That(delta.Adds[0].Counter, Is.EqualTo(7));
        Assert.That(delta.Adds[0].Element, Is.EqualTo(Encoding.UTF8.GetBytes("x")));
    }

    [Test]
    public async Task PnCounterAccessor_IncrementAsync_stamps_pn_counter_delta_with_replica_total()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-pnc-inc");

        await tree.PnCounter("hits").IncrementAsync("r1", 5);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "hits"
            && m.TreeId == "obs-e2e-crdt-pnc-inc"
            && m.Delta is not null);

        var delta = JsonLatticeSerializer<PnCounterDelta>.Default.Deserialize(m.Delta!);
        Assert.That(delta.Decrements, Is.Empty);
        Assert.That(delta.Increments["r1"], Is.EqualTo(5));
    }

    [Test]
    public async Task PnCounterAccessor_DecrementAsync_stamps_pn_counter_delta_with_replica_total()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-pnc-dec");

        await tree.PnCounter("hits").DecrementAsync("r2", 3);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "hits"
            && m.TreeId == "obs-e2e-crdt-pnc-dec"
            && m.Delta is not null);

        var delta = JsonLatticeSerializer<PnCounterDelta>.Default.Deserialize(m.Delta!);
        Assert.That(delta.Increments, Is.Empty);
        Assert.That(delta.Decrements["r2"], Is.EqualTo(3));
    }

    [Test]
    public async Task PnCounterAccessor_MergeAsync_stamps_pn_counter_delta_with_other_state()
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
            && m.Delta is not null
            && JsonLatticeSerializer<PnCounterDelta>.Default.Deserialize(m.Delta!).Increments.Count > 0);

        var delta = JsonLatticeSerializer<PnCounterDelta>.Default.Deserialize(m.Delta!);
        Assert.That(delta.Increments["r1"], Is.EqualTo(4));
        Assert.That(delta.Decrements["r2"], Is.EqualTo(1));
    }

    [Test]
    public async Task VersionVectorAccessor_TickAsync_stamps_version_vector_delta_for_ticked_replica()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-vvc-tick");

        await tree.VersionVector("vec").TickAsync("r1");

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "vec"
            && m.TreeId == "obs-e2e-crdt-vvc-tick"
            && m.Delta is not null);

        var delta = JsonLatticeSerializer<VersionVectorDelta>.Default.Deserialize(m.Delta!);
        Assert.That(delta.Entries.ContainsKey("r1"), Is.True);
        Assert.That(delta.Entries["r1"].WallClockTicks, Is.GreaterThan(0));
    }

    [Test]
    public async Task VersionVectorAccessor_MergeAsync_stamps_version_vector_delta_with_other_state()
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
            && m.Delta is not null
            && JsonLatticeSerializer<VersionVectorDelta>.Default.Deserialize(m.Delta!).Entries.Count >= 2);

        var delta = JsonLatticeSerializer<VersionVectorDelta>.Default.Deserialize(m.Delta!);
        Assert.That(delta.Entries, Has.Count.EqualTo(2));
        Assert.That(delta.Entries.ContainsKey("r1"), Is.True);
        Assert.That(delta.Entries.ContainsKey("r2"), Is.True);
    }

    [Test]
    public async Task Plain_SetAsync_leaves_delta_slot_null_when_no_producer_stamps_context()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-crdt-plain-set");

        await tree.SetAsync("k", [1]);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set
            && m.Key == "k"
            && m.TreeId == "obs-e2e-crdt-plain-set");

        Assert.That(m.Delta, Is.Null);
    }
}