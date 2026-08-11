using System.Buffers.Binary;
using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the CRDT delta wire-shape contract:
/// <see cref="LwwRegisterDelta"/>, <see cref="OrSetDelta"/>,
/// <see cref="OrSetDeltaDot"/>, <see cref="PnCounterDelta"/>, and
/// <see cref="VersionVectorDelta"/> are public, serialisable record
/// structs with the documented <c>Empty</c> /
/// <c>Tombstone</c> factories, and the typed CRDT accessors
/// (<see cref="OrSetAccessor"/>, <see cref="PnCounterAccessor"/>,
/// <see cref="VersionVectorAccessor"/>) drive cross-cluster
/// convergence under the matching merge mode.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public void LwwRegisterDelta_tombstone_factory_produces_value_carrying_timestamp_and_origin()
    {
        var ts = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 3 };
        var delta = LwwRegisterDelta.Tombstone(ts, "origin-x");

        Assert.Multiple(() =>
        {
            Assert.That(delta.IsTombstone, Is.True);
            Assert.That(delta.Timestamp, Is.EqualTo(ts));
            Assert.That(delta.OriginClusterId, Is.EqualTo("origin-x"));
            Assert.That(delta.Value, Is.Null);
        });
    }

    [Test]
    public void OrSetDelta_empty_singleton_has_non_null_collections()
    {
        var empty = OrSetDelta.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(empty.Adds, Is.Not.Null.And.Empty);
            Assert.That(empty.Removes, Is.Not.Null.And.Empty);
        });
    }

    [Test]
    public void OrSetDeltaDot_carries_element_replica_and_counter_slots()
    {
        var dot = new OrSetDeltaDot
        {
            Element = Bytes("a"),
            ReplicaId = "replica-1",
            Counter = 42,
        };

        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(dot.Element), Is.EqualTo("a"));
            Assert.That(dot.ReplicaId, Is.EqualTo("replica-1"));
            Assert.That(dot.Counter, Is.EqualTo(42));
        });
    }

    [Test]
    public void PnCounterDelta_empty_singleton_has_non_null_dictionaries()
    {
        var empty = PnCounterDelta.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(empty.Increments, Is.Not.Null.And.Empty);
            Assert.That(empty.Decrements, Is.Not.Null.And.Empty);
        });
    }

    [Test]
    public void GCounterDelta_empty_singleton_has_non_null_dictionary()
    {
        var empty = GCounterDelta.Empty;
        Assert.That(empty.Increments, Is.Not.Null.And.Empty);
    }

    [Test]
    public void VersionVectorDelta_empty_singleton_has_non_null_entries()
    {
        var empty = VersionVectorDelta.Empty;
        Assert.That(empty.Entries, Is.Not.Null.And.Empty);
    }

    [Test]
    public void GSetDelta_empty_singleton_has_non_null_adds()
    {
        var empty = GSetDelta.Empty;
        Assert.That(empty.Adds, Is.Not.Null.And.Empty);
    }

    [Test]
    public async Task GSetAccessor_replicates_grow_only_adds_across_clusters()
    {
        var treeId = NextTreeId("crdt-gset");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aSet = treeOnA.GSet("tags");
        var bSet = treeOnB.GSet("tags");

        await aSet.AddAsync(Bytes("red"));
        await bSet.AddAsync(Bytes("blue"));

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onA = await aSet.GetAsync();
                var onB = await bSet.GetAsync();
                return onA.Contains(Bytes("red")) && onA.Contains(Bytes("blue"))
                    && onB.Contains(Bytes("red")) && onB.Contains(Bytes("blue"));
            },
            $"G-Set additions from both sites must converge via union for '{treeId}/tags'.");
    }

    [Test]
    public async Task OrSetAccessor_replicates_observed_remove_set_across_clusters()
    {
        var treeId = NextTreeId("crdt-orset");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aSet = treeOnA.OrSet("members");
        var bSet = treeOnB.OrSet("members");

        await aSet.AddAsync(Bytes("alice"), replicaId: "site-a");
        await aSet.AddAsync(Bytes("bob"), replicaId: "site-a");

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var observed = await bSet.GetAsync();
                return observed.Contains(Bytes("alice"))
                    && observed.Contains(Bytes("bob"));
            },
            $"OrSet additions on Site A must converge to Site B for '{treeId}/members'.");
    }

    [Test]
    public async Task PnCounterAccessor_replicates_increment_and_decrement_across_clusters()
    {
        var treeId = NextTreeId("crdt-pncounter");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aCounter = treeOnA.PnCounter("count");
        var bCounter = treeOnB.PnCounter("count");

        await aCounter.IncrementAsync("site-a", 5);
        await aCounter.DecrementAsync("site-a", 2);

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => await bCounter.ValueAsync() == 3,
            $"PnCounter increments on Site A must converge to value=3 on Site B for '{treeId}/count'.");
    }

    [Test]
    public async Task GCounterAccessor_replicates_increment_across_clusters()
    {
        var treeId = NextTreeId("crdt-gcounter");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aCounter = treeOnA.GCounter("count");
        var bCounter = treeOnB.GCounter("count");

        await aCounter.IncrementAsync("site-a", 5);
        await aCounter.IncrementAsync("site-a", 2);

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => await bCounter.ValueAsync() == 7,
            $"GCounter increments on Site A must converge to value=7 on Site B for '{treeId}/count'.");
    }

    [Test]
    public async Task RwSetAccessor_replicates_remove_wins_across_clusters()
    {
        var treeId = NextTreeId("crdt-rwset");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aSet = treeOnA.RwSet("blocklist");
        var bSet = treeOnB.RwSet("blocklist");

        await aSet.AddAsync(Bytes("alice"), replicaId: "site-a");
        await aSet.AddAsync(Bytes("bob"), replicaId: "site-a");

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var observed = await bSet.GetAsync();
                return observed.Contains(Bytes("alice")) && observed.Contains(Bytes("bob"));
            },
            $"RW-Set additions on Site A must converge to Site B for '{treeId}/blocklist'.");

        // A remove authored on Site B must win over the earlier add once it
        // converges back to Site A - the remove-wins contract across clusters.
        await bSet.RemoveAsync(Bytes("alice"), replicaId: "site-b");

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var onA = await aSet.GetAsync();
                var onB = await bSet.GetAsync();
                return !onA.Contains(Bytes("alice")) && !onB.Contains(Bytes("alice"))
                    && onA.Contains(Bytes("bob")) && onB.Contains(Bytes("bob"));
            },
            $"RW-Set remove on Site B must win and converge at both sites for '{treeId}/blocklist'.");
    }

    [Test]
    public async Task MaxRegisterAccessor_replicates_high_water_mark_across_clusters()
    {
        var treeId = NextTreeId("crdt-maxregister");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aReg = treeOnA.MaxRegister<int>("ceiling", OrderKey);
        var bReg = treeOnB.MaxRegister<int>("ceiling", OrderKey);

        // Concurrent writes from both sites; the greatest value must win at
        // both sites regardless of delivery order.
        await aReg.SetAsync(42);
        await bReg.SetAsync(17);
        await aReg.SetAsync(90);

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => await aReg.GetAsync() == 90 && await bReg.GetAsync() == 90,
            $"Max-register writes must converge to the greatest value at both sites for '{treeId}/ceiling'.");
    }

    [Test]
    public async Task MinRegisterAccessor_replicates_low_water_mark_across_clusters()
    {
        var treeId = NextTreeId("crdt-minregister");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aReg = treeOnA.MinRegister<int>("floor", OrderKey);
        var bReg = treeOnB.MinRegister<int>("floor", OrderKey);

        await aReg.SetAsync(58);
        await bReg.SetAsync(83);
        await aReg.SetAsync(10);

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => await aReg.GetAsync() == 10 && await bReg.GetAsync() == 10,
            $"Min-register writes must converge to the least value at both sites for '{treeId}/floor'.");
    }

    // Order-preserving big-endian key for the register tests: the unsigned
    // big-endian encoding sorts lexicographically in the same order as the
    // non-negative integers the tests use, so the receiver folds the
    // directional extreme without the domain comparer.
    private static byte[] OrderKey(int value)
    {
        var buffer = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(buffer, (uint)value);
        return buffer;
    }

    [Test]
    public async Task VersionVectorAccessor_replicates_tick_across_clusters()
    {
        var treeId = NextTreeId("crdt-vv");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aClock = treeOnA.VersionVector("vclock");
        var bClock = treeOnB.VersionVector("vclock");

        await aClock.TickAsync("site-a");
        await aClock.TickAsync("site-a");
        await aClock.TickAsync("site-a");

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var observed = await bClock.GetAsync();
                return observed.GetClock("site-a").CompareTo(HybridLogicalClock.Zero) > 0;
            },
            $"VersionVector ticks on Site A must converge to Site B for '{treeId}/vclock'.");
    }

    [Test]
    public async Task SequenceAccessor_replicates_ordered_inserts_across_clusters()
    {
        var treeId = NextTreeId("crdt-sequence");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aSeq = treeOnA.Sequence<string>("doc");
        var bSeq = treeOnB.Sequence<string>("doc");

        // Author a three-element sequence on Site A. The producer ships
        // dot-explicit RgaDelta inserts; Site B must converge on the same
        // ordered traversal, not merely the same node set.
        await aSeq.InsertAtAsync(0, "site-a", "Hello");
        await aSeq.InsertAtAsync(1, "site-a", " ");
        await aSeq.InsertAtAsync(2, "site-a", "World");

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () =>
            {
                var observed = await bSeq.ToListAsync();
                return observed.SequenceEqual(new[] { "Hello", " ", "World" });
            },
            $"Sequence inserts on Site A must converge to identical order on Site B for '{treeId}/doc'.");
    }

    [Test]
    public async Task SequenceAccessor_replicates_remove_across_clusters()
    {
        var treeId = NextTreeId("crdt-sequence");
        var treeOnA = await CreateReplicatedTreeAsync(treeId);
        var treeOnB = _fixture.TreeOnB(treeId);

        var aSeq = treeOnA.Sequence<string>("doc");
        var bSeq = treeOnB.Sequence<string>("doc");

        await aSeq.InsertAtAsync(0, "site-a", "a");
        await aSeq.InsertAtAsync(1, "site-a", "b");
        await aSeq.InsertAtAsync(2, "site-a", "c");

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => (await bSeq.ToListAsync()).SequenceEqual(new[] { "a", "b", "c" }),
            $"Initial sequence on Site A must converge to Site B for '{treeId}/doc'.");

        // Remove the middle element on Site A; the tombstone dot must
        // ship and Site B must drop exactly that visible element.
        await aSeq.RemoveAtAsync(1);

        await PublicReplicationApiClusterFixture.WaitForConvergenceAsync(
            async () => (await bSeq.ToListAsync()).SequenceEqual(new[] { "a", "c" }),
            $"Sequence remove on Site A must converge to Site B for '{treeId}/doc'.");
    }

    [Test]
    public void BoundedRegisterDelta_empty_singleton_carries_no_candidate()
    {
        var empty = BoundedRegisterDelta.Empty;

        Assert.Multiple(() =>
        {
            Assert.That(empty.HasValue, Is.False);
            Assert.That(empty.Value, Is.Null);
            Assert.That(empty.OrderKey, Is.Null);
        });
    }

    [Test]
    public void BoundedRegisterDelta_carries_value_and_order_key_slots()
    {
        var delta = new BoundedRegisterDelta
        {
            Value = Bytes("v"),
            OrderKey = Bytes("k"),
            HasValue = true,
        };

        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetString(delta.Value!), Is.EqualTo("v"));
            Assert.That(Encoding.UTF8.GetString(delta.OrderKey!), Is.EqualTo("k"));
            Assert.That(delta.HasValue, Is.True);
        });
    }
}
