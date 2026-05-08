using System.Text;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the CRDT delta wire-shape contract:
/// <see cref="LwwRegisterDelta"/>, <see cref="OrSetDelta"/>,
/// <see cref="OrSetDot"/>, <see cref="PnCounterDelta"/>, and
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
    public void OrSetDot_carries_element_replica_and_counter_slots()
    {
        var dot = new OrSetDot
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
    public void VersionVectorDelta_empty_singleton_has_non_null_entries()
    {
        var empty = VersionVectorDelta.Empty;
        Assert.That(empty.Entries, Is.Not.Null.And.Empty);
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
}
