using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Orleans serializer round-trip tests for the persisted leaf-access histogram
/// (issue #332). The alias-hygiene test proves the alias is registered and the
/// unit tests prove the model captures and restores its own snapshot, but only a
/// real serialize / deserialize round-trip proves the codegen produces a working
/// envelope for the parallel <see cref="LeafAccessModelSnapshot.Leaves"/> /
/// <see cref="LeafAccessModelSnapshot.Visits"/> lists and that no
/// <c>[Id(...)]</c> slot has been silently reordered or dropped. The snapshot
/// rides inside <see cref="ShardRootState"/>, so a broken envelope here would
/// fail every shard-root state write, not just the pre-warm feature.
/// </summary>
[TestFixture]
public class LeafAccessModelSnapshotSerializerRoundTripTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        return serializer.Deserialize(serializer.SerializeToArray(value));
    }

    private static LeafAccessModelSnapshot Populated() => new()
    {
        Leaves = ["leaf/a", "leaf/b", "leaf/c"],
        Visits = [100L, 55L, 7L],
    };

    [Test]
    public void LeafAccessModelSnapshot_round_trips_leaves_and_visits()
    {
        var original = Populated();

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Leaves, Is.EqualTo(original.Leaves));
            Assert.That(copy.Visits, Is.EqualTo(original.Visits));
        });
    }

    [Test]
    public void LeafAccessModelSnapshot_round_trips_large_visit_counts()
    {
        // Visit counts are long-valued because a hot leaf on a long-lived silo
        // can exceed int range; prove the slot really is 64-bit on the wire.
        var original = new LeafAccessModelSnapshot
        {
            Leaves = ["leaf/hot"],
            Visits = [long.MaxValue - 1],
        };

        Assert.That(RoundTrip(original).Visits, Is.EqualTo(new[] { long.MaxValue - 1 }));
    }

    [Test]
    public void LeafAccessModelSnapshot_round_trips_the_empty_snapshot()
    {
        var copy = RoundTrip(LeafAccessModelSnapshot.Empty);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Leaves, Is.Empty);
            Assert.That(copy.Visits, Is.Empty);
        });
    }

    [Test]
    public void Snapshot_survives_a_round_trip_nested_inside_ShardRootState()
    {
        // The snapshot is never serialized on its own in production - it rides
        // at [Id(17)] of ShardRootState. Prove the nesting works, because a
        // broken envelope here breaks every shard-root write.
        var original = new ShardRootState { LeafAccessModel = Populated() };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.LeafAccessModel, Is.Not.Null);
            Assert.That(copy.LeafAccessModel!.Leaves, Is.EqualTo(original.LeafAccessModel!.Leaves));
            Assert.That(copy.LeafAccessModel.Visits, Is.EqualTo(new List<long> { 100L, 55L, 7L }));
        });
    }

    [Test]
    public void A_shard_root_state_without_a_model_round_trips_as_null()
    {
        // The default-off posture on the wire: an untracked shard must not pay
        // for the new slot.
        var copy = RoundTrip(new ShardRootState());

        Assert.That(copy.LeafAccessModel, Is.Null);
    }

    [Test]
    public void Deep_copy_preserves_the_snapshot()
    {
        // Memory grain storage deep-copies state rather than serializing it, so
        // the generated copier is on the real persistence path in tests and in
        // any in-memory provider.
        var copier = _services.GetRequiredService<DeepCopier<ShardRootState>>();
        var original = new ShardRootState { LeafAccessModel = Populated() };

        var copy = copier.Copy(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.LeafAccessModel, Is.Not.Null);
            Assert.That(copy.LeafAccessModel!.Visits, Is.EqualTo(new List<long> { 100L, 55L, 7L }));
            Assert.That(copy.LeafAccessModel.Leaves, Is.EqualTo(new List<string> { "leaf/a", "leaf/b", "leaf/c" }));
        });
    }
}
