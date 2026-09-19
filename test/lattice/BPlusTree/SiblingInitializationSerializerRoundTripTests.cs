using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Orleans serializer round-trip for <see cref="SiblingInitialization"/>, the
/// payload a dividing leaf uses to seed every birth-time slot on its freshly
/// minted sibling in one round-trip.
/// <para>
/// The grain-level fixtures for the moved-away seal (issue 3121) drive a
/// substituted sibling in-process, so they never exercise the wire. A split
/// across silos does: the donor and the sibling need not be co-located, and the
/// seal is precisely the slot whose loss is silent - an unsealed sibling serves
/// migrated orphans rather than failing. These tests pin that the seal's two
/// slots survive serialization in both the present and absent cases, so a future
/// renumbering or a dropped <c>[Id]</c> fails here rather than in a cross-silo
/// reshard.
/// </para>
/// </summary>
[TestFixture]
public class SiblingInitializationSerializerRoundTripTests
{
    private ServiceProvider _services = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection()
            .AddSerializer()
            .BuildServiceProvider();
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private T RoundTrip<T>(T value)
    {
        var serializer = _services.GetRequiredService<Serializer<T>>();
        var bytes = serializer.SerializeToArray(value);
        return serializer.Deserialize(bytes);
    }

    [Test]
    public void Round_trips_every_birth_time_slot_including_the_moved_away_seal()
    {
        var next = GrainId.Create("leaf", "next");
        var prev = GrainId.Create("leaf", "prev");

        var original = new SiblingInitialization
        {
            TreeId = "tree-1",
            ShardIndex = 4,
            LowKeyInclusive = "m",
            HighKeyExclusive = "z",
            NextSibling = next,
            PrevSibling = prev,
            MovedAwaySlots = new[] { 3, 9, 27 },
            MovedAwayVirtualShardCount = 64,
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.TreeId, Is.EqualTo("tree-1"));
            Assert.That(copy.ShardIndex, Is.EqualTo(4));
            Assert.That(copy.LowKeyInclusive, Is.EqualTo("m"));
            Assert.That(copy.HighKeyExclusive, Is.EqualTo("z"));
            Assert.That(copy.NextSibling, Is.EqualTo(next));
            Assert.That(copy.PrevSibling, Is.EqualTo(prev));
            Assert.That(copy.MovedAwaySlots, Is.EqualTo(new[] { 3, 9, 27 }));
            Assert.That(copy.MovedAwayVirtualShardCount, Is.EqualTo(64));
        });
    }

    /// <summary>
    /// The overwhelmingly common split is of an unsealed donor, so the absent
    /// case is the one that runs constantly and must not materialise an empty
    /// seal on the far side: <c>MovedAwaySealInheritance</c> treats a virtual
    /// shard count with no slots as the inert "seal just lifted" stamp, and a
    /// serializer that turned <see langword="null"/> into an empty array would
    /// quietly change which branch the receiver takes.
    /// </summary>
    [Test]
    public void Round_trips_an_absent_seal_as_null_rather_than_an_empty_array()
    {
        var original = new SiblingInitialization
        {
            TreeId = "tree-1",
            LowKeyInclusive = "m",
            HighKeyExclusive = "z",
        };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.MovedAwaySlots, Is.Null);
            Assert.That(copy.MovedAwayVirtualShardCount, Is.Null);
            Assert.That(copy.ShardIndex, Is.Null);
            Assert.That(copy.NextSibling, Is.Null);
            Assert.That(copy.PrevSibling, Is.Null);
        });
    }
}
