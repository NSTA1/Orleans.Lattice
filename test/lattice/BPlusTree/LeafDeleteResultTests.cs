using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Wire-format tests for the split each delete result carries back to the
/// shard root (issue #3523): <see cref="LeafDeleteResult"/> is new, and
/// <see cref="RangeDeleteResult.Split"/> is a new field on an existing type, so
/// both must survive a cross-silo round trip and an absent split must
/// deserialize as <see langword="null"/>.
/// </summary>
[TestFixture]
public sealed class LeafDeleteResultTests
{
    private ServiceProvider provider = null!;
    private Serializer serializer = null!;

    [SetUp]
    public void SetUp()
    {
        var services = new ServiceCollection();
        services.AddSerializer();
        provider = services.BuildServiceProvider();
        serializer = provider.GetRequiredService<Serializer>();
    }

    [TearDown]
    public void TearDown() => provider?.Dispose();

    private static SplitResult Split() => new()
    {
        PromotedKey = "d",
        NewSiblingId = GrainId.Create("leaf", "sibling"),
        ChildIsLeaf = true,
        Additional = [new SplitResult { PromotedKey = "q", NewSiblingId = GrainId.Create("leaf", "second"), ChildIsLeaf = true }],
    };

    private T RoundTrip<T>(T value) => serializer.Deserialize<T>(serializer.SerializeToArray(value));

    [Test]
    public void LeafDeleteResult_round_trips_deleted_and_split()
    {
        var original = new LeafDeleteResult { Deleted = true, Split = Split() };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Deleted, Is.True);
            Assert.That(copy.Split, Is.Not.Null);
            Assert.That(copy.Split!.PromotedKey, Is.EqualTo("d"));
            Assert.That(copy.Split.NewSiblingId, Is.EqualTo(GrainId.Create("leaf", "sibling")));
            Assert.That(copy.Split.Additional, Has.Length.EqualTo(1));
            Assert.That(copy.Split.Additional![0].PromotedKey, Is.EqualTo("q"));
        });
    }

    [Test]
    public void LeafDeleteResult_without_a_split_round_trips_a_null_split()
    {
        var copy = RoundTrip(new LeafDeleteResult { Deleted = false });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Deleted, Is.False);
            Assert.That(copy.Split, Is.Null);
        });
    }

    [Test]
    public void LeafDeleteResult_default_reports_nothing_deleted_and_no_split()
    {
        var value = default(LeafDeleteResult);

        Assert.Multiple(() =>
        {
            Assert.That(value.Deleted, Is.False);
            Assert.That(value.Split, Is.Null);
        });
    }

    [Test]
    public void RangeDeleteResult_round_trips_its_split()
    {
        var original = new RangeDeleteResult { Deleted = 3, PastRange = true, Split = Split() };

        var copy = RoundTrip(original);

        Assert.Multiple(() =>
        {
            Assert.That(copy.Deleted, Is.EqualTo(3));
            Assert.That(copy.PastRange, Is.True);
            Assert.That(copy.Split, Is.Not.Null);
            Assert.That(copy.Split!.PromotedKey, Is.EqualTo("d"));
            Assert.That(copy.Split.Additional, Has.Length.EqualTo(1));
        });
    }

    [Test]
    public void RangeDeleteResult_without_a_split_round_trips_a_null_split()
    {
        var copy = RoundTrip(new RangeDeleteResult { Deleted = 1, PastRange = false });

        Assert.That(copy.Split, Is.Null);
    }
}
