using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;
using Orleans.Serialization.Cloning;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class LeafSummaryPageSerializerRoundTripTests
{
    [TestCase(true)]
    [TestCase(false)]
    public void Keys_and_rebuild_links_preserve_payload_and_topology_on_the_wire(bool tail)
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var link = new LeafChainLink
        {
            KeyRange = new LeafKeyRange { LowKeyInclusive = "a", HighKeyExclusive = "m" },
            NextSibling = tail ? null : GrainId.Create("leaf", "next"),
        };
        var original = (Keys: new List<string> { "a", "b" }, Link: link);
        var serializer = services.GetRequiredService<Serializer<(List<string> Keys, LeafChainLink Link)>>();
        var copy = serializer.Deserialize(serializer.SerializeToArray(original));
        var local = services.GetRequiredService<DeepCopier<(List<string> Keys, LeafChainLink Link)>>()
            .Copy(original);
        var linkSerializer = services.GetRequiredService<Serializer<LeafChainLink>>();

        Assert.Multiple(() =>
        {
            Assert.That(copy.Keys, Is.EqualTo(original.Keys));
            Assert.That(copy.Link, Is.EqualTo(link));
            Assert.That(local.Keys, Is.EqualTo(original.Keys).And.Not.SameAs(original.Keys));
            Assert.That(local.Link, Is.EqualTo(link));
            Assert.That(linkSerializer.Deserialize(linkSerializer.SerializeToArray(link)), Is.EqualTo(link));
        });
    }

    [TestCase(true)]
    [TestCase(false)]
    public void Count_and_statistics_preserve_payload_and_topology_on_the_wire(bool tail)
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var link = new LeafChainLink
        {
            KeyRange = new LeafKeyRange { LowKeyInclusive = "a", HighKeyExclusive = "m" },
            NextSibling = tail ? null : GrainId.Create("leaf", "next"),
        };
        var count = new LeafCountPage { Count = 17, Link = link };
        var stats = new LeafStatsPage
        {
            Stats = new LeafStats { LiveKeys = 17, Tombstones = 3, StateBytes = 200 },
            Link = link,
        };
        var countSerializer = services.GetRequiredService<Serializer<LeafCountPage>>();
        var statsSerializer = services.GetRequiredService<Serializer<LeafStatsPage>>();

        Assert.That(countSerializer.Deserialize(countSerializer.SerializeToArray(count)), Is.EqualTo(count));
        Assert.That(statsSerializer.Deserialize(statsSerializer.SerializeToArray(stats)), Is.EqualTo(stats));
    }
}
