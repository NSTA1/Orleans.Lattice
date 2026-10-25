using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree;

[TestFixture]
public class LeafSummaryPageSerializerRoundTripTests
{
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
