using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Checks that combined checkpoint metadata survives a cross-silo wire round-trip.
/// </summary>
[TestFixture]
public class LeafCheckpointPageSerializerRoundTripTests
{
    [TestCase(-1L, true)]
    [TestCase(0L, false)]
    [TestCase(42L, false)]
    public void Checkpoint_and_chain_link_round_trip(long checkpoint, bool tail)
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<LeafCheckpointPage>>();
        var original = new LeafCheckpointPage
        {
            CheckpointOffset = checkpoint,
            Link = new LeafChainLink
            {
                KeyRange = new LeafKeyRange { LowKeyInclusive = "a", HighKeyExclusive = "k" },
                NextSibling = tail ? null : GrainId.Create("leaf", "next"),
            },
        };

        var copy = serializer.Deserialize(serializer.SerializeToArray(original));

        Assert.That(copy, Is.EqualTo(original));
    }
}
