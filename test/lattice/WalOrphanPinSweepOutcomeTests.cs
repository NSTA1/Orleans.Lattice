using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Serialization;

namespace Orleans.Lattice.Tests;

/// <summary>Wire serialization and same-silo immutability of shared sweep results.</summary>
[TestFixture]
public sealed class WalOrphanPinSweepOutcomeTests
{
    [TestCase(false)]
    [TestCase(true)]
    public void Serialization_round_trip_preserves_every_field(bool unavailable)
    {
        var original = unavailable ? WalOrphanPinSweepOutcome.Unavailable : new WalOrphanPinSweepOutcome
        {
            Epoch = Guid.NewGuid(),
            Generation = 17,
            Classified = true,
            RepairConsumerIds = ["leaf-a", "leaf-b"],
            RequireOffsetAdvance = ImmutableHashSet.Create(StringComparer.Ordinal, "leaf-b"),
            PinPopulation = 123,
            PartitionPopulations = ImmutableDictionary<int, int>.Empty.Add(0, 7).Add(3, 11),
            OffsetPopulation = 18,
        };
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer<WalOrphanPinSweepOutcome>>();
        var decoded = serializer.Deserialize(serializer.SerializeToArray(original));
        var polymorphic = services.GetRequiredService<Serializer<object>>();
        var aliased = (WalOrphanPinSweepOutcome)polymorphic.Deserialize(polymorphic.SerializeToArray(original));

        foreach (var result in new[] { decoded, aliased })
        {
            Assert.Multiple(() =>
            {
                Assert.That(result, Is.Not.SameAs(original));
                Assert.That(result.Epoch, Is.EqualTo(original.Epoch));
                Assert.That(result.Generation, Is.EqualTo(original.Generation));
                Assert.That(result.Classified, Is.EqualTo(original.Classified));
                Assert.That(result.RepairConsumerIds, Is.EqualTo(original.RepairConsumerIds));
                Assert.That(result.RequireOffsetAdvance, Is.EquivalentTo(original.RequireOffsetAdvance));
                Assert.That(result.PartitionPopulations, Is.EqualTo(original.PartitionPopulations));
                Assert.That(result.PinPopulation, Is.EqualTo(original.PinPopulation));
                Assert.That(result.OffsetPopulation, Is.EqualTo(original.OffsetPopulation));
                Assert.That(result.Available, Is.EqualTo(original.Available));
            });
        }
    }

    [Test]
    public void Same_silo_copy_shares_only_collections_that_cannot_be_mutated()
    {
        var original = new WalOrphanPinSweepOutcome
        {
            RepairConsumerIds = ["leaf-a"],
            RequireOffsetAdvance = ImmutableHashSet.Create(StringComparer.Ordinal, "leaf-a"),
            PartitionPopulations = ImmutableDictionary<int, int>.Empty.Add(0, 1),
        };
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var copy = services.GetRequiredService<DeepCopier>().Copy(original);

        Assert.That(copy, Is.SameAs(original), "Immutable outcomes must not allocate a same-silo deep copy.");
        Assert.Throws<NotSupportedException>(() => ((IList<string>)copy.RepairConsumerIds)[0] = "changed");
        Assert.Throws<NotSupportedException>(() => ((ICollection<string>)copy.RequireOffsetAdvance).Add("changed"));
        Assert.Throws<NotSupportedException>(() => ((IDictionary<int, int>)copy.PartitionPopulations!)[0] = 999);
        Assert.Multiple(() =>
        {
            Assert.That(original.RepairConsumerIds[0], Is.EqualTo("leaf-a"));
            Assert.That(original.RequireOffsetAdvance, Is.EquivalentTo(new[] { "leaf-a" }));
            Assert.That(original.PartitionPopulations![0], Is.EqualTo(1));
        });
    }
}
