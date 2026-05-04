using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the core <see cref="WalEntry"/> provider-boundary DTO.
/// Pins the offset / mutation slot map and equality behaviour after the
/// type was promoted from the replication package to core.
/// </summary>
[TestFixture]
public class WalEntryTests
{
    [Test]
    public void Default_value_has_zero_offset_and_default_mutation()
    {
        var def = default(WalEntry);

        Assert.Multiple(() =>
        {
            Assert.That(def.Offset, Is.EqualTo(0L));
            Assert.That(def.Mutation, Is.EqualTo(default(LatticeMutation)));
        });
    }

    [Test]
    public void With_initialiser_sets_offset_and_mutation()
    {
        var mutation = new LatticeMutation
        {
            TreeId = "t",
            Kind = MutationKind.Set,
            Key = "k",
            Value = new byte[] { 1, 2, 3 },
            Timestamp = HybridLogicalClock.Tick(HybridLogicalClock.Zero),
        };

        var sut = new WalEntry { Offset = 7, Mutation = mutation };

        Assert.Multiple(() =>
        {
            Assert.That(sut.Offset, Is.EqualTo(7L));
            Assert.That(sut.Mutation, Is.EqualTo(mutation));
        });
    }

    [Test]
    public void Records_with_same_values_are_equal()
    {
        var mutation = new LatticeMutation { TreeId = "t", Kind = MutationKind.Set, Key = "k" };
        var a = new WalEntry { Offset = 1, Mutation = mutation };
        var b = new WalEntry { Offset = 1, Mutation = mutation };

        Assert.That(a, Is.EqualTo(b));
    }

    [Test]
    public void Records_with_different_offsets_are_not_equal()
    {
        var mutation = new LatticeMutation { TreeId = "t", Kind = MutationKind.Set, Key = "k" };
        var a = new WalEntry { Offset = 1, Mutation = mutation };
        var b = new WalEntry { Offset = 2, Mutation = mutation };

        Assert.That(a, Is.Not.EqualTo(b));
    }

    [Test]
    public void Records_with_different_mutations_are_not_equal()
    {
        var ma = new LatticeMutation { TreeId = "t", Kind = MutationKind.Set, Key = "a" };
        var mb = new LatticeMutation { TreeId = "t", Kind = MutationKind.Set, Key = "b" };
        var a = new WalEntry { Offset = 1, Mutation = ma };
        var b = new WalEntry { Offset = 1, Mutation = mb };

        Assert.That(a, Is.Not.EqualTo(b));
    }
}
