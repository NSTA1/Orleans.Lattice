using Orleans.Lattice.Explorer.DesignSystem.Tokens;

namespace Orleans.Lattice.Explorer.Tests.DesignSystem;

/// <summary>
/// Unit tests for the density token scale and its stable names.
/// </summary>
[TestFixture]
public sealed class LatticeDensitiesTests
{
    [Test]
    public void All_lists_every_density_roomiest_first()
    {
        Assert.That(LatticeDensities.All, Is.EqualTo(new[]
        {
            LatticeDensity.Comfortable,
            LatticeDensity.Cosy,
            LatticeDensity.Compact,
        }));
    }

    [Test]
    public void All_covers_every_declared_enum_member()
    {
        Assert.That(LatticeDensities.All, Is.EquivalentTo(Enum.GetValues<LatticeDensity>()));
    }

    [Test]
    public void All_returns_the_same_instance_so_enumeration_allocates_no_array()
    {
        Assert.That(LatticeDensities.All, Is.SameAs(LatticeDensities.All));
    }

    [TestCase(LatticeDensity.Comfortable, LatticeDensities.ComfortableName)]
    [TestCase(LatticeDensity.Cosy, LatticeDensities.CosyName)]
    [TestCase(LatticeDensity.Compact, LatticeDensities.CompactName)]
    public void Name_returns_the_stable_lowercase_name(LatticeDensity density, string expected)
    {
        Assert.That(LatticeDensities.Name(density), Is.EqualTo(expected));
    }

    [Test]
    public void Name_returns_an_interned_literal_so_a_render_path_allocates_nothing()
    {
        foreach (var density in LatticeDensities.All)
        {
            Assert.That(LatticeDensities.Name(density), Is.SameAs(LatticeDensities.Name(density)));
        }
    }

    [Test]
    public void Name_rejects_an_undeclared_density()
    {
        Assert.That(
            () => LatticeDensities.Name((LatticeDensity)42),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void TryParseName_round_trips_every_name_produced_by_Name()
    {
        foreach (var density in LatticeDensities.All)
        {
            Assert.That(LatticeDensities.TryParseName(LatticeDensities.Name(density), out var parsed), Is.True);
            Assert.That(parsed, Is.EqualTo(density));
        }
    }

    [TestCase("COMFORTABLE", LatticeDensity.Comfortable)]
    [TestCase("Cosy", LatticeDensity.Cosy)]
    [TestCase("cOmPaCt", LatticeDensity.Compact)]
    public void TryParseName_is_case_insensitive(string name, LatticeDensity expected)
    {
        Assert.That(LatticeDensities.TryParseName(name, out var parsed), Is.True);
        Assert.That(parsed, Is.EqualTo(expected));
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("roomy")]
    public void TryParseName_returns_false_and_the_standard_density_for_an_unknown_name(string? name)
    {
        Assert.That(LatticeDensities.TryParseName(name, out var parsed), Is.False);
        Assert.That(parsed, Is.EqualTo(LatticeDensity.Cosy));
    }

    [Test]
    public void The_density_names_are_distinct()
    {
        var names = LatticeDensities.All.Select(LatticeDensities.Name).ToArray();

        Assert.That(names, Is.Unique);
    }
}
