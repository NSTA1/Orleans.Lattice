namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexTreeNames"/>: the reserved namespace every
/// index-owned tree lives in, and the two helpers that build and recognise a
/// name inside it.
/// </summary>
[TestFixture]
public sealed class GrainIndexTreeNamesTests
{
    [Test]
    public void Reserved_prefix_is_the_documented_grain_index_namespace() =>
        Assert.That(GrainIndexTreeNames.ReservedPrefix, Is.EqualTo("__grainindex/"),
            "The reserved prefix is a documented 'do not select these' namespace for custom "
            + "replication resolvers, so its value is part of the contract.");

    [Test]
    public void For_index_places_the_index_name_under_the_reserved_prefix() =>
        Assert.That(GrainIndexTreeNames.ForIndex("users"), Is.EqualTo("__grainindex/users"));

    [Test]
    public void For_index_rejects_a_null_name() =>
        Assert.That(() => GrainIndexTreeNames.ForIndex(null!), Throws.ArgumentNullException);

    [TestCase("")]
    [TestCase("   ")]
    public void For_index_rejects_an_empty_or_whitespace_name(string indexName) =>
        Assert.That(() => GrainIndexTreeNames.ForIndex(indexName), Throws.ArgumentException);

    [Test]
    public void Is_index_owned_accepts_a_name_inside_the_reserved_namespace() =>
        Assert.That(GrainIndexTreeNames.IsIndexOwned("__grainindex/users"), Is.True);

    [TestCase("users")]
    [TestCase("__grainindex")]
    [TestCase("app/__grainindex/users")]
    public void Is_index_owned_rejects_a_name_outside_the_reserved_namespace(string treeName) =>
        Assert.That(GrainIndexTreeNames.IsIndexOwned(treeName), Is.False);

    [Test]
    public void Is_index_owned_rejects_a_null_name() =>
        Assert.That(() => GrainIndexTreeNames.IsIndexOwned(null!), Throws.ArgumentNullException);
}
