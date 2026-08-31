using Microsoft.Extensions.Options;
using NSubstitute;

namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Construction of the query surface: both forms of the constructor, their
/// argument guards, and the options-driven tree resolution.
/// </summary>
[TestFixture]
public sealed class GrainIndexTests
{
    [Test]
    public void The_tree_constructor_rejects_null_arguments()
    {
        var definition = IndexedTestIndex.Definition();
        var tree = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(null!, tree, factory));
            Assert.Throws<ArgumentNullException>(
                () => new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(definition, (ILattice)null!, factory));
            Assert.Throws<ArgumentNullException>(
                () => new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(definition, tree, null!));
        });
    }

    [Test]
    public void The_options_constructor_rejects_null_arguments()
    {
        var definition = IndexedTestIndex.Definition();
        var factory = Substitute.For<IGrainFactory>();
        var options = Substitute.For<IOptionsMonitor<GrainIndexOptions>>();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(
                () => new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(null!, factory, options));
            Assert.Throws<ArgumentNullException>(
                () => new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(definition, null!, options));
            Assert.Throws<ArgumentNullException>(
                () => new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(
                    definition, factory, (IOptionsMonitor<GrainIndexOptions>)null!));
        });
    }

    [Test]
    public void The_options_constructor_resolves_the_tree_named_by_the_index_options()
    {
        var definition = IndexedTestIndex.Definition();
        var tree = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(GrainIndexTreeNames.ForIndex("Subjects"), null).Returns(tree);

        var options = Substitute.For<IOptionsMonitor<GrainIndexOptions>>();
        options.Get("Subjects").Returns(new GrainIndexOptions
        {
            TreeName = GrainIndexTreeNames.ForIndex("Subjects"),
        });

        var index = new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(definition, factory, options);

        Assert.Multiple(() =>
        {
            Assert.That(index.Name, Is.EqualTo("Subjects"));
            factory.Received().GetGrain<ILattice>(GrainIndexTreeNames.ForIndex("Subjects"), null);
        });
    }

    [Test]
    public void An_index_over_an_empty_projection_is_rejected_by_both_constructors()
    {
        var definition = IndexedTestIndex.Empty();
        var tree = Substitute.For<ILattice>();
        var factory = Substitute.For<IGrainFactory>();
        var options = Substitute.For<IOptionsMonitor<GrainIndexOptions>>();
        options.Get(Arg.Any<string>()).Returns(new GrainIndexOptions
        {
            TreeName = GrainIndexTreeNames.ForIndex("Subjects"),
        });

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentException>(
                () => new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(definition, tree, factory));
            Assert.Throws<ArgumentException>(
                () => new GrainIndex<ITestStringKeyedGrain, IndexedTestState>(definition, factory, options));
        });
    }
}
