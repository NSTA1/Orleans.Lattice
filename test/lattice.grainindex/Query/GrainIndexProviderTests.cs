using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.GrainIndex.Query;

namespace Orleans.Lattice.GrainIndex.Tests.Query;

/// <summary>
/// Resolution of the query surface from the silo's declared index set.
/// </summary>
[TestFixture]
public sealed class GrainIndexProviderTests
{
    [Test]
    public void Get_index_by_name_returns_the_declared_index()
    {
        var provider = Provider(IndexedTestIndex.Definition("Subjects"));

        var index = provider.GetIndex<ITestStringKeyedGrain, IndexedTestState>("Subjects");

        Assert.Multiple(() =>
        {
            Assert.That(index.Name, Is.EqualTo("Subjects"));
            Assert.That(index.IndexedProperties, Is.EqualTo(new[] { "Age", "Country", "LastSeen", "Status" }));
        });
    }

    [Test]
    public void Get_index_returns_the_same_instance_for_repeat_calls()
    {
        var provider = Provider(IndexedTestIndex.Definition("Subjects"));

        var first = provider.GetIndex<ITestStringKeyedGrain, IndexedTestState>("Subjects");
        var second = provider.GetIndex<ITestStringKeyedGrain, IndexedTestState>("Subjects");

        Assert.That(second, Is.SameAs(first));
    }

    [Test]
    public void Get_index_without_a_name_selects_the_only_index_over_those_types()
    {
        var provider = Provider(IndexedTestIndex.Definition("Subjects"));

        var index = provider.GetIndex<ITestStringKeyedGrain, IndexedTestState>();

        Assert.That(index.Name, Is.EqualTo("Subjects"));
    }

    [Test]
    public void Get_index_without_a_name_rejects_an_ambiguous_declaration_set()
    {
        var provider = Provider(
            IndexedTestIndex.Definition("Subjects"),
            IndexedTestIndex.Definition("Others"));

        var exception = Assert.Throws<InvalidOperationException>(
            () => provider.GetIndex<ITestStringKeyedGrain, IndexedTestState>());

        Assert.That(exception!.Message, Does.Contain("More than one grain index"));
    }

    [Test]
    public void Get_index_reports_an_unknown_name_with_the_declared_set()
    {
        var provider = Provider(IndexedTestIndex.Definition("Subjects"));

        var exception = Assert.Throws<InvalidOperationException>(
            () => provider.GetIndex<ITestStringKeyedGrain, IndexedTestState>("missing"));

        Assert.That(exception!.Message, Does.Contain("missing").And.Contain("Subjects"));
    }

    [Test]
    public void Get_index_reports_a_type_mismatch_on_a_declared_name()
    {
        var provider = Provider(IndexedTestIndex.Definition("Subjects"));

        var exception = Assert.Throws<InvalidOperationException>(
            () => provider.GetIndex<ITestGuidKeyedGrain, IndexedTestState>("Subjects"));

        Assert.That(exception!.Message, Does.Contain("declared over grain type"));
    }

    [Test]
    public void Get_index_reports_an_empty_declaration_set()
    {
        var provider = Provider();

        var exception = Assert.Throws<InvalidOperationException>(
            () => provider.GetIndex<ITestStringKeyedGrain, IndexedTestState>());

        Assert.That(exception!.Message, Does.Contain("(none)"));
    }

    [Test]
    public void Declared_indexes_lists_every_declaration_in_order()
    {
        var provider = Provider(
            IndexedTestIndex.Definition("Subjects"),
            IndexedTestIndex.Definition("Others"));

        Assert.That(provider.DeclaredIndexes, Is.EqualTo(new[] { "Subjects", "Others" }));
    }

    [Test]
    public void The_provider_rejects_null_dependencies()
    {
        var declarations = Options.Create(new GrainIndexDeclarationOptions());
        var factory = Substitute.For<IGrainFactory>();
        var options = Substitute.For<IOptionsMonitor<GrainIndexOptions>>();

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new GrainIndexProvider(null!, factory, options));
            Assert.Throws<ArgumentNullException>(() => new GrainIndexProvider(declarations, null!, options));
            Assert.Throws<ArgumentNullException>(() => new GrainIndexProvider(declarations, factory, null!));
        });
    }

    [Test]
    public void Add_grain_index_registers_the_provider_once()
    {
        var builder = new StubSiloBuilder();

        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(cfg => cfg
            .WithName("first")
            .Include(x => x.Age));
        builder.AddGrainIndex<ITestStringKeyedGrain, TestGrainState>(cfg => cfg
            .WithName("second")
            .Include(x => x.Country));

        Assert.That(
            builder.Services.Count(d => d.ServiceType == typeof(IGrainIndexProvider)),
            Is.EqualTo(1));
    }

    private static GrainIndexProvider Provider(params IGrainIndexDefinition[] definitions)
    {
        var declarations = new GrainIndexDeclarationOptions();
        foreach (var definition in definitions)
        {
            declarations.Definitions.Add(definition);
        }

        var options = Substitute.For<IOptionsMonitor<GrainIndexOptions>>();
        options.Get(Arg.Any<string>()).Returns(call => new GrainIndexOptions
        {
            TreeName = GrainIndexTreeNames.ForIndex(call.ArgAt<string>(0)),
        });

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILattice>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(Substitute.For<ILattice>());

        return new GrainIndexProvider(Options.Create(declarations), factory, options);
    }
}
