namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// Covers <see cref="GrainIndexOptions"/> and
/// <see cref="GrainIndexDeclarationOptions"/>: the defaults a host inherits when
/// it overrides nothing.
/// </summary>
[TestFixture]
public sealed class GrainIndexOptionsTests
{
    [Test]
    public void Allow_replication_defaults_to_false() =>
        Assert.That(new GrainIndexOptions().AllowReplication, Is.False,
            "Grain indexes are cluster-local by definition, so cross-cluster replication of an "
            + "index tree is an explicit opt-in.");

    [Test]
    public void Tree_name_starts_empty_so_the_declaration_seeds_it() =>
        Assert.That(new GrainIndexOptions().TreeName, Is.Empty);

    [Test]
    public void Backfill_knobs_default_to_the_published_constants()
    {
        var options = new GrainIndexOptions();

        Assert.Multiple(() =>
        {
            Assert.That(options.BackfillBatchSize, Is.EqualTo(GrainIndexOptions.DefaultBackfillBatchSize));
            Assert.That(options.BackfillInterval, Is.EqualTo(GrainIndexOptions.DefaultBackfillInterval));
        });
    }

    [Test]
    public void Published_backfill_defaults_are_usable_values()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainIndexOptions.DefaultBackfillBatchSize, Is.GreaterThan(0));
            Assert.That(GrainIndexOptions.DefaultBackfillInterval, Is.GreaterThan(TimeSpan.Zero));
        });
    }

    [Test]
    public void Every_option_round_trips_through_its_setter()
    {
        var options = new GrainIndexOptions
        {
            TreeName = "__grainindex/custom",
            AllowReplication = true,
            BackfillBatchSize = 64,
            BackfillInterval = TimeSpan.FromMinutes(5),
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.TreeName, Is.EqualTo("__grainindex/custom"));
            Assert.That(options.AllowReplication, Is.True);
            Assert.That(options.BackfillBatchSize, Is.EqualTo(64));
            Assert.That(options.BackfillInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
        });
    }

    [Test]
    public void Declaration_options_start_with_no_declared_indexes() =>
        Assert.That(new GrainIndexDeclarationOptions().Definitions, Is.Empty);

    [Test]
    public void Declaration_options_accept_appended_definitions()
    {
        var declarations = new GrainIndexDeclarationOptions();
        var definition = new GrainIndexDefinition<ITestStringKeyedGrain, TestGrainState>(
            "users",
            StringGrainKeyCodec<ITestStringKeyedGrain>.Instance,
            [new TypedGrainIndexProperty<TestGrainState, int>("Age", static s => s.Age)]);

        declarations.Definitions.Add(definition);

        Assert.That(declarations.Definitions, Is.EqualTo(new[] { definition }));
    }
}
