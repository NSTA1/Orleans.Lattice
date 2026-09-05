namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Tests for <see cref="RepoContextIndexingOptions.FromEnvironment"/>: it reads the
/// background-indexing cadence from <c>LATTICE_*</c> environment variables and falls back
/// to the behaviour-preserving defaults for any variable that is absent or malformed.
/// </summary>
[TestFixture]
public sealed class RepoContextIndexingOptionsTests
{
    private static readonly string[] AllKeys =
    [
        RepoContextIndexingOptions.TickIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileJitterSecondsKey,
        RepoContextIndexingOptions.FullWalkIntervalSecondsKey,
        RepoContextIndexingOptions.TokenizerProfileKey,
        RepoContextIndexingOptions.IndexingRoleKey,
        RepoContextIndexingOptions.SemanticRetrievalKey,
    ];

    [SetUp]
    [TearDown]
    public void ClearEnvironment()
    {
        foreach (var key in AllKeys)
        {
            Environment.SetEnvironmentVariable(key, null);
        }
    }

    [Test]
    public void FromEnvironment_uses_the_defaults_when_no_variables_are_set()
    {
        var options = RepoContextIndexingOptions.FromEnvironment();

        var defaults = new RepoContextIndexingOptions();
        Assert.Multiple(() =>
        {
            Assert.That(options.TickInterval, Is.EqualTo(defaults.TickInterval));
            Assert.That(options.ReconcileInterval, Is.EqualTo(defaults.ReconcileInterval));
            Assert.That(options.ReconcileIntervalJitter, Is.EqualTo(defaults.ReconcileIntervalJitter));
            Assert.That(options.FullWalkInterval, Is.EqualTo(defaults.FullWalkInterval));
        });
    }

    [Test]
    public void FromEnvironment_reads_each_variable_in_seconds()
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.TickIntervalSecondsKey, "5");
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileIntervalSecondsKey, "5");
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileJitterSecondsKey, "0");
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.FullWalkIntervalSecondsKey, "120");

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.TickInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.ReconcileInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.ReconcileIntervalJitter, Is.EqualTo(TimeSpan.Zero));
            Assert.That(options.FullWalkInterval, Is.EqualTo(TimeSpan.FromSeconds(120)));
        });
    }

    [Test]
    public void FromEnvironment_falls_back_to_the_default_for_a_malformed_value()
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileIntervalSecondsKey, "not-a-number");

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.That(options.ReconcileInterval, Is.EqualTo(new RepoContextIndexingOptions().ReconcileInterval));
    }

    [Test]
    public void FromEnvironment_falls_back_to_the_default_for_a_negative_value()
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.TickIntervalSecondsKey, "-3");

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.That(options.TickInterval, Is.EqualTo(new RepoContextIndexingOptions().TickInterval));
    }

    [Test]
    public void FromEnvironment_defaults_the_tokenizer_profile_to_o200k()
    {
        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.That(options.TokenizerProfile, Is.EqualTo(RepoContextIndexingOptions.TokenizerProfileO200k));
    }

    [Test]
    public void FromEnvironment_defaults_the_role_to_hub()
    {
        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.Role, Is.EqualTo(RepoContextIndexingRole.Hub));
            Assert.That(options.IndexingEnabled, Is.True, "A hub is the authoritative indexer.");
        });
    }

    [Test]
    [TestCase("hub", true)]
    [TestCase("HUB", true)]
    [TestCase("  hub  ", true)]
    [TestCase("spoke", false)]
    [TestCase("SPOKE", false)]
    [TestCase("  spoke  ", false)]
    public void FromEnvironment_resolves_a_recognised_role(string raw, bool expectHub)
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.IndexingRoleKey, raw);

        var options = RepoContextIndexingOptions.FromEnvironment();

        var expected = expectHub ? RepoContextIndexingRole.Hub : RepoContextIndexingRole.Spoke;
        Assert.That(options.Role, Is.EqualTo(expected));
    }

    [Test]
    public void FromEnvironment_resolving_spoke_disables_indexing()
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.IndexingRoleKey, "spoke");

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.Role, Is.EqualTo(RepoContextIndexingRole.Spoke));
            Assert.That(options.IndexingEnabled, Is.False, "A spoke never mutates source-derived index state.");
        });
    }

    [Test]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("primary")]
    [TestCase("replica")]
    [TestCase("not-a-role")]
    public void FromEnvironment_fails_closed_to_hub_for_an_absent_or_unrecognised_role(string raw)
    {
        // Fail closed: a typo can never silently turn a cluster into an inert spoke
        // that indexes nothing. An unrecognised value keeps the default hub role.
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.IndexingRoleKey, raw);

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.Role, Is.EqualTo(RepoContextIndexingRole.Hub));
            Assert.That(options.IndexingEnabled, Is.True);
        });
    }

    [Test]
    public void IndexingEnabled_reflects_the_role_on_a_directly_constructed_options()
    {
        Assert.Multiple(() =>
        {
            Assert.That(new RepoContextIndexingOptions { Role = RepoContextIndexingRole.Hub }.IndexingEnabled, Is.True);
            Assert.That(new RepoContextIndexingOptions { Role = RepoContextIndexingRole.Spoke }.IndexingEnabled, Is.False);
            Assert.That(new RepoContextIndexingOptions().Role, Is.EqualTo(RepoContextIndexingRole.Hub),
                "The default role preserves single-cluster behaviour.");
        });
    }
    [TestCase("CL100K", "cl100k")]
    [TestCase("  cl100k  ", "cl100k")]
    [TestCase("o200k", "o200k")]
    [TestCase("O200K", "o200k")]
    public void FromEnvironment_resolves_a_recognised_tokenizer_profile(string raw, string expected)
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.TokenizerProfileKey, raw);

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.That(options.TokenizerProfile, Is.EqualTo(expected));
    }

    [Test]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("gpt2")]
    [TestCase("p50k")]
    [TestCase("not-a-profile")]
    public void FromEnvironment_falls_back_to_the_default_profile_for_an_absent_or_unrecognised_value(string raw)
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.TokenizerProfileKey, raw);

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.That(options.TokenizerProfile, Is.EqualTo(RepoContextIndexingOptions.TokenizerProfileO200k));
    }

    [Test]
    public void The_shipped_defaults_leave_room_for_pruning_to_engage()
    {
        // A reconcile prunes only while less than FullWalkInterval has elapsed
        // since the last full sweep, and consecutive reconciles are never closer
        // together than ReconcileInterval plus up to ReconcileIntervalJitter. If
        // the full-walk interval does not strictly exceed that spacing, every
        // reconcile is forced full and the prune cache is written but never acted
        // on - a silent, whole-feature regression with no other symptom.
        var defaults = new RepoContextIndexingOptions();

        Assert.Multiple(() =>
        {
            Assert.That(
                defaults.FullWalkInterval,
                Is.GreaterThan(defaults.ReconcileInterval + defaults.ReconcileIntervalJitter),
                "the default full-walk interval must exceed the widest reconcile spacing");
            Assert.That(defaults.PruningCanEngage, Is.True);
        });
    }

    [Test]
    public void PruningCanEngage_is_false_when_the_full_walk_interval_cannot_be_outlived()
    {
        var options = new RepoContextIndexingOptions
        {
            ReconcileInterval = TimeSpan.FromMinutes(15),
            ReconcileIntervalJitter = TimeSpan.FromMinutes(5),
            FullWalkInterval = TimeSpan.FromMinutes(20),
        };

        // Exactly equal is still dead: the elapsed-since-full-sweep comparison is
        // inclusive, so a reconcile landing at the boundary forces a full sweep.
        Assert.That(options.PruningCanEngage, Is.False);
    }
}
