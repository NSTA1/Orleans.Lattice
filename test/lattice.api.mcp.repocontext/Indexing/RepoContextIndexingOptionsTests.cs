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
        RepoContextIndexingOptions.EmbeddingGapScanIntervalSecondsKey,
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
            Assert.That(options.EmbeddingGapScanInterval, Is.EqualTo(defaults.EmbeddingGapScanInterval));
        });
    }

    [Test]
    public void FromEnvironment_reads_each_variable_in_seconds()
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.TickIntervalSecondsKey, "5");
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileIntervalSecondsKey, "5");
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileJitterSecondsKey, "0");
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.FullWalkIntervalSecondsKey, "120");
        Environment.SetEnvironmentVariable(
            RepoContextIndexingOptions.EmbeddingGapScanIntervalSecondsKey, "600");

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.TickInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.ReconcileInterval, Is.EqualTo(TimeSpan.FromSeconds(5)));
            Assert.That(options.ReconcileIntervalJitter, Is.EqualTo(TimeSpan.Zero));
            Assert.That(options.FullWalkInterval, Is.EqualTo(TimeSpan.FromSeconds(120)));
            Assert.That(options.EmbeddingGapScanInterval, Is.EqualTo(TimeSpan.FromSeconds(600)));
        });
    }

    // --- The pass-count deadlines the reconcile actually enforces (issue #2048) ---

    [Test]
    public void An_interval_is_expressed_as_a_pass_count_at_the_widest_reconcile_spacing()
    {
        // The reconcile is single-flight, so the deadline it can enforce is a count
        // of passes, not a wall clock. A pass count is derived by rounding the
        // configured interval up against the widest spacing the scheduler produces.
        var options = new RepoContextIndexingOptions
        {
            ReconcileInterval = TimeSpan.FromSeconds(10),
            ReconcileIntervalJitter = TimeSpan.FromSeconds(10),
            FullWalkInterval = TimeSpan.FromSeconds(100),
            EmbeddingGapScanInterval = TimeSpan.FromSeconds(41),
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.MaximumReconcileSpacing, Is.EqualTo(TimeSpan.FromSeconds(20)));
            Assert.That(options.PassesPerFullWalk, Is.EqualTo(5));
            Assert.That(options.PassesPerEmbeddingGapScan, Is.EqualTo(3), "41s over 20s rounds up to 3 passes.");
        });
    }

    [Test]
    [TestCase(0)]
    [TestCase(-30)]
    public void A_non_positive_interval_degenerates_to_every_pass(int seconds)
    {
        // Rounding up must never yield zero: a zero deadline would mean "never
        // force", silently disabling the safety net instead of tightening it.
        var options = new RepoContextIndexingOptions
        {
            FullWalkInterval = TimeSpan.FromSeconds(seconds),
            EmbeddingGapScanInterval = TimeSpan.FromSeconds(seconds),
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.PassesPerFullWalk, Is.EqualTo(1));
            Assert.That(options.PassesPerEmbeddingGapScan, Is.EqualTo(1));
        });
    }

    [Test]
    public void Pruning_cannot_engage_when_every_pass_is_a_forced_full_walk()
    {
        // Issue #2048: a full-walk interval that does not outlive the reconcile
        // spacing makes every pass a forced full sweep, so the prune snapshot is
        // written every run and never read. That is dead code with no other symptom,
        // and this is the property that names it.
        var tooTight = new RepoContextIndexingOptions
        {
            ReconcileInterval = TimeSpan.FromSeconds(20),
            ReconcileIntervalJitter = TimeSpan.Zero,
            FullWalkInterval = TimeSpan.FromSeconds(20),
        };

        Assert.Multiple(() =>
        {
            Assert.That(tooTight.PassesPerFullWalk, Is.EqualTo(1));
            Assert.That(tooTight.PruningCanEngage, Is.False);
            Assert.That(new RepoContextIndexingOptions().PruningCanEngage, Is.True, "the shipped defaults prune");
        });
    }

    [Test]
    public void A_degenerate_reconcile_spacing_reports_that_pruning_cannot_engage()
    {
        // With no spacing at all there is no meaningful pass budget, so the honest
        // answer is that pruning cannot engage rather than a deadline nobody meets.
        var options = new RepoContextIndexingOptions
        {
            ReconcileInterval = TimeSpan.Zero,
            ReconcileIntervalJitter = TimeSpan.Zero,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.PassesPerFullWalk, Is.EqualTo(1));
            Assert.That(options.PruningCanEngage, Is.False);
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
