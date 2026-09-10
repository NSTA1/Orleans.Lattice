namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Tests for the approximate-index build sweep's own cadence knob (issue #2459).
/// <para>
/// The sweep used to take its interval from <c>LATTICE_RECONCILE_INTERVAL_SECONDS</c>,
/// which paces an unrelated subsystem. These tests pin the separation itself, not just
/// the new default: the assertion that matters most is
/// <see cref="The_sweep_cadence_does_not_move_when_the_reconcile_interval_does"/>,
/// because that is the property whose absence was the defect, and it is the one a future
/// simplification ("both of these are cadences, why two knobs?") would silently undo.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextAnnSweepIntervalOptionTests
{
    private static readonly string[] TouchedKeys =
    [
        RepoContextIndexingOptions.AnnSweepIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileIntervalSecondsKey,
    ];

    [SetUp]
    [TearDown]
    public void ClearEnvironment()
    {
        foreach (var key in TouchedKeys)
        {
            Environment.SetEnvironmentVariable(key, null);
        }
    }

    [Test]
    public void The_sweep_cadence_does_not_move_when_the_reconcile_interval_does()
    {
        // The regression test for #2459. A day-long reconcile interval is the exact
        // setting observed on the deployment that could not build an index: it is a
        // reasonable thing for an operator to configure, and it used to throttle
        // arming by the same factor - 86400s against the 60s floor, a factor of 1440.
        var quiesced = new RepoContextIndexingOptions { ReconcileInterval = TimeSpan.FromHours(24) };
        var busy = new RepoContextIndexingOptions { ReconcileInterval = TimeSpan.FromSeconds(1) };

        Assert.Multiple(() =>
        {
            Assert.That(quiesced.EffectiveAnnSweepInterval, Is.EqualTo(TimeSpan.FromMinutes(15)),
                "Raising the reconcile interval must not throttle arming: for a converged repository "
                + "whose index was never built there is no vectorising pass, so the sweep is its only "
                + "arming path.");
            Assert.That(busy.EffectiveAnnSweepInterval, Is.EqualTo(TimeSpan.FromMinutes(15)),
                "Lowering it must not speed the sweep either - the coupling was wrong in both "
                + "directions, and a host sweeping every minute was doing so by accident.");
        });
    }

    [Test]
    public void The_default_is_the_reconcile_interval_default_so_an_unconfigured_host_is_unchanged()
    {
        var defaults = new RepoContextIndexingOptions();

        Assert.That(defaults.AnnSweepInterval, Is.EqualTo(defaults.ReconcileInterval),
            "Decoupling the two knobs must not change any behaviour on a host that sets neither. "
            + "Equality here is the whole argument for choosing 15 minutes rather than a tighter "
            + "value: it makes this change a no-op for every deployment except the ones the "
            + "coupling was mistreating.");
    }

    [Test]
    public void FromEnvironment_reads_the_sweep_interval_in_seconds()
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.AnnSweepIntervalSecondsKey, "300");

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.AnnSweepInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(options.EffectiveAnnSweepInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(options.AnnSweepIntervalIsFloored, Is.False);
        });
    }

    [Test]
    public void FromEnvironment_defaults_the_sweep_interval_when_the_variable_is_absent()
    {
        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.That(options.AnnSweepInterval,
            Is.EqualTo(new RepoContextIndexingOptions().AnnSweepInterval));
    }

    [Test]
    [TestCase("not-a-number")]
    [TestCase("-30")]
    [TestCase("")]
    public void FromEnvironment_falls_back_to_the_default_for_an_unusable_value(string value)
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.AnnSweepIntervalSecondsKey, value);

        var options = RepoContextIndexingOptions.FromEnvironment();

        Assert.That(options.AnnSweepInterval,
            Is.EqualTo(new RepoContextIndexingOptions().AnnSweepInterval),
            "A malformed cadence must not become a zero-second sweep, which would be a hot loop of "
            + "grain calls rather than a misconfiguration.");
    }

    [Test]
    public void A_configured_interval_below_the_floor_is_raised_to_it()
    {
        var options = new RepoContextIndexingOptions { AnnSweepInterval = TimeSpan.FromSeconds(10) };

        Assert.Multiple(() =>
        {
            Assert.That(options.AnnSweepInterval, Is.EqualTo(TimeSpan.FromSeconds(10)),
                "The configured value is preserved, so the report can say what the operator asked for.");
            Assert.That(options.EffectiveAnnSweepInterval,
                Is.EqualTo(RepoContextIndexingOptions.MinimumAnnSweepInterval),
                "The effective value is what the sweep runs at.");
            Assert.That(options.AnnSweepIntervalIsFloored, Is.True,
                "Which of the two decided the outcome is exactly what an operator who set 10s and "
                + "observes 60s needs told, and cannot infer from either number alone.");
        });
    }

    [Test]
    public void A_zero_or_negative_interval_is_raised_to_the_floor_rather_than_disabling_the_sweep()
    {
        // Distinct from the malformed-value case above: this is a value that parsed and
        // was assigned directly. It must not be read as "never sweep", because the sweep
        // is the only arming path for a repository with no vectorising pass in flight,
        // so a silent disable is indistinguishable from the #2406 failure it would cause.
        var zero = new RepoContextIndexingOptions { AnnSweepInterval = TimeSpan.Zero };
        var negative = new RepoContextIndexingOptions { AnnSweepInterval = TimeSpan.FromSeconds(-5) };

        Assert.Multiple(() =>
        {
            Assert.That(zero.EffectiveAnnSweepInterval,
                Is.EqualTo(RepoContextIndexingOptions.MinimumAnnSweepInterval));
            Assert.That(negative.EffectiveAnnSweepInterval,
                Is.EqualTo(RepoContextIndexingOptions.MinimumAnnSweepInterval));
            Assert.That(zero.AnnSweepIntervalIsFloored, Is.True);
            Assert.That(negative.AnnSweepIntervalIsFloored, Is.True);
        });
    }

    [Test]
    public void The_floor_is_a_minute_and_is_not_itself_configurable()
    {
        Assert.That(RepoContextIndexingOptions.MinimumAnnSweepInterval,
            Is.EqualTo(TimeSpan.FromMinutes(1)),
            "Arming is idempotent and cheap, but it is one grain call and one reminder "
            + "re-registration per registered repository, so the floor is a real bound and not "
            + "decoration.");
    }

    [Test]
    public void An_interval_exactly_on_the_floor_reports_as_floored()
    {
        // The boundary is deliberately inclusive: at equality the two candidate values
        // are identical, so nothing observable turns on it except the provenance clause,
        // and attributing it to the floor is the honest reading of a value that could not
        // have gone lower.
        var options = new RepoContextIndexingOptions
        {
            AnnSweepInterval = RepoContextIndexingOptions.MinimumAnnSweepInterval,
        };

        Assert.Multiple(() =>
        {
            Assert.That(options.EffectiveAnnSweepInterval,
                Is.EqualTo(RepoContextIndexingOptions.MinimumAnnSweepInterval));
            Assert.That(options.AnnSweepIntervalIsFloored, Is.True);
        });
    }
}
