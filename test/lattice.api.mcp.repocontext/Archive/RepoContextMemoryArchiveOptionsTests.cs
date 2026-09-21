using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Archive;

/// <summary>
/// Unit tests for the durable-memory archive's configuration: which environment
/// variables it reads, what it does with a value it cannot use, and the clamps that
/// stop a misconfiguration turning the exporter into a busy loop or into a shutdown
/// that outstays its grace period.
/// <para>
/// Marked <c>NonParallelizable</c> because it mutates process environment variables,
/// which are global to the test host.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class RepoContextMemoryArchiveOptionsTests
{
    private static readonly string[] Keys =
    [
        RepoContextMemoryArchiveOptions.DirectoryKey,
        RepoContextMemoryArchiveOptions.IntervalSecondsKey,
        RepoContextMemoryArchiveOptions.RestoreKey,
        RepoContextMemoryArchiveOptions.StopTimeoutSecondsKey,
    ];

    private readonly Dictionary<string, string?> saved = [];

    [SetUp]
    public void SaveEnvironment()
    {
        foreach (var key in Keys)
        {
            saved[key] = Environment.GetEnvironmentVariable(key);
            Environment.SetEnvironmentVariable(key, null);
        }
    }

    [TearDown]
    public void RestoreEnvironment()
    {
        foreach (var (key, value) in saved)
        {
            Environment.SetEnvironmentVariable(key, value);
        }

        saved.Clear();
    }

    [Test]
    public void Archive_is_disabled_when_no_directory_is_configured()
    {
        var options = RepoContextMemoryArchiveOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.IsEnabled, Is.False);
            Assert.That(options.Directory, Is.Null);
        });
    }

    [Test]
    public void A_blank_directory_disables_the_archive_rather_than_naming_an_empty_path()
    {
        Environment.SetEnvironmentVariable(RepoContextMemoryArchiveOptions.DirectoryKey, "   ");

        var options = RepoContextMemoryArchiveOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.IsEnabled, Is.False);
            Assert.That(options.Directory, Is.Null);
        });
    }

    [Test]
    public void A_configured_directory_enables_the_archive_and_is_trimmed()
    {
        Environment.SetEnvironmentVariable(
            RepoContextMemoryArchiveOptions.DirectoryKey, "  /memory-archive  ");

        var options = RepoContextMemoryArchiveOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.IsEnabled, Is.True);
            Assert.That(options.Directory, Is.EqualTo("/memory-archive"));
        });
    }

    [Test]
    public void Defaults_are_a_five_minute_cadence_auto_restore_and_a_twenty_second_stop_budget()
    {
        var options = RepoContextMemoryArchiveOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.EffectiveInterval, Is.EqualTo(TimeSpan.FromMinutes(5)));
            Assert.That(options.RestoreMode, Is.EqualTo(RepoContextMemoryArchiveRestoreMode.Auto));
            Assert.That(options.EffectiveStopTimeout, Is.EqualTo(TimeSpan.FromSeconds(20)));
        });
    }

    [Test]
    public void An_interval_below_the_floor_is_raised_to_it()
    {
        Environment.SetEnvironmentVariable(RepoContextMemoryArchiveOptions.IntervalSecondsKey, "1");

        var options = RepoContextMemoryArchiveOptions.FromEnvironment();

        Assert.Multiple(() =>
        {
            Assert.That(options.Interval, Is.EqualTo(TimeSpan.FromSeconds(1)), "the declared value is preserved");
            Assert.That(
                options.EffectiveInterval,
                Is.EqualTo(RepoContextMemoryArchiveOptions.MinimumInterval),
                "the value actually in force is the floor");
        });
    }

    [Test]
    public void An_interval_above_the_floor_is_honoured()
    {
        Environment.SetEnvironmentVariable(RepoContextMemoryArchiveOptions.IntervalSecondsKey, "600");

        Assert.That(
            RepoContextMemoryArchiveOptions.FromEnvironment().EffectiveInterval,
            Is.EqualTo(TimeSpan.FromMinutes(10)));
    }

    [TestCase("nonsense")]
    [TestCase("0")]
    [TestCase("-30")]
    public void An_unusable_interval_falls_back_to_the_default_rather_than_failing_the_host(string raw)
    {
        Environment.SetEnvironmentVariable(RepoContextMemoryArchiveOptions.IntervalSecondsKey, raw);

        Assert.That(
            RepoContextMemoryArchiveOptions.FromEnvironment().EffectiveInterval,
            Is.EqualTo(TimeSpan.FromMinutes(5)));
    }

    [Test]
    public void The_interval_is_parsed_invariantly_so_a_comma_decimal_locale_does_not_change_it()
    {
        var original = Thread.CurrentThread.CurrentCulture;
        try
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("de-DE");
            Environment.SetEnvironmentVariable(
                RepoContextMemoryArchiveOptions.IntervalSecondsKey, "90.5");

            Assert.That(
                RepoContextMemoryArchiveOptions.FromEnvironment().EffectiveInterval,
                Is.EqualTo(TimeSpan.FromSeconds(90.5)));
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = original;
        }
    }

    // The expectation travels as a string rather than the enum: the enum is internal,
    // and an internal parameter type on a public fixture method is a compile error.
    [TestCase("off", "Off")]
    [TestCase("OFF", "Off")]
    [TestCase("none", "Off")]
    [TestCase("false", "Off")]
    [TestCase("auto", "Auto")]
    [TestCase("on-empty", "Auto")]
    [TestCase(" always ", "Always")]
    public void The_restore_mode_is_parsed_case_insensitively(string raw, string expected)
    {
        Environment.SetEnvironmentVariable(RepoContextMemoryArchiveOptions.RestoreKey, raw);

        Assert.That(
            RepoContextMemoryArchiveOptions.FromEnvironment().RestoreMode.ToString(),
            Is.EqualTo(expected));
    }

    [Test]
    public void An_unrecognised_restore_mode_falls_back_to_auto()
    {
        Environment.SetEnvironmentVariable(RepoContextMemoryArchiveOptions.RestoreKey, "sometimes");

        Assert.That(
            RepoContextMemoryArchiveOptions.FromEnvironment().RestoreMode,
            Is.EqualTo(RepoContextMemoryArchiveRestoreMode.Auto));
    }

    [Test]
    public void A_stop_budget_above_the_ceiling_is_clamped_so_it_cannot_outstay_a_grace_period()
    {
        Environment.SetEnvironmentVariable(
            RepoContextMemoryArchiveOptions.StopTimeoutSecondsKey, "600");

        Assert.That(
            RepoContextMemoryArchiveOptions.FromEnvironment().EffectiveStopTimeout,
            Is.EqualTo(RepoContextMemoryArchiveOptions.MaximumStopTimeout));
    }

    [Test]
    public void A_stop_budget_below_the_floor_is_raised_to_it()
    {
        var options = new RepoContextMemoryArchiveOptions { StopTimeout = TimeSpan.Zero };

        Assert.That(
            options.EffectiveStopTimeout,
            Is.EqualTo(RepoContextMemoryArchiveOptions.MinimumStopTimeout));
    }

    [Test]
    public void The_stop_budget_stays_a_fraction_of_the_containers_grace_period()
    {
        // Not an arbitrary bound. The final export runs inside the container's
        // stop_grace_period, which the store's own drain is also budgeted from
        // (issue #2598). A ceiling at or above that grace period would let the
        // archive spend the whole of a budget it does not own.
        Assert.That(
            RepoContextMemoryArchiveOptions.MaximumStopTimeout,
            Is.LessThan(TimeSpan.FromSeconds(120)));
    }
}
