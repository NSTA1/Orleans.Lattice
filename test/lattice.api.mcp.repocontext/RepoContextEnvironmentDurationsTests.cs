using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Tests for <see cref="RepoContextEnvironmentDurations"/> and the three option
/// readers built on it (<see cref="RepoContextMemoryArchiveOptions"/>,
/// <see cref="RepoContextIndexingOptions"/>, and <see cref="RepoContextAnnOptions"/>).
/// Each reader documents that an unusable value falls back to its default rather than
/// failing the host, but a number too large for a <see cref="TimeSpan"/> -
/// <c>Infinity</c> in two of them, or a finite value such as <c>1e20</c> in all three -
/// reached <see cref="TimeSpan.FromSeconds(double)"/> and threw
/// <see cref="OverflowException"/> while the host's services were being registered.
/// <para>
/// Marked <c>NonParallelizable</c> because it mutates process environment variables,
/// which are global to the test host.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class RepoContextEnvironmentDurationsTests
{
    private const string ScratchKey = "LATTICE_REPOCONTEXT_TEST_DURATION_SECONDS";

    private static readonly string[] Keys =
    [
        ScratchKey,
        RepoContextMemoryArchiveOptions.IntervalSecondsKey,
        RepoContextMemoryArchiveOptions.StopTimeoutSecondsKey,
        RepoContextIndexingOptions.TickIntervalSecondsKey,
        RepoContextIndexingOptions.ReconcileIntervalSecondsKey,
        RepoContextAnnOptions.OpenSliceBudgetSecondsVariable,
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

    // Values no TimeSpan can hold. The first three are finite, so a guard that only
    // excluded infinity (as the archive reader's did) still let them through.
    private static readonly string[] UnrepresentableSeconds =
    [
        "1e20",
        "922337203686",
        "1.7976931348623157E+308",
        "Infinity",
    ];

    [TestCaseSource(nameof(UnrepresentableSeconds))]
    public void A_value_too_large_for_a_timespan_is_not_a_usable_duration(string raw)
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                RepoContextEnvironmentDurations.TryParseSeconds(raw, allowZero: true, out var value),
                Is.False);
            Assert.That(value, Is.EqualTo(TimeSpan.Zero));
            Assert.That(
                () => TimeSpan.FromSeconds(double.Parse(raw, CultureInfo.InvariantCulture)),
                Throws.InstanceOf<OverflowException>(),
                "anti-vacuity: the rejected value is one TimeSpan itself cannot hold");
        });
    }

    [TestCase(null)]
    [TestCase("")]
    [TestCase("   ")]
    [TestCase("soon")]
    [TestCase("NaN")]
    [TestCase("-Infinity")]
    [TestCase("-1")]
    public void An_absent_malformed_or_negative_value_is_not_a_usable_duration(string? raw)
        => Assert.That(RepoContextEnvironmentDurations.TryParseSeconds(raw, allowZero: true, out _), Is.False);

    [Test]
    public void Zero_is_usable_only_where_the_reader_allows_it()
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextEnvironmentDurations.TryParseSeconds("0", allowZero: true, out var zero), Is.True);
            Assert.That(zero, Is.EqualTo(TimeSpan.Zero));
            Assert.That(RepoContextEnvironmentDurations.TryParseSeconds("0", allowZero: false, out _), Is.False);
        });
    }

    [TestCase("90", 90d)]
    [TestCase("90.5", 90.5d)]
    [TestCase("1e6", 1_000_000d)]
    [TestCase(" 45 ", 45d)]
    public void A_representable_positive_value_is_read_in_seconds(string raw, double expectedSeconds)
    {
        Assert.Multiple(() =>
        {
            Assert.That(RepoContextEnvironmentDurations.TryParseSeconds(raw, allowZero: false, out var value), Is.True);
            Assert.That(value, Is.EqualTo(TimeSpan.FromSeconds(expectedSeconds)));
        });
    }

    [Test]
    public void The_value_is_parsed_invariantly_so_a_comma_decimal_locale_does_not_change_it()
    {
        var original = Thread.CurrentThread.CurrentCulture;
        try
        {
            Thread.CurrentThread.CurrentCulture = new CultureInfo("de-DE");

            Assert.Multiple(() =>
            {
                Assert.That(
                    RepoContextEnvironmentDurations.TryParseSeconds("90.5", allowZero: false, out var value),
                    Is.True);
                Assert.That(value, Is.EqualTo(TimeSpan.FromSeconds(90.5)));
            });
        }
        finally
        {
            Thread.CurrentThread.CurrentCulture = original;
        }
    }

    [Test]
    public void ReadSeconds_returns_the_fallback_for_an_absent_or_unusable_variable()
    {
        var fallback = TimeSpan.FromMinutes(7);

        var absent = RepoContextEnvironmentDurations.ReadSeconds(ScratchKey, fallback, allowZero: true);
        Environment.SetEnvironmentVariable(ScratchKey, "1e20");
        var unrepresentable = RepoContextEnvironmentDurations.ReadSeconds(ScratchKey, fallback, allowZero: true);
        Environment.SetEnvironmentVariable(ScratchKey, "12");
        var configured = RepoContextEnvironmentDurations.ReadSeconds(ScratchKey, fallback, allowZero: true);

        Assert.Multiple(() =>
        {
            Assert.That(absent, Is.EqualTo(fallback));
            Assert.That(unrepresentable, Is.EqualTo(fallback));
            Assert.That(configured, Is.EqualTo(TimeSpan.FromSeconds(12)));
        });
    }

    [TestCaseSource(nameof(UnrepresentableSeconds))]
    public void The_archive_reader_falls_back_rather_than_failing_the_host(string raw)
    {
        Environment.SetEnvironmentVariable(RepoContextMemoryArchiveOptions.IntervalSecondsKey, raw);
        Environment.SetEnvironmentVariable(RepoContextMemoryArchiveOptions.StopTimeoutSecondsKey, raw);
        var defaults = new RepoContextMemoryArchiveOptions();

        RepoContextMemoryArchiveOptions? options = null;
        Assert.That(() => options = RepoContextMemoryArchiveOptions.FromEnvironment(), Throws.Nothing);
        Assert.Multiple(() =>
        {
            Assert.That(options!.Interval, Is.EqualTo(defaults.Interval));
            Assert.That(options.StopTimeout, Is.EqualTo(defaults.StopTimeout));
        });
    }

    [TestCaseSource(nameof(UnrepresentableSeconds))]
    public void The_indexing_reader_falls_back_rather_than_failing_the_host(string raw)
    {
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.TickIntervalSecondsKey, raw);
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileIntervalSecondsKey, raw);
        var defaults = new RepoContextIndexingOptions();

        RepoContextIndexingOptions? options = null;
        Assert.That(() => options = RepoContextIndexingOptions.FromEnvironment(), Throws.Nothing);
        Assert.Multiple(() =>
        {
            Assert.That(options!.TickInterval, Is.EqualTo(defaults.TickInterval));
            Assert.That(options.ReconcileInterval, Is.EqualTo(defaults.ReconcileInterval));
        });
    }

    [Test]
    public void The_indexing_reader_still_honours_a_huge_but_representable_reconcile_interval()
    {
        // A very large reconcile interval is a legitimate way to ask for the rarest
        // cadence; the self-index grain saturates its tick arithmetic for exactly this.
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileIntervalSecondsKey, "1e9");

        Assert.That(
            RepoContextIndexingOptions.FromEnvironment().ReconcileInterval,
            Is.EqualTo(TimeSpan.FromSeconds(1e9)));
    }

    [TestCaseSource(nameof(UnrepresentableSeconds))]
    public void The_ann_reader_falls_back_rather_than_failing_the_host(string raw)
    {
        Environment.SetEnvironmentVariable(RepoContextAnnOptions.OpenSliceBudgetSecondsVariable, raw);

        RepoContextAnnOptions? options = null;
        Assert.That(() => options = RepoContextAnnOptions.FromEnvironment(), Throws.Nothing);
        Assert.That(options!.OpenSliceBudget, Is.EqualTo(new RepoContextAnnOptions().OpenSliceBudget));
    }

    [Test]
    public void The_resolved_settings_report_survives_an_unrepresentable_value()
    {
        // DescribeResolvedSettings is public and resolves every reader, so it threw too.
        Environment.SetEnvironmentVariable(RepoContextIndexingOptions.ReconcileIntervalSecondsKey, "1e20");

        Assert.That(() => RepoContextEnvironmentVariables.DescribeResolvedSettings(), Throws.Nothing);
    }
}
