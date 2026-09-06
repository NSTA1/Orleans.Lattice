using Microsoft.Extensions.Logging;
using NUnit.Framework;
using Orleans.Lattice.Api.Mcp.RepoContext;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Covers the startup cadence report: the wall-clock indexing knobs are converted to
/// pass counts against the reconcile spacing, so they are a matched set, and raising
/// the reconcile interval can switch directory-modification-time pruning off with no
/// error and nothing in the log. See issue #2075.
/// </summary>
[TestFixture]
public sealed class RepoContextIndexingCadenceReporterTests
{
    private static (IReadOnlyCollection<CapturedLogEntry> Entries, RepoContextIndexingOptions Options)
        Report(int reconcileSeconds, int fullWalkSeconds, int gapScanSeconds)
    {
        var options = new RepoContextIndexingOptions
        {
            ReconcileInterval = TimeSpan.FromSeconds(reconcileSeconds),
            ReconcileIntervalJitter = TimeSpan.Zero,
            FullWalkInterval = TimeSpan.FromSeconds(fullWalkSeconds),
            EmbeddingGapScanInterval = TimeSpan.FromSeconds(gapScanSeconds),
        };

        var provider = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(b => b.AddProvider(provider));

        var reporter = new RepoContextIndexingCadenceReporter(
            options, factory.CreateLogger<RepoContextIndexingCadenceReporter>());

        reporter.StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        return (provider.Entries, options);
    }

    [Test]
    public void The_shipped_container_cadence_reports_its_derived_pass_counts()
    {
        // The container defaults: 5 / 120 / 300. The operator sets seconds; the
        // reconcile enforces passes. Both must appear on the same line, because the
        // whole failure mode of #2075 is that the conversion is invisible.
        var (entries, options) = Report(5, 120, 300);

        Assert.That(options.PassesPerFullWalk, Is.EqualTo(24));
        Assert.That(options.PassesPerEmbeddingGapScan, Is.EqualTo(60));
        Assert.That(options.PruningCanEngage, Is.True);

        var line = entries.SingleOrDefault(e => e.Level == LogLevel.Information);
        Assert.That(line, Is.Not.Null, "The cadence must be reported once at startup.");
        Assert.That(line!.Message, Does.Contain("120").And.Contain("24 pass(es)"),
            "The full walk must show both its configured seconds and its derived passes.");
        Assert.That(line.Message, Does.Contain("300").And.Contain("60 pass(es)"),
            "The gap scan must show both its configured seconds and its derived passes.");
        Assert.That(line.Message, Does.Contain("pruning can engage: True"));
    }

    [Test]
    public void Raising_only_the_reconcile_interval_warns_that_it_has_disabled_pruning()
    {
        // The exact trap from #2075: raise the reconcile interval to 300 s and change
        // nothing else. ceil(120/300) floors to 1 pass, PruningCanEngage goes false,
        // and directory-mtime pruning silently stops - the inert state #2052 fixed.
        var (entries, options) = Report(300, 120, 300);

        Assert.That(options.PassesPerFullWalk, Is.EqualTo(1),
            "A full walk shorter than the reconcile spacing floors to one pass.");
        Assert.That(options.PruningCanEngage, Is.False);

        var warning = entries.SingleOrDefault(e => e.Level == LogLevel.Warning);
        Assert.That(warning, Is.Not.Null,
            "Pruning being disabled by the arithmetic must not be silent.");
        Assert.That(warning!.Message, Does.Contain("DISABLED"));
        Assert.That(warning.Message, Does.Contain("matched set"),
            "The warning must say why, or the operator cannot act on it.");
        Assert.That(warning.Message, Does.Contain("600"),
            "The warning must name a full walk interval that would re-enable pruning.");
    }

    [Test]
    public void A_cadence_that_prunes_reports_no_warning()
    {
        // The negative control. Without this, a reporter that warned unconditionally
        // would pass the test above while telling an operator nothing.
        var (entries, options) = Report(300, 900, 3600);

        Assert.That(options.PassesPerFullWalk, Is.EqualTo(3));
        Assert.That(options.PruningCanEngage, Is.True);
        Assert.That(entries.Where(e => e.Level == LogLevel.Warning), Is.Empty,
            "A cadence that can prune must not warn.");
    }

    [Test]
    public void The_report_accounts_for_jitter_in_the_spacing()
    {
        // The conversion divides by ReconcileInterval + ReconcileIntervalJitter, so
        // jitter alone can change the derived pass count. A report that ignored it
        // would print arithmetic the reconcile does not actually use.
        var options = new RepoContextIndexingOptions
        {
            ReconcileInterval = TimeSpan.FromSeconds(5),
            ReconcileIntervalJitter = TimeSpan.FromSeconds(5),
            FullWalkInterval = TimeSpan.FromSeconds(120),
            EmbeddingGapScanInterval = TimeSpan.FromSeconds(300),
        };

        Assert.That(options.MaximumReconcileSpacing, Is.EqualTo(TimeSpan.FromSeconds(10)));
        Assert.That(options.PassesPerFullWalk, Is.EqualTo(12),
            "Jitter widens the spacing, which halves the derived pass count here.");

        var provider = new CapturingLoggerProvider();
        using var factory = LoggerFactory.Create(b => b.AddProvider(provider));
        new RepoContextIndexingCadenceReporter(
                options, factory.CreateLogger<RepoContextIndexingCadenceReporter>())
            .StartAsync(CancellationToken.None).GetAwaiter().GetResult();

        var line = provider.Entries.Single(e => e.Level == LogLevel.Information);
        Assert.That(line.Message, Does.Contain("12 pass(es)"));
        Assert.That(line.Message, Does.Contain("including jitter"),
            "The reported spacing must be the one the conversion actually divides by.");
    }
}
