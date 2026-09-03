using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="AtomicLeafWalk"/>, the diagnostic that makes a
/// deliberately unbounded leaf-chain walk attributable (issue 1956).
/// <para>
/// These walks cannot be work-bounded: for each of them the fact that no other
/// message runs on the shard between the first and last leaf is exactly the
/// invariant the surrounding protocol depends on. Since the stall cannot be
/// removed, it is instead made explainable, so a shard held for minutes does
/// not surface only as a flood of Orleans warnings naming the blocked messages
/// rather than the blocker (issue 1953).
/// </para>
/// </summary>
[TestFixture]
public class AtomicLeafWalkTests
{
    private sealed class CapturingLogger : ILogger
    {
        public List<string> Warnings { get; } = [];

        IDisposable? ILogger.BeginScope<TState>(TState state) => null;
        public bool IsEnabled(LogLevel logLevel) => true;
        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (logLevel == LogLevel.Warning)
                Warnings.Add(formatter(state, exception));
        }
    }

    private static GrainId AnyShard() => GrainId.Create("shard", "tree/0");

    [Test]
    public void A_walk_under_the_threshold_reports_nothing()
    {
        var logger = new CapturingLogger();
        var walk = new AtomicLeafWalk("TestOp", TimeSpan.FromHours(1));
        walk.RecordLeafVisited();

        walk.ReportIfSlow(logger, AnyShard());

        Assert.That(logger.Warnings, Is.Empty);
    }

    [Test]
    public void A_walk_over_the_threshold_reports_the_operation_and_leaf_count()
    {
        var logger = new CapturingLogger();
        var walk = new AtomicLeafWalk("MarkLeavesMovedAwayAsync", TimeSpan.Zero);
        for (var i = 0; i < 17; i++)
            walk.RecordLeafVisited();

        walk.ReportIfSlow(logger, AnyShard());

        Assert.That(logger.Warnings, Has.Count.EqualTo(1));
        Assert.Multiple(() =>
        {
            Assert.That(logger.Warnings[0], Does.Contain("MarkLeavesMovedAwayAsync"),
                "the warning must name the walk, since that is what Orleans' own " +
                "long-request warnings cannot say");
            Assert.That(logger.Warnings[0], Does.Contain("17"),
                "the leaf count is what explains the duration");
        });
    }

    [Test]
    public void Reporting_tolerates_a_null_logger()
    {
        var walk = new AtomicLeafWalk("TestOp", TimeSpan.Zero);
        walk.RecordLeafVisited();

        Assert.That(() => walk.ReportIfSlow(null!, AnyShard()), Throws.Nothing,
            "the monitor is a diagnostic and must never alter a walk's outcome");
    }

    [Test]
    public void LeavesVisited_counts_every_recorded_leaf()
    {
        var walk = new AtomicLeafWalk("TestOp");

        for (var i = 0; i < 5; i++)
            walk.RecordLeafVisited();

        Assert.That(walk.LeavesVisited, Is.EqualTo(5));
    }

    /// <summary>
    /// A call site whose walk is executed by a shared helper already has the
    /// leaf count in hand, so it hands the whole tally over at once rather than
    /// replaying it one increment at a time (issue 1973).
    /// </summary>
    [Test]
    public void RecordLeavesVisited_adds_a_whole_tally_at_once()
    {
        var walk = new AtomicLeafWalk("TestOp");

        walk.RecordLeafVisited();
        walk.RecordLeavesVisited(12);

        Assert.That(walk.LeavesVisited, Is.EqualTo(13));
    }

    /// <summary>
    /// The threshold has to land below Orleans' own long-request warning
    /// (<c>MaxWarningRequestProcessingTime</c> = <c>ResponseTimeout x 5</c>,
    /// 150s by default) so this warning explains the flood rather than
    /// arriving after it.
    /// </summary>
    [Test]
    public void The_default_threshold_precedes_the_Orleans_long_request_warning()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AtomicLeafWalk.WarnAfter, Is.LessThan(TimeSpan.FromSeconds(150)));
            Assert.That(AtomicLeafWalk.WarnAfter, Is.GreaterThan(TimeSpan.Zero));
        });
    }
}
