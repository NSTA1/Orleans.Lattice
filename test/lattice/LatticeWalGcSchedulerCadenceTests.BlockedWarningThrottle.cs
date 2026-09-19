using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Tests for the per-episode throttle on the WAL GC cannot-reclaim warning
/// (issue #2815).
/// <para>
/// The warning used to fire once per <i>reported blocker</i>, which is no
/// throttle at all for the population that needs one. A blocked tree is
/// deliberately held at the cadence floor, so a tree whose blockers churn faster
/// than the sweep's minimum block age admitted a new consumer on every pass and
/// emitted the warning at the floor rate indefinitely - about two a minute at
/// stock defaults.
/// </para>
/// <para>
/// That population is the same one the unreachable-block escalation exists for:
/// no blocker holds still long enough to be touched, so no attempt-derived
/// budget can report it however monotonic. It was therefore both invisible to
/// the give-up budget and the loudest thing in the log, on exactly the rigs
/// where a log stream is the only diagnosis available.
/// </para>
/// <para>
/// These fixtures assert both halves of the trade. Throttling is only correct if
/// the identity sequence it stops streaming is paid back somewhere, so the
/// suppression tests are paired with tests that the blocker is still named once,
/// that the churn width and both ends of the sequence survive to the end of the
/// episode, and that the escalation carries the count.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>The distinguishing fragment of the per-episode cannot-reclaim warning.</summary>
    private const string CannotReclaimWarning = "cannot reclaim: durable materialiser pin";

    /// <summary>The distinguishing fragment of the end-of-episode churn summary.</summary>
    private const string BlockerChurnSummary = "is no longer blocked after";

    private static int CountMatching(RecordingLoggerFactory logs, string fragment) =>
        logs.Entries.Count(e => e.Message.Contains(fragment, StringComparison.Ordinal));

    private static RecordedLogEntry SingleMatching(RecordingLoggerFactory logs, string fragment)
    {
        var matches = logs.Entries
            .Where(e => e.Message.Contains(fragment, StringComparison.Ordinal))
            .ToArray();

        Assert.That(matches, Has.Length.EqualTo(1),
            $"expected exactly one log entry containing '{fragment}'.");

        return matches[0];
    }

    /// <summary>
    /// A GC whose floor names a consumer never seen before on every blocked
    /// pass, which is the churn the throttle exists for. Blocked passes append
    /// to <paramref name="reported"/> in admission order, so a test can assert
    /// against both ends of the real sequence rather than against a literal.
    /// </summary>
    private static ILatticeWalGc GcChurningBlockers(List<string> reported, Func<bool> blocked)
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
            {
                if (!blocked())
                {
                    return Task.FromResult(Report(entriesTrimmed: 9));
                }

                var consumerId = ConsumerIdFor($"leaf-{reported.Count}");
                reported.Add(consumerId);
                return Task.FromResult(BlockedReportNaming(consumerId));
            });

        return gc;
    }

    [Test]
    public async Task ExecuteAsync_warns_once_per_episode_when_the_reported_blocker_churns()
    {
        // The defect. Every pass reports a consumer the episode has not seen, so
        // no blocker ever reaches ReactivationMinBlockAge and none is ever
        // touched - the tree is stuck, unreachable by the remedy, and pinned at
        // the cadence floor. Keyed on the blocker identity the warning fired on
        // every one of those passes.
        var reported = new List<string>();
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);
        var (scheduler, logs) = SchedulerWithLogs(factory, GcChurningBlockers(reported, () => true), time);

        await StartAndRunFirstPassAsync(scheduler, time);

        // Stops short of UnreachableBlockEscalation (50 min at stock defaults),
        // so this window isolates the per-blocker warning from the escalation
        // that ExecuteAsync_carries_the_blocker_count_on_the_escalation covers.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(40));

        Assert.Multiple(() =>
        {
            // The non-vacuity anchor. Were the churn not happening - a cadence
            // that stopped advancing, or a floor that reported one stable
            // consumer - a single warning would be the correct output and this
            // fixture would pass while testing nothing.
            Assert.That(reported, Has.Count.GreaterThan(60),
                "the fixture must actually churn its blocker on every pass, or the throttle is not under test.");

            Assert.That(reported.Distinct(StringComparer.Ordinal).Count(), Is.EqualTo(reported.Count),
                "every pass must report an identity the episode has not seen, or the old per-identity key would throttle it too.");

            Assert.That(CountMatching(logs, CannotReclaimWarning), Is.EqualTo(1),
                "a tree whose blockers churn must be warned about once per episode, not once per pass.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_still_names_the_blocking_consumer_once()
    {
        // The counterweight, and the reason the throttle is not simply a
        // deletion. The warning's whole diagnostic value is that it names which
        // pin is holding the floor; a throttle that dropped it would trade a
        // noise problem for a silence problem, which on these rigs is worse.
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(BlockedReportNaming(BlockedConsumerId())));
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);
        var (scheduler, logs) = SchedulerWithLogs(factory, gc, time);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(40));

        var warning = SingleMatching(logs, CannotReclaimWarning);

        Assert.Multiple(() =>
        {
            Assert.That(warning.Level, Is.EqualTo(LogLevel.Warning),
                "a tree that cannot reclaim is a defect state and must stay at warning.");

            Assert.That(warning.Value("Consumer"), Is.EqualTo(BlockedConsumerId()),
                "the warning must still name the pin holding the floor, which is the only actionable thing in it.");

            Assert.That(warning.Value("Tree"), Is.EqualTo(StrandedTree));
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_reports_the_blocker_churn_when_the_episode_ends()
    {
        // Where the suppressed identity sequence is paid back. One line per
        // episode loses the sequence unless it is summarised, so the episode
        // carries a churn width and both ends of the sequence and reports them
        // when it ends. Without this the throttle would be a silent drop.
        var reported = new List<string>();
        var blocked = true;
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);
        var (scheduler, logs) = SchedulerWithLogs(factory, GcChurningBlockers(reported, () => blocked), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(20));

        var churned = reported.Count;
        blocked = false;
        await TickAsync(time);

        var summary = SingleMatching(logs, BlockerChurnSummary);

        Assert.Multiple(() =>
        {
            Assert.That(churned, Is.GreaterThan(30),
                "the episode must have churned widely, or the summary would have nothing to summarise.");

            Assert.That(summary.Int64("DistinctBlockers"), Is.EqualTo(churned),
                "the summary must report the real churn width; every identity here is unique, so admissions and identities coincide.");

            Assert.That(summary.Value("FirstConsumer"), Is.EqualTo(reported[0]),
                "the first blocker is the one the single warning named, and is the starting point for a post-hoc read.");

            Assert.That(summary.Value("Consumer"), Is.EqualTo(reported[churned - 1]),
                "the most recent blocker is the one that held the floor last, and is the actionable end of the sequence.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_does_not_report_churn_for_a_single_blocker_episode()
    {
        // The summary is silent when there is nothing to summarise: the warning
        // already named the one consumer, so a second line restating it would be
        // noise added by the change that exists to remove noise.
        var blocked = true;
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(blocked
                ? BlockedReportNaming(BlockedConsumerId())
                : Report(entriesTrimmed: 9)));
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);
        var (scheduler, logs) = SchedulerWithLogs(factory, gc, time);

        await StartAndRunFirstPassAsync(scheduler, time);
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(10));

        blocked = false;
        await TickAsync(time);

        Assert.Multiple(() =>
        {
            Assert.That(CountMatching(logs, CannotReclaimWarning), Is.EqualTo(1),
                "the single blocker must still have been warned about, or this fixture proves nothing about the summary.");

            Assert.That(CountMatching(logs, BlockerChurnSummary), Is.Zero,
                "an episode with one blocker has no rotation to report.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_warns_again_on_a_second_blocked_episode()
    {
        // The throttle is per episode, not per process. A tree that recovers and
        // is stranded again is a new condition and must alarm again; keyed on
        // anything longer-lived than the episode this would be a permanent
        // silence after the first block, which is the failure mode a throttle is
        // most likely to introduce.
        var reported = new List<string>();
        var blocked = true;
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);
        var (scheduler, logs) = SchedulerWithLogs(factory, GcChurningBlockers(reported, () => blocked), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        Assert.That(CountMatching(logs, CannotReclaimWarning), Is.EqualTo(1),
            "the first episode must warn, or the second warning would not be attributable to a second episode.");

        blocked = false;
        await TickAsync(time);

        blocked = true;
        await TickAsync(time);

        Assert.That(CountMatching(logs, CannotReclaimWarning), Is.EqualTo(2),
            "a tree that is stranded again after recovering is a new episode and must be warned about again.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    [Test]
    public async Task ExecuteAsync_carries_the_blocker_count_on_the_escalation()
    {
        // The escalation is the churn population's own alarm - it fires exactly
        // when no blocker has held still long enough to be touched - so it is
        // the one place the churn width is certain to reach a reader even on a
        // tree that never recovers and so never emits the end-of-episode
        // summary.
        var reported = new List<string>();
        var time = new VirtualTimeProvider();
        var (factory, _) = FactoryWithBlockedLeaf(StrandedTree);
        var (scheduler, logs) = SchedulerWithLogs(factory, GcChurningBlockers(reported, () => true), time);

        await StartAndRunFirstPassAsync(scheduler, time);

        // Past UnreachableBlockEscalation, which is 5 min + 3 * 15 min.
        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(60));

        var escalation = SingleMatching(logs, UnreachableBlockWarning);
        var count = escalation.Int64("DistinctBlockers");

        Assert.Multiple(() =>
        {
            // A missing placeholder reads back as 0 through Convert.ToInt64, so
            // this clause also fails if the count is simply not carried.
            Assert.That(count, Is.GreaterThan(1),
                "the escalation must carry the churn width, which is the reading the suppressed warnings used to provide.");

            Assert.That(count, Is.LessThanOrEqualTo(reported.Count),
                "the reported width must be drawn from the real admissions and not fabricated.");

            Assert.That(CountMatching(logs, CannotReclaimWarning), Is.EqualTo(1),
                "the escalation must not reintroduce the per-pass warning it replaces.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }
}
