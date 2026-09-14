using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Fault-recovery and scheduler-wide backoff observability tests for
/// <see cref="LatticeWalGcScheduler"/> (issue #3064).
/// <para>
/// The defect these cover is <b>latency, not liveness</b>. The scheduler always
/// did recover from a registry enumeration failure; it recovered on a ladder
/// that relaxed toward <see cref="LatticeOptions.WalGcInterval"/>, an hour at
/// stock defaults, so a fault that cleared in seconds could leave WAL GC idle
/// for the rest of that hour with no series anywhere saying which of "asleep"
/// and "dead" was true. A live container was scraped five times over twenty
/// minutes and recorded not one WAL GC interval while leaf activation was
/// demonstrably healthy, and the scrape could not settle the question.
/// </para>
/// <para>
/// Every fixture here is perturbation-checked against the pre-fix source: the
/// shared ladder, the shared ceiling, and the absence of the instruments each
/// redden at least one assertion below. A test that merely proves the reset
/// line executes is worthless - that line executed before this change too.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// The ceiling the faulted ladder saturates at under the fixture's band -
    /// the scheduler's own <c>ReactivationMinBlockAge</c>, which sits inside the
    /// 30s-1h band these fixtures configure and so is not clamped.
    /// </summary>
    /// <remarks>
    /// Written here as the value an <i>operator</i> would read off the wire
    /// rather than recomputed from the production constant. Deriving it would
    /// make the assertion agree with the implementation by construction - the
    /// vacuous "constant compared against itself" shape - and a test that cannot
    /// disagree with the code it guards proves nothing about it.
    /// </remarks>
    private static readonly TimeSpan FaultCeiling = TimeSpan.FromMinutes(5);

    /// <summary>A registry enumeration failure of the shape issue #3064 was reported against.</summary>
    private static IReadOnlyList<string> RegistryUnreachable() =>
        throw new TimeoutException("registry fan-out did not complete");

    /// <summary>
    /// A registry enumeration failure that is <b>not</b> a timeout, and so lands
    /// in the general fault arm rather than the timed-out one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Both helpers exist because the scheduler catches those two shapes in
    /// separate arms that each choose a backoff, and a fixture that only ever
    /// throws one of them leaves the other arm's choice unfireable. That is not
    /// hypothetical here: every fault test in this file originally threw
    /// <see cref="TimeoutException"/> and exercised the general arm, and when
    /// issue #3074 later inserted a <c>catch (TimeoutException)</c> ahead of it,
    /// all of them silently migrated to the new arm. Nothing failed, nothing
    /// announced it, and the general arm's ladder went uncovered - a real file,
    /// a real passing test, and a different clause than the one it names.
    /// </para>
    /// <para>
    /// So the pairing is load-bearing rather than tidy: keep a fault test on
    /// each helper, or a future arm inserted between them will silently take
    /// over the coverage of the one below it.
    /// </para>
    /// </remarks>
    private static IReadOnlyList<string> RegistryFaulted() =>
        throw new InvalidOperationException("registry fan-out failed");

    // ------------------------------------------------- bounded recovery latency

    /// <summary>
    /// The load-bearing fixture. A registry that keeps failing must not be able
    /// to push the scheduler's retry wait above the fault ceiling, because that
    /// wait <i>is</i> the window in which a recovered silo goes on looking dead.
    /// </summary>
    /// <remarks>
    /// Perturbation: restoring the pre-fix <c>return Quiet(minInterval, interval)</c>
    /// in the enumeration catch reddens this at the fifth pass, where the shared
    /// ladder reaches 8 minutes on its way to the hourly ceiling. That is the
    /// defect, stated as a measurement.
    /// </remarks>
    [Test]
    public async Task ExecuteAsync_bounds_the_retry_wait_when_the_registry_keeps_failing()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        var scheduler = CreateScheduler(
            FactoryFor(RegistryUnreachable), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 7; i++)
        {
            await TickAsync(time);
        }

        var waits = time.ScheduledDelays.Skip(1).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(waits, Has.Length.EqualTo(8), "each pass must arm exactly one retry.");
            Assert.That(waits, Has.All.LessThanOrEqualTo(FaultCeiling),
                "a failed enumeration is the scheduler's blindness window; it must stay bounded.");
            Assert.That(waits[^1], Is.EqualTo(FaultCeiling),
                "sustained failure must still saturate, not poll the floor forever.");
            Assert.That(waits[0], Is.EqualTo(Floor),
                "the very first failure must be retried promptly.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// The backoff must still climb under sustained failure. Bounding the
    /// ceiling is not licence to retry at the floor forever: the enumeration
    /// that fails is a 64-shard fan-out onto the very gate whose saturation
    /// caused the fault, so retrying it hard is how a fault becomes a storm.
    /// </summary>
    [Test]
    public async Task ExecuteAsync_still_backs_off_while_the_registry_keeps_failing()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        var scheduler = CreateScheduler(
            FactoryFor(RegistryUnreachable), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 3; i++)
        {
            await TickAsync(time);
        }

        var waits = time.ScheduledDelays.Skip(1).ToArray();
        Assert.That(waits, Is.EqualTo(new[]
        {
            TimeSpan.FromSeconds(30),
            TimeSpan.FromSeconds(60),
            TimeSpan.FromSeconds(120),
            TimeSpan.FromSeconds(240),
        }).AsCollection, "a repeated fault must double away from the floor.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// A successful enumeration that finds <b>no tree</b> must still reset the
    /// fault ladder. This is the fixture that separates the fix from the code it
    /// replaced.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Before this change one field carried both conditions, so "the registry
    /// answered, and the answer was 'nothing'" - an unambiguous proof the
    /// registry is readable - not only failed to reset the ladder, it relaxed it
    /// further. A fault arriving after it therefore inherited a wait already
    /// several rungs up.
    /// </para>
    /// <para>
    /// Perturbation: reinstating the shared <c>_quietWait</c> field reddens the
    /// final assertion at 8 minutes rather than 30 seconds - a sixteen-fold
    /// recovery penalty for an observation that was <i>good news</i>.
    /// </para>
    /// </remarks>
    [Test]
    public async Task ExecuteAsync_resets_the_fault_ladder_when_a_successful_enumeration_finds_no_tree()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        var failing = true;
        var scheduler = CreateScheduler(
            FactoryFor(() => failing ? RegistryUnreachable() : []), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 3; i++)
        {
            await TickAsync(time);
        }

        var laddered = time.LastScheduledDelay;

        // The registry answers, and answers "nothing here".
        failing = false;
        await TickAsync(time);
        var empty = time.LastScheduledDelay;

        // And now it fails again.
        failing = true;
        await TickAsync(time);

        Assert.Multiple(() =>
        {
            Assert.That(laddered, Is.EqualTo(TimeSpan.FromSeconds(240)),
                "guard: the fault ladder must actually have climbed before the reset is tested.");
            Assert.That(empty, Is.EqualTo(Floor),
                "an empty registry runs its own ladder, which starts at the floor.");
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
                "a registry that answered has proved it is readable; the fault ladder must start over.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// The mirror image: a fault must <b>not</b> reset the ladder. Recovery that
    /// is bounded is the goal; recovery that forgets is a retry storm.
    /// </summary>
    [Test]
    public async Task ExecuteAsync_does_not_reset_the_fault_ladder_on_a_further_fault()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        var scheduler = CreateScheduler(
            FactoryFor(RegistryUnreachable), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);

        Assert.That(time.LastScheduledDelay, Is.GreaterThan(Floor),
            "a second consecutive fault must inherit the first one's backoff, not restart it.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// The same bound, proved on the <b>general</b> fault arm rather than the
    /// timed-out one. A registry that fails for any reason other than a timeout
    /// must be retried on the same bounded ladder, because the property that
    /// justifies bounding it is that the scheduler learned nothing - and that is
    /// true of every failure shape, not only of a slow one.
    /// </summary>
    /// <remarks>
    /// Perturbation: restoring the pre-fix <c>Quiet(minInterval, interval)</c>
    /// in the general enumeration catch reddens this at the fifth pass, where
    /// the shared ladder reaches 8 minutes on its way to the hourly ceiling.
    /// No other fixture in this file reddens on that mutation, which is the
    /// whole reason this one exists alongside its timeout twin.
    /// </remarks>
    [Test]
    public async Task ExecuteAsync_bounds_the_retry_wait_when_the_registry_keeps_faulting()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        var scheduler = CreateScheduler(
            FactoryFor(RegistryFaulted), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 7; i++)
        {
            await TickAsync(time);
        }

        var waits = time.ScheduledDelays.Skip(1).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(waits, Has.Length.EqualTo(8), "each pass must arm exactly one retry.");
            Assert.That(waits, Has.All.LessThanOrEqualTo(FaultCeiling),
                "a failed enumeration is the scheduler's blindness window; it must stay bounded.");
            Assert.That(waits[^1], Is.EqualTo(FaultCeiling),
                "sustained failure must still saturate, not poll the floor forever.");
            Assert.That(waits[0], Is.EqualTo(Floor),
                "the very first failure must be retried promptly.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// The general fault arm must reset on a successful enumeration too. Covered
    /// separately from its timed-out twin for the reason given on
    /// <see cref="RegistryFaulted"/>: the two arms reach the reset by different
    /// routes, and a single fixture cannot witness both.
    /// </summary>
    [Test]
    public async Task ExecuteAsync_resets_the_general_fault_ladder_on_a_successful_enumeration()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        var faulting = true;
        var scheduler = CreateScheduler(
            FactoryFor(IReadOnlyList<string> () => faulting ? RegistryFaulted() : []),
            gc,
            Adaptive(),
            time);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 3; i++)
        {
            await TickAsync(time);
        }

        var laddered = time.LastScheduledDelay;
        faulting = false;
        await TickAsync(time);
        var afterSuccess = time.LastScheduledDelay;
        faulting = true;
        await TickAsync(time);

        Assert.Multiple(() =>
        {
            Assert.That(laddered, Is.GreaterThan(Floor),
                "the ladder must have climbed, or the reset below proves nothing.");
            Assert.That(time.LastScheduledDelay, Is.EqualTo(Floor),
                "a fault following a successful enumeration must start from the floor again.");
            Assert.That(afterSuccess, Is.Not.EqualTo(laddered),
                "the successful pass must not inherit the fault ladder's wait.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// The empty-registry ladder keeps its original, deliberately generous
    /// ceiling. Only the <i>faulted</i> ceiling moved, because only the faulted
    /// ceiling was wrong: a silo with nothing to collect is not blind, it is
    /// correct, and making it poll every five minutes forever would be a
    /// regression dressed as a fix.
    /// </summary>
    [Test]
    public async Task ExecuteAsync_leaves_the_empty_registry_ladder_at_the_configured_ceiling()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        var scheduler = CreateScheduler(
            FactoryFor(() => []), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 9; i++)
        {
            await TickAsync(time);
        }

        Assert.That(time.LastScheduledDelay, Is.EqualTo(Ceiling),
            "an empty silo must still settle at the configured interval, not at the fault ceiling.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// The fault ceiling is clamped into the operator's own band. An operator
    /// who configured a tighter interval than the derived ceiling is asking for
    /// faster collection, and must not be silently given slower fault recovery.
    /// </summary>
    [Test]
    public async Task ExecuteAsync_clamps_the_fault_ceiling_below_a_tighter_configured_interval()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        var tight = Adaptive(floor: TimeSpan.FromSeconds(10), ceiling: TimeSpan.FromSeconds(40));
        var scheduler = CreateScheduler(
            FactoryFor(RegistryUnreachable), gc, tight, time);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 5; i++)
        {
            await TickAsync(time);
        }

        Assert.That(time.ScheduledDelays.Skip(1), Has.All.LessThanOrEqualTo(TimeSpan.FromSeconds(40)),
            "the derived ceiling must never override a tighter configured interval upward.");

        await scheduler.StopAsync(CancellationToken.None);
    }

    // --------------------------------------------------------- per-tree lever

    /// <summary>
    /// The scheduler-wide ladder must leave the <b>per-tree</b> lever alone. A
    /// blocked tree is pinned at the floor rather than relaxed (the repair
    /// carried by <c>CollectTreeAsync</c>, which stopped a starved tree getting
    /// the fewest passes exactly when it needed the most), and that pin is
    /// reached on the scheduled path, which this change does not ladder.
    /// </summary>
    /// <remarks>
    /// Sited here rather than left to the blocked-backoff fixtures because the
    /// two levers look alike and the risk being guarded is a future edit
    /// collapsing them. The assertion on the reported backoff level is the other
    /// half: a pinned tree must not read as a scheduler that has backed off.
    /// </remarks>
    [Test]
    public async Task ExecuteAsync_leaves_a_blocked_tree_pinned_to_the_floor()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(BlockedReport());
        var time = new VirtualTimeProvider();
        using var probe = new BackoffProbe();
        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        for (var i = 0; i < 3; i++)
        {
            await TickAsync(time);
        }

        Assert.Multiple(() =>
        {
            Assert.That(time.ScheduledDelays.Skip(1), Has.All.EqualTo(Floor),
                "a blocked tree must keep its floor pin; the scheduler-wide ladder must not override it.");
            Assert.That(probe.Backoff.Select(b => b.Cause), Has.All.EqualTo("scheduled"),
                "a blocked tree is an observation, not a scheduler fault.");
            Assert.That(probe.Backoff.Select(b => b.Seconds), Has.All.EqualTo(Floor.TotalSeconds),
                "no scheduler-wide backoff is in force while passes are running normally.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    // ------------------------------------------------------------ instruments

    /// <summary>
    /// The instruments are primed on an <b>ordinary healthy pass</b>, which is
    /// the entire point of where they are recorded.
    /// </summary>
    /// <remarks>
    /// Primed on the fault path instead, an absent series would mean either "no
    /// fault has occurred" or "this build is not deployed", and this epic
    /// confused exactly those two more than once at real cost. Primed here, an
    /// absent series means only the latter. Note also that the reported backoff
    /// level is the floor even though the pass armed a longer sleep: publishing
    /// the sleep would make a quiet cadence indistinguishable from a backoff,
    /// which is the ambiguity being removed.
    /// </remarks>
    [Test]
    public async Task ExecuteAsync_primes_the_backoff_instruments_on_a_healthy_pass()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        gc.RunOnceAsync(Arg.Any<string>(), Arg.Any<CancellationToken>()).Returns(Report(0));
        var time = new VirtualTimeProvider();
        using var probe = new BackoffProbe();
        var scheduler = CreateScheduler(FactoryWithTrees("alpha"), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);

        Assert.Multiple(() =>
        {
            Assert.That(probe.Backoff, Has.Count.EqualTo(1), "every pass must record the backoff level.");
            Assert.That(probe.Faults, Has.Count.EqualTo(1), "every pass must record the fault streak.");
            Assert.That(probe.Backoff[0].Cause, Is.EqualTo("scheduled"));
            Assert.That(probe.Backoff[0].Seconds, Is.EqualTo(Floor.TotalSeconds));
            Assert.That(probe.Faults[0].Count, Is.Zero, "a healthy pass reports a measured zero streak.");
            Assert.That(probe.Backoff[0].Tenant, Is.EqualTo(LatticeTenantLabel.PlatformTenant),
                "a silo-wide instrument carries the platform sentinel.");
            Assert.That(probe.Faults[0].Tenant, Is.EqualTo(LatticeTenantLabel.PlatformTenant));
            Assert.That(time.LastScheduledDelay, Is.GreaterThan(Floor),
                "guard: the pass armed a longer sleep than the backoff level it reported, "
                + "so the two cannot be the same observable by accident.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// A silo with no trees at all must still record. This is the case
    /// <see cref="LatticeMetrics.WalGcInterval"/> cannot witness - it is
    /// per-tree, so it is silent exactly where a liveness signal is most
    /// needed - and it is why the new instruments do not share its site.
    /// </summary>
    [Test]
    public async Task ExecuteAsync_records_the_backoff_instruments_on_a_silo_with_no_trees()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        using var probe = new BackoffProbe();
        var scheduler = CreateScheduler(FactoryFor(() => []), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);

        Assert.Multiple(() =>
        {
            Assert.That(probe.Backoff, Has.Count.EqualTo(1));
            Assert.That(probe.Backoff[0].Cause, Is.EqualTo("empty"));
            Assert.That(probe.Backoff[0].Seconds, Is.EqualTo(Floor.TotalSeconds));
            Assert.That(probe.Faults[0].Count, Is.Zero,
                "an empty registry answered, so it contributes no fault streak.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// The cause tag separates "I could not read the registry" from "I read it
    /// and there is nothing to do", and the fault streak moves with the first
    /// and resets on the second. Those two produced byte-identical scheduler
    /// behaviour before this change, and it is the most important distinction
    /// this scheduler has.
    /// </summary>
    [Test]
    public async Task ExecuteAsync_reports_the_backoff_cause_and_the_fault_streak()
    {
        var gc = Substitute.For<ILatticeWalGc>();
        var time = new VirtualTimeProvider();
        var failing = true;
        using var probe = new BackoffProbe();
        var scheduler = CreateScheduler(
            FactoryFor(() => failing ? RegistryUnreachable() : []), gc, Adaptive(), time);

        await StartAndRunFirstPassAsync(scheduler, time);
        await TickAsync(time);
        await TickAsync(time);

        failing = false;
        await TickAsync(time);

        Assert.Multiple(() =>
        {
            Assert.That(probe.Backoff.Select(b => b.Cause),
                Is.EqualTo(new[] { "faulted", "faulted", "faulted", "empty" }).AsCollection,
                "the cause must follow the pass, not the ladder it happens to share.");
            Assert.That(probe.Faults.Select(f => f.Count),
                Is.EqualTo(new long[] { 1, 2, 3, 0 }).AsCollection,
                "the streak must climb with consecutive faults and reset on the first success.");
            Assert.That(probe.Backoff.Select(b => b.Seconds),
                Is.EqualTo(new[] { 30d, 60d, 120d, 30d }).AsCollection,
                "the published level must be the backoff actually in force on each pass.");
        });

        await scheduler.StopAsync(CancellationToken.None);
    }

    /// <summary>
    /// Captures the scheduler-wide backoff instruments for one fixture.
    /// </summary>
    private sealed class BackoffProbe : IDisposable
    {
        private readonly Lock _gate = new();
        private readonly List<(string Cause, double Seconds, string Tenant)> _backoff = [];
        private readonly List<(string Cause, long Count, string Tenant)> _faults = [];
        private readonly MeterListener _listener;

        public BackoffProbe()
        {
            _listener = Orleans.Lattice.Testing.MeterListening.StartForMeter(
                LatticeMetrics.Meter,
                [LatticeMetrics.WalGcSchedulerBackoff.Name, LatticeMetrics.WalGcSchedulerConsecutiveFaults.Name],
                listener =>
                {
                    listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
                    {
                        var (cause, tenant) = Read(tags);
                        lock (_gate) { _backoff.Add((cause, value, tenant)); }
                    });
                    listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
                    {
                        var (cause, tenant) = Read(tags);
                        lock (_gate) { _faults.Add((cause, value, tenant)); }
                    });
                });
        }

        /// <summary>Every backoff-level measurement seen so far, in order.</summary>
        public IReadOnlyList<(string Cause, double Seconds, string Tenant)> Backoff
        {
            get { lock (_gate) { return _backoff.ToArray(); } }
        }

        /// <summary>Every fault-streak measurement seen so far, in order.</summary>
        public IReadOnlyList<(string Cause, long Count, string Tenant)> Faults
        {
            get { lock (_gate) { return _faults.ToArray(); } }
        }

        public void Dispose() => _listener.Dispose();

        private static (string Cause, string Tenant) Read(ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            var cause = string.Empty;
            var tenant = string.Empty;
            foreach (var tag in tags)
            {
                if (string.Equals(tag.Key, LatticeMetrics.TagWalGcBackoffCause, StringComparison.Ordinal))
                {
                    cause = tag.Value as string ?? string.Empty;
                }
                else if (string.Equals(tag.Key, LatticeTenantLabel.TagTenant, StringComparison.Ordinal))
                {
                    tenant = tag.Value as string ?? string.Empty;
                }
            }

            return (cause, tenant);
        }
    }
}
