using System.Diagnostics.Metrics;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Coverage for
/// <see cref="LatticeMetrics.CoordinatorPhaseTickConsecutiveFailures"/>, the
/// observable gauge that reports how long a coordinator's <i>current</i> run of
/// failed phase ticks is.
/// <para>
/// The counter these tests sit beside cannot answer that question. A coordinator
/// failing one tick in a thousand and one that has failed every tick since the
/// process started both present as a rising total, yet the first is a transient
/// the pump absorbs by design and the second is a phase machine that has stopped
/// advancing. Issue #2814 settled exactly that distinction for the
/// repository-context approximate-index build, and the evidence that made it a
/// wedge rather than a flaky read was the phrase "156 times in a row" in a 53 MB
/// log stream - a reading no exported series could have produced.
/// </para>
/// <para>
/// Every assertion here drives real ticks and reads real measurements off a
/// <see cref="MeterListener"/>. Each test uses a tree id of its own, because the
/// census behind the gauge is process-wide: filtering on the tree tag is what
/// keeps a fixture from reading a sibling's enrolment.
/// </para>
/// </summary>
public partial class CoordinatorGrainPhaseTickMetricsTests
{
    /// <summary>
    /// Runs <paramref name="body"/> with a listener enabled for exactly the
    /// consecutive-failure gauge, then forces one observation and returns what it
    /// reported. The instrument is passed by reference to
    /// <see cref="MeterListening.StartForInstrument"/>, so the owning type
    /// initialiser has necessarily completed before the listener exists.
    /// </summary>
    private static async Task<List<Measurement>> ObserveRunAsync(Func<Task> body)
    {
        var measurements = new List<Measurement>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.CoordinatorPhaseTickConsecutiveFailures,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                var copy = new Dictionary<string, string?>(StringComparer.Ordinal);
                foreach (var tag in tags)
                {
                    copy[tag.Key] = tag.Value?.ToString();
                }

                lock (measurements)
                {
                    measurements.Add(new Measurement(value, copy));
                }
            }));

        await body();
        listener.RecordObservableInstruments();

        lock (measurements)
        {
            return [.. measurements];
        }
    }

    private static async Task FailTimesAsync(Harness h, int times)
    {
        h.Grain.PhaseThrow = new InvalidOperationException("phase exploded");
        var tick = CapturedTick(h.Timers);
        for (var i = 0; i < times; i++)
        {
            await tick(CancellationToken.None);
        }
    }

    [Test]
    public async Task Arming_the_phase_timer_enrols_the_run_gauge_at_zero()
    {
        // The priming half. A gauge that reported only coordinators currently
        // failing would make a healthy coordinator byte-identical to an absent
        // one, which is the ambiguity this instrument exists to remove.
        const string tree = "consec-primed";
        var h = Create(tree);

        var measurements = await ObserveRunAsync(() =>
        {
            h.Grain.ArmPhaseTimer();
            return Task.CompletedTask;
        });

        var mine = ForTree(measurements, tree);
        Assert.Multiple(() =>
        {
            Assert.That(mine, Has.Count.EqualTo(1),
                "A coordinator that armed its pump must report before it can fail.");
            Assert.That(mine[0].Value, Is.Zero);
        });
    }

    [Test]
    public async Task The_gauge_reports_the_length_of_the_current_failure_run()
    {
        // The load-bearing test: consecutiveness is the whole reason this
        // instrument exists, and it is the one thing the counter beside it
        // cannot express.
        const string tree = "consec-run";
        var h = Create(tree);

        var measurements = await ObserveRunAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            await FailTimesAsync(h, 4);
        });

        Assert.That(ForTree(measurements, tree).Single().Value, Is.EqualTo(4),
            "Four back-to-back swallowed ticks is a run of four, not four unrelated faults.");
    }

    [Test]
    public async Task A_tick_that_advances_returns_the_gauge_to_zero()
    {
        // The negative control. Without it, an arm that only ever incremented
        // would pass every other test here - and would report a wedge for a
        // coordinator that recovered hours ago.
        const string tree = "consec-reset";
        var h = Create(tree);

        var measurements = await ObserveRunAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            await FailTimesAsync(h, 3);
            h.Grain.PhaseThrow = null;
            await CapturedTick(h.Timers)(CancellationToken.None);
        });

        Assert.That(ForTree(measurements, tree).Single().Value, Is.Zero,
            "The run restarts from the first tick that returned normally.");
    }

    [Test]
    public async Task The_run_carries_the_same_tags_the_failure_counter_does()
    {
        // The two series are only usable together if they join, and they only
        // join on an identical tag set.
        const string tree = "consec-tags";
        var h = Create(tree);

        var measurements = await ObserveRunAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            await FailTimesAsync(h, 1);
        });

        var run = ForTree(measurements, tree).Single();
        Assert.Multiple(() =>
        {
            Assert.That(run.Tags[LatticeMetrics.TagKind], Is.EqualTo(TestCoordinator.ReminderName),
                "The kind tag is what separates a wedged snapshot coordinator from a wedged resize.");
            Assert.That(run.Tags[LatticeMetrics.TagTree], Is.EqualTo(tree));
            Assert.That(run.Tags.ContainsKey(LatticeTenantLabel.TagTenant), Is.True,
                "Without the tenant label the gauge cannot be filtered per tenant as the counter can.");
        });
    }

    [Test]
    public async Task A_composite_key_coordinator_reports_the_subject_alone()
    {
        // A coordinator keyed 'tree/shard' that reported the raw key would emit a
        // fresh series per shard, which no dashboard can group by tree.
        var h = Create("consec-composite/9", composite: true);

        var measurements = await ObserveRunAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            await FailTimesAsync(h, 2);
        });

        Assert.Multiple(() =>
        {
            Assert.That(ForTree(measurements, "consec-composite").Single().Value, Is.EqualTo(2));
            Assert.That(ForTree(measurements, "consec-composite/9"), Is.Empty,
                "The raw composite key must never reach the tag.");
        });
    }

    [Test]
    public async Task Activations_sharing_a_tag_set_report_the_worst_run_between_them()
    {
        // A gauge may report a tag set once, and a composite-key coordinator
        // deliberately collapses several activations onto one. Summing would
        // invent a run no activation experienced; last-writer-wins would let a
        // healthy sibling hide a wedged one. Only max answers 'is anything here
        // wedged, and for how long'.
        //
        // The three runs are 5, 3 and 0 rather than the one-wedged-one-healthy
        // pair this test first used, because that pair could not fail. With runs
        // of 5 and 0, sum and max are both 5, so the assertion held just as well
        // against a reduction this test exists to reject. Three distinct non-zero
        // runs separate every candidate: max is 5, sum is 8, min is 0, and first
        // or last writer is whichever the dictionary happened to yield.
        var wedged = Create("consec-shared/1", composite: true);
        var struggling = Create("consec-shared/2", composite: true);
        var healthy = Create("consec-shared/3", composite: true);

        var measurements = await ObserveRunAsync(async () =>
        {
            wedged.Grain.ArmPhaseTimer();
            struggling.Grain.ArmPhaseTimer();
            healthy.Grain.ArmPhaseTimer();
            await FailTimesAsync(wedged, 5);
            await FailTimesAsync(struggling, 3);
            await CapturedTick(healthy.Timers)(CancellationToken.None);
        });

        var shared = ForTree(measurements, "consec-shared");
        Assert.Multiple(() =>
        {
            Assert.That(shared, Has.Count.EqualTo(1),
                "One tag set must produce exactly one measurement per observation.");
            Assert.That(shared[0].Value, Is.EqualTo(5),
                "Max reports the worst run present; a sum would report 8, a run no activation was ever in.");
        });
    }

    [Test]
    public async Task Completing_the_coordinator_withdraws_its_enrolment()
    {
        // A coordinator that has finished is not wedged, and leaving its last run
        // pinned on the gauge would report a permanent alert for work that
        // succeeded.
        const string tree = "consec-completed";
        var h = Create(tree);

        var measurements = await ObserveRunAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            await FailTimesAsync(h, 3);
            await h.Grain.CompleteAsync();
        });

        Assert.That(ForTree(measurements, tree), Is.Empty,
            "A completed coordinator reports nothing, rather than freezing its final run.");
    }

    [Test]
    public async Task Deactivating_the_activation_withdraws_its_enrolment()
    {
        // Enrolment is activation-scoped: a successor must start from zero rather
        // than inherit its predecessor's run.
        const string tree = "consec-deactivated";
        var h = Create(tree);

        var measurements = await ObserveRunAsync(async () =>
        {
            h.Grain.ArmPhaseTimer();
            await FailTimesAsync(h, 2);
            await ((IGrainBase)h.Grain).OnDeactivateAsync(
                new DeactivationReason(DeactivationReasonCode.ShuttingDown, "test"),
                CancellationToken.None);
        });

        Assert.That(ForTree(measurements, tree), Is.Empty);
    }
}
