using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Unit coverage for <see cref="ScanStallFutilityWatch"/> driven directly against
/// the instrument rather than through a resilient scan.
/// <para>
/// WHY A DIRECT FIXTURE EXISTS ALONGSIDE THE SCAN-DRIVEN ONE.
/// <c>ResilientScanExtensionsTests.StallFutilityRecovery</c> proves the watch is
/// wired into the scan path and that each arm fires for the right field reason.
/// It cannot reach the watch's own edges, because a scan only ever hands it an
/// ascending, exclusive, non-null bound and a single shard: the descending and
/// inclusive comparisons, the per-shard chaining rule, the defensive null
/// arguments, the capacity clamp, and the no-op sweep are all unreachable from
/// that direction. Those are properties of this class, so they are proved here,
/// where the inputs can be stated exactly.
/// </para>
/// <para>
/// EVERY RESOLUTION IS ASSERTED AGAINST THE RECORDED MEASUREMENT, not against an
/// internal field. The whole purpose of the type is to emit
/// <see cref="LatticeMetrics.ScanStallFutilityOutcomes"/>, so a test that checked
/// a counter instead would pass even if nothing were ever recorded - which is the
/// one failure that matters.
/// </para>
/// </summary>
[TestFixture]
[NonParallelizable]
public sealed class ScanStallFutilityWatchTests
{
    private const double CeilingSeconds = 10d;
    private const string Tree = "futility-tree";
    private const string Phase = "leaf-walk";

    private static TimeSpan Window => ScanStallFutilityWatch.WindowFor(CeilingSeconds);

    // -- construction ------------------------------------------------

    [Test]
    public void A_watch_with_nothing_open_reports_no_watches()
        => Assert.That(new ScanStallFutilityWatch(new ManualTimeProvider()).HasWatches, Is.False);

    [Test]
    public void A_watch_built_without_a_time_provider_uses_the_system_clock()
    {
        // The default constructor argument is what the production singleton in
        // LatticeExtensions uses, so it must be exercised somewhere. A watch
        // opened against the real clock has a deadline a whole window away, so an
        // immediate sweep must leave it open rather than expire it.
        var watch = new ScanStallFutilityWatch();
        using var outcomes = new FutilityOutcomes();

        watch.OpenWatch(new object(), Stall(), bound: "a", boundExclusive: true, reverse: false);
        watch.Sweep();

        Assert.Multiple(() =>
        {
            Assert.That(watch.HasWatches, Is.True);
            Assert.That(outcomes.Snapshot(), Is.Empty);
        });
    }

    [Test]
    public void A_capacity_below_one_is_clamped_so_the_table_can_still_hold_a_watch()
    {
        // Without the clamp a zero or negative capacity would make
        // EvictWhileOverCapacityLocked evict the entry it was about to admit, so
        // the instrument would silently record nothing but "dropped".
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock, capacity: 0);
        using var outcomes = new FutilityOutcomes();

        watch.OpenWatch(new object(), Stall(), bound: null, boundExclusive: false, reverse: false);

        Assert.Multiple(() =>
        {
            Assert.That(watch.HasWatches, Is.True, "a clamped capacity of one still admits one watch");
            Assert.That(outcomes.Snapshot(), Is.Empty, "admitting the first watch must not evict it");
        });
    }

    // -- defensive arguments -----------------------------------------

    [Test]
    public void OpenWatch_ignores_a_null_source_or_a_null_stall()
    {
        var watch = new ScanStallFutilityWatch(new ManualTimeProvider());

        watch.OpenWatch(null!, Stall(), bound: "a", boundExclusive: true, reverse: false);
        watch.OpenWatch(new object(), null!, bound: "a", boundExclusive: true, reverse: false);

        Assert.That(
            watch.HasWatches, Is.False,
            "observability must never open a watch it cannot key or describe");
    }

    [Test]
    public void NoteProgress_ignores_a_null_source_or_a_null_key()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: "a", boundExclusive: true, reverse: false);

        watch.NoteProgress(null!, "z");
        watch.NoteProgress(source, null!);

        Assert.Multiple(() =>
        {
            Assert.That(watch.HasWatches, Is.True, "neither call names an observation");
            Assert.That(outcomes.Snapshot(), Is.Empty);
        });
    }

    [Test]
    public void Progress_on_an_unwatched_source_resolves_nothing()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();

        watch.OpenWatch(new object(), Stall(), bound: "a", boundExclusive: true, reverse: false);
        watch.NoteProgress(new object(), "z");

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Snapshot(), Is.Empty, "a different tree says nothing about this one");
            Assert.That(watch.HasWatches, Is.True);
        });
    }

    // -- recovered, and the position rule that gives it meaning ------

    [Test]
    public void A_record_past_an_exclusive_bound_resolves_recovered_and_carries_the_stall_tags()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: "m", boundExclusive: true, reverse: false);
        watch.NoteProgress(source, "n");

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.EqualTo(1));
            Assert.That(
                watch.HasWatches, Is.False,
                "the resolved watch is removed, so the per-record hook goes back to one volatile read");
            Assert.That(outcomes.Trees, Is.EqualTo(new[] { Tree }), "the measurement names the tree");
            Assert.That(outcomes.Phases, Is.EqualTo(new[] { Phase }), "and the phase the walk died in");
        });
    }

    [Test]
    public void A_record_exactly_at_an_exclusive_bound_does_not_resolve_recovered()
    {
        // The bound of an exclusive watch is the last key the abandoned walk
        // already yielded, so re-serving it proves nothing about the region that
        // was refused. Only a strictly greater key does.
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: "m", boundExclusive: true, reverse: false);
        watch.NoteProgress(source, "m");

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Snapshot(), Is.Empty);
            Assert.That(watch.HasWatches, Is.True);
        });
    }

    [Test]
    public void A_record_exactly_at_an_inclusive_bound_resolves_recovered()
    {
        // An inclusive bound is the start the walk never got past, so the very
        // key it was asked for is already evidence the source served the region.
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: "m", boundExclusive: false, reverse: false);
        watch.NoteProgress(source, "m");

        Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.EqualTo(1));
    }

    [Test]
    public void A_record_before_the_bound_does_not_resolve_recovered()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: "m", boundExclusive: false, reverse: false);
        watch.NoteProgress(source, "b");

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Snapshot(), Is.Empty);
            Assert.That(watch.HasWatches, Is.True);
        });
    }

    [Test]
    public void A_watch_with_no_bound_is_resolved_by_any_later_record()
    {
        // A walk that died at the origin yielded nothing and started from no
        // bound, so there is no position to be past: any record at all is proof.
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: null, boundExclusive: true, reverse: false);
        watch.NoteProgress(source, string.Empty);

        Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.EqualTo(1));
    }

    [Test]
    public void A_descending_walk_is_recovered_by_a_record_below_its_bound()
    {
        // Ordering is relative to the walk's own direction. Comparing a
        // descending walk with ascending logic would read every later record as
        // "not past", and the recovered arm would be dead for reverse scans
        // without any test failing.
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: "m", boundExclusive: true, reverse: true);
        watch.NoteProgress(source, "d");

        Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.EqualTo(1));
    }

    [Test]
    public void A_descending_walk_is_not_recovered_by_a_record_above_its_bound()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: "m", boundExclusive: true, reverse: true);
        watch.NoteProgress(source, "z");

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Snapshot(), Is.Empty);
            Assert.That(watch.HasWatches, Is.True);
        });
    }

    [Test]
    public void A_descending_walk_with_an_inclusive_bound_is_recovered_at_the_bound()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: "m", boundExclusive: false, reverse: true);
        watch.NoteProgress(source, "m");

        Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.EqualTo(1));
    }

    // -- per-shard chaining ------------------------------------------

    [Test]
    public void A_second_futility_termination_on_the_same_shard_resolves_the_first_as_still_stalled()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(shard: 3), bound: "a", boundExclusive: true, reverse: false);
        watch.OpenWatch(source, Stall(shard: 3), bound: "a", boundExclusive: true, reverse: false);

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeStillStalled), Is.EqualTo(1));
            Assert.That(
                watch.HasWatches, Is.True,
                "the second termination chains: it replaces the resolved watch rather than vanishing");
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.Zero);
        });
    }

    [Test]
    public void A_termination_on_a_different_shard_of_the_same_source_opens_a_second_watch()
    {
        // Chaining is keyed on the shard, not merely on the source. If it were
        // keyed on the source alone, a tree that stalled on two shards would
        // report one of them as still-stalled on the strength of the other's
        // termination, which is a different claim entirely.
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(shard: 1), bound: "m", boundExclusive: true, reverse: false);
        watch.OpenWatch(source, Stall(shard: 2), bound: "m", boundExclusive: true, reverse: false);

        Assert.That(
            outcomes.Snapshot(), Is.Empty,
            "two distinct shards are two independent observations, so nothing is resolved yet");

        // Both watches share a source and a bound, so one record resolves both -
        // which also drives the multi-entry removal path inside NoteProgress.
        watch.NoteProgress(source, "z");

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.EqualTo(2));
            Assert.That(watch.HasWatches, Is.False);
        });
    }

    [Test]
    public void Resolving_one_of_two_watches_on_a_source_leaves_the_other_open()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(shard: 1), bound: "c", boundExclusive: true, reverse: false);
        watch.OpenWatch(source, Stall(shard: 2), bound: "y", boundExclusive: true, reverse: false);

        watch.NoteProgress(source, "m");

        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.EqualTo(1),
                "only the watch whose abandoned position was passed is resolved");
            Assert.That(watch.HasWatches, Is.True, "the further shard is still unanswered");
        });
    }

    // -- unobserved --------------------------------------------------

    [Test]
    public void A_sweep_with_nothing_open_is_a_no_op()
    {
        var watch = new ScanStallFutilityWatch(new ManualTimeProvider());
        using var outcomes = new FutilityOutcomes();

        watch.Sweep();

        Assert.That(outcomes.Snapshot(), Is.Empty);
    }

    [Test]
    public void A_sweep_inside_the_window_resolves_nothing()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();

        watch.OpenWatch(new object(), Stall(), bound: "a", boundExclusive: true, reverse: false);
        clock.Advance(Window - TimeSpan.FromSeconds(1));
        watch.Sweep();

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Snapshot(), Is.Empty, "the window has not closed, so the answer is not yet in");
            Assert.That(watch.HasWatches, Is.True);
        });
    }

    [Test]
    public void A_sweep_after_the_window_closes_resolves_unobserved()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();

        watch.OpenWatch(new object(), Stall(), bound: "a", boundExclusive: true, reverse: false);
        clock.Advance(Window + TimeSpan.FromSeconds(1));
        watch.Sweep();

        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.Count(ScanStallFutilityWatch.OutcomeUnobserved), Is.EqualTo(1),
                "nobody came back inside the window, so this termination answers nothing - and "
                + "saying so is what stops the missing recovered reading as evidence of a dead source");
            Assert.That(watch.HasWatches, Is.False);
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.Zero);
        });
    }

    [Test]
    public void An_expired_watch_is_swept_by_the_next_observation_rather_than_resolved_by_it()
    {
        // Expiry needs no timer of its own: every entry point sweeps first. A
        // record arriving after the window therefore reads unobserved, not
        // recovered, however far past the bound it is.
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock);
        using var outcomes = new FutilityOutcomes();
        var source = new object();

        watch.OpenWatch(source, Stall(), bound: "a", boundExclusive: true, reverse: false);
        clock.Advance(Window + TimeSpan.FromSeconds(1));
        watch.NoteProgress(source, "z");

        Assert.Multiple(() =>
        {
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeUnobserved), Is.EqualTo(1));
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.Zero);
        });
    }

    // -- saturation --------------------------------------------------

    [Test]
    public void A_watch_evicted_by_capacity_is_recorded_as_dropped_oldest_first()
    {
        var clock = new ManualTimeProvider();
        var watch = new ScanStallFutilityWatch(clock, capacity: 2);
        using var outcomes = new FutilityOutcomes();

        var first = new object();
        watch.OpenWatch(first, Stall(), bound: "a", boundExclusive: true, reverse: false);

        // A later deadline, so the first entry is unambiguously the oldest.
        clock.Advance(TimeSpan.FromSeconds(1));
        var second = new object();
        watch.OpenWatch(second, Stall(), bound: "a", boundExclusive: true, reverse: false);

        clock.Advance(TimeSpan.FromSeconds(1));
        var third = new object();
        watch.OpenWatch(third, Stall(), bound: "a", boundExclusive: true, reverse: false);

        Assert.That(
            outcomes.Count(ScanStallFutilityWatch.OutcomeDropped), Is.EqualTo(1),
            "a saturated table must say so, or the burst that overflows it would depress every "
            + "other arm at exactly the moment the instrument is being read");

        // The evicted watch is the oldest, so the first source is the one that no
        // longer resolves. Proving which one was dropped is what makes the
        // eviction order a tested property rather than an incidental one.
        watch.NoteProgress(first, "z");
        Assert.That(
            outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.Zero,
            "the oldest watch was the one evicted, so its source has nothing left to resolve");

        watch.NoteProgress(second, "z");
        Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.EqualTo(1));
    }

    // -- window derivation -------------------------------------------

    [Test]
    public void The_window_is_a_multiple_of_the_ceiling_the_stall_reported()
        => Assert.That(
            ScanStallFutilityWatch.WindowFor(CeilingSeconds),
            Is.EqualTo(TimeSpan.FromSeconds(CeilingSeconds * ScanStallFutilityWatch.WindowCeilingMultiple)));

    [Test]
    public void The_window_falls_back_to_the_page_duration_for_an_unusable_ceiling()
    {
        var expected = TimeSpan.FromSeconds(
            LatticeOptions.DefaultMaxScanPageDuration.TotalSeconds
            * ScanStallFutilityWatch.WindowCeilingMultiple);

        Assert.Multiple(() =>
        {
            Assert.That(ScanStallFutilityWatch.WindowFor(0), Is.EqualTo(expected));
            Assert.That(ScanStallFutilityWatch.WindowFor(-1), Is.EqualTo(expected));
            Assert.That(ScanStallFutilityWatch.WindowFor(double.NaN), Is.EqualTo(expected));
            Assert.That(ScanStallFutilityWatch.WindowFor(double.PositiveInfinity), Is.EqualTo(expected));
        });
    }

    // -- harness -----------------------------------------------------

    private static ScanPageStalledException Stall(int shard = 0) => new()
    {
        TreeId = Tree,
        ShardIndex = shard,
        Phase = Phase,
        TimeoutSeconds = CeilingSeconds,
    };

    /// <summary>
    /// Collects the outcome, tree, and phase tags of every
    /// <see cref="LatticeMetrics.ScanStallFutilityOutcomes"/> measurement.
    /// </summary>
    private sealed class FutilityOutcomes : IDisposable
    {
        private readonly List<(string Outcome, string Tree, string Phase)> _measurements = [];
        private readonly IDisposable _listener;

        internal FutilityOutcomes() => _listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanStallFutilityOutcomes,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                string outcome = string.Empty, tree = string.Empty, phase = string.Empty;
                foreach (var tag in tags)
                {
                    var value = tag.Value?.ToString() ?? string.Empty;
                    if (tag.Key == LatticeMetrics.TagOutcome)
                    {
                        outcome = value;
                    }
                    else if (tag.Key == LatticeMetrics.TagTree)
                    {
                        tree = value;
                    }
                    else if (tag.Key == LatticeMetrics.TagPhase)
                    {
                        phase = value;
                    }
                }

                lock (_measurements)
                {
                    _measurements.Add((outcome, tree, phase));
                }
            }));

        internal string[] Trees
        {
            get { lock (_measurements) return [.. _measurements.Select(m => m.Tree).Distinct()]; }
        }

        internal string[] Phases
        {
            get { lock (_measurements) return [.. _measurements.Select(m => m.Phase).Distinct()]; }
        }

        internal int Count(string outcome)
        {
            lock (_measurements) return _measurements.Count(m => m.Outcome == outcome);
        }

        internal string[] Snapshot()
        {
            lock (_measurements) return [.. _measurements.Select(m => m.Outcome)];
        }

        public void Dispose() => _listener.Dispose();
    }
}
