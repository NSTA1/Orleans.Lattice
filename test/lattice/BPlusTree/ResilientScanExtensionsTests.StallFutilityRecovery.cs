using NSubstitute;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Coverage for the futility discriminator (issue 2575): the sibling series that
/// says whether a source a resilient scan abandoned at the consecutive stall
/// bound was merely busy or genuinely not yielding.
/// <para>
/// WHY EVERY TEST HERE IS PAIRED. <c>budget-exhausted</c> cannot distinguish
/// those two cases, which is the whole reason this counter exists; a test suite
/// that could only ever assert an arm firing would inherit exactly that defect,
/// because a positive-only assertion cannot separate "the change landed" from
/// "the check is broken". So each test that drives one arm also asserts its
/// opposite reads zero, and
/// <see cref="Futility_recovery_requires_passing_the_abandoned_position"/> pins
/// the position condition by driving a case that must NOT be recorded as
/// recovered.
/// </para>
/// </summary>
public partial class ResilientScanExtensionsTests
{
    // ── recovered ──────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task Futility_termination_whose_source_later_yields_past_it_is_recorded_as_recovered()
    {
        var clock = new FutilityClock();
        using var watch = UseFutilityWatch(clock);
        var outcomes = new FutilityOutcomes();

        var lattice = Substitute.For<ILattice>();
        ExhaustFutilityBudgetAfterYielding(lattice, new[] { "a", "b" });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });

        Assert.That(
            outcomes.Snapshot(), Is.Empty,
            "the watch is open, not resolved: the termination alone says nothing about the source");

        // The same tree, read again, now serves a record beyond the position the
        // futile walk died on.
        StubKeys(lattice, _ => ScriptedKeys(new[] { "c", "d" }, abortAfter: int.MaxValue));
        var keys = await CollectAsync(lattice.ScanKeysAsync());
        outcomes.Dispose();

        Assert.That(keys, Is.EqualTo(new[] { "c", "d" }));
        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.EqualTo(1),
                "the abandoned source served the region it had refused, so the consecutive bound "
                + "cut off a walk that was recoverable. Any sustained non-zero reading of this arm "
                + "in the field is the finding that DefaultScanStallResumeAttempts is too tight.");
            Assert.That(
                outcomes.Count(ScanStallFutilityWatch.OutcomeStillStalled), Is.Zero,
                "a source that recovered must never also be recorded dead - the two arms are the "
                + "opposite conclusions this counter exists to separate");
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeUnobserved), Is.Zero);
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeDropped), Is.Zero);
        });
    }

    [Test]
    [NonParallelizable]
    public async Task Futility_recovery_requires_passing_the_abandoned_position()
    {
        var clock = new FutilityClock();
        using var watch = UseFutilityWatch(clock);
        var outcomes = new FutilityOutcomes();

        var lattice = Substitute.For<ILattice>();
        ExhaustFutilityBudgetAfterYielding(lattice, new[] { "d", "e" });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });

        // A later scan of a lower range: it reads the same tree and yields
        // records, but never reaches the region the futile walk died in.
        StubKeys(lattice, _ => ScriptedKeys(new[] { "a", "b" }, abortAfter: int.MaxValue));
        await CollectAsync(lattice.ScanKeysAsync(endExclusive: "c"));
        outcomes.Dispose();

        Assert.That(
            outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.Zero,
            "records from elsewhere in the tree are not evidence the abandoned region recovered. "
            + "This test is what keeps the recovered arm meaningful: delete the position check in "
            + "ScanStallFutilityWatch and this is the assertion that fails, while every test that "
            + "only drives the arm positively would still pass.");
    }

    // ── still-stalled ──────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public void A_source_that_kills_a_second_walk_for_futility_is_recorded_as_still_stalled()
    {
        var clock = new FutilityClock();
        using var watch = UseFutilityWatch(clock);
        var outcomes = new FutilityOutcomes();

        var lattice = Substitute.For<ILattice>();
        StubKeys(lattice, _ => StalledKeys(Array.Empty<string>(), stallAfter: 0));

        for (var pass = 0; pass < 2; pass++)
        {
            Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            {
                await foreach (var _ in lattice.ScanKeysAsync())
                {
                }
            });
        }

        outcomes.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.Count(ScanStallFutilityWatch.OutcomeStillStalled), Is.EqualTo(1),
                "the second walk reached the same source and also died for futility there, which "
                + "is the only evidence that the source was not merely busy");
            Assert.That(
                outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.Zero,
                "the paired negative: a genuinely dead source must read zero recovered. Without "
                + "this the recovered arm could be wired to fire on any later scan at all and the "
                + "positive test above would not notice.");
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeUnobserved), Is.Zero);
        });
    }

    // ── unobserved ─────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task A_futility_termination_nobody_revisits_is_recorded_as_unobserved()
    {
        var clock = new FutilityClock();
        using var watch = UseFutilityWatch(clock);
        var outcomes = new FutilityOutcomes();

        var abandoned = Substitute.For<ILattice>();
        StubKeys(abandoned, _ => StalledKeys(Array.Empty<string>(), stallAfter: 0));

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in abandoned.ScanKeysAsync())
            {
            }
        });

        clock.Advance(ScanStallFutilityWatch.WindowFor(TestStallCeilingSeconds) + TimeSpan.FromSeconds(1));

        // Any later scan sweeps; an unrelated tree is used so nothing about the
        // abandoned source is observed.
        var unrelated = Substitute.For<ILattice>();
        StubKeys(unrelated, _ => ScriptedKeys(new[] { "z" }, abortAfter: int.MaxValue));
        await CollectAsync(unrelated.ScanKeysAsync());
        outcomes.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.Count(ScanStallFutilityWatch.OutcomeUnobserved), Is.EqualTo(1),
                "nobody came back to the abandoned source inside the window, so this termination "
                + "says nothing either way. Recording it is what stops the missing recovered "
                + "reading as evidence the source was dead.");
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.Zero);
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeStillStalled), Is.Zero);
        });
    }

    // ── what must not be watched ───────────────────────────────

    [Test]
    [NonParallelizable]
    public async Task A_scan_that_never_stalls_records_no_futility_outcome()
    {
        var clock = new FutilityClock();
        using var watch = UseFutilityWatch(clock);
        var outcomes = new FutilityOutcomes();

        var lattice = Substitute.For<ILattice>();
        StubKeys(lattice, _ => ScriptedKeys(new[] { "a", "b", "c" }, abortAfter: int.MaxValue));

        var keys = await CollectAsync(lattice.ScanKeysAsync());
        outcomes.Dispose();

        Assert.That(keys, Is.EqualTo(new[] { "a", "b", "c" }));
        Assert.That(
            outcomes.Snapshot(), Is.Empty,
            "the healthy path must be silent, and the HasWatches guard that makes it silent is "
            + "also what makes the per-record hook cheap enough to leave enabled by default");
        Assert.That(
            LatticeExtensions.FutilityWatch.HasWatches, Is.False,
            "no watch is open, so the per-record hook is a single volatile read");
    }

    [Test]
    [NonParallelizable]
    public async Task A_ceiling_exhausted_termination_opens_no_futility_watch()
    {
        var clock = new FutilityClock();
        using var watch = UseFutilityWatch(clock);
        var outcomes = new FutilityOutcomes();

        var lattice = Substitute.For<ILattice>();
        var next = 'a';
        StubKeys(lattice, _ => StalledKeys(new[] { next++.ToString() }, stallAfter: 1));

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });

        Assert.That(
            LatticeExtensions.FutilityWatch.HasWatches, Is.False,
            "ceiling-exhausted already reports a fact about a constant chosen here rather than "
            + "about the source, so there is no ambiguity for a follow-up observation to resolve");

        StubKeys(lattice, _ => ScriptedKeys(new[] { "z" }, abortAfter: int.MaxValue));
        await CollectAsync(lattice.ScanKeysAsync());
        outcomes.Dispose();

        Assert.That(outcomes.Snapshot(), Is.Empty);
    }

    // ── saturation ─────────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public void A_watch_evicted_by_capacity_is_recorded_as_dropped()
    {
        var clock = new FutilityClock();
        using var watch = UseFutilityWatch(clock, capacity: 1);
        var outcomes = new FutilityOutcomes();

        for (var pass = 0; pass < 2; pass++)
        {
            var lattice = Substitute.For<ILattice>();
            StubKeys(lattice, _ => StalledKeys(Array.Empty<string>(), stallAfter: 0));
            Assert.ThrowsAsync<ScanPageStalledException>(async () =>
            {
                await foreach (var key in lattice.ScanKeysAsync())
                {
                    Assert.Fail($"the scripted source yields nothing, but produced {key}");
                }
            });
        }

        outcomes.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(
                outcomes.Count(ScanStallFutilityWatch.OutcomeDropped), Is.EqualTo(1),
                "a saturated table must say so. Discarding the evicted watch silently would "
                + "depress every other arm during exactly the burst that makes this instrument "
                + "worth reading, and the shortfall would be invisible.");
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeRecovered), Is.Zero);
            Assert.That(outcomes.Count(ScanStallFutilityWatch.OutcomeStillStalled), Is.Zero);
        });
    }

    // ── view surface ───────────────────────────────────────────

    [Test]
    [NonParallelizable]
    public void The_futility_watch_never_re_drives_the_abandoned_work()
    {
        var clock = new FutilityClock();
        using var watch = UseFutilityWatch(clock);

        var lattice = Substitute.For<ILattice>();
        var calls = 0;
        StubKeys(lattice, _ =>
        {
            calls++;
            return StalledKeys(Array.Empty<string>(), stallAfter: 0);
        });

        Assert.ThrowsAsync<ScanPageStalledException>(async () =>
        {
            await foreach (var _ in lattice.ScanKeysAsync())
            {
            }
        });

        Assert.That(
            calls, Is.EqualTo(LatticeExtensions.DefaultScanStallResumeAttempts + 1),
            "the discriminator observes recoverability; it must not change the termination policy. "
            + "One call per permitted resume plus the terminal one, and not a single probe more - "
            + "raising the consecutive budget trades away the futility protection that stops a "
            + "genuinely dead source being retried forever, and that is a separate decision.");
    }

    // ── window derivation ──────────────────────────────────────

    [Test]
    public void The_futility_window_is_derived_from_the_ceiling_the_stall_reported()
    {
        Assert.That(
            ScanStallFutilityWatch.WindowFor(20),
            Is.EqualTo(TimeSpan.FromSeconds(20 * ScanStallFutilityWatch.WindowCeilingMultiple)));
    }

    [Test]
    public void The_futility_window_falls_back_to_the_page_duration_when_no_ceiling_is_reported()
    {
        var expected = TimeSpan.FromSeconds(
            LatticeOptions.DefaultMaxScanPageDuration.TotalSeconds
            * ScanStallFutilityWatch.WindowCeilingMultiple);

        Assert.Multiple(() =>
        {
            Assert.That(ScanStallFutilityWatch.WindowFor(0), Is.EqualTo(expected));
            Assert.That(ScanStallFutilityWatch.WindowFor(-1), Is.EqualTo(expected));
            Assert.That(ScanStallFutilityWatch.WindowFor(double.NaN), Is.EqualTo(expected));
        });
    }

    // ── harness ────────────────────────────────────────────────

    private static IDisposable UseFutilityWatch(
        FutilityClock clock,
        int capacity = ScanStallFutilityWatch.DefaultCapacity)
        => LatticeExtensions.UseFutilityWatch(new ScanStallFutilityWatch(clock, capacity));

    /// <summary>
    /// Drives a walk that banks <paramref name="keys"/> and then dies for
    /// futility, so the watch it opens carries a real abandoned position rather
    /// than the trivially-satisfied origin.
    /// </summary>
    private static void ExhaustFutilityBudgetAfterYielding(ILattice lattice, string[] keys)
    {
        var callIndex = 0;
        StubKeys(lattice, _ => callIndex++ == 0
            ? StalledKeys(keys, stallAfter: keys.Length)
            : StalledKeys(Array.Empty<string>(), stallAfter: 0));
    }

    private sealed class FutilityClock : TimeProvider
    {
        private DateTimeOffset _now = new(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        public override DateTimeOffset GetUtcNow() => _now;

        internal void Advance(TimeSpan by) => _now += by;
    }

    /// <summary>
    /// Collects the outcome tag of every
    /// <see cref="LatticeMetrics.ScanStallFutilityOutcomes"/> measurement.
    /// </summary>
    private sealed class FutilityOutcomes : IDisposable
    {
        private readonly List<string> _outcomes = [];
        private readonly IDisposable _listener;

        internal FutilityOutcomes() => _listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanStallFutilityOutcomes,
            l => l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome)
                    {
                        lock (_outcomes) _outcomes.Add(tag.Value?.ToString() ?? string.Empty);
                    }
                }
            }));

        internal int Count(string outcome)
        {
            lock (_outcomes) return _outcomes.Count(o => o == outcome);
        }

        internal string[] Snapshot()
        {
            lock (_outcomes) return [.. _outcomes];
        }

        public void Dispose() => _listener.Dispose();
    }
}
