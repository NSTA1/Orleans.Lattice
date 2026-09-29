using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
[NonParallelizable]
public sealed class StarvationReplayAdmissionTests
{
    private const BPlusLeafGrain.StarvationDriveOrigin Sweep = BPlusLeafGrain.StarvationDriveOrigin.WalGcSweep;
    private const BPlusLeafGrain.StarvationDriveOrigin Timer = BPlusLeafGrain.StarvationDriveOrigin.CoverageLagTimer;

    [TearDown]
    public void Reset() => BPlusLeafGrain.ResetReplayConcurrencyGateForTest();

    [TestCase(1, 1)]
    [TestCase(2, 1)]
    [TestCase(6, 3)]
    [TestCase(7, 3)]
    public void Acquisition_reserves_only_the_gc_share(int ceiling, int expected)
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, queued: 0);
        using var gate = new SemaphoreSlim(ceiling, ceiling);
        for (var i = 0; i < expected; i++)
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True);
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.False);
        Assert.That(gate.CurrentCount, Is.EqualTo(ceiling - expected));
        for (var i = 0; i < expected; i++)
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        Assert.That(gate.CurrentCount, Is.EqualTo(ceiling));
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True);
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
    }

    [Test]
    public void Failed_shared_acquisition_returns_the_gc_reservation()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 2, queued: 0);
        using var gate = new SemaphoreSlim(0, 2);
        for (var i = 0; i < 10; i++)
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.False);
        gate.Release(2);
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True);
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        Assert.That(gate.CurrentCount, Is.EqualTo(2));
    }

    [Test]
    public void Concurrent_acquisitions_cannot_exceed_the_gc_share()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);
        using var gate = new SemaphoreSlim(6, 6);
        var acquired = 0;
        Parallel.For(0, 64, _ =>
        {
            if (BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep))
                Interlocked.Increment(ref acquired);
        });
        Assert.That(acquired, Is.EqualTo(3));
        Assert.That(gate.CurrentCount, Is.EqualTo(3));
        for (var i = 0; i < acquired; i++)
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        Assert.That(gate.CurrentCount, Is.EqualTo(6));
    }

    /// <summary>
    /// Issue #3575. The coverage-lag timer and the WAL GC sweep drew on one
    /// share first come, first served, and the timer won by volume. A timer
    /// drive is now admitted only below one less than the share, wherever the
    /// share has a slot to spare.
    /// </summary>
    [TestCase(1, 1, 1)]
    [TestCase(2, 1, 1)]
    [TestCase(3, 1, 1)]
    [TestCase(4, 2, 1)]
    [TestCase(6, 3, 2)]
    [TestCase(7, 3, 2)]
    [TestCase(32, 16, 15)]
    public void StarvationReplayLimit_keeps_the_last_slot_from_the_timer_whenever_the_share_can_spare_one(
        int ceiling, int share, int timerLimit)
    {
        Assert.Multiple(() =>
        {
            Assert.That(BPlusLeafGrain.StarvationReplayLimit(ceiling, Sweep), Is.EqualTo(share),
                "the sweep may use the whole GC share, which is unchanged by issue #3575.");
            Assert.That(BPlusLeafGrain.StarvationReplayLimit(ceiling, Timer), Is.EqualTo(timerLimit),
                "the timer must stop one short of the share, except where the share is one slot and "
                + "a reservation would leave the timer nothing at all.");
        });
    }

    /// <summary>
    /// The defect shape of issue #3575, on the arithmetic alone: timer drives
    /// fill everything they may, and a sweep drive is still admitted.
    /// </summary>
    [TestCase(4, 2, 1)]
    [TestCase(6, 3, 2)]
    [TestCase(16, 8, 7)]
    public void Timer_drives_holding_every_slot_they_may_leave_the_sweep_a_slot(int ceiling, int share, int timerLimit)
    {
        // The expected widths are spelled out rather than read back from
        // StarvationReplayLimit, so a regression in that function cannot move
        // the expectation along with the behaviour.
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling, queued: 0);
        using var gate = new SemaphoreSlim(ceiling, ceiling);

        var timers = 0;
        while (BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer))
        {
            timers++;
            Assert.That(timers, Is.LessThanOrEqualTo(share), "the timer must never exceed the GC share.");
        }

        var sweeps = 0;
        while (BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep))
            sweeps++;

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(timers, Is.EqualTo(timerLimit),
                    "the timer must fill exactly its own limit, so the refusal that ended it is that limit.");
                Assert.That(sweeps, Is.EqualTo(share - timerLimit),
                    "the slot the timer may not take must be free for a WAL GC sweep drive - the only drive "
                    + "that lifts a pin holding a tree's cursor floor. Before issue #3575 the timer could hold "
                    + "the whole share and the sweep was refused.");
                Assert.That(gate.CurrentCount, Is.EqualTo(ceiling - share),
                    "and the two together must still stay inside the GC share, leaving interactive "
                    + "activations the rest of the gate (issue #3480).");
            });
        }
        finally
        {
            for (var i = 0; i < timers + sweeps; i++)
                BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }

        Assert.That(gate.CurrentCount, Is.EqualTo(ceiling), "every permit must come back.");
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.True,
            "and every GC reservation with it, or the timer is locked out for the process lifetime.");
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
    }

    /// <summary>
    /// The reservation limits the held count, not which drives hold it, so a
    /// timer drive never takes the last free slot even when the sweep holds the
    /// others (issue #3575). A per-origin cap would let it, and the sweep's next
    /// touch would be refused.
    /// </summary>
    [Test]
    public void A_timer_drive_is_refused_the_last_free_slot_whoever_holds_the_others()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);
        using var gate = new SemaphoreSlim(6, 6);

        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True);
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True);
        try
        {
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.False,
                "with two of three slots held by the sweep, the free one is the last: a timer drive that "
                + "took it would leave the sweep's next touch nothing.");
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True,
                "the last slot stays free for the sweep, which may fill the share.");
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }
        finally
        {
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }

        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True);
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.True,
            "one slot held leaves two free, so the timer may take one of them.");
        try
        {
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.False,
                "but not the last, whatever the mix of holders.");
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True);
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }
        finally
        {
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }

        Assert.That(gate.CurrentCount, Is.EqualTo(6));
    }

    [Test]
    public void Concurrent_timer_acquisitions_cannot_take_the_slot_kept_for_the_sweep()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);
        using var gate = new SemaphoreSlim(6, 6);
        var acquired = 0;
        Parallel.For(0, 64, _ =>
        {
            if (BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer))
                Interlocked.Increment(ref acquired);
        });

        try
        {
            Assert.That(acquired, Is.EqualTo(2),
                "racing timer drives must still stop one short of the three-slot share.");
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True,
                "so the sweep's slot is free after the race.");
            BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }
        finally
        {
            for (var i = 0; i < acquired; i++)
                BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }

        Assert.That(gate.CurrentCount, Is.EqualTo(6));
    }

    [Test]
    public void Failed_timer_acquisition_returns_the_gc_reservation()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);
        using var gate = new SemaphoreSlim(0, 6);
        for (var i = 0; i < 10; i++)
            Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.False,
                "a gate with no free permit refuses whoever asks.");

        gate.Release(6);
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.True);
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.True,
            "a refusal at the gate must return its GC reservation, or ten refusals would leave the timer "
            + "locked out of a share it holds none of.");
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        Assert.That(gate.CurrentCount, Is.EqualTo(6));
    }

    /// <summary>
    /// Where the share is a single slot there is nothing to reserve, so the
    /// timer yields the slot to a refused sweep drive until the sweep is
    /// admitted again (issue #3575).
    /// </summary>
    [Test]
    public void A_single_slot_share_is_yielded_to_a_refused_sweep_drive_until_the_sweep_is_admitted()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 2, queued: 0);
        using var gate = new SemaphoreSlim(2, 2);

        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True, "precondition");
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.False,
            "precondition: a second sweep drive is refused by the one-slot share.");
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);

        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.False,
            "the slot is free, but a sweep drive was just refused it: the timer must leave it for the "
            + "sweep's retry rather than take it first, or the sweep loses on volume exactly as before.");

        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True,
            "the sweep's retry takes the slot the timer left.");
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);

        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.True,
            "once the sweep has been admitted the timer may use the slot again, or a single refusal "
            + "would shut the timer out of the share for good.");
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
    }

    [Test]
    public void A_single_slot_share_stops_yielding_once_the_sweep_refusal_is_older_than_the_window()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 2, queued: 0);
        using var gate = new SemaphoreSlim(2, 2);
        var window = BPlusLeafGrain.SweepDriveRefusalTimerYield;

        BPlusLeafGrain.SeedSweepDriveRefusalForTest(window - TimeSpan.FromMinutes(1));
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.False,
            "inside the window the timer yields.");

        BPlusLeafGrain.SeedSweepDriveRefusalForTest(window + TimeSpan.FromMinutes(1));
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.True,
            "a sweep that stopped asking must not hold the timer off for longer than the window.");
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
    }

    [Test]
    public void A_refused_sweep_drive_does_not_make_the_timer_yield_a_share_with_a_reserved_slot()
    {
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(ceiling: 6, queued: 0);
        using var gate = new SemaphoreSlim(6, 6);
        BPlusLeafGrain.SeedSweepDriveRefusalForTest(TimeSpan.Zero);

        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.True,
            "where the last free slot is already kept for the sweep, the timer keeps the rest of the share: "
            + "yielding it as well would starve the only remedy for a live leaf the sweep never reaches.");
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
    }

    [TestCase(6, 0, 6)]
    [TestCase(6, 2, 4)]
    [TestCase(6, 5, 1)]
    [TestCase(6, 6, 1)]
    [TestCase(1, 0, 1)]
    public void CirculatingReplayPermits_subtracts_the_withheld_permits_with_a_floor_of_one(
        int ceiling, int withheld, int expected)
        => Assert.That(BPlusLeafGrain.CirculatingReplayPermits(ceiling, withheld), Is.EqualTo(expected));

    /// <summary>
    /// Issue #3610. The share was sized from the configured ceiling, so with
    /// two of six permits withheld the timer could take two of the four still
    /// circulating and the GC drives three, leaving interactive activations
    /// one. Sized from what circulates, the share is two, the timer one.
    /// </summary>
    [Test]
    public void The_gc_share_is_sized_from_the_permits_in_circulation_not_the_configured_ceiling()
    {
        const int Ceiling = 6;
        const int Withheld = 2;
        const int Circulating = Ceiling - Withheld;
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(Ceiling, queued: 0);
        BPlusLeafGrain.SeedWithheldReplayPermitsForTest(Withheld);
        using var gate = new SemaphoreSlim(Circulating, Ceiling);

        var timers = 0;
        while (BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer))
            timers++;

        var sweeps = 0;
        while (BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep))
            sweeps++;

        try
        {
            Assert.Multiple(() =>
            {
                Assert.That(timers, Is.EqualTo(1),
                    "four permits circulate, so the share is two and the timer stops one short of it. Sized "
                    + "from the configured six, the timer took two.");
                Assert.That(sweeps, Is.EqualTo(1),
                    "the slot the timer may not take stays free for a WAL GC sweep drive.");
                Assert.That(gate.CurrentCount, Is.EqualTo(Circulating / 2),
                    "and GC drives together hold at most half the permits in circulation, leaving the rest "
                    + "to interactive activations (issue #3480).");
            });
        }
        finally
        {
            for (var i = 0; i < timers + sweeps; i++)
                BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);
        }

        Assert.That(gate.CurrentCount, Is.EqualTo(Circulating), "every permit must come back.");
    }

    /// <summary>
    /// Issue #3610, at the withholding floor: six configured, five withheld, one
    /// circulating. Sized from six the share had three slots, so the timer held
    /// the last permit and never yielded it to a refused sweep drive. Sized from
    /// what circulates it is a single-slot share, which the timer yields.
    /// </summary>
    [Test]
    public void At_the_withholding_floor_the_timer_yields_the_last_circulating_permit_to_a_refused_sweep_drive()
    {
        const int Ceiling = 6;
        BPlusLeafGrain.SeedReplayAdmissionStateForTest(Ceiling, queued: 0);
        BPlusLeafGrain.SeedWithheldReplayPermitsForTest(Ceiling - 1);
        using var gate = new SemaphoreSlim(1, Ceiling);

        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.True,
            "precondition: with no sweep refusal outstanding the timer may use the single slot.");
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.False,
            "precondition: the timer holds the only circulating permit, so the sweep drive is refused.");
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);

        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Timer), Is.False,
            "one permit circulates, so the share is a single slot and the timer must leave it for the refused "
            + "sweep drive. Sized from the configured ceiling the share had three slots, the yield never "
            + "applied, and coverage-lag timer drives could hold every permit still in circulation.");
        Assert.That(BPlusLeafGrain.TryAcquireStarvationReplayPermit(gate, Sweep), Is.True,
            "the sweep's retry takes the permit the timer left.");
        BPlusLeafGrain.ReleaseStarvationReplayPermit(gate);

        Assert.That(gate.CurrentCount, Is.EqualTo(1), "every permit must come back.");
    }
}
