using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

[TestFixture]
public sealed class ReplayServiceRateControllerTests
{
    [TestCase(1)]
    [TestCase(2)]
    [TestCase(6)]
    [TestCase(64)]
    public void Observe_store_bound_service_converges_to_one_except_recovery_probes(int ceiling)
    {
        var controller = new ReplayServiceRateController(100);
        var withheld = 0;
        var served = 0L;
        var floorWindows = 0;
        for (var window = 0; window < 1000; window++)
        {
            // A serial store completes the same work per window at every limit.
            served += 100;
            var change = controller.Observe(window * 100L, served, true, 0, ceiling - withheld, withheld);
            withheld -= change;
            Assert.That(ceiling - withheld, Is.InRange(1, ceiling));
            if (window > 500)
            {
                Assert.That(ceiling - withheld, Is.LessThanOrEqualTo(Math.Min(2, ceiling)));
                if (ceiling - withheld == 1)
                    floorWindows++;
            }
        }

        Assert.That(floorWindows, Is.GreaterThan(250),
            "the low limit must be held, not merely visited during an oscillation");
    }

    [TestCase(2)]
    [TestCase(6)]
    [TestCase(64)]
    public void Observe_cpu_bound_service_rejects_reductions_and_returns_to_ceiling(int ceiling)
    {
        var controller = new ReplayServiceRateController(100);
        var withheld = 0;
        var served = 0L;
        var restored = 0;
        for (var window = 0; window < 1000; window++)
        {
            // CPU work scales linearly with circulating concurrency.
            served += (ceiling - withheld) * 100L;
            var change = controller.Observe(window * 100L, served, true, 0, ceiling - withheld, withheld);
            withheld -= change;
            Assert.That(withheld, Is.InRange(0, 1),
                "a CPU-bound reduction must be undone before a second reduction is attempted");
            if (change > 0)
            {
                restored++;
                Assert.That(withheld, Is.Zero);
            }
        }

        Assert.That(restored, Is.GreaterThan(50));
    }

    [Test]
    public void Observe_recovery_probes_find_new_cpu_parallelism_after_store_bottleneck_clears()
    {
        var controller = new ReplayServiceRateController(100);
        var withheld = 0;
        var served = 0L;
        var sawCeilingAgain = false;
        const int Ceiling = 6;
        for (var window = 0; window < 500; window++)
        {
            served += window < 200 ? 100 : (Ceiling - withheld) * 100L;
            var change = controller.Observe(window * 100L, served, true, 0, Ceiling - withheld, withheld);
            withheld -= change;
            if (window == 199)
                Assert.That(Ceiling - withheld, Is.LessThanOrEqualTo(2));
            if (window > 200 && withheld == 0)
                sawCeilingAgain = true;
        }

        Assert.That(sawCeilingAgain, Is.True);
    }

    [Test]
    public void Observe_idle_or_memory_pressure_never_licenses_a_new_probe()
    {
        foreach (var memory in new[] { 0, 1, 5 })
        {
            var controller = new ReplayServiceRateController(100);
            for (var window = 0; window < 100; window++)
                Assert.That(controller.Observe(window * 100L, window * 100L, memory > 0, memory, 1, 0), Is.Zero);
        }
    }

    [Test]
    public void Observe_short_windows_and_sparse_completions_do_not_grade_service_rate()
    {
        var controller = new ReplayServiceRateController(100);
        Assert.That(controller.Observe(0, 0, true, 0, 6, 0), Is.Zero);
        Assert.That(controller.Observe(99, 100, true, 0, 6, 0), Is.Zero);
        controller.Reset();
        Assert.That(controller.Observe(100, 100, true, 0, 6, 0), Is.Zero);
        Assert.That(controller.Observe(1000, 107, true, 0, 6, 0), Is.Zero);
        Assert.That(controller.Observe(1100, 108, true, 0, 6, 0), Is.EqualTo(-1));
    }

    [Test]
    public void Observe_clock_or_counter_regression_invalidates_the_window()
    {
        var controller = new ReplayServiceRateController(100);
        Assert.That(controller.Observe(1000, 1000, true, 0, 6, 0), Is.Zero);
        Assert.That(controller.Observe(100, 100, true, 0, 6, 0), Is.Zero);
        Assert.That(controller.Observe(200, 200, true, 0, 6, 0), Is.EqualTo(-1));
    }

    [Test]
    public void Observe_never_grades_a_probe_before_every_old_cold_hold_completes()
    {
        var controller = new ReplayServiceRateController(100);
        var oldHolds = Enumerable.Range(0, 6).Select(_ => controller.Begin()).ToArray();
        controller.Observe(0, 0, true, 0, 6, 0);
        Assert.That(controller.Observe(100, 100, true, 0, 6, 0), Is.EqualTo(-1));
        for (var i = 0; i < 5; i++)
            controller.Complete(oldHolds[i]);
        controller.Complete(controller.Begin());
        Assert.That(controller.Observe(10000, 10000, true, 0, 5, 1), Is.Zero,
            "new holds cannot substitute for the last old cold replay");
        controller.Complete(oldHolds[5]);
        Assert.That(controller.Observe(10100, 10100, true, 0, 5, 1), Is.Zero);
        Assert.That(controller.Observe(10200, 10200, true, 0, 5, 1), Is.Zero);
        Assert.That(controller.Observe(10300, 10300, true, 0, 5, 1), Is.Zero);
        Assert.That(controller.Observe(10400, 10400, true, 0, 5, 1), Is.EqualTo(-1));
    }

    [Test]
    public void Observe_interrupted_downward_probe_restores_only_its_service_permit_after_memory_relief()
    {
        var controller = new ReplayServiceRateController(100);
        controller.Observe(0, 0, true, 0, 6, 0);
        Assert.That(controller.Observe(100, 100, true, 0, 6, 0), Is.EqualTo(-1));
        Assert.That(controller.Observe(200, 200, true, 1, 4, 1), Is.Zero);
        Assert.That(controller.Observe(300, 300, false, 0, 5, 1), Is.EqualTo(1));
        Assert.That(controller.Observe(400, 400, false, 0, 6, 0), Is.Zero);
    }

    [Test]
    public void Observe_no_demand_rolls_back_an_ungraded_probe_once_without_starting_another()
    {
        var controller = new ReplayServiceRateController(100);
        controller.Observe(0, 0, true, 0, 6, 0);
        Assert.That(controller.Observe(100, 100, true, 0, 6, 0), Is.EqualTo(-1));
        Assert.That(controller.Observe(200, 200, false, 0, 5, 1), Is.EqualTo(1));
        for (var window = 3; window < 100; window++)
            Assert.That(controller.Observe(window * 100L, window * 100L, false, 0, 6, 0), Is.Zero);
    }

    [Test]
    public void Observe_generation_enrollment_ignores_clock_ties_and_repeated_resets_keep_one_rollback()
    {
        var controller = new ReplayServiceRateController(100);
        var hold = controller.Begin();
        controller.Observe(0, 0, true, 0, 6, 0);
        Assert.That(controller.Observe(100, 100, true, 0, 6, 0), Is.EqualTo(-1));
        controller.Reset();
        controller.Reset();
        controller.Complete(hold);
        Assert.That(controller.Observe(200, 200, true, 0, 5, 1), Is.EqualTo(1));
        Assert.That(controller.Observe(300, 300, true, 0, 6, 0), Is.Zero);
    }

}
