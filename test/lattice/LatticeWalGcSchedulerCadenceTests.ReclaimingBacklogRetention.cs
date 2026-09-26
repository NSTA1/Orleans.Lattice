using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Gate for the repairable floor-holder sample and its per-consumer budgets
/// surviving a pass that reclaims something while still retaining a backlog
/// (issue #3609).
/// <para>
/// <b>Progress caused the wipe.</b> The sample was retired on any pass that was
/// neither over its byte ceiling nor <c>stranded</c>, and <c>stranded</c> is
/// <c>!reclaimed &amp;&amp; RetainedBacklog</c>. On a partitioned WAL one
/// partition trims a little while another stays pinned by the very holders the
/// sample names, so such a pass reclaims AND retains - and it retired the
/// sample, then the blocked observation, and with it every consumer's
/// <c>FirstObserved</c>. Each re-sample restarted the five-minute
/// <c>ReactivationMinBlockAge</c> clock, so on the live container every episode
/// ended at exactly 3:00 and no holder ever aged into eligibility while the
/// floor stayed frozen.
/// </para>
/// <para>
/// The sibling guard - that a tree which genuinely stops holding a backlog still
/// drops its sample - is
/// <c>A_tree_that_stops_holding_a_backlog_drops_its_candidate_set</c>, and it
/// must keep passing: the fix retires on <c>!RetainedBacklog</c>, not never.
/// </para>
/// </summary>
public sealed partial class LatticeWalGcSchedulerCadenceTests
{
    /// <summary>
    /// A pass that trimmed a little and still stopped on WAL it may not trim -
    /// the shape a partitioned tree reports when one partition moves and
    /// another stays pinned.
    /// </summary>
    private static LatticeWalGcReport ReclaimingStrandedReport() =>
        StrandedReport(entriesTrimmed: 5);

    [Test]
    public async Task A_pass_that_reclaims_while_retaining_a_backlog_keeps_driving_the_sample()
    {
        // Phase one is stranded and trims nothing, so the sweep admits the
        // population and the first budget is spent. Phase two trims something
        // on every pass while still retaining the backlog. The defective build
        // read "trimmed something" as healthy and retired the sample at the
        // phase boundary, freezing the drain at the first pass's budget.
        const int Population = 8;

        var (pins, storage) = RepairablePopulation(Population);

        var time = new VirtualTimeProvider();
        var reclaiming = false;
        var (scheduler, leaves) = SchedulerRepairingFrom(
            pins,
            storage,
            time,
            () => reclaiming ? ReclaimingStrandedReport() : StrandedReport());

        await StartAndRunFirstPassAsync(scheduler, time);
        await DriveToFirstTouchAsync(time, leaves);

        var touchedBeforeReclaiming = leaves.Touched.Count;

        reclaiming = true;

        await AdvanceAtLeastAsync(time, TimeSpan.FromMinutes(20));
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(touchedBeforeReclaiming, Is.EqualTo(TouchesPerPass),
                "the fixture's own premise: the boundary has to fall with the population part-drained, "
                    + "or there is nothing left for the reclaiming phase to finish.");
            Assert.That(leaves.Touched.Distinct().Count(), Is.EqualTo(Population),
                "every holder must still be drained while the tree reclaims a little and retains the rest. "
                    + "A count frozen at the first pass's budget means a pass that trimmed something was "
                    + "read as healthy and the sample was retired behind a floor that is still pinned.");
        });
    }

    [Test]
    public async Task A_holder_keeps_its_block_age_across_a_pass_that_reclaims_while_retaining()
    {
        // The budget half of the defect. A holder that has already served most
        // of ReactivationMinBlockAge sees one pass that trims a little while the
        // backlog is retained. It must become eligible when its ORIGINAL block
        // age reaches the minimum, not a full minimum after that pass - the
        // defective build dropped its FirstObserved there and re-admitted it
        // fresh on the next sample, so every such pass restarted the clock.
        var (pins, storage) = RepairablePopulation(1);

        var time = new VirtualTimeProvider();
        DateTimeOffset? admitted = null;
        DateTimeOffset? dropPass = null;
        var (scheduler, leaves) = SchedulerRepairingFrom(
            pins,
            storage,
            time,
            () =>
            {
                if (admitted is { } start
                    && dropPass is null
                    && time.GetUtcNow() - start >= TimeSpan.FromMinutes(3))
                {
                    dropPass = time.GetUtcNow();
                    return ReclaimingStrandedReport();
                }

                return StrandedReport();
            });

        await StartAndRunFirstPassAsync(scheduler, time);
        admitted = time.GetUtcNow();

        var guard = 0;
        while (leaves.Touched.Count == 0)
        {
            await TickAsync(time);
            Assert.That(++guard, Is.LessThan(500), "the arm never drove the holder at all.");
        }

        var firstTouch = time.GetUtcNow();
        await scheduler.StopAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(dropPass, Is.Not.Null,
                "the fixture's own premise: the reclaiming pass must have run before the first touch, "
                    + "or the budget was never put at risk.");
            Assert.That(firstTouch - admitted!.Value, Is.GreaterThanOrEqualTo(TimeSpan.FromMinutes(5)),
                "the minimum block age must still be served from first admission; a touch before it "
                    + "means the gate was bypassed rather than preserved.");
            Assert.That(firstTouch - dropPass!.Value, Is.LessThan(TimeSpan.FromMinutes(5)),
                $"the holder must be touched on its original block age (admitted {admitted}, reclaiming "
                    + $"pass {dropPass}, first touch {firstTouch}). A touch a full ReactivationMinBlockAge "
                    + "or more after the reclaiming pass means that pass retired the observation and the "
                    + "re-sample restarted FirstObserved.");
        });
    }
}
