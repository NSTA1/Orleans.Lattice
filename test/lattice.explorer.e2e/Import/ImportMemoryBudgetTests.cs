namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Import;

/// <summary>
/// The verdict of the import's bounded-memory proof (#5558), on readings recorded by real CI
/// runs of <see cref="ImportMemoryTests"/> and on processes built to hold or leak the file. A
/// silo keeps some state the first time the upsert path runs through it, and whether the
/// quarter-size warm-up or the measured import pays for it varies from run to run; that
/// one-time state must not read as the file, and a process holding or leaking the file must
/// still fail.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class ImportMemoryBudgetTests
{
    private const long Mb = 1024L * 1024;
    private const long FileBytes = 200 * Mb;
    private const long WarmUpBytes = FileBytes / 4;

    /// <summary>
    /// Explorer Journeys run 41265312788 (job 124728475978), west-silo-0 during the 200 MB
    /// upsert: live baseline 140.6 MB, peak 176.0 MB, settled 162.1 MB, warm-up live growth
    /// 20.8 MB. The silo held 13.9 MB above its settled heap while the file streamed, and kept
    /// 21.5 MB of the state its first warm upsert path builds, which the warm-up had not built
    /// on this silo. The warm-up baseline is the lowest the recorded warm-up growth allows.
    /// </summary>
    [Test]
    public void A_silo_paying_its_one_time_state_in_the_measured_import_is_within_budget()
    {
        var reading = Recorded("west-silo-0", baseline: 1406, peak: 1760, settled: 1621, warmUpGrowth: 208);

        Assert.That(ImportMemoryBudget.Violations(reading, "upsert", FileBytes), Is.Empty);
    }

    /// <summary>
    /// Explorer Journeys run 41254018379, west-silo-1 during the 200 MB upsert: live baseline
    /// 140.7 MB, peak 179.3 MB, settled 168.8 MB, warm-up live growth 17.8 MB - 38.6 MB of
    /// live growth, of which 28.1 MB stayed once the import settled.
    /// </summary>
    [Test]
    public void The_largest_late_one_time_state_recorded_is_within_budget()
    {
        var reading = Recorded("west-silo-1", baseline: 1407, peak: 1793, settled: 1688, warmUpGrowth: 178);

        Assert.That(ImportMemoryBudget.Violations(reading, "upsert", FileBytes), Is.Empty);
    }

    /// <summary>
    /// Explorer Journeys run 41251581260, west-silo-1 during the 200 MB upsert: the warm-up
    /// built 50.8 MB of state and the import kept 2.2 MB more, the most any recorded silo
    /// kept across both imports.
    /// </summary>
    [Test]
    public void The_largest_one_time_state_recorded_across_both_imports_is_within_budget()
    {
        var reading = Recorded("west-silo-1", baseline: 1694, peak: 1856, settled: 1716, warmUpGrowth: 508);

        Assert.That(ImportMemoryBudget.Violations(reading, "upsert", FileBytes), Is.Empty);
    }

    /// <summary>
    /// Explorer Journeys run 45008970668 (job 137166900799), west-silo-1 during the 200 MB
    /// upsert: warm-up live baseline 143.4 MB, live baseline 188.5 MB, peak 246.3 MB, settled
    /// 245.7 MB - 0.7 MB in flight, read as keeping 102.2 MB because its heap had not yet come
    /// down (#6115). Judged on the heap it drains to, it passes; the in-flight bound still
    /// reads the settled heap, so draining further does not widen it.
    /// </summary>
    [Test]
    public void A_silo_that_drains_after_its_quiet_rounds_is_judged_on_its_drained_heap()
    {
        var reading = Reading(warmUpBaseline: 1434 * Mb / 10, baseline: 1885 * Mb / 10, peak: 2463 * Mb / 10, settled: 2457 * Mb / 10);

        Assert.Multiple(() =>
        {
            Assert.That(ImportMemoryBudget.Violations(reading, "upsert", FileBytes), Has.Count.EqualTo(1), "the recorded reading, before the drain");
            Assert.That(ImportMemoryBudget.Violations(reading with { LiveDrained = 150 * Mb }, "upsert", FileBytes), Is.Empty);
            Assert.That((reading with { LiveDrained = 150 * Mb }).LiveInFlight, Is.EqualTo(reading.LiveInFlight));
        });
    }

    /// <summary>A process that keeps state past the hold is still read as keeping it.</summary>
    [Test]
    public void A_process_still_keeping_state_once_drained_fails()
    {
        var reading = Reading(warmUpBaseline: 120 * Mb, baseline: 125 * Mb, peak: 200 * Mb, settled: 199 * Mb) with { LiveDrained = 190 * Mb };

        Assert.That(ImportMemoryBudget.Violations(reading, "upsert", FileBytes), Has.Count.EqualTo(1));
    }

    /// <summary>
    /// Explorer Journeys run 45042005049, west-silo-1 during the 200 MB upsert (#6150): kept
    /// 97.9 MB, settled 245.6 MB and drained only to 241.8 MB, so the hold did not release it.
    /// A heap still falling by a drain step, or one under the kept ceiling, is not retained.
    /// </summary>
    [Test]
    public void A_heap_the_hold_does_not_bring_down_is_retained()
    {
        var reading = Reading(warmUpBaseline: 1439 * Mb / 10, baseline: 2139 * Mb / 10, peak: 2794 * Mb / 10, settled: 2456 * Mb / 10) with { LiveDrained = 2418 * Mb / 10 };

        Assert.Multiple(() =>
        {
            Assert.That(ImportMemoryBudget.IsRetained(reading), Is.True);
            Assert.That(ImportMemoryBudget.IsRetained(reading with { LiveDrained = 2400 * Mb / 10 }), Is.False, "still draining");
            Assert.That(ImportMemoryBudget.IsRetained(reading with { WarmUpLiveBaseline = 200 * Mb }), Is.False, "under the kept ceiling");
        });
    }

    /// <summary>A process holding a fifth of the file while it streams, and releasing it after, fails.</summary>
    [Test]
    public void A_process_holding_a_fifth_of_the_file_in_flight_fails()
    {
        var reading = Reading(warmUpBaseline: 120 * Mb, baseline: 125 * Mb, peak: 125 * Mb + FileBytes / 5, settled: 126 * Mb);

        Assert.That(ImportMemoryBudget.Violations(reading, "upsert", FileBytes), Has.Count.EqualTo(1));
    }

    /// <summary>
    /// A process keeping three tenths of every byte that streams through it fails: it keeps
    /// 15 MB of the warm-up's file and 60 MB of the import's.
    /// </summary>
    [Test]
    public void A_process_keeping_a_share_of_every_byte_streamed_fails()
    {
        var keptByWarmUp = WarmUpBytes * 3 / 10;
        var keptByImport = FileBytes * 3 / 10;
        var reading = Reading(
            warmUpBaseline: 120 * Mb,
            baseline: 120 * Mb + keptByWarmUp,
            peak: 120 * Mb + keptByWarmUp + keptByImport,
            settled: 120 * Mb + keptByWarmUp + keptByImport);

        Assert.That(ImportMemoryBudget.Violations(reading, "upsert", FileBytes), Has.Count.EqualTo(1));
    }

    /// <summary>A process whose live heap was never read fails rather than passing unread.</summary>
    [Test]
    public void A_process_whose_live_heap_was_never_read_fails()
    {
        var reading = new WorkingSetGrowth("west-silo-0", Baseline: 400 * Mb, Peak: 420 * Mb, Samples: 90);

        Assert.That(ImportMemoryBudget.Violations(reading, "upsert", FileBytes), Is.Not.Empty);
    }

    /// <summary>
    /// A recorded reading, in tenths of a megabyte as the job printed them. Only the warm-up's
    /// growth was printed, not its baseline: the warm-up started at least that growth below the
    /// settled heap it ended on, which is the import's baseline, so the reading takes the
    /// lowest warm-up baseline the record allows - the one that charges the most.
    /// </summary>
    private static WorkingSetGrowth Recorded(string process, long baseline, long peak, long settled, long warmUpGrowth) =>
        new(process, Baseline: 400 * Mb, Peak: 420 * Mb, Samples: 200)
        {
            LiveBaseline = baseline * Mb / 10,
            LivePeak = peak * Mb / 10,
            LiveAfter = settled * Mb / 10,
            LiveSamples = 45,
            WarmUpLiveGrowth = warmUpGrowth * Mb / 10,
            WarmUpLiveBaseline = (baseline - warmUpGrowth) * Mb / 10,
        };

    private static WorkingSetGrowth Reading(long warmUpBaseline, long baseline, long peak, long settled) =>
        new("west-silo-0", Baseline: 400 * Mb, Peak: 420 * Mb, Samples: 200)
        {
            LiveBaseline = baseline,
            LivePeak = peak,
            LiveAfter = settled,
            LiveSamples = 45,
            WarmUpLiveBaseline = warmUpBaseline,
        };
}
