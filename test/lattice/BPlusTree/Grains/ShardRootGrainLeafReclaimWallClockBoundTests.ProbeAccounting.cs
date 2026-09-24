using NSubstitute;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue 2682: the reclaim walk issued its probes from four sites and counted
/// only one of them against its budget, and it did not consult the deadline
/// until the walk had already been entered.
/// <para>
/// The two uncounted sites that ran before the budget existed were the resume
/// probe and the head probe that enter the walk; the third was the re-probe of
/// the predecessor after every fold. So a pass that reported <c>probed 0</c>
/// had probed once, a pass that folded understated by two, and a pass could
/// issue one probe more than its budget. And because the entry ran ahead of the
/// first deadline check, a pass whose prologue had already consumed the whole
/// deadline still descended and probed before standing down - a field pass
/// reported <c>folded 0, probed 0, stopped on deadline</c> after 25 seconds.
/// </para>
/// <para>
/// <b>Every expectation here is derived from the chain, not from the report</b>,
/// so the report cannot satisfy an assertion by agreeing with itself: the probe
/// count is compared against <see cref="ChainHarness.ProbeOrder"/>, which the
/// substitute leaves append to on every call, and against a probe total worked
/// out from the chain's shape. Timing dependence is one-directional as in the
/// rest of the fixture: each stand-down test puts a real delay several times
/// the deadline in front of the walk, so a slower machine only makes the
/// stand-down more certain.
/// </para>
/// </summary>
public sealed partial class ShardRootGrainLeafReclaimWallClockBoundTests
{
    /// <summary>
    /// Short enough that the prologue delays below overrun it by a wide margin,
    /// long enough that a pass with no such delay reliably reaches its walk.
    /// </summary>
    private static readonly TimeSpan ShortDeadline = TimeSpan.FromMilliseconds(100);

    /// <summary>
    /// Four times <see cref="ShortDeadline"/>: a delay of this length in front
    /// of the walk means the deadline has certainly passed when it ends.
    /// </summary>
    private static readonly TimeSpan PrologueOverrun = TimeSpan.FromMilliseconds(400);

    private static RecordedLogEntry LastPassSummary(ChainHarness h) =>
        h.Logs.Entries.Last(e => e.Message.Contains(
            "finished an empty-leaf reclaim pass", StringComparison.Ordinal));

    /// <summary>
    /// A folding pass run to the tail of the chain issues a known number of
    /// probes: the head probe that enters the walk, one per remaining leaf, and
    /// one re-probe of the predecessor for the fold - <c>ChainLength + 1</c>.
    /// The report must count all of them. Before the fix it reported
    /// <c>ChainLength - 1</c>, missing the entry probe and the re-probe.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_folding_pass_reports_every_probe_it_issued()
    {
        var h = CreateHarness(TimeSpan.Zero);

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(8);

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.EqualTo(1), "the harness offers exactly one fold");
            Assert.That(h.ProbeCount, Is.EqualTo(ChainLength + 1),
                "head probe + one per remaining leaf + the post-fold re-probe of the predecessor");
            Assert.That(h.PassSummary.Int64("Visited"), Is.EqualTo(h.ProbeCount),
                "the reported probe count must be the number of probes actually issued");
            Assert.That(h.PassSummary.Value("StopReason"), Is.EqualTo("end-of-chain"));
        });
    }

    /// <summary>
    /// The probe budget bounds every probe, the entry probe included. With one
    /// fold allowed the budget is 16 probes and no leaf is foldable, so the
    /// walk runs until the budget binds and must have issued exactly 16. Before
    /// the fix the uncounted entry probe made it 17 while reporting 16.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_pass_issues_no_more_probes_than_its_budget()
    {
        var h = CreateHarness(TimeSpan.FromHours(1), candidateIndex: null);

        await h.Grain.ReclaimEmptyLeavesAsync(1);

        Assert.Multiple(() =>
        {
            Assert.That(h.PassSummary.Int64("ProbeBudget"), Is.EqualTo(16), "1 fold x 16 probes per fold");
            Assert.That(h.ProbeCount, Is.EqualTo(16),
                "the entry probe spends the budget like any other");
            Assert.That(h.PassSummary.Int64("Visited"), Is.EqualTo(h.ProbeCount));
            Assert.That(h.PassSummary.Value("StopReason"), Is.EqualTo("probe-budget"));
        });
    }

    /// <summary>
    /// A pass whose descent to the head of the chain outlasts the deadline
    /// must not then issue the head probe. The descent is the routing-table
    /// fetch, delayed past the deadline, so this observes the check that sits
    /// between the descent and the probe. Before the fix the head probe ran
    /// regardless and the report still said <c>probed 0</c>.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_pass_whose_descent_consumed_the_deadline_probes_nothing()
    {
        var h = CreateHarness(ShortDeadline, descentDelay: PrologueOverrun);

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(8);

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.Zero);
            Assert.That(h.ProbeCount, Is.Zero,
                "no probe may be started once the deadline has passed");
            Assert.That(h.PassSummary.Int64("Visited"), Is.Zero);
            Assert.That(h.PassSummary.Value("StopReason"), Is.EqualTo("deadline"));
            Assert.That(h.ChildIds, Has.Count.EqualTo(ChainLength),
                "a pass that stood down must leave the tree untouched");
        });
    }

    /// <summary>
    /// A pass whose prologue - here, retrying a clear owed by an earlier fold -
    /// outlasts the deadline must neither descend nor probe. The routing-table
    /// fetch is the only descent call, so receiving none observes the check
    /// that sits in front of the descent, independently of the one after it.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_pass_whose_prologue_consumed_the_deadline_neither_descends_nor_probes()
    {
        var h = CreateHarness(ShortDeadline);
        h.OweSlowClear(PrologueOverrun);

        var reclaimed = await h.Grain.ReclaimEmptyLeavesAsync(8);

        _ = h.Root.DidNotReceive().GetRoutingTableAsync();

        Assert.Multiple(() =>
        {
            Assert.That(reclaimed, Is.Zero);
            Assert.That(h.State.State.PendingLeafClears, Is.Empty,
                "the owed clear is the prologue work this test measures, and it must have landed");
            Assert.That(h.ProbeCount, Is.Zero);
            Assert.That(h.PassSummary.Int64("Visited"), Is.Zero);
            Assert.That(h.PassSummary.Value("StopReason"), Is.EqualTo("deadline"));
        });
    }

    /// <summary>
    /// The resume path is gated the same way, and a pass that stands down must
    /// leave the resume cursor where it found it. Pass one stops on the
    /// deadline part-way along the chain. Pass two owes a slow clear, so its
    /// prologue consumes the deadline: it must probe nothing. Pass three must
    /// then resume exactly where pass one stopped. Before the fix pass two
    /// probed its resume leaf and moved the cursor on by one, so pass three
    /// skipped a leaf it had never considered.
    /// </summary>
    [Test]
    public async Task ReclaimEmptyLeaves_resumed_pass_whose_prologue_consumed_the_deadline_keeps_its_cursor()
    {
        var h = CreateHarness(TimeSpan.FromMilliseconds(200), candidateIndex: null);

        await h.Grain.ReclaimEmptyLeavesAsync(8);
        var firstPass = h.ProbeOrder.ToArray();

        h.ProbeOrder.Clear();
        h.OweSlowClear(TimeSpan.FromMilliseconds(800));
        await h.Grain.ReclaimEmptyLeavesAsync(8);
        var standDown = h.ProbeOrder.ToArray();
        var standDownSummary = LastPassSummary(h);

        h.ProbeOrder.Clear();
        await h.Grain.ReclaimEmptyLeavesAsync(8);
        var thirdPass = h.ProbeOrder.ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(firstPass, Is.Not.Empty);
            Assert.That(firstPass, Has.Length.LessThan(ChainLength),
                "pass one must stop part-way, so pass two has a cursor to resume from");
            Assert.That(standDown, Is.Empty,
                "a resumed pass must not probe once its prologue has consumed the deadline");
            Assert.That(standDownSummary.Int64("Visited"), Is.Zero);
            Assert.That(standDownSummary.Value("StopReason"), Is.EqualTo("deadline"));
            Assert.That(thirdPass, Is.Not.Empty);
            Assert.That(
                Array.IndexOf(h.LeafIds, thirdPass[0]),
                Is.EqualTo(Array.IndexOf(h.LeafIds, firstPass[^1]) + 1),
                "the pass that stood down must leave the cursor where pass one put it");
        });
    }
}
