using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2952: where
/// <c>orleans_lattice_shard_root_scan_page_stalls_total</c> publishes its four
/// <c>phase</c> arms, rather than merely that one of them eventually appears.
/// <para>
/// <b>What was wrong.</b> The only write to the counter was the increment on
/// the stall path, so exactly the arm that had already fired existed as a
/// series. A live scrape of the rig carried <c>leaf-walk</c> and nothing at all
/// for <c>prologue</c>, <c>descent</c> and <c>baseline-fold</c>. Those three
/// absences are byte-identical to three arms that were never reached, which are
/// byte-identical to three call sites that do not exist in the build - so the
/// reading an operator actually wants ("no page fill has ever stalled during
/// descent") was not available from the data at all.
/// </para>
/// <para>
/// That cost the epic a written-down discriminator. A pre-registered acceptance
/// clause claimed a stall surfacing under <c>prologue</c> or <c>descent</c>
/// would prove the fault had moved off the leaf read; its contrapositive - the
/// continued absence of such a stall - was unusable, because absence produced
/// by machinery that never executed is byte-identical to measured absence.
/// </para>
/// <para>
/// <b>Why there are two priming arms and not one.</b> The fix has two call
/// sites that are not redundant, and reverting either reddens exactly one test,
/// naming which site was lost. This mirrors the structure issue #2809 settled
/// for the sibling leaf-read counter on the same grain.
/// </para>
/// <list type="bullet">
/// <item><description>
/// <see cref="Activation_alone_primes_every_scan_page_stall_phase_arm_at_zero"/>
/// pins the <c>OnActivateAsync</c> call, which is what makes the series
/// workload-independent. Issue #2952 proposed binding the prime to the point a
/// shard first begins a page fill; that bound is taken as the second call site
/// below, but on its own it is strictly weaker - it leaves an absent series
/// meaning either "the build does not carry the instrument" or "it does and no
/// page fill ever ran", which is the same two-reading ambiguity one layer out.
/// </description></item>
/// <item><description>
/// <see cref="A_page_fill_primes_every_scan_page_stall_phase_arm_at_zero"/>
/// pins the <c>BeginScanPage</c> call, so the scan path satisfies "no stall is
/// ever recorded on an unprimed phase arm" on its own rather than by relying on
/// a caller having activated first.
/// </description></item>
/// </list>
/// <para>
/// <b>And one positive control.</b>
/// <see cref="A_real_stall_is_reported_as_one_on_its_phase_arm_by_this_same_harness"/>
/// drives an actual stall and asserts the same listener reports the increment.
/// Without it, every assertion here is compatible with a harness that observes
/// nothing whatsoever and reports zero for that reason - a priming test is
/// unusually prone to that failure, because "the value I expect is zero" and
/// "I measured nothing" produce identical assertions. The control is what makes
/// a zero in the two tests above a measured zero.
/// </para>
/// <para>
/// <b>The expected arms are derived from the declared tag constants</b>, not
/// from string literals, so a fifth phase added without a matching prime
/// reddens these tests rather than slipping past an expectation that still
/// enumerates four.
/// </para>
/// <para>
/// Priming is correct here only because this is a <c>Counter&lt;long&gt;</c>:
/// adding zero to a counter is the identity and changes no reading of it. The
/// same pattern on a <c>Histogram&lt;T&gt;</c> would fabricate a sample
/// claiming the operation was instantaneous, so it must not be carried across.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainScanPageStallPhasePrimingTests
{
    /// <summary>
    /// The phase arms this counter declares, read from the tag constants so the
    /// expectation cannot go stale against the source.
    /// </summary>
    private static readonly string[] StallPhaseArms =
    [
        (string)LatticeMetrics.PhaseScanPagePrologueTag.Value!,
        (string)LatticeMetrics.PhaseScanPageDescentTag.Value!,
        (string)LatticeMetrics.PhaseScanPageLeafWalkTag.Value!,
        (string)LatticeMetrics.PhaseScanPageBaselineFoldTag.Value!,
    ];

    /// <summary>
    /// Accumulates per-phase totals for the stall counter, restricted to one
    /// tree.
    /// <para>
    /// The tree filter is load-bearing rather than tidiness: the counter is a
    /// process-wide static and other fixtures in this assembly drive stalls, so
    /// an unfiltered listener would let a sibling's increment break an
    /// exact-zero assertion here. Each test below therefore runs on a tree id of
    /// its own, and the totals describe its grain and no other.
    /// </para>
    /// <para>
    /// The caller owns the returned listener and must dispose it before
    /// asserting, so no measurement can land between the last observation and
    /// the assertion.
    /// </para>
    /// </summary>
    private static (MeterListener Listener, Dictionary<string, long> Totals) ListenForStallPhases(
        string treeId)
    {
        var totals = new Dictionary<string, long>(StringComparer.Ordinal);
        var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanPageStalls,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? phase = null;
                var onThisTree = false;

                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagPhase && tag.Value is string arm)
                    {
                        phase = arm;
                    }
                    else if (tag.Key == LatticeMetrics.TagTree && tag.Value is string tree)
                    {
                        onThisTree = string.Equals(tree, treeId, StringComparison.Ordinal);
                    }
                }

                if (!onThisTree || phase is null)
                {
                    return;
                }

                lock (totals)
                {
                    totals[phase] = totals.TryGetValue(phase, out var running)
                        ? running + value
                        : value;
                }
            }));

        return (listener, totals);
    }

    /// <summary>
    /// Activation alone, with no scan of any kind, must publish all four phase
    /// arms at zero.
    /// <para>
    /// This is the arm that makes the series usable as evidence. A shard root
    /// that has activated but never stalled is the ordinary resting state of
    /// every healthy shard, and before this fix that state published nothing at
    /// all for three of the four phases.
    /// </para>
    /// <para>
    /// Reverting the prime in <c>ShardRootGrain.OnActivateAsync</c> reddens this
    /// and nothing else: no measurement is published, so the arms are absent
    /// rather than zero. Adding the prime to <c>BeginScanPage</c> alone does not
    /// rescue it, because no page fill ever happens here - which is the point.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Activation_alone_primes_every_scan_page_stall_phase_arm_at_zero()
    {
        const string TreeId = "stall-prime-activation-tree";

        var (listener, totals) = ListenForStallPhases(TreeId);
        var harness = ScanPagePrimingHarness.CreateShard(TreeId, Timeout.InfiniteTimeSpan);

        await ((IGrainBase)harness.Grain).OnActivateAsync(CancellationToken.None);

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(totals.Keys, Is.EquivalentTo(StallPhaseArms),
                "activation must publish every declared phase arm, so that an absent series means the build "
                + "does not carry the instrument and nothing else (issue #2952). An arm missing here is "
                + "unreadable in exactly the case a reader cares about - the phase that never stalls.");

            foreach (var arm in StallPhaseArms)
            {
                Assert.That(totals.GetValueOrDefault(arm, -1), Is.Zero,
                    $"the '{arm}' arm must be primed at zero, not incremented: activation fills no page, so a "
                    + "non-zero total here would mean the arm was published for the wrong reason and the "
                    + "assertion would be passing on a path it does not characterise.");
            }
        });
    }

    /// <summary>
    /// A page fill that completes without stalling must still leave all four
    /// phase arms published at zero, without any activation call.
    /// <para>
    /// Activation is deliberately NOT invoked. If it were, the activation prime
    /// would satisfy the assertion and this test would go green with the
    /// <c>BeginScanPage</c> prime removed, which is the precise regression it
    /// exists to catch.
    /// </para>
    /// <para>
    /// The page is asserted to have returned the leaf's rows, so a future change
    /// that stops this scenario reaching <c>BeginScanPage</c> at all fails here
    /// rather than passing vacuously on a path that was never executed.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_page_fill_primes_every_scan_page_stall_phase_arm_at_zero()
    {
        const string TreeId = "stall-prime-pagefill-tree";

        var (listener, totals) = ListenForStallPhases(TreeId);
        var harness = ScanPagePrimingHarness.CreateShard(TreeId, Timeout.InfiniteTimeSpan);

        var page = await harness.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(ScanPagePrimingHarness.RowCount),
                "the page fill must actually reach the leaf, or this fixture asserts about a path it never ran");

            Assert.That(totals.Keys, Is.EquivalentTo(StallPhaseArms),
                "a page fill must publish every declared phase arm before it can stall (issue #2952), so that "
                + "the three phases that have never stalled in production are readable as measured zeros "
                + "rather than as an absence that also covers 'the call site does not exist'.");

            foreach (var arm in StallPhaseArms)
            {
                Assert.That(totals.GetValueOrDefault(arm, -1), Is.Zero,
                    $"the '{arm}' arm must be primed at zero on a page fill that completes. A non-zero total "
                    + "would mean this scan stalled, so the value check is also what proves the scenario ran "
                    + "in the direction it claims.");
            }
        });
    }

    /// <summary>
    /// The positive control for the two tests above: a real stall must be
    /// reported as a one on its phase arm by this same listener.
    /// <para>
    /// Without this, both priming tests are compatible with a harness that
    /// observes nothing at all - <c>MeterListening.StartForInstrument</c>
    /// silently enabling no instrument, the tag filter never matching, or the
    /// listener being disposed before the callback runs would each produce
    /// exactly the zeros those tests assert. A priming test is unusually prone
    /// to that class of vacuity, because "the value I expect is zero" and "I
    /// measured nothing" are the same assertion.
    /// </para>
    /// <para>
    /// So this test drives an actual stall on the same grain shape, through the
    /// same listener, and requires a one. The three phases that did not stall
    /// remain at zero, which additionally shows the harness discriminates
    /// between arms rather than attributing every measurement to whichever arm
    /// it looked at last.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_real_stall_is_reported_as_one_on_its_phase_arm_by_this_same_harness()
    {
        const string TreeId = "stall-prime-control-tree";
        const string Stalled = "leaf-walk";

        var (listener, totals) = ListenForStallPhases(TreeId);
        var harness = ScanPagePrimingHarness.CreateShard(
            TreeId, TimeSpan.FromMilliseconds(150), park: true);

        Assert.That(
            async () => await harness.Grain.GetSortedEntriesBatchAsync(
                startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null),
            Throws.InstanceOf<ScanPageStalledException>(),
            "the control must actually stall, or it demonstrates nothing about the harness's ability to see one");

        harness.Release();
        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(Stalled, Is.AnyOf(StallPhaseArms),
                "the phase this control expects must be one the instrument declares, or the control is "
                + "asserting against a literal the source no longer uses");

            Assert.That(totals.GetValueOrDefault(Stalled, -1), Is.EqualTo(1),
                "this harness must observe a real stall as a one. If it reports zero here, then the zeros the "
                + "two priming tests assert are 'the harness saw nothing' rather than 'the arm was primed', "
                + "and both of those tests are vacuous.");

            foreach (var arm in StallPhaseArms.Where(a => !string.Equals(a, Stalled, StringComparison.Ordinal)))
            {
                Assert.That(totals.GetValueOrDefault(arm, -1), Is.Zero,
                    $"the '{arm}' arm did not stall and must still read zero, which is what shows the harness "
                    + "attributes a measurement to the arm that produced it rather than to all of them.");
            }
        });
    }
}
