using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The priming arms of issue #2809: where
/// <c>orleans_lattice_shard_root_scan_page_leaf_read_outcomes_total</c> is
/// published at zero, rather than merely that it eventually is.
/// <para>
/// <b>What was wrong.</b> The prime sat below
/// <c>if (!scan.IsStallGuarded) return</c> in <c>ReadLeafAsync</c>. Priming
/// exists so that an ABSENT series has exactly one meaning - the build does not
/// carry the instrument - and a prime below a condition gives absence a second
/// meaning, "the build carries it and the condition was false". That second
/// meaning is not hypothetical here: the guard is false for an entire process
/// lifetime whenever the silo's response timeout is infinite and no explicit
/// stall ceiling is configured, so the prime could simply never run.
/// </para>
/// <para>
/// <b>Why the existing coverage is not a guard.</b>
/// <see cref="All_leaf_read_outcomes_are_readable_including_those_that_never_fire"/>
/// runs a stall-guarded scan to completion and asserts the arms are readable
/// afterwards. It passed with the defect in place and would pass again if the
/// defect were reintroduced, because a completed guarded scan runs the prime on
/// either layout. Observing the series after a successful scan cannot
/// distinguish the two layouts at all; only driving the code down a path that
/// returns early can. That is the entire reason these two arms exist as
/// separate fixtures rather than as extra assertions on that one.
/// </para>
/// <para>
/// <b>Why there are two arms and not one.</b> The fix has two call sites that
/// are not redundant, so each is pinned by its own named test and reverting
/// either reddens exactly one of them, naming which site was lost:
/// </para>
/// <list type="bullet">
/// <item><description>
/// <see cref="Activation_alone_primes_every_leaf_read_outcome_arm_at_zero"/>
/// pins the activation call, which is what makes the series
/// workload-independent. Hoisting within <c>ReadLeafAsync</c> alone would still
/// have left the series conditional on some scan-page leaf read having
/// happened, which is a weaker property than the one the acceptance predicate
/// reads it for.
/// </description></item>
/// <item><description>
/// <see cref="An_unguarded_scan_primes_every_leaf_read_outcome_arm_at_zero"/>
/// pins the read-path call above the early return, so that the read path
/// satisfies "no outcome is ever recorded on an unprimed series" on its own
/// rather than by relying on a caller having activated first.
/// </description></item>
/// </list>
/// <para>
/// <b>Both arms assert the VALUE is zero, not merely that the arm is present.</b>
/// Presence alone would also be satisfied by a real increment, which would make
/// the assertion pass for the wrong reason on the very path it is meant to
/// characterise. On an unguarded scan in particular, <c>issued</c> sitting at
/// zero is what proves the scan took the early return rather than the coalescing
/// path, so the value check doubles as the direction check for the scenario.
/// </para>
/// <para>
/// <b>The expected arms are derived from the declared tag constants, not from
/// string literals.</b> A fourth outcome arm added without a matching prime
/// therefore reddens these tests rather than slipping past an expectation that
/// still enumerates three - which matters because the arm a reader cares about
/// is exactly the one that never fires, and a partially primed series is
/// ambiguous at precisely that arm.
/// </para>
/// <para>
/// Priming is correct here only because this is a <c>Counter&lt;long&gt;</c>: a
/// zero added to a counter is the identity and changes no reading of it. The
/// same pattern on a <c>Histogram&lt;T&gt;</c> would fabricate a sample claiming
/// the operation was instantaneous, so it must not be carried across.
/// </para>
/// </summary>
public partial class ShardRootGrainScanPageLeafReadCoalescingTests
{
    /// <summary>
    /// The outcome arms this counter declares, read from the tag constants so
    /// the expectation cannot go stale against the source.
    /// </summary>
    private static readonly string[] LeafReadOutcomeArms =
    [
        (string)LatticeMetrics.OutcomeScanPageLeafReadIssuedTag.Value!,
        (string)LatticeMetrics.OutcomeScanPageLeafReadJoinedTag.Value!,
        (string)LatticeMetrics.OutcomeScanPageLeafReadServedTag.Value!,
    ];

    /// <summary>
    /// Accumulates per-arm totals for the leaf-read outcome counter, restricted
    /// to one tree.
    /// <para>
    /// <b>The tree filter is load-bearing, not tidiness.</b> The counter is a
    /// process-wide static, and sibling fixtures in this class start scans with
    /// <c>_ =</c> that are still running after their test method returns; one of
    /// those emitting <c>issued</c> inside this listener's window is enough to
    /// break an exact-zero assertion, which was observed before the filter was
    /// added. Both priming arms therefore run on a tree id of their own, so the
    /// totals describe their grain and no other.
    /// </para>
    /// <para>
    /// The caller owns the returned listener and must dispose it before
    /// asserting, so no measurement can land between the last observation and
    /// the assertion.
    /// </para>
    /// </summary>
    private static (MeterListener Listener, Dictionary<string, long> Totals) ListenForLeafReadOutcomes(
        string treeId)
    {
        var totals = new Dictionary<string, long>(StringComparer.Ordinal);
        var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanPageLeafReadOutcomes,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? outcome = null;
                var onThisTree = false;

                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeMetrics.TagOutcome && tag.Value is string arm)
                    {
                        outcome = arm;
                    }
                    else if (tag.Key == LatticeMetrics.TagTree && tag.Value is string tree)
                    {
                        onThisTree = string.Equals(tree, treeId, StringComparison.Ordinal);
                    }
                }

                if (!onThisTree || outcome is null)
                {
                    return;
                }

                lock (totals)
                {
                    totals[outcome] = totals.TryGetValue(outcome, out var running)
                        ? running + value
                        : value;
                }
            }));

        return (listener, totals);
    }

    /// <summary>
    /// Activation alone, with no scan of any kind, must publish every outcome
    /// arm at zero.
    /// <para>
    /// This is the arm that makes the series usable as evidence. A deployment
    /// that has activated a shard root but never scanned it is the ordinary
    /// resting state of most shards, and before this fix that state was
    /// indistinguishable from a build that did not carry the instrument at all.
    /// </para>
    /// <para>
    /// Reverting the prime in <c>ShardRootGrain.OnActivateAsync</c> reddens this
    /// and nothing else: no measurement is published, so the arms are absent
    /// rather than zero. Hoisting inside <c>ReadLeafAsync</c> does not rescue it,
    /// because no leaf read ever happens here - which is the point.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Activation_alone_primes_every_leaf_read_outcome_arm_at_zero()
    {
        const string TreeId = "prime-activation-tree";

        var (listener, totals) = ListenForLeafReadOutcomes(TreeId);
        var chain = CreateParkableLeaf(TimeSpan.FromMilliseconds(150), shardKey: TreeId + "/0");

        await ((IGrainBase)chain.Grain).OnActivateAsync(CancellationToken.None);

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(totals.Keys, Is.EquivalentTo(LeafReadOutcomeArms),
                "activation must publish every declared outcome arm, so that an absent series means the build "
                + "does not carry the instrument and nothing else (issue #2809). An arm missing here is "
                + "unreadable in exactly the case a reader cares about - the one that never fires.");

            foreach (var arm in LeafReadOutcomeArms)
            {
                Assert.That(totals.GetValueOrDefault(arm, -1), Is.Zero,
                    $"the '{arm}' arm must be primed at zero, not incremented: activation performs no leaf read, "
                    + "so a non-zero total here would mean the arm was published for the wrong reason and the "
                    + "assertion would be passing on a path it does not characterise.");
            }
        });
    }

    /// <summary>
    /// A scan that takes the unguarded early return must still leave every
    /// outcome arm published at zero.
    /// <para>
    /// This drives the exact path the defect hid behind. The harness configures
    /// <c>MaxScanPageStallDuration = Timeout.InfiniteTimeSpan</c>, which
    /// <c>LatticeOptionsResolver.ResolveStallDuration</c> returns verbatim, so
    /// <c>ScanPageBounds.IsStallGuarded</c> is false, no deadline is armed, and
    /// <c>ReadLeafAsync</c> returns at <c>if (!scan.IsStallGuarded)</c> - the
    /// return the prime used to sit below.
    /// </para>
    /// <para>
    /// Activation is deliberately NOT invoked. If it were, the activation prime
    /// would satisfy the assertion and this arm would go green with the read-path
    /// prime pushed back below the return, which is the precise regression it
    /// exists to catch.
    /// </para>
    /// <para>
    /// The scan is asserted to have returned the leaf's rows, so a future change
    /// that stops this scenario reaching <c>ReadLeafAsync</c> at all fails here
    /// rather than passing vacuously on a path that was never executed.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task An_unguarded_scan_primes_every_leaf_read_outcome_arm_at_zero()
    {
        const string TreeId = "prime-unguarded-tree";

        var (listener, totals) = ListenForLeafReadOutcomes(TreeId);
        var chain = CreateParkableLeaf(Timeout.InfiniteTimeSpan, shardKey: TreeId + "/0");
        chain.Park = false;

        var page = await chain.Grain.GetSortedEntriesBatchAsync(
            startInclusive: null, endExclusive: null, pageSize: 64, continuationToken: null);

        listener.Dispose();

        Assert.Multiple(() =>
        {
            Assert.That(page.Entries, Has.Count.EqualTo(chain.Rows.Count),
                "the scan must actually reach the leaf, or this fixture asserts about a path it never executed");

            Assert.That(totals.Keys, Is.EquivalentTo(LeafReadOutcomeArms),
                "an unguarded leaf read must publish every declared outcome arm before it returns (issue #2809). "
                + "With the prime below the early return the series is absent here, so an operator cannot tell "
                + "that case apart from a build that does not carry the instrument.");

            foreach (var arm in LeafReadOutcomeArms)
            {
                Assert.That(totals.GetValueOrDefault(arm, -1), Is.Zero,
                    $"the '{arm}' arm must be primed at zero on the unguarded path. A non-zero 'issued' in "
                    + "particular would mean the scan took the coalescing path instead, so this assertion is "
                    + "also what proves the scenario ran in the direction it claims.");
            }
        });
    }
}
