using System.Diagnostics.Metrics;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue 3341: how a shard root <em>reports</em> a chain-regressed leaf during
/// a range scan - bounded on the log channel, unbounded on a counter.
/// <para>
/// Sibling to <see cref="ShardRootGrainScanChainRegressionTests"/>, which pins
/// the suppression's read-correctness behaviour for issue 3271. That fixture
/// owns "the orphan's rows must not reach the page"; this one owns "and the
/// operator must be able to find out, without losing the host's logs".
/// </para>
/// <para>
/// <b>What was wrong.</b> The suppression was reported <em>only</em> as a log
/// warning, one per offending leaf per page fill. That dedupe is scoped to a
/// single page, so a tree carrying 2,886 damaged leaves re-reported every one of
/// them on every page: the field burst was 110,322 warnings in about eight
/// minutes, 99.3% of all warning output, at roughly 2,354 lines a second. The
/// host's json-file ring is 100 MB (<c>max-size=20m</c> times
/// <c>max-file=5</c>) and the burst occupied half of it, collapsing log
/// retention on that container to about 107.6 seconds. No post-hoc diagnosis of
/// anything on that host was possible while it ran, and a "we checked the logs
/// and saw nothing" conclusion was drawn and later retracted because of it.
/// </para>
/// <para>
/// <b>Why the fix has two halves, and why both are tested here.</b> Silencing
/// the log alone would trade one blindness for another: the operator would stop
/// losing retention and start losing the signal entirely, because a suppression
/// reported nowhere is indistinguishable from a tree with no damage. So the
/// volume is bounded on the log channel, and the magnitude moves onto
/// <c>orleans_lattice_shard_root_scan_page_chain_regressions_total</c>, which is
/// unbounded in count but fixed in cardinality.
/// </para>
/// <list type="bullet">
/// <item><description>
/// <see cref="Re_walking_a_damaged_chain_counts_every_suppression_but_each_leaf_once"/>
/// is the counter half. The two arms answer different questions and their
/// <em>ratio</em> is the diagnosis: <c>suppression</c> is how much work the
/// damage is costing, <c>distinct-leaf</c> is how much damage there is.
/// </description></item>
/// <item><description>
/// <see cref="Log_volume_stays_bounded_however_many_leaves_regress_and_however_often_the_shard_is_scanned"/>
/// is the log half, and is the test that goes red without the fix. It drives
/// the field burst's shape in miniature and asserts the shard wrote a bounded
/// number of lines rather than one per suppression.
/// </description></item>
/// </list>
/// <para>
/// <b>And a positive control.</b>
/// <see cref="A_chain_regressed_leaf_is_counted_on_both_arms_by_this_same_harness"/>
/// drives real regressions through the production call site and asserts this
/// same listener reports them, so the zeros the two priming tests assert are
/// measured zeros rather than a harness that observes nothing. A priming test
/// is unusually prone to that failure, because "the value I expect is zero" and
/// "I measured nothing" produce identical assertions.
/// </para>
/// </summary>
[TestFixture]
public sealed class ShardRootGrainScanChainRegressionReportingTests
{
    /// <summary>
    /// The arm carrying one count per suppression event, read from the tag
    /// constant the production recorder passes rather than re-spelled here. A
    /// literal would let a rename drift this fixture green against an arm no
    /// dashboard is charting, and would couple it to the landing order of any
    /// sibling change that retunes the tag.
    /// </summary>
    private static string SuppressionArm =>
        (string)LatticeMetrics.OutcomeScanChainRegressionSuppressionTag.Value!;

    /// <summary>
    /// The arm carrying one count the first time an activation sees a given
    /// leaf regress. Read from its live tag constant for the same reason.
    /// </summary>
    private static string DistinctLeafArm =>
        (string)LatticeMetrics.OutcomeScanChainRegressionDistinctLeafTag.Value!;

    /// <summary>
    /// Activation alone, with no scan of any kind, must publish both arms at
    /// zero. Without it a freshly started silo exports no series until a tree is
    /// actually damaged, so "this build does not carry the instrument" and "it
    /// does, and nothing has regressed" read identically - which is precisely
    /// the ambiguity the counter exists to remove.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Activation_alone_primes_both_chain_regression_arms_at_zero()
    {
        // The per-tree prime latch is process-wide static, so a fixed tree id
        // would let a sibling fixture's earlier activation satisfy this one and
        // hide a lost call site.
        var treeId = $"tree-chain-regression-activation-{Guid.NewGuid():N}";
        var harness = ScanChainRegressionHarness.CreateShard(treeId, damagedLeafCount: 0);
        var (listener, totals) = ListenForChainRegressions(treeId);

        try
        {
            await ((IGrainBase)harness.Grain).OnActivateAsync(CancellationToken.None);
        }
        finally
        {
            listener.Dispose();
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                totals.Keys,
                Is.EquivalentTo(new[] { SuppressionArm, DistinctLeafArm }),
                "Activating a shard root must publish both chain-regression arms, so an "
                + "operator can read a zero as 'measured, none' rather than as 'absent'.");
            Assert.That(
                totals.GetValueOrDefault(SuppressionArm, -1),
                Is.Zero,
                "The suppression arm must be primed at zero, not incremented by activation.");
            Assert.That(
                totals.GetValueOrDefault(DistinctLeafArm, -1),
                Is.Zero,
                "The distinct-leaf arm must be primed at zero, not incremented by activation.");
        });
    }

    /// <summary>
    /// A page fill must prime both arms on its own, without relying on a caller
    /// having activated the grain first. This is the second, independent
    /// reachability seam: removing the prime from the page-fill prologue reddens
    /// this test and leaves the activation test above green, so a lost call site
    /// names itself.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_page_fill_primes_both_chain_regression_arms_at_zero()
    {
        var treeId = $"tree-chain-regression-page-fill-{Guid.NewGuid():N}";
        var harness = ScanChainRegressionHarness.CreateShard(treeId, damagedLeafCount: 0);
        var (listener, totals) = ListenForChainRegressions(treeId);

        EntriesPage page;
        try
        {
            // Deliberately no OnActivateAsync: this test must fail if the page
            // fill's own prime is removed, even though the activation prime
            // would otherwise cover for it.
            page = await harness.FillPageAsync();
        }
        finally
        {
            listener.Dispose();
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                page.Entries, Has.Count.EqualTo(ScanChainRegressionHarness.HeadRowCount),
                "The intact chain must serve its rows, so the zeros below describe a scan "
                + "that ran and found no damage rather than a scan that never walked.");
            Assert.That(
                totals.Keys,
                Is.EquivalentTo(new[] { SuppressionArm, DistinctLeafArm }),
                "A page fill must publish both chain-regression arms without an activation "
                + "having primed them first.");
            Assert.That(
                totals.GetValueOrDefault(SuppressionArm, -1), Is.Zero,
                "An intact chain must leave the suppression arm at its primed zero.");
            Assert.That(
                totals.GetValueOrDefault(DistinctLeafArm, -1), Is.Zero,
                "An intact chain must leave the distinct-leaf arm at its primed zero.");
        });
    }

    /// <summary>
    /// The positive control. Drives real chain regressions through the
    /// production scan path and asserts this same listener reports them on both
    /// arms, which is what makes the zeros asserted above measured zeros.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task A_chain_regressed_leaf_is_counted_on_both_arms_by_this_same_harness()
    {
        const int DamagedLeaves = 3;

        var treeId = $"tree-chain-regression-control-{Guid.NewGuid():N}";
        var harness = ScanChainRegressionHarness.CreateShard(treeId, DamagedLeaves);
        var (listener, totals) = ListenForChainRegressions(treeId);

        EntriesPage page;
        try
        {
            page = await harness.FillPageAsync();
        }
        finally
        {
            listener.Dispose();
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                page.Entries, Has.Count.EqualTo(ScanChainRegressionHarness.HeadRowCount),
                "A damaged leaf must still be suppressed from the page: the counter reports "
                + "the damage, it does not admit it.");
            Assert.That(
                totals.GetValueOrDefault(SuppressionArm, -1), Is.EqualTo(DamagedLeaves),
                "Every suppression on the page must be counted once on the suppression arm.");
            Assert.That(
                totals.GetValueOrDefault(DistinctLeafArm, -1), Is.EqualTo(DamagedLeaves),
                "Each damaged leaf seen for the first time must be counted once on the "
                + "distinct-leaf arm.");
        });
    }

    /// <summary>
    /// The counter half of the fix. Re-walking the same damaged shard must keep
    /// counting every suppression - that is the magnitude the log used to carry,
    /// and it has to survive the log being silenced - while counting each
    /// damaged leaf exactly once, which is the census the issue asks for.
    /// <para>
    /// The ratio of the two is the diagnosis. Here three leaves are walked four
    /// times, so twelve suppressions over three leaves reads as "a small, fixed
    /// set of damaged leaves, re-paid on every page" - the field shape (2,886
    /// leaves, about 43 repetitions each) rather than damage that is spreading.
    /// </para>
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Re_walking_a_damaged_chain_counts_every_suppression_but_each_leaf_once()
    {
        const int DamagedLeaves = 3;
        const int PageFills = 4;

        var treeId = $"tree-chain-regression-ratio-{Guid.NewGuid():N}";
        var harness = ScanChainRegressionHarness.CreateShard(treeId, DamagedLeaves);
        var (listener, totals) = ListenForChainRegressions(treeId);

        try
        {
            for (var i = 0; i < PageFills; i++)
            {
                await harness.FillPageAsync();
            }
        }
        finally
        {
            listener.Dispose();
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                totals.GetValueOrDefault(SuppressionArm, -1),
                Is.EqualTo(DamagedLeaves * PageFills),
                "The suppression arm must count every suppression event, including the "
                + "repeats across page fills, because that repetition is the cost the "
                + "damage imposes and is what the log volume used to be measuring.");
            Assert.That(
                totals.GetValueOrDefault(DistinctLeafArm, -1), Is.EqualTo(DamagedLeaves),
                "The distinct-leaf arm must count each damaged leaf once for the life of "
                + "the activation, so it reads as a census of the damage rather than as a "
                + "restatement of the suppression arm.");
        });
    }

    /// <summary>
    /// The log half of the fix, and the test that goes red without it. Drives
    /// the field burst's shape in miniature - forty damaged leaves re-walked
    /// five times, two hundred suppressions - and asserts the shard wrote one
    /// line per distinct leaf up to the detail cap plus a single summary line.
    /// Before the fix this fixture observes two hundred warnings.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task Log_volume_stays_bounded_however_many_leaves_regress_and_however_often_the_shard_is_scanned()
    {
        const int DamagedLeaves = 40;
        const int PageFills = 5;

        // Bound to the live cap rather than to a hard-coded eleven: a sibling
        // change that retunes the cap must neither silently loosen this
        // assertion nor redden it.
        var cap = ShardRootGrain.ChainRegressionWarnDetailCap;

        var treeId = $"tree-chain-regression-log-bound-{Guid.NewGuid():N}";
        var harness = ScanChainRegressionHarness.CreateShard(treeId, DamagedLeaves);
        var (listener, totals) = ListenForChainRegressions(treeId);

        try
        {
            for (var i = 0; i < PageFills; i++)
            {
                await harness.FillPageAsync();
            }
        }
        finally
        {
            listener.Dispose();
        }

        var warnings = harness.Logs.Warnings;

        Assert.Multiple(() =>
        {
            Assert.That(
                totals.GetValueOrDefault(SuppressionArm, -1),
                Is.EqualTo(DamagedLeaves * PageFills),
                "The scenario must actually produce the suppressions whose logging is being "
                + "bounded; without this the bound below could pass on a walk that never "
                + "regressed at all.");
            Assert.That(
                warnings, Has.Count.EqualTo(cap + 1),
                $"A shard that suppressed {DamagedLeaves * PageFills} times must write at "
                + $"most {cap} detail lines and one summary line. Reporting every "
                + "suppression is what filled a 100 MB log ring in 107.6 seconds in the "
                + "field (issue 3341).");
            Assert.That(
                warnings.Count(e => e.Value("LeafId") is not null), Is.EqualTo(cap),
                $"Exactly {cap} of the lines must be per-leaf detail, so an operator still "
                + "gets named leaves to act on rather than a bare count.");
        });
    }

    /// <summary>
    /// Silencing a log channel is only acceptable because the signal moved
    /// somewhere an operator can find it, so the summary line must say where.
    /// Without this, the last line a reader sees says the shard has stopped
    /// talking and nothing says what to read instead.
    /// </summary>
    [Test]
    [NonParallelizable]
    public async Task The_summary_line_names_the_counter_that_carries_the_remaining_detail()
    {
        var treeId = $"tree-chain-regression-summary-{Guid.NewGuid():N}";
        var harness = ScanChainRegressionHarness.CreateShard(
            treeId, ShardRootGrain.ChainRegressionWarnDetailCap + 1);

        await harness.FillPageAsync();

        var warnings = harness.Logs.Warnings;
        var summary = warnings[^1];

        Assert.Multiple(() =>
        {
            Assert.That(
                warnings, Has.Count.EqualTo(ShardRootGrain.ChainRegressionWarnDetailCap + 1),
                "One leaf past the cap must produce exactly the detail lines plus the one "
                + "summary line, so the line inspected below really is the summary.");
            Assert.That(
                summary.Message, Does.Contain(LatticeMetrics.ScanChainRegressions.Name),
                "The summary line must name the counter that carries the detail it is "
                + "withholding, so the reader is redirected rather than left silent.");
            Assert.That(
                summary.Value("LeafId"), Is.Null,
                "The summary line is the one that stops naming individual leaves; were it "
                + "still carrying a leaf id it would be a detail line and the cap would be "
                + "off by one.");
        });
    }

    /// <summary>
    /// Builds a listener that accumulates the chain-regression counter per arm
    /// for one tree.
    /// <para>
    /// Values are summed rather than measurements counted, because the zero
    /// prime is itself a measurement: a fixture that counted events would read a
    /// primed-but-never-incremented arm as one. The caller disposes the listener
    /// before asserting, so no measurement can land between the last observation
    /// and the assertion.
    /// </para>
    /// </summary>
    private static (MeterListener Listener, Dictionary<string, long> Totals) ListenForChainRegressions(
        string treeId)
    {
        var totals = new Dictionary<string, long>(StringComparer.Ordinal);
        var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ScanChainRegressions,
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
}
