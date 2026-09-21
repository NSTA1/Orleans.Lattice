using System.Diagnostics.Metrics;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for the pin-advance classification
/// <see cref="WalMaterialiserPinGrain"/> publishes on
/// <see cref="LatticeMetrics.MaterialiserPinAdvances"/> (issues #2694, #3163).
/// <para>
/// Durable-write counts cannot answer "is the WAL GC floor able to move?":
/// bucketing means one advancing pin rewrites its whole bucket, so writes are
/// not proportional to advances, and a frontier-only advance rewrites the pin at
/// the same checkpoint offset. Offset advancement is the quantity that lets the
/// GC offset floor move, so it needs a series of its own.
/// </para>
/// <para>
/// The arms were <b>first-match</b> until issue #3163, so an offset advance was
/// tagged <c>offset</c> whatever the frontier did alongside it and the joint
/// distribution was unrecoverable. The fixture now pins all four cells of the
/// truth table, and in particular that a merge advancing both axes is never
/// reported as offset-only.
/// </para>
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class WalMaterialiserPinGrainAdvanceMetricTests
{
    private const string Consumer = "_lattice_materialiser_tree-1_leaf-7";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

    private static WalMaterialiserPinGrain CreateGrain(string treeId)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("wal-materialiser-pin", treeId));
        var options = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeOptions { WalMaterialiserPinFlushIntervalMs = 0 });
        return new WalMaterialiserPinGrain(context, new FakePersistentState<WalMaterialiserPinState>(), options);
    }

    /// <summary>
    /// The outcome tag value carried by an advance-classification tag constant.
    /// </summary>
    private static string Outcome(KeyValuePair<string, object?> tag) => (string)tag.Value!;

    /// <summary>
    /// Collects the advance measurements recorded for one tree while
    /// <paramref name="act"/> runs, in order, each as its outcome tag and the
    /// delta added. Priming adds zero, so a caller that cares only about real
    /// merges reads <see cref="OutcomesAsync"/> instead.
    /// </summary>
    private static async Task<List<(string Outcome, long Value)>> MeasurementsAsync(string treeId, Func<Task> act)
    {
        var measurements = new List<(string Outcome, long Value)>();
        using var listener = MeterListening.StartForInstrument(
            LatticeMetrics.MaterialiserPinAdvances,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? tree = null;
                string? outcome = null;
                foreach (var t in tags)
                {
                    if (t.Key == LatticeMetrics.TagTree)
                    {
                        tree = t.Value as string;
                    }
                    else if (t.Key == LatticeMetrics.TagOutcome)
                    {
                        outcome = t.Value as string;
                    }
                }

                if (tree == treeId && outcome is not null)
                {
                    lock (measurements)
                    {
                        measurements.Add((outcome, value));
                    }
                }
            }));

        await act();
        return measurements;
    }

    /// <summary>
    /// Collects the advance outcomes recorded for one tree while
    /// <paramref name="act"/> runs, in order.
    /// </summary>
    private static async Task<List<string>> OutcomesAsync(string treeId, Func<Task> act)
    {
        var measurements = await MeasurementsAsync(treeId, act);
        return measurements.Select(m => m.Outcome).ToList();
    }

    [Test]
    public async Task Report_advancing_both_axes_is_not_reported_as_offset_only()
    {
        var tree = $"pa-{Guid.NewGuid():N}";
        var grain = CreateGrain(tree);
        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(100), 10)]);

        var outcomes = await OutcomesAsync(tree, () => grain.ReportManyAsync(
            [new MaterialiserPinReport(Consumer, Hlc(200), 20)]));

        Assert.Multiple(() =>
        {
            Assert.That(outcomes, Is.EqualTo(new[] { Outcome(LatticeMetrics.OutcomePinBothAdvanced) }),
                "a merge that moved the frontier and the offset together must say so");
            Assert.That(outcomes, Has.None.EqualTo(Outcome(LatticeMetrics.OutcomePinOffsetOnly)),
                "the first-match form tagged this offset-only, which is what made a flat frontier unreadable (issue #3163)");
        });
    }

    [Test]
    public async Task Report_advancing_only_the_offset_records_an_offset_only_advance()
    {
        var tree = $"pa-{Guid.NewGuid():N}";
        var grain = CreateGrain(tree);
        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(100), 10)]);

        // Same frontier, higher offset: the shape of a consumer advancing in
        // offset space alone, which is the hypothesis this arm exists to test.
        var outcomes = await OutcomesAsync(tree, () => grain.ReportManyAsync(
            [new MaterialiserPinReport(Consumer, Hlc(100), 20)]));

        Assert.That(outcomes, Is.EqualTo(new[] { Outcome(LatticeMetrics.OutcomePinOffsetOnly) }));
    }

    [Test]
    public async Task A_new_consumer_reporting_an_offset_advances_both_axes()
    {
        var tree = $"pa-{Guid.NewGuid():N}";
        var grain = CreateGrain(tree);

        // A birth report moves both axes: it seeds a frontier where there was
        // none and an offset above the no-offset sentinel. The first-match form
        // reported it as offset-only.
        var outcomes = await OutcomesAsync(tree, () => grain.ReportManyAsync(
            [new MaterialiserPinReport(Consumer, Hlc(100), 10)]));

        Assert.That(outcomes, Is.EqualTo(new[] { Outcome(LatticeMetrics.OutcomePinBothAdvanced) }));
    }

    [Test]
    public async Task Report_advancing_only_the_frontier_records_a_frontier_only_advance()
    {
        var tree = $"pa-{Guid.NewGuid():N}";
        var grain = CreateGrain(tree);
        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(100), 10)]);

        var outcomes = await OutcomesAsync(tree, () => grain.ReportManyAsync(
            [new MaterialiserPinReport(Consumer, Hlc(200), 10)]));

        Assert.That(outcomes, Is.EqualTo(new[] { Outcome(LatticeMetrics.OutcomePinFrontierOnly) }),
            "a pin rewritten at the same offset cannot move the GC offset floor");
    }

    [Test]
    public async Task Report_that_moves_nothing_records_no_advance()
    {
        var tree = $"pa-{Guid.NewGuid():N}";
        var grain = CreateGrain(tree);
        await grain.ReportManyAsync([new MaterialiserPinReport(Consumer, Hlc(200), 10)]);

        var outcomes = await OutcomesAsync(tree, () => grain.ReportManyAsync(
            [new MaterialiserPinReport(Consumer, Hlc(100), 5)]));

        Assert.That(outcomes, Is.EqualTo(new[] { Outcome(LatticeMetrics.OutcomePinNoAdvance) }),
            "a stale report is still an observation, so it is counted as no advance rather than dropped");
    }

    [Test]
    public async Task Report_from_a_new_consumer_with_no_offset_is_not_an_offset_advance()
    {
        var tree = $"pa-{Guid.NewGuid():N}";
        var grain = CreateGrain(tree);

        // ReportAsync supplies the no-offset sentinel. Storing it for a
        // previously unseen consumer does dirty the shard, but it moves no
        // offset the GC can use, so it must not be counted as one.
        var outcomes = await OutcomesAsync(tree, () => grain.ReportAsync(Consumer, Hlc(100)));

        Assert.That(outcomes, Is.EqualTo(new[] { Outcome(LatticeMetrics.OutcomePinFrontierOnly) }));
    }

    [Test]
    public async Task Every_merged_report_records_exactly_one_advance_measurement()
    {
        var tree = $"pa-{Guid.NewGuid():N}";
        var grain = CreateGrain(tree);

        var outcomes = await OutcomesAsync(tree, () => grain.ReportManyAsync(
        [
            new MaterialiserPinReport(Consumer, Hlc(100), 10),
            new MaterialiserPinReport($"{Consumer}-b", Hlc(100), 10),
        ]));

        Assert.That(outcomes, Has.Count.EqualTo(2),
            "the counter is per merged report, so a batched call must not collapse to one measurement");
    }

    [Test]
    public async Task Every_advance_outcome_maps_to_a_distinct_arm()
    {
        var arms = Enum.GetValues<MaterialiserPinAdvanceOutcome>().ToArray();
        var tree = $"pa-{Guid.NewGuid():N}";
        var grain = CreateGrain(tree);

        var outcomes = await OutcomesAsync(tree, () => ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));

        Assert.That(outcomes.Distinct().Count(), Is.EqualTo(arms.Length),
            "each outcome must own its own tag value, or one arm silently reports under another's");
    }

    [Test]
    public async Task Activation_primes_every_advance_arm_at_zero()
    {
        var tree = $"pa-{Guid.NewGuid():N}";
        var grain = CreateGrain(tree);

        var measurements = await MeasurementsAsync(
            tree,
            () => ((IGrainBase)grain).OnActivateAsync(CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(
                measurements.Select(m => m.Outcome).OrderBy(o => o, StringComparer.Ordinal),
                Is.EqualTo(new[]
                {
                    Outcome(LatticeMetrics.OutcomePinBothAdvanced),
                    Outcome(LatticeMetrics.OutcomePinFrontierOnly),
                    Outcome(LatticeMetrics.OutcomePinNoAdvance),
                    Outcome(LatticeMetrics.OutcomePinOffsetOnly),
                }.OrderBy(o => o, StringComparer.Ordinal)),
                "an arm that is never primed is absent until its condition occurs, which is indistinguishable from an undeployed build");
            Assert.That(measurements.Select(m => m.Value), Is.All.Zero,
                "priming must create the series without perturbing the value");
        });
    }
}