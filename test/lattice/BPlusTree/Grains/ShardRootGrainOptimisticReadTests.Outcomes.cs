using System.Diagnostics.Metrics;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The <c>orleans.lattice.shard_root.optimistic_read.outcomes</c> counter
/// (issue #3474): every return of <see cref="ShardRootGrain.TryGetOptimisticAsync"/>
/// records exactly one outcome, so the share of point reads handed back to the
/// serial, non-interleaved shard-root read is observable on a live rig. Each test
/// runs on a tree id of its own because the counter is a process-wide static.
/// </summary>
public sealed partial class ShardRootGrainOptimisticReadTests
{
    private static readonly string[] OptimisticReadOutcomeArms =
    [
        (string)LatticeMetrics.OutcomeOptimisticReadValidatedTag.Value!,
        (string)LatticeMetrics.OutcomeOptimisticReadDisabledTag.Value!,
        (string)LatticeMetrics.OutcomeOptimisticReadBusyTag.Value!,
        (string)LatticeMetrics.OutcomeOptimisticReadGateClosedTag.Value!,
        (string)LatticeMetrics.OutcomeOptimisticReadRoutingCacheMissTag.Value!,
        (string)LatticeMetrics.OutcomeOptimisticReadEpochChangedTag.Value!,
        (string)LatticeMetrics.OutcomeOptimisticReadAbsentTag.Value!,
    ];

    private static (MeterListener Listener, Dictionary<string, long> Totals) ListenForOptimisticReadOutcomes(string treeId)
    {
        var totals = new Dictionary<string, long>(StringComparer.Ordinal);
        var listener = MeterListening.StartForInstrument(
            LatticeMetrics.ShardRootOptimisticReadOutcomes,
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
                    totals[outcome] = totals.GetValueOrDefault(outcome) + value;
                }
            }));

        return (listener, totals);
    }

    [Test]
    [NonParallelizable]
    public async Task Activation_primes_every_optimistic_read_outcome_arm_at_zero()
    {
        const string TreeId = "optimistic-outcome-prime-tree";
        var (listener, totals) = ListenForOptimisticReadOutcomes(TreeId);
        var (grain, _, _, _) = CreateGrain(shardKey: TreeId + "/0");

        await ((IGrainBase)grain).OnActivateAsync(CancellationToken.None);
        listener.Dispose();

        Assert.That(totals.Keys, Is.EquivalentTo(OptimisticReadOutcomeArms),
            "activation must publish every declared outcome arm, so an absent arm means the build does not carry it");
        Assert.That(totals.Values, Has.All.Zero, "activation performs no read, so every arm must be primed at zero");
    }

    [Test]
    [NonParallelizable]
    public async Task Validated_read_records_the_validated_outcome()
    {
        const string TreeId = "optimistic-outcome-validated-tree";
        var (listener, totals) = ListenForOptimisticReadOutcomes(TreeId);
        var (grain, _, leaf, _) = CreateGrain(shardKey: TreeId + "/0");
        leaf.GetAsync("k1").Returns(Bytes("v1"));

        var result = await grain.TryGetOptimisticAsync("k1");
        listener.Dispose();

        Assert.That(result.IsValidated, Is.True);
        Assert.That(totals, Is.EquivalentTo(new Dictionary<string, long> { ["validated"] = 1 }));
    }

    [Test]
    [NonParallelizable]
    public async Task Absent_key_records_the_absent_outcome()
    {
        const string TreeId = "optimistic-outcome-absent-tree";
        var (listener, totals) = ListenForOptimisticReadOutcomes(TreeId);
        var (grain, _, leaf, _) = CreateGrain(shardKey: TreeId + "/0");
        leaf.GetAsync("missing").Returns((byte[]?)null);

        var result = await grain.TryGetOptimisticAsync("missing");
        listener.Dispose();

        Assert.That(result.IsValidated, Is.False);
        Assert.That(totals, Is.EquivalentTo(new Dictionary<string, long> { ["absent"] = 1 }),
            "an absent key is handed to the serial read, and the counter must name why");
    }

    [Test]
    [NonParallelizable]
    public async Task Disabled_option_records_the_disabled_outcome()
    {
        const string TreeId = "optimistic-outcome-disabled-tree";
        var (listener, totals) = ListenForOptimisticReadOutcomes(TreeId);
        var (grain, _, _, _) = CreateGrain(optimisticReads: false, shardKey: TreeId + "/0");

        var result = await grain.TryGetOptimisticAsync("k1");
        listener.Dispose();

        Assert.That(result.IsValidated, Is.False);
        Assert.That(totals, Is.EquivalentTo(new Dictionary<string, long> { ["disabled"] = 1 }));
    }

    [Test]
    [NonParallelizable]
    public async Task Pending_promotion_records_the_busy_outcome()
    {
        const string TreeId = "optimistic-outcome-busy-tree";
        var (listener, totals) = ListenForOptimisticReadOutcomes(TreeId);
        var (grain, state, _, _) = CreateGrain(shardKey: TreeId + "/0");
        state.State.PendingPromotion = new SplitResult
        {
            PromotedKey = "m",
            NewSiblingId = GrainId.Create("leaf", "sibling"),
        };

        var result = await grain.TryGetOptimisticAsync("k1");
        listener.Dispose();

        Assert.That(result.IsValidated, Is.False);
        Assert.That(totals, Is.EquivalentTo(new Dictionary<string, long> { ["busy"] = 1 }));
    }

    [Test]
    [NonParallelizable]
    public async Task Repeated_reads_of_one_leaf_resolve_its_grain_reference_once()
    {
        var factory = Substitute.For<IGrainFactory>();
        var (grain, _, leaf, _) = CreateGrain(shardKey: "optimistic-ref-cache-tree/0", factory: factory);
        leaf.GetAsync(Arg.Any<string>()).Returns(Bytes("v"));

        for (var i = 0; i < 5; i++)
        {
            Assert.That((await grain.TryGetOptimisticAsync($"k{i}")).IsValidated, Is.True);
        }

        factory.Received(1).GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>());
    }
}
