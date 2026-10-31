using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A12;

/// <summary>
/// A12, cluster in depth (#5382): olivia rehearses the operations she hopes never to need in
/// anger - a tree created with explicit sizing, a grow and a shrink, online and offline
/// snapshots, a WAL move stopped before its flip, every WAL reclamation state, and the repair
/// of an orphaned leaf left by a crash mid-split.
/// </summary>
/// <remarks>
/// The off-UI actions are the harness acting as the outside world, each named in the issue: a
/// burst of upstream writes that splits one leaf of <c>factory-lines-restored</c> (before step
/// 4, so its WAL holds entries to move and step 6 has a split to interrupt); a WAL target that
/// stops answering (step 4); the materialiser pins a leaf leaves behind (step 5); the orphan a
/// crash mid-split leaves (step 6); and, once the repair is proved, the removal of the burst's
/// rows, so the tree holds what the F6 seeder leaves.
/// </remarks>
[JourneyChapter("A12", After = new[] { "A8", "A10" })]
public sealed class A12ClusterInDepth : JourneyChapter
{
    /// <summary>The journey id the screenshots are filed under.</summary>
    public const string JourneyId = "a12";

    /// <summary>The prefix of the burst of rows that splits one leaf of <c>factory-lines-restored</c>.</summary>
    public const string SplitPrefix = "split/";

    /// <summary>How many rows the burst writes into one shard: more than a leaf of the restored tree holds (128).</summary>
    public const int SplitRows = 160;

    private static readonly string Restored = Names.Trees.FactoryLinesRestored;

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        var recorder = JourneyRecorder.For(JourneyId);
        await using var olivia = await context.Estate.SignInAsAsync(Personas.Olivia);

        await StepAsync(1, () => TreeListAsync(context, olivia, recorder));
        await StepAsync(2, () => GrowAndShrinkAsync(olivia, recorder));
        await StepAsync(3, () => SnapshotsAsync(context, olivia, recorder));

        await WriteSplitBurstAsync(context.Estate);

        await StepAsync(4, () => StopMoveBeforeFlipAsync(context, olivia, recorder));
        await StepAsync(5, () => ReclamationStatesAsync(context, olivia, recorder));
        await StepAsync(6, () => RepairOrphanAsync(context, olivia, recorder));

        await DeleteSplitBurstAsync(context.Estate);
    }

    private static async Task StepAsync(int step, Func<Task> body)
    {
        using var scope = Coverage.BeginScope($"A12.{step}");
        await body();
        scope.Complete();
    }

    private static Task Shot(PersonaSession session, JourneyRecorder recorder, int step, string substep, string title) =>
        session.SubstepAsync(recorder, step.ToString(System.Globalization.CultureInfo.InvariantCulture), substep, title);

    // 1. Tree list: filter by name, owner and state; New tree... factory-sized, explicitly sized.
    private static async Task TreeListAsync(ChapterContext context, PersonaSession olivia, JourneyRecorder recorder)
    {
        var trees = new ClusterTreesPage(olivia.Shell);
        await trees.OpenAsync();
        await trees.ExpectListedAsync([Names.Trees.FactoryLines, Restored, Names.Trees.FactoryTelemetry], []);
        await Shot(olivia, recorder, 1, "list", "Olivia opens the cluster's tree list");

        await trees.FilterAsync("factory-lines");
        await trees.ExpectListedAsync([Names.Trees.FactoryLines, Restored, Names.Trees.FactoryLinesCold], [Names.Trees.FactoryTelemetry, Names.Trees.FactoryOrders]);
        await Shot(olivia, recorder, 1, "filter-name", "Filtered by name: only the lines trees are listed");

        // An app owns the task board's tree once A5 has installed it; without A5 nothing an
        // app owns exists, and the owner filter says it matches nothing.
        await trees.FilterAsync("app task-board");
        if (context.IsProduced("A5"))
        {
            await trees.ExpectListedAsync([context.Recall(StoryKeys.TaskBoardTree)], [Names.Trees.FactoryLines]);
        }
        else
        {
            await trees.ExpectNoMatchAsync();
        }

        await Shot(olivia, recorder, 1, "filter-owner", "Filtered by owner: only trees the task-board app owns");

        await trees.FilterAsync("aliased");
        await trees.ExpectListedAsync([Restored], [Names.Trees.FactoryLines]);
        await Shot(olivia, recorder, 1, "filter-state", "Filtered by state: the restored tree reads Live, aliased; the repaired lines do not");
        Coverage.Hit("use-case:cluster/tree-list-filters");

        await trees.FilterAsync(string.Empty);
        await trees.ComposeTreeAsync(Names.Trees.FactorySized, A12Contract.SizedShardCount, A12Contract.SizedMaxLeafKeys, A12Contract.SizedMaxInternalChildren);
        await Shot(olivia, recorder, 1, "new-tree", "New tree...: factory-sized with 4 shards, 32 keys per leaf and 16 children per node");
        await trees.ReviewTreeAsync(Names.Trees.FactorySized, A12Contract.SizedShardCount, A12Contract.SizedMaxLeafKeys, A12Contract.SizedMaxInternalChildren);
        await Shot(olivia, recorder, 1, "new-tree-review", "The review names the tree and all three sizes");
        await trees.CreateTreeAsync(Names.Trees.FactorySized, A12Contract.SizedShardCount, A12Contract.SizedMaxLeafKeys, A12Contract.SizedMaxInternalChildren);
        await Shot(olivia, recorder, 1, "created", "The tree is created and its Summary says what to do next");
        await trees.ExpectConfigurationAsync(A12Contract.SizedShardCount, A12Contract.SizedMaxLeafKeys, A12Contract.SizedMaxInternalChildren);
        await Shot(olivia, recorder, 1, "configuration", "Configuration shows the shard count, keys per leaf and children per node");
    }

    // 2. Reshard factory-lines-restored up (grow) to 128, then down (shrink) to 64.
    private static async Task GrowAndShrinkAsync(PersonaSession olivia, JourneyRecorder recorder)
    {
        var trees = new ClusterTreesPage(olivia.Shell);
        var operations = new ClusterOperationPages(olivia.Shell);
        await trees.OpenAsync();
        await trees.OpenReshardAsync(Restored);
        await operations.ExpectReshardIdleAsync(Restored, A12Contract.RestoredShardCount);
        await Shot(olivia, recorder, 2, "reshard", "Reshard factory-lines-restored: idle at 64 physical shards");

        await operations.ReviewReshardAsync(Restored, A12Contract.RestoredShardCount, A12Contract.RestoredGrowTo);
        await Shot(olivia, recorder, 2, "grow-review", "Grow from 64 to 128 physical shards, reviewed");
        await operations.ComposeReshardConfirmationAsync(Restored);
        await Shot(olivia, recorder, 2, "grow-confirm", "The grow is confirmed by typing the tree's name");
        await operations.RunReshardAsync(Restored, A12Contract.RestoredGrowTo);
        await Shot(olivia, recorder, 2, "grown", "The grow has run: 128 physical shards, no reshard running");

        await operations.ReviewReshardAsync(Restored, A12Contract.RestoredGrowTo, A12Contract.RestoredShardCount);
        await Shot(olivia, recorder, 2, "shrink-review", "Shrink back to 64, reviewed with its throughput trade-off");
        await operations.ComposeReshardConfirmationAsync(Restored);
        await operations.RunReshardAsync(Restored, A12Contract.RestoredShardCount);
        await Shot(olivia, recorder, 2, "shrunk", "The shrink has run: 64 physical shards again");
        Coverage.Hit("use-case:cluster/reshard-grow-and-shrink");
        Coverage.Hit("use-case:cluster/reshard");
    }

    // 3. Online snapshot of the telemetry while the line feed writes; offline snapshot of the restored lines, sized.
    private static async Task SnapshotsAsync(ChapterContext context, PersonaSession olivia, JourneyRecorder recorder)
    {
        var operations = new ClusterOperationPages(olivia.Shell);
        var feed = context.Estate.LineFeed;
        var before = await feed.StatusAsync();
        Assert.That(before is { Started: true, Paused: false }, Is.True, "the line feed is writing telemetry");

        await operations.OpenSnapshotAsync(Names.Trees.FactoryTelemetry);
        await operations.ComposeSnapshotAsync(Names.Trees.FactoryTelemetry, Names.Trees.TelemetrySnapOnline, offline: false, null, null);
        await Shot(olivia, recorder, 3, "online", "Snapshot factory-telemetry online into telemetry-snap-online");
        await operations.ReviewSnapshotAsync(Names.Trees.FactoryTelemetry, Names.Trees.TelemetrySnapOnline, offline: false);
        await Shot(olivia, recorder, 3, "online-confirm", "Reviewed: the source keeps serving and live writes are forwarded");
        await operations.RunSnapshotAsync();
        await feed.WaitUntilAsync(status => status.TelemetryWritten > before.TelemetryWritten, TimeSpan.FromMinutes(1));
        await Shot(olivia, recorder, 3, "online-done", "The online snapshot is done; the line feed wrote throughout");

        await operations.OpenSnapshotAsync(Restored);
        await operations.ComposeSnapshotAsync(
            Restored, Names.Trees.LinesSnapOffline, offline: true, A12Contract.OfflineSnapshotMaxLeafKeys, A12Contract.OfflineSnapshotMaxInternalChildren);
        await Shot(olivia, recorder, 3, "offline", "Snapshot factory-lines-restored offline into lines-snap-offline, sized 16 and 8");
        await operations.ReviewSnapshotAsync(Restored, Names.Trees.LinesSnapOffline, offline: true);
        await Shot(olivia, recorder, 3, "offline-confirm", "Reviewed: the source stops serving until the copy finishes");
        await operations.RunSnapshotAsync();
        await Shot(olivia, recorder, 3, "offline-done", "The offline snapshot is done");
        await operations.ExpectSizingAsync(Names.Trees.LinesSnapOffline, A12Contract.OfflineSnapshotMaxLeafKeys, A12Contract.OfflineSnapshotMaxInternalChildren);
        await Shot(olivia, recorder, 3, "offline-sizing", "lines-snap-offline's Configuration shows its explicit sizing");
        Coverage.Hit("use-case:cluster/snapshot-online-and-offline");
        Coverage.Hit("use-case:cluster/snapshot");
    }

    // 4. Plan and start a WAL move, then Stop it before its flip; the partition is unchanged.
    private static async Task StopMoveBeforeFlipAsync(ChapterContext context, PersonaSession olivia, JourneyRecorder recorder)
    {
        var wal = new ClusterWalPage(olivia.Shell);
        await wal.OpenAsync(Restored);
        var placement = await wal.AuditTextAsync();
        Assert.That(placement, Is.Not.Empty.And.Not.Contain(JourneyHostEnvironment.SecondaryWalProviderKey), "partition 0 starts on the default provider");
        await Shot(olivia, recorder, 4, "audit", "factory-lines-restored's WAL placement, audited");

        await wal.PlanAsync(0, JourneyHostEnvironment.SecondaryWalProviderKey);
        await Shot(olivia, recorder, 4, "plan", "A move of partition 0 to the secondary provider, planned");

        // Off-UI: the target storage stops answering, so the move waits in its copy, before its flip.
        await context.Estate.HoldWalTargetAsync(Restored);
        try
        {
            await wal.ComposeMoveAsync(Restored);
            await Shot(olivia, recorder, 4, "move-confirm", "The move is confirmed by typing the tree's name");
            await wal.StartMoveAsync();
            await Shot(olivia, recorder, 4, "moving", "The move is running and offers Stop: it has not flipped");
            await wal.StopMoveAsync();
            await Shot(olivia, recorder, 4, "stopped", "Stopped before its flip: the partition stays on its source");
        }
        finally
        {
            await context.Estate.ReleaseWalTargetAsync(Restored);
        }

        await wal.OpenAsync(Restored);
        Assert.That(await wal.AuditTextAsync(), Is.EqualTo(placement), "the partition's placement is unchanged");
        await Shot(olivia, recorder, 4, "unchanged", "Audited again: every partition is still on the provider it was on");
        Coverage.Hit("use-case:cluster/wal-stop-before-flip");
    }

    // 5. WAL reclamation's Blocked, Waiting and Not established states, as the harness holds and releases them.
    private static async Task ReclamationStatesAsync(ChapterContext context, PersonaSession olivia, JourneyRecorder recorder)
    {
        var wal = new ClusterWalPage(olivia.Shell);
        var tree = Names.Trees.FactorySized;
        var states = new (JourneyWalPin Pin, string Substep, string Label, string Explanation, string Title)[]
        {
            (JourneyWalPin.Stranded, "blocked", "Blocked", "Reclamation is blocked", "A leaf pinned the WAL and crashed before checkpointing: Blocked"),
            (JourneyWalPin.Unchecked, "waiting", "Waiting for a checkpoint", "has not checkpointed partition", "A leaf has not checkpointed yet: Waiting for a checkpoint"),
            (JourneyWalPin.Unidentified, "not-established", "Not established", "but its state could not be read", "The pin's leaf cannot be read: Not established"),
        };

        foreach (var state in states)
        {
            var pin = await context.Estate.PlantWalPinAsync(tree, state.Pin);
            try
            {
                await wal.OpenAsync(tree);
                await wal.ExpectVerdictAsync(state.Label, state.Explanation);
                await Shot(olivia, recorder, 5, state.Substep, state.Title);
            }
            finally
            {
                await context.Estate.ReleaseWalPinAsync(tree, pin);
            }
        }

        await wal.OpenAsync(tree);
        foreach (var state in states)
        {
            await Microsoft.Playwright.Assertions.Expect(wal.Verdict.GetByText(state.Label, new() { Exact = true })).ToHaveCountAsync(0);
        }

        await Shot(olivia, recorder, 5, "released", "Every condition released: reclamation is no longer held back");
        Coverage.Hit("use-case:cluster/wal-reclamation-states");
    }

    // 6. Off-UI, an orphaned leaf; the audit finds it, Show each leaf, Repair..., and a re-audit is clean.
    private static async Task RepairOrphanAsync(ChapterContext context, PersonaSession olivia, JourneyRecorder recorder)
    {
        await context.Estate.InjectOrphanedLeafAsync(Restored);

        var orphans = new ClusterOrphansPage(olivia.Shell);
        await orphans.OpenAsync();
        await orphans.NameTreeAsync(Restored);
        await Shot(olivia, recorder, 6, "choose", "Orphaned leaves: factory-lines-restored is named");
        await orphans.ChooseAsync(Restored);
        await orphans.AuditAsync("Orphans found", "1 orphaned leaf, 1 repairable.");
        await Shot(olivia, recorder, 6, "found", "The audit finds the orphaned leaf, and it is repairable");
        await orphans.ShowEachLeafAsync();
        await Shot(olivia, recorder, 6, "each-leaf", "Show each leaf: its shard, range, keys and disposition");
        await orphans.ComposeRepairAsync(Restored);
        await Shot(olivia, recorder, 6, "repair-confirm", "Repair orphaned leaves, confirmed by typing the tree's name");
        await orphans.RepairAsync();
        await Shot(olivia, recorder, 6, "clean", "The repair unspliced the leaf, and the audit after it is clean");
        Coverage.Hit("use-case:cluster/orphan-audit-and-repair");
    }

    private static async Task WriteSplitBurstAsync(JourneyEstate estate)
    {
        var data = ContractFacades.Of(estate).Data;
        var map = await ContractFacades.Of(estate).TreeAdmin.InspectShardMapAsync(Restored);
        await ContractCalls.SetManyAsync(data, Restored, SplitBurst(map.VirtualShardCount).Select(key => new StoryEntry(key, "{\"burst\":true}")));
    }

    private static async Task DeleteSplitBurstAsync(JourneyEstate estate)
    {
        var data = ContractFacades.Of(estate).Data;
        await data.DeleteRangeAsync(new DataRangeDeleteRequest { TreeId = Restored, StartInclusive = SplitPrefix, EndExclusive = ContractCalls.PrefixEnd(SplitPrefix) });
    }

    /// <summary>
    /// <see cref="SplitRows"/> keys that all hash to one virtual slot, so they land in one
    /// physical shard whatever the shard map, and overflow one of its leaves.
    /// </summary>
    /// <param name="virtualShardCount">The tree's virtual slot count.</param>
    internal static IReadOnlyList<string> SplitBurst(int virtualShardCount)
    {
        var slot = ShardMap.GetVirtualSlot(SplitPrefix + "anchor", virtualShardCount);
        var keys = new List<string>(SplitRows);
        for (var n = 0; keys.Count < SplitRows; n++)
        {
            var key = $"{SplitPrefix}{n:D8}";
            if (ShardMap.GetVirtualSlot(key, virtualShardCount) == slot)
            {
                keys.Add(key);
            }
        }

        return keys;
    }
}
