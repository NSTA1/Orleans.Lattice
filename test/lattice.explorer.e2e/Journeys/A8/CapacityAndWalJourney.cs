using Orleans.Lattice.Explorer.EndToEnd.HostContract;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A8;

/// <summary>
/// A8, capacity and WAL operations (#5380): after a week of line-feed traffic, olivia reads
/// the boards, finds <c>factory-telemetry</c> running hot, reshards it, resizes it (undoing the
/// first attempt), snapshots the orders before month-end, rebalances a WAL partition, and
/// counts the telemetry's tombstones. Every step is driven through the Explorer.
/// </summary>
/// <remarks>
/// Steps are numbered as in the issue and recorded as the screenshot manifest's step ids
/// (<c>1</c> to <c>7</c>) of journey <c>a8</c>; each distinct UI state is a substep. The
/// reshard runs before the resize: until a resize's previous copy is purged the resize stays
/// undoable, and the cluster refuses to split or fold an undoable tree's shards.
/// </remarks>
[JourneyChapter("A8", After = new[] { "A3" })]
public sealed class CapacityAndWalJourney : JourneyChapter
{
    /// <summary>The journey id the screenshot manifest is filed under.</summary>
    public const string JourneyId = "a8";

    private const string Telemetry = Names.Trees.FactoryTelemetry;
    private const string Orders = Names.Trees.FactoryOrders;
    private const string Snapshot = Names.Trees.FactoryOrdersSnapshot;
    private const float ExpectTimeoutMs = 30_000;
    private const float PlaywrightDefaultExpectTimeoutMs = 5_000;

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var estate = context.Estate;

        // The boards read Prometheus: every silo has been scraped twice before olivia looks.
        await estate.WaitForPrometheusScrapesAsync();
        await using var olivia = await estate.SignInAsAsync(Personas.Olivia);
        var recorder = JourneyRecorder.For(JourneyId);
        Task Capture(string step, string substep, string title) => olivia.SubstepAsync(recorder, step, substep, title);

        // A busy estate answers the cluster pages' reads in seconds, not milliseconds: give every
        // assertion the context's own action timeout, and restore Playwright's default after.
        SetDefaultExpectTimeout(ExpectTimeoutMs);
        try
        {
            await StepAsync("1", () => ReadTheWritesBoardAsync(olivia, Capture));
            await StepAsync("2", () => MeasureAndFindTheHotShardAsync(olivia, Capture));
            await StepAsync("3", () => ReshardTheTelemetryAsync(olivia, Capture));
            await StepAsync("4", () => ResizeUndoAndResizeAgainAsync(olivia, Capture));
            await StepAsync("5", () => SnapshotTheOrdersAsync(olivia, Capture));
            await StepAsync("6", () => MoveAndReclaimAWalPartitionAsync(olivia, Capture));
            await StepAsync("7", () => CountTheTombstonesAsync(olivia, Capture));
        }
        finally
        {
            SetDefaultExpectTimeout(PlaywrightDefaultExpectTimeoutMs);
        }
    }

    // Each step commits its coverage hits on its own, so a later step's failure keeps the
    // evidence of the steps before it.
    private static async Task StepAsync(string step, Func<Task> body)
    {
        using var scope = Coverage.BeginScope("A8." + step);
        await body();
        scope.Complete();
    }

    private static async Task ReadTheWritesBoardAsync(PersonaSession olivia, Func<string, string, string, Task> capture)
    {
        var telemetry = new TelemetryBoardPage(olivia.Shell);
        await telemetry.OpenAsync();
        await capture("1", "boards", "Telemetry lists the cluster's boards; Throughput carries the write rates");

        await telemetry.OpenThroughputAsync();
        await telemetry.ExpectWritesAsync(Telemetry, TimeSpan.FromMinutes(3));
        await capture("1", "throughput", "The Throughput board: write operations per second by tree, with the line feed's factory-telemetry series");

        await telemetry.RefreshAsync(Telemetry);
        await capture("1", "refreshed", "Refresh re-reads every chart, and the line feed's writes are still drawn");
    }

    private static async Task MeasureAndFindTheHotShardAsync(PersonaSession olivia, Func<string, string, string, Task> capture)
    {
        var overview = new ClusterOverviewPage(olivia.Shell);
        await overview.OpenAsync();
        await capture("2", "overview", "The Cluster overview: the estate's identity, and storage figures from the cached WAL poll");

        var measure = await overview.AskToMeasureAsync();
        await capture("2", "measure-confirm", "Re-measure every shard asks for the cluster's id: the leaf walk is expensive and changes nothing");
        await overview.MeasureToTheEndAsync(measure);
        await capture("2", "re-measured", "The storage figures, re-measured by a leaf walk of every shard");

        await overview.FollowStopAsync("Trees");
        var trees = new ClusterTreesPage(olivia.Shell);
        await trees.ExpectListedAsync(Telemetry);
        await trees.OpenTreeAsync(Telemetry);
        var tree = new ClusterTreePage(olivia.Shell, Telemetry);
        await tree.OpenShardsAsync();
        var hot = await tree.ReadHotShardAsync();
        await capture("2", "hotness", hot is { } shard
            ? $"factory-telemetry's shards sorted by writes: shard {shard.Shard} is the hot one, under the line feed"
            : "factory-telemetry's shards, each with its live keys and its rate under the line feed");

        await overview.OpenAsync();
        var stopped = false;
        for (var attempt = 1; attempt <= 3 && !stopped; attempt++)
        {
            measure = await overview.AskToMeasureAsync();
            stopped = await overview.MeasureAndStopAsync(measure);
            if (!stopped)
            {
                TestContext.Out.WriteLine($"A8.2: re-measure attempt {attempt} finished its leaf walk before Stop re-measuring could be pressed; measuring again.");
            }
        }

        Assert.That(stopped, Is.True, "Three re-measures in a row finished before the overview offered Stop re-measuring.");
        await overview.ExpectStoppedAsync();
        await capture("2", "stopped", "Stop re-measuring: the walk is cancelled on the cluster, and the overview says it stopped before it finished");
    }

    private static async Task ReshardTheTelemetryAsync(PersonaSession olivia, Func<string, string, string, Task> capture)
    {
        var overview = new ClusterOverviewPage(olivia.Shell);
        await overview.OpenAsync();
        await overview.FollowStopAsync("Trees");
        var trees = new ClusterTreesPage(olivia.Shell);
        await trees.ExpectListedAsync(Telemetry);
        await capture("3", "tree-list", "The tree list, which offers Reshard tree...");

        await trees.NameTreeToReshardAsync(Telemetry);
        await capture("3", "reshard-dialog", "Reshard a tree: olivia names factory-telemetry");
        await trees.ContinueToReshardAsync(Telemetry);

        var reshard = new ClusterReshardPage(olivia.Shell, Telemetry);
        var from = await reshard.ExpectIdleAsync();
        Assert.That(from, Is.Not.EqualTo(A8Contract.TelemetryShardCount), "factory-telemetry already has the shard count A8 reshards it to.");
        await capture("3", "reshard-page", $"The reshard page: factory-telemetry has {from} physical shards and no reshard is running");

        await reshard.ReviewAsync(from, A8Contract.TelemetryShardCount);
        await capture("3", "review", $"The review: fold {from} physical shards into {A8Contract.TelemetryShardCount}, and what that trades away");

        var confirm = await reshard.AskToReshardAsync();
        await capture("3", "confirm", "Reshard tree asks for the tree's name");
        await reshard.StartAsync(confirm, A8Contract.TelemetryShardCount);
        await capture("3", "running", $"Resharding to {A8Contract.TelemetryShardCount} physical shards, online: the page follows the cluster");

        await reshard.ExpectCompleteAsync(A8Contract.TelemetryShardCount);
        await capture("3", "resharded", $"The reshard is done: factory-telemetry has {A8Contract.TelemetryShardCount} physical shards");
    }

    private static async Task ResizeUndoAndResizeAgainAsync(PersonaSession olivia, Func<string, string, string, Task> capture)
    {
        var tree = new ClusterTreePage(olivia.Shell, Telemetry);
        await tree.OpenAsync();
        await tree.FollowOperationAsync("Resize");

        var resize = new ClusterResizePage(olivia.Shell, Telemetry);
        var original = await resize.ExpectIdleAsync();
        var target = (A8Contract.TelemetryMaxLeafKeys, A8Contract.TelemetryMaxInternalChildren);
        Assert.That(original, Is.Not.EqualTo(target), "factory-telemetry already has the capacity A8 resizes it to.");
        await capture("4", "resize-page", $"The resize page: factory-telemetry holds {original.Leaf} keys per leaf and {original.Internal} children per node");

        await resize.ReviewAsync(original, target);
        await capture("4", "review", $"The review: rebuild with {target.Item1} keys per leaf and {target.Item2} children per node, undoable while the old tree can be recovered");

        var confirm = await resize.AskToResizeAsync();
        await capture("4", "confirm", "Resize tree asks for the tree's name");
        await resize.StartAsync(confirm);
        await resize.ExpectRunningAsync(target);
        await capture("4", "running", "The resize runs online: the tree is copied into a shadow at the new size");

        await resize.ExpectSizeAsync(target);
        await capture("4", "resized", $"The resize is done: {target.Item1} keys per leaf and {target.Item2} children per node, with Undo offered");

        var undo = await resize.AskToUndoAsync();
        await capture("4", "undo-confirm", "Undo resize asks for the tree's name: writes only the resized copy took are lost with it");
        await resize.UndoAsync(undo);
        await resize.ExpectSizeAsync(original);
        await capture("4", "undone", $"The resize is undone: factory-telemetry is back at {original.Leaf} keys per leaf and {original.Internal} children per node");

        await resize.ReviewAsync(original, target);
        var again = await resize.AskToResizeAsync();
        await capture("4", "resize-again-confirm", "Resize again, to the same capacity");
        await resize.StartAsync(again);
        await resize.ExpectSizeAsync(target);
        await capture("4", "resized-again", $"Resized again: factory-telemetry holds {target.Item1} keys per leaf and {target.Item2} children per node");
        Coverage.Hit("use-case:cluster/resize-and-undo");
    }

    private static async Task SnapshotTheOrdersAsync(PersonaSession olivia, Func<string, string, string, Task> capture)
    {
        var tree = new ClusterTreePage(olivia.Shell, Orders);
        await tree.OpenAsync();
        await tree.FollowOperationAsync("Snapshot");

        var snapshot = new ClusterSnapshotPage(olivia.Shell, Orders);
        await snapshot.ExpectIdleAsync();
        await snapshot.ComposeOnlineAsync(Snapshot);
        await capture("5", "compose", "The snapshot page: copy factory-orders into factory-orders-snapshot, online");

        await snapshot.ReviewAsync(Snapshot);
        await capture("5", "review", "The review: the source keeps serving, and live writes are forwarded to the copy");

        var confirm = await snapshot.AskToSnapshotAsync();
        await capture("5", "confirm", "Snapshot tree asks for the source tree's name");
        await snapshot.SnapshotAsync(confirm);
        await capture("5", "captured", "The capture has finished: no snapshot is running");

        var data = new DataDirectoryPage(olivia.Shell);
        await data.OpenAsync();
        await data.ExpectTreeAsync(Snapshot);
        await capture("5", "in-data", "factory-orders-snapshot is a live tree in Data");
        Coverage.Hit("use-case:cluster/snapshot");
    }

    private static async Task MoveAndReclaimAWalPartitionAsync(PersonaSession olivia, Func<string, string, string, Task> capture)
    {
        const int partition = A8Contract.MovedPartition;
        const string target = JourneyHostEnvironment.SecondaryWalProviderKey;

        var overview = new ClusterOverviewPage(olivia.Shell);
        await overview.OpenAsync();
        await overview.FollowStopAsync("WAL placement");
        var wal = new ClusterWalPage(olivia.Shell);
        await wal.ExpectChooseTreeAsync();

        await wal.AuditAsync(Telemetry);
        var source = await wal.PartitionKeyAsync(partition);
        Assert.That(source, Is.Not.EqualTo(target), $"factory-telemetry's WAL partition {partition} is already on {target}.");
        await capture("6", "audit", $"Audit: factory-telemetry's WAL partitions and their provider keys, every key resolving");

        await wal.ComposePlanAsync(Telemetry, partition, target);
        await capture("6", "plan-dialog", $"Plan a WAL move: partition {partition} to {target}");
        await wal.PlanAsync(Telemetry, partition, target);
        await capture("6", "plan", "The move plan, a read-only preview at its own address: the range to copy, and the target resolving here");

        var moved = false;
        for (var attempt = 1; attempt <= 2 && !moved; attempt++)
        {
            var confirm = await wal.AskToMoveAsync(Telemetry);
            if (attempt == 1)
            {
                await capture("6", "move-confirm", "Move WAL partition asks for the tree's name: the partition quiesces briefly while its tail is copied");
            }

            moved = await wal.MoveAsync(confirm);
            if (!moved)
            {
                // A move off an Azure Table source can fail just after the line feed's writes
                // (#5470, #5466); the page says so, the partition stays on its source, and a
                // second attempt is what an operator does.
                TestContext.Out.WriteLine($"A8.6: WAL move attempt {attempt} failed (see #5470 and #5466); moving again.");
            }
        }

        Assert.That(moved, Is.True, "The WAL move failed twice.");
        await capture("6", "moved", $"Moved: partition {partition} now lives on {target}, and its source is retained until reclaimed");

        var reclaim = await wal.AskToReclaimAsync(Telemetry);
        await capture("6", "reclaim-confirm", "Reclaim WAL source asks for the tree's name: after it the move cannot be reverted");
        await wal.ReclaimAsync(reclaim);
        await capture("6", "reclaimed", "The source's retained log is reclaimed");
    }

    private static async Task CountTheTombstonesAsync(PersonaSession olivia, Func<string, string, string, Task> capture)
    {
        var tree = new ClusterTreePage(olivia.Shell, Telemetry);
        await tree.OpenAsync();
        await tree.OpenShardsAsync();
        await Expect(tree.Term("Physical shards")).ToHaveTextAsync(A8PageObject.Count(A8Contract.TelemetryShardCount));
        await capture("7", "shards", $"factory-telemetry's Shards tab after the reshard and resize: {A8Contract.TelemetryShardCount} physical shards, live keys only");

        var confirm = await tree.AskToCountTombstonesAsync();
        await capture("7", "confirm", "Count tombstones asks for the tree's name: a deep read walks every leaf");
        await tree.CountTombstonesAsync(confirm);
        await capture("7", "counted", "A deep read: every shard's tombstones and expired entries are counted");
    }
}
