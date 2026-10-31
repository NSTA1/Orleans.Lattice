using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.EndToEnd.HostContract;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A8, capacity and WAL operations (#5380). Produces <c>factory-telemetry</c> resized to
/// <see cref="TelemetryMaxLeafKeys"/>/<see cref="TelemetryMaxInternalChildren"/> and resharded to
/// <see cref="TelemetryShardCount"/> physical shards; <c>factory-orders-snapshot</c>, an online
/// snapshot of the orders; and <c>factory-telemetry</c>'s WAL partition
/// <see cref="MovedPartition"/> moved to the estate's secondary WAL provider
/// (<see cref="JourneyHostEnvironment.SecondaryWalProviderKey"/>) with its source reclaimed.
/// </summary>
public sealed class A8Contract() : ChapterContract("A8")
{
    /// <summary>The telemetry's leaf capacity after the resize.</summary>
    public const int TelemetryMaxLeafKeys = 64;

    /// <summary>The telemetry's internal-node capacity after the resize.</summary>
    public const int TelemetryMaxInternalChildren = 64;

    /// <summary>The telemetry's physical shard count after the reshard.</summary>
    public const int TelemetryShardCount = 32;

    /// <summary>The telemetry WAL partition A8 moves.</summary>
    public const int MovedPartition = 0;

    private static readonly TimeSpan StructuralPatience = TimeSpan.FromMinutes(6);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var admin = ContractFacades.Of(context).TreeAdmin;
        var telemetry = Names.Trees.FactoryTelemetry;

        // Reshard before resizing: a resize stays undoable until its previous copy is purged,
        // and while it can be undone the tree's shards cannot be split or folded.
        if (!await ReshardedAsync(admin))
        {
            await admin.ReshardTreeAsync(telemetry, TelemetryShardCount);
            await ContractWait.UntilAsync("the telemetry reshard", async () => await ReshardedAsync(admin), StructuralPatience);
        }

        var resize = await admin.GetResizeStatusAsync(telemetry);
        if (resize.CurrentMaxLeafKeys != TelemetryMaxLeafKeys || resize.CurrentMaxInternalChildren != TelemetryMaxInternalChildren)
        {
            await admin.ResizeTreeAsync(telemetry, TelemetryMaxLeafKeys, TelemetryMaxInternalChildren);
        }

        await ContractWait.UntilAsync("the telemetry resize", async () => await ResizedAsync(admin), StructuralPatience);

        if (!await ContractCalls.TreeIsLiveAsync(admin, Names.Trees.FactoryOrdersSnapshot))
        {
            await admin.SnapshotTreeAsync(Names.Trees.FactoryOrders, Names.Trees.FactoryOrdersSnapshot, TreeSnapshotMode.Online);
            await ContractWait.UntilAsync("the orders snapshot", async () =>
                !(await admin.GetSnapshotStatusAsync(Names.Trees.FactoryOrders)).InProgress
                && (await admin.CheckTreeExistsAsync(Names.Trees.FactoryOrdersSnapshot)).Exists, StructuralPatience);
        }

        if (await PartitionKeyAsync(admin) != JourneyHostEnvironment.SecondaryWalProviderKey)
        {
            var source = await PartitionKeyAsync(admin);
            var move = await admin.StartWalMoveAsync(telemetry, MovedPartition, JourneyHostEnvironment.SecondaryWalProviderKey);
            await ContractWait.SucceededAsync("the telemetry WAL move", () => admin.GetTreeAdminOperationStatusAsync(move.OperationId), StructuralPatience);
            await admin.ReclaimMovedWalSourceAsync(telemetry, MovedPartition, source);
        }
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var admin = ContractFacades.Of(context).TreeAdmin;
        Require(await ResizedAsync(admin), $"factory-telemetry is resized to {TelemetryMaxLeafKeys} keys per leaf and {TelemetryMaxInternalChildren} children per node");
        Require(await ReshardedAsync(admin), $"factory-telemetry is resharded to {TelemetryShardCount} physical shards");
        if (!context.IsProduced("C3"))
        {
            Require(await ContractCalls.TreeIsLiveAsync(admin, Names.Trees.FactoryOrdersSnapshot), "factory-orders-snapshot exists");
        }

        RequireEqual(await PartitionKeyAsync(admin), JourneyHostEnvironment.SecondaryWalProviderKey, $"factory-telemetry's WAL partition {MovedPartition} is on the secondary provider");
    }

    private static async Task<bool> ResizedAsync(Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiGrpcClient admin)
    {
        var status = await admin.GetResizeStatusAsync(Names.Trees.FactoryTelemetry);
        return !status.InProgress && !status.UndoRequested
            && status.CurrentMaxLeafKeys == TelemetryMaxLeafKeys
            && status.CurrentMaxInternalChildren == TelemetryMaxInternalChildren;
    }

    private static async Task<bool> ReshardedAsync(Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiGrpcClient admin)
    {
        var status = await admin.GetReshardStatusAsync(Names.Trees.FactoryTelemetry);
        return !status.InProgress && status.CurrentPhysicalShardCount == TelemetryShardCount;
    }

    private static async Task<string> PartitionKeyAsync(Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiGrpcClient admin)
    {
        var placement = await admin.GetWalPlacementAsync(Names.Trees.FactoryTelemetry);
        return placement.Partitions.IsDefaultOrEmpty || !placement.Partitions.Any(partition => partition.Partition == MovedPartition)
            ? placement.DefaultProviderKey
            : placement.Partitions.First(partition => partition.Partition == MovedPartition).ProviderKey;
    }
}
