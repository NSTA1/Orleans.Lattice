using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A12, cluster in depth (#5382). Produces <c>factory-sized</c>, empty, created with
/// <see cref="SizedShardCount"/> shards, <see cref="SizedMaxLeafKeys"/> keys per leaf and
/// <see cref="SizedMaxInternalChildren"/> children per node; <c>factory-lines-restored</c>
/// resharded up to <see cref="RestoredGrowTo"/> and back down to <see cref="RestoredShardCount"/>,
/// with a clean orphaned-leaf audit; <c>telemetry-snap-online</c>, an online snapshot of the
/// telemetry; and <c>lines-snap-offline</c>, an offline snapshot of the restored lines sized
/// <see cref="OfflineSnapshotMaxLeafKeys"/>/<see cref="OfflineSnapshotMaxInternalChildren"/>.
/// The seeder does not inject an orphan: a repaired tree and a never-damaged one audit alike.
/// C3 purges the two snapshots.
/// </summary>
public sealed class A12Contract() : ChapterContract("A12")
{
    /// <summary><c>factory-sized</c>'s physical shard count.</summary>
    public const int SizedShardCount = 4;

    /// <summary><c>factory-sized</c>'s leaf capacity.</summary>
    public const int SizedMaxLeafKeys = 32;

    /// <summary><c>factory-sized</c>'s internal-node capacity.</summary>
    public const int SizedMaxInternalChildren = 16;

    /// <summary>What <c>factory-lines-restored</c> grows to.</summary>
    public const int RestoredGrowTo = 128;

    /// <summary>What <c>factory-lines-restored</c> shrinks back to: the library default it was restored with.</summary>
    public const int RestoredShardCount = 64;

    /// <summary>The offline snapshot's leaf capacity.</summary>
    public const int OfflineSnapshotMaxLeafKeys = 16;

    /// <summary>The offline snapshot's internal-node capacity.</summary>
    public const int OfflineSnapshotMaxInternalChildren = 8;

    private static readonly TimeSpan StructuralPatience = TimeSpan.FromMinutes(6);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var admin = ContractFacades.Of(context).TreeAdmin;
        await admin.CreateTreeAsync(Names.Trees.FactorySized, SizedShardCount, SizedMaxLeafKeys, SizedMaxInternalChildren);

        if ((await admin.GetReshardStatusAsync(Names.Trees.FactoryLinesRestored)).CurrentPhysicalShardCount != RestoredShardCount)
        {
            await ReshardAsync(admin, RestoredShardCount);
        }
        else
        {
            await ReshardAsync(admin, RestoredGrowTo);
            await ReshardAsync(admin, RestoredShardCount);
        }

        await SnapshotAsync(admin, Names.Trees.FactoryTelemetry, Names.Trees.TelemetrySnapOnline, TreeSnapshotMode.Online, null, null);
        await SnapshotAsync(
            admin, Names.Trees.FactoryLinesRestored, Names.Trees.LinesSnapOffline, TreeSnapshotMode.Offline, OfflineSnapshotMaxLeafKeys, OfflineSnapshotMaxInternalChildren);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var admin = ContractFacades.Of(context).TreeAdmin;
        var sized = await admin.GetTreeConfigAsync(Names.Trees.FactorySized);
        Require(sized.Exists, "factory-sized exists");
        RequireEqual((sized.ShardCount, sized.MaxLeafKeys, sized.MaxInternalChildren), ((int?)SizedShardCount, (int?)SizedMaxLeafKeys, (int?)SizedMaxInternalChildren), "factory-sized is explicitly sized");

        var reshard = await admin.GetReshardStatusAsync(Names.Trees.FactoryLinesRestored);
        Require(!reshard.InProgress && reshard.CurrentPhysicalShardCount == RestoredShardCount, $"factory-lines-restored is resharded back to {RestoredShardCount} shards");

        TreeOrphanedLeafReport? report = null;
        var orphans = 0;
        do
        {
            report = await admin.AuditOrphanedLeavesAsync(Names.Trees.FactoryLinesRestored, report?.ResumeFrom);
            orphans += report.OrphanedLeafCount;
        }
        while (!report.IsComplete);

        Require(orphans == 0 && report.VerdictComplete, "factory-lines-restored audits clean of orphaned leaves");

        if (!context.IsProduced("C3"))
        {
            Require(await ContractCalls.TreeIsLiveAsync(admin, Names.Trees.TelemetrySnapOnline), "telemetry-snap-online exists");
            Require(await ContractCalls.TreeIsLiveAsync(admin, Names.Trees.LinesSnapOffline), "lines-snap-offline exists");
            var offline = await admin.GetTreeConfigAsync(Names.Trees.LinesSnapOffline);
            RequireEqual((offline.MaxLeafKeys, offline.MaxInternalChildren), ((int?)OfflineSnapshotMaxLeafKeys, (int?)OfflineSnapshotMaxInternalChildren), "lines-snap-offline is explicitly sized");
        }
    }

    private static async Task ReshardAsync(LatticeTreeAdminApiGrpcClient admin, int target)
    {
        await admin.ReshardTreeAsync(Names.Trees.FactoryLinesRestored, target);
        await ContractWait.UntilAsync($"factory-lines-restored to reach {target} shards", async () =>
        {
            var status = await admin.GetReshardStatusAsync(Names.Trees.FactoryLinesRestored);
            return !status.InProgress && status.CurrentPhysicalShardCount == target;
        }, StructuralPatience);
    }

    private static async Task SnapshotAsync(LatticeTreeAdminApiGrpcClient admin, string source, string destination, TreeSnapshotMode mode, int? maxLeafKeys, int? maxInternalChildren)
    {
        if (await ContractCalls.TreeIsLiveAsync(admin, destination))
        {
            return;
        }

        await admin.SnapshotTreeAsync(source, destination, mode, maxLeafKeys, maxInternalChildren);
        TreeSnapshotStatus? status = null;
        var exists = false;
        try
        {
            await ContractWait.UntilAsync($"the snapshot {destination}", async () =>
            {
                status = await admin.GetSnapshotStatusAsync(source);
                exists = (await admin.CheckTreeExistsAsync(destination)).Exists;
                return !status.InProgress && exists;
            }, StructuralPatience);
        }
        catch (TimeoutException ex)
        {
            throw new TimeoutException(
                $"{ex.Message} Last status of {source}: in progress {status?.InProgress}, phase {status?.Phase}, {status?.CopiedShardCount} of {status?.ShardCount} shards copied, destination {status?.RequestedDestinationTreeId}; {destination} exists: {exists}.",
                ex);
        }
    }
}
