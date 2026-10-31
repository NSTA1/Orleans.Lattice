using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A10, backups in depth (#5378). Consumes 60 historical telemetry backups
/// (<see cref="ContosoHarness.SeedHistoricalBackupsAsync"/>). Produces the prefix capture
/// <c>orders-prefix-0001</c>, the one-key capture <c>orders-key-42</c>, <c>lines-full</c> and its
/// incremental <c>lines-inc-1</c> (taken after <see cref="StoryData.LinesEditedBeforeIncrement"/>),
/// the cross-tree consistent set <c>factory-set-1</c> of the orders and the lines;
/// <c>factory-lines</c> repaired in place from <c>lines-full</c> - a repair only fills a key no
/// write has touched since the backup, so the edits are kept and the deleted
/// <see cref="StoryData.LineDeletedBeforeRepair"/> stays deleted (its deletion is the newer write);
/// <c>factory-lines-restored</c> (point-in-time replace from <c>lines-inc-1</c>) and
/// <c>factory-lines-cold</c> (cold restore of <c>lines-full</c>); and an incremental schedule
/// on <c>factory-lines</c> at <see cref="LinesScheduleInterval"/>. Every backup id is recorded at
/// <see cref="StoryKeys.Backup"/> of its name.
/// </summary>
public sealed class A10Contract() : ChapterContract("A10")
{
    /// <summary>The incremental schedule's interval on <c>factory-lines</c>: hourly, never firing inside a run.</summary>
    public static readonly TimeSpan LinesScheduleInterval = TimeSpan.FromHours(1);

    /// <summary>The lines' backup scope.</summary>
    public static BackupScopeSelector LinesScope => BackupScopeSelector.WholeTree(Names.Trees.FactoryLines);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        await ContosoHarness.SeedHistoricalBackupsAsync(context.Estate);
        var facades = ContractFacades.Of(context);
        var backup = facades.Backup;

        await CaptureAsync(context, Names.Backups.OrdersPrefix0001, BackupScopeSelector.Prefix(Names.Trees.FactoryOrders, Names.Orders.BackupPrefix));
        await CaptureAsync(context, Names.Backups.OrdersKey42, BackupScopeSelector.Key(Names.Trees.FactoryOrders, Names.Orders.Order42));
        var linesFull = await CaptureAsync(context, Names.Backups.LinesFull, LinesScope);

        if (await ContractCalls.FindBackupAsync(backup, Names.Backups.LinesInc1, Names.Trees.FactoryLines) is not { } increment)
        {
            await ContractCalls.SetManyAsync(facades.Data, Names.Trees.FactoryLines, StoryData.LinesEditedBeforeIncrement);
            var handle = await backup.StartIncrementalBackupAsync(new LatticeBackupIncrementalCaptureRequest(Names.Backups.LinesInc1, LinesScope, linesFull));
            context.Record(StoryKeys.Backup(Names.Backups.LinesInc1), await ContractCalls.CapturedBackupIdAsync(backup, handle.OperationId, "the lines-inc-1 capture"));
        }
        else
        {
            context.Record(StoryKeys.Backup(Names.Backups.LinesInc1), increment.Id);
        }

        if (!(await ContractCalls.BackupsAsync(backup)).Any(manifest => manifest.SetName == Names.Backups.FactorySet1))
        {
            var set = await backup.StartBackupSetAsync(new LatticeBackupSetCaptureRequest(
                Names.Backups.FactorySet1,
                [BackupScopeSelector.WholeTree(Names.Trees.FactoryOrders), LinesScope],
                crossTreeConsistent: true));
            await ContractWait.SucceededAsync("the factory-set-1 capture", () => backup.GetBackupOperationStatusAsync(set.OperationId), TimeSpan.FromMinutes(5));
        }

        if (!(await ContractCalls.TreeIsLiveAsync(facades.TreeAdmin, Names.Trees.FactoryLinesRestored)))
        {
            await facades.Data.DeleteAsync(new DataDeleteRequest { TreeId = Names.Trees.FactoryLines, Key = StoryData.LineDeletedBeforeRepair });
            await RestoreAsync(context, cold: false, new LatticeRestoreRequest(linesFull, Names.Trees.FactoryLines, mode: LatticeRestoreMode.InPlace), "the lines-full repair");
            await RestoreAsync(
                context,
                cold: false,
                new LatticeRestoreRequest(context.Recall(StoryKeys.Backup(Names.Backups.LinesInc1)), Names.Trees.FactoryLinesRestored, mode: LatticeRestoreMode.ShadowCutover),
                "the lines-inc-1 point-in-time restore into factory-lines-restored");
        }

        if (!(await ContractCalls.TreeIsLiveAsync(facades.TreeAdmin, Names.Trees.FactoryLinesCold)))
        {
            await RestoreAsync(context, cold: true, new LatticeRestoreRequest(linesFull, Names.Trees.FactoryLinesCold), "the lines-full cold restore into factory-lines-cold");
        }

        await backup.ScheduleBackupAsync(LinesScope, incremental: true, LinesScheduleInterval);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var facades = ContractFacades.Of(context);
        var backup = facades.Backup;
        var offboarded = context.IsProduced("C3");

        // A just-captured backup can be missing from the listing for a moment: the catalogue
        // hides a row until its manifest is in the backup store, and a cold restore rebuilds
        // the whole catalogue from the store. Read until every row checked below is listed.
        var expected = new List<string> { Names.Backups.OrdersPrefix0001, Names.Backups.OrdersKey42, Names.Backups.LinesInc1 };
        if (!offboarded)
        {
            expected.Add(Names.Backups.LinesFull);
        }

        IReadOnlyList<BackupManifest> catalogue = [];
        try
        {
            await ContractWait.UntilAsync("the catalogue to list A10's backups", async () =>
            {
                catalogue = await ContractCalls.BackupsAsync(backup, newestFirst: false);
                return HistoricalCount(catalogue) >= Names.Backups.HistoricalCount
                    && expected.All(name => catalogue.Any(manifest => manifest.Name == name))
                    && catalogue.Count(manifest => manifest.SetName == Names.Backups.FactorySet1) >= 2;
            }, TimeSpan.FromMinutes(3));
        }
        catch (TimeoutException)
        {
            // The assertions below name what is missing.
        }

        // The Explorer's catalogue reads the newest-first index; report any row it lacks that
        // the catalogue itself holds, so a lagging or lossy index is visible in the run.
        var indexed = await ContractCalls.BackupsAsync(backup);
        var unindexed = expected.Where(name => catalogue.Any(manifest => manifest.Name == name) && !indexed.Any(manifest => manifest.Name == name)).ToList();
        if (unindexed.Count > 0)
        {
            TestContext.Progress.WriteLine($"A10: the newest-first catalogue index lacks {string.Join(", ", unindexed)} ({indexed.Count} indexed, {catalogue.Count} catalogued).");
        }

        Require(
            HistoricalCount(catalogue) >= Names.Backups.HistoricalCount,
            $"the 60 historical telemetry backups are catalogued ({HistoricalCount(catalogue)} listed)");
        RequireBackup(context, catalogue, Names.Backups.OrdersPrefix0001, BackupKind.Full, BackupScopeSelector.Prefix(Names.Trees.FactoryOrders, Names.Orders.BackupPrefix));
        RequireBackup(context, catalogue, Names.Backups.OrdersKey42, BackupKind.Full, BackupScopeSelector.Key(Names.Trees.FactoryOrders, Names.Orders.Order42));
        if (!offboarded)
        {
            // C3 deletes lines-full's manifest from storage and prunes its catalogue row.
            RequireBackup(context, catalogue, Names.Backups.LinesFull, BackupKind.Full, LinesScope);
        }

        RequireBackup(context, catalogue, Names.Backups.LinesInc1, BackupKind.Incremental, LinesScope);
        var set = catalogue.Where(manifest => manifest.SetName == Names.Backups.FactorySet1).ToList();
        RequireEqual(set.Count, 2, "factory-set-1 holds two backups, the orders and the lines");
        Require(set.Select(member => member.SetId).Distinct().Count() == 1, "factory-set-1's members share one set");

        Require(
            await ContractCalls.GetAsync(facades.Data, Names.Trees.FactoryLines, StoryData.LineDeletedBeforeRepair) is null,
            $"{StoryData.LineDeletedBeforeRepair}, deleted after the backup, stays deleted through the repair");

        foreach (var edited in StoryData.LinesEditedBeforeIncrement)
        {
            var value = await ContractCalls.GetAsync(facades.Data, Names.Trees.FactoryLines, edited.Key);
            Require(value is not null && RateOf(value) == RateOf(edited.Value), $"the edit of {edited.Key} survives the repair");
        }

        foreach (var tree in new[] { Names.Trees.FactoryLinesRestored, Names.Trees.FactoryLinesCold })
        {
            Require(await ContractCalls.TreeIsLiveAsync(facades.TreeAdmin, tree), $"{tree} exists");
            RequireEqual(await ContractCalls.CountPrefixAsync(facades.Data, tree, "line/"), Names.Lines.Count, $"{tree} holds the 12 lines");
        }

        if (!offboarded)
        {
            Require((await backup.GetScopeStatusAsync(LinesScope)) is { IncrementalScheduleRegistered: true }, "an incremental schedule is registered on factory-lines");
        }
    }

    private static async Task<string> CaptureAsync(ChapterContext context, string name, BackupScopeSelector scope)
    {
        var backup = ContractFacades.Of(context).Backup;
        var id = (await ContractCalls.FindBackupAsync(backup, name, scope.TreeId))?.Id;
        if (id is null)
        {
            var handle = await backup.StartBackupAsync(new LatticeBackupCaptureRequest(name, scope));
            id = await ContractCalls.CapturedBackupIdAsync(backup, handle.OperationId, $"the {name} capture");
        }

        context.Record(StoryKeys.Backup(name), id);
        return id;
    }

    private static async Task RestoreAsync(ChapterContext context, bool cold, LatticeRestoreRequest request, string what)
    {
        var backup = ContractFacades.Of(context).Backup;
        var handle = cold ? await backup.StartColdRestoreAsync(request) : await backup.StartRestoreAsync(request);
        await ContractWait.SucceededAsync(what, () => backup.GetBackupOperationStatusAsync(handle.OperationId), TimeSpan.FromMinutes(5));
    }

    private void RequireBackup(ChapterContext context, IReadOnlyList<BackupManifest> catalogue, string name, BackupKind kind, BackupScopeSelector scope)
    {
        var named = catalogue.Where(manifest => manifest.Name == name).Select(manifest => $"{manifest.Kind} {manifest.Scope.Kind} {manifest.Scope.TreeId} {manifest.Scope.KeyOrPrefix}").ToList();
        var id = RecordedId(context, name);
        var recorded = catalogue.Where(manifest => manifest.Id == id).Select(manifest => manifest.Name).ToList();
        Require(
            catalogue.Any(manifest => manifest.Name == name && manifest.Kind == kind && manifest.Scope == scope),
            $"{name} is a {kind} backup of {scope.Kind} {scope.TreeId} {scope.KeyOrPrefix} (listed as: {(named.Count == 0 ? "absent" : string.Join("; ", named))}; its recorded id is listed under: [{string.Join(", ", recorded)}]; {catalogue.Count} rows)");
    }

    private static string? RecordedId(ChapterContext context, string name)
    {
        try
        {
            return context.Recall(StoryKeys.Backup(name));
        }
        catch (Exception ex) when (ex is KeyNotFoundException or InvalidOperationException)
        {
            return null;
        }
    }

    private static int HistoricalCount(IReadOnlyList<BackupManifest> catalogue) =>
        catalogue.Select(manifest => manifest.Name).Where(name => name.StartsWith(ContosoHarness.HistoricalBackupPrefix, StringComparison.Ordinal)).Distinct(StringComparer.Ordinal).Count();

    private static int RateOf(byte[] value)
    {
        using var document = System.Text.Json.JsonDocument.Parse(value);
        return document.RootElement.GetProperty("rate").GetInt32();
    }
}
