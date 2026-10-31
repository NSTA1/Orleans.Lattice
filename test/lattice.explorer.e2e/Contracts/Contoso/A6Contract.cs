using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A6, backup posture (#5377). Produces the full backup <c>orders-baseline</c> of
/// <c>factory-orders</c> (its id at <see cref="StoryKeys.OrdersBaselineBackup"/>), monitored with
/// a 1-hour verify interval and healthy; and the "hourly" Full schedule on
/// <c>factory-telemetry</c>, registered at <see cref="TelemetryScheduleInterval"/> - the story's
/// compressed hour, the scheduler's minimum - with at least one success. C3 deletes the
/// backup and cancels the schedule.
/// </summary>
public sealed class A6Contract() : ChapterContract("A6")
{
    /// <summary>
    /// The interval the telemetry's "hourly" schedule is registered with: one minute, the
    /// scheduler's minimum, standing for an hour so it fires inside the run.
    /// </summary>
    public static readonly TimeSpan TelemetryScheduleInterval = TimeSpan.FromMinutes(1);

    /// <summary>How often the baseline's health is verified.</summary>
    public static readonly TimeSpan BaselineVerifyInterval = TimeSpan.FromHours(1);

    /// <summary>The telemetry's backup scope.</summary>
    public static BackupScopeSelector TelemetryScope => BackupScopeSelector.WholeTree(Names.Trees.FactoryTelemetry);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var backup = ContractFacades.Of(context).Backup;
        var baseline = (await ContractCalls.FindBackupAsync(backup, Names.Backups.OrdersBaseline, Names.Trees.FactoryOrders))?.Id;
        if (baseline is null)
        {
            var handle = await backup.StartBackupAsync(new LatticeBackupCaptureRequest(
                Names.Backups.OrdersBaseline, BackupScopeSelector.WholeTree(Names.Trees.FactoryOrders)));
            baseline = await ContractCalls.CapturedBackupIdAsync(backup, handle.OperationId, "the orders-baseline capture");
        }

        context.Record(StoryKeys.OrdersBaselineBackup, baseline);
        await backup.ConfigureBackupHealthAsync(baseline, new BackupHealthConfig(monitoringEnabled: true, BaselineVerifyInterval));
        var check = await backup.StartBackupHealthCheckAsync(baseline);
        await ContractWait.SucceededAsync("the orders-baseline health check", () => backup.GetBackupOperationStatusAsync(check.OperationId));

        await backup.ScheduleBackupAsync(TelemetryScope, incremental: false, TelemetryScheduleInterval);
        await ContractWait.UntilAsync(
            "the telemetry schedule's first success",
            async () => (await backup.GetScopeStatusAsync(TelemetryScope))?.LastFullSuccessUtc is not null,
            TimeSpan.FromMinutes(4));
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var baseline = context.Recall(StoryKeys.OrdersBaselineBackup);
        if (context.IsProduced("C3"))
        {
            return;
        }

        var backup = ContractFacades.Of(context).Backup;
        var described = await backup.DescribeBackupAsync(baseline);
        Require(described is { Manifest.Name: Names.Backups.OrdersBaseline }, "orders-baseline is catalogued");
        RequireEqual(described!.Manifest.Kind, BackupKind.Full, "orders-baseline is a full backup");
        RequireEqual(described.Manifest.Scope, BackupScopeSelector.WholeTree(Names.Trees.FactoryOrders), "orders-baseline captures the whole of factory-orders");

        var health = await backup.GetBackupHealthAsync(baseline);
        Require(health is { IsHealthy: true }, $"orders-baseline is monitored and healthy ({health?.Status})");

        var scope = await backup.GetScopeStatusAsync(TelemetryScope);
        Require(scope is { FullScheduleRegistered: true }, "a Full schedule is registered on factory-telemetry");
        Require(scope!.LastFullSuccessUtc is not null, "the telemetry schedule has succeeded at least once");
    }
}
