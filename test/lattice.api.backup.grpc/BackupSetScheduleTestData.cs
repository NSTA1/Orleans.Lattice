using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Shared backup-set schedule fixtures (#6067) for the gRPC binding tests: a
/// request and a fully populated status, so every round trip and mapping test
/// exercises the nested runs, retained sets and members.
/// </summary>
internal static class BackupSetScheduleTestData
{
    public static LatticeBackupSetScheduleRequest Request(string name = "nightly") =>
        LatticeBackupSetScheduleRequest.ForTrees(
            name,
            new[] { BackupScopeSelector.WholeTree("orders"), BackupScopeSelector.Prefix("users", "eu/") },
            TimeSpan.FromHours(1),
            retentionKeepLast: 3,
            retentionMaxAge: TimeSpan.FromDays(2));

    public static BackupSetScheduleStatus Status(string name = "nightly")
    {
        var request = Request(name);
        return new BackupSetScheduleStatus(
            name,
            request,
            request.Scopes,
            isScheduled: true,
            nextRunUtc: DateTimeOffset.UnixEpoch.AddHours(1),
            isRunning: false,
            registeredAtUtc: DateTimeOffset.UnixEpoch,
            lastRunUtc: DateTimeOffset.UnixEpoch.AddMinutes(5),
            lastSuccessUtc: DateTimeOffset.UnixEpoch.AddMinutes(6),
            BackupScopeRunOutcome.Success,
            lastFailureReason: null,
            consecutiveFailures: 0,
            new[]
            {
                new BackupSetScheduleRun(
                    DateTimeOffset.UnixEpoch.AddMinutes(5), DateTimeOffset.UnixEpoch.AddMinutes(6),
                    BackupSetScheduleTrigger.Manual, BackupScopeRunOutcome.Success, "set-1",
                    memberCount: 2, attempts: 1, failureReason: null),
            },
            new[]
            {
                new ScheduledBackupSet(
                    "set-1", name, DateTimeOffset.UnixEpoch, DateTimeOffset.UnixEpoch.AddMinutes(6),
                    crossTreeConsistent: true,
                    new[]
                    {
                        new ScheduledBackupSetMember(BackupScopeSelector.WholeTree("orders"), "bk-orders"),
                        new ScheduledBackupSetMember(BackupScopeSelector.Prefix("users", "eu/"), "bk-users"),
                    }),
            });
    }
}
