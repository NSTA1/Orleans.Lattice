using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Backup.Grpc.Tests;

/// <summary>
/// Round trips of the backup-set schedule wire messages (#6067), which embed the
/// Orleans-serializable <see cref="LatticeBackupSetScheduleRequest"/> and
/// <see cref="BackupSetScheduleStatus"/> records directly.
/// </summary>
public sealed partial class BackupGrpcDtoSerializationTests
{
    [Test]
    public void BackupSetScheduleRequestMessage_round_trips_the_request()
    {
        var copy = RoundTrip(new BackupSetScheduleRequestMessage { Request = BackupSetScheduleTestData.Request() });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Request.Name, Is.EqualTo("nightly"));
            Assert.That(copy.Request.Scopes.Select(s => s.TreeId), Is.EqualTo(new[] { "orders", "users" }));
            Assert.That(copy.Request.Interval, Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(copy.Request.RetentionKeepLast, Is.EqualTo(3));
            Assert.That(copy.Request.RetentionMaxAge, Is.EqualTo(TimeSpan.FromDays(2)));
        });
    }

    [Test]
    public void BackupSetScheduleStatusResponse_round_trips_the_status()
    {
        var copy = RoundTrip(new BackupSetScheduleStatusResponse { Found = true, Status = BackupSetScheduleTestData.Status() });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Found, Is.True);
            Assert.That(copy.Status!.Name, Is.EqualTo("nightly"));
            Assert.That(copy.Status.RecentRuns.Single().Trigger, Is.EqualTo(BackupSetScheduleTrigger.Manual));
            Assert.That(copy.Status.RetainedSets.Single().Members.Select(m => m.BackupId), Is.EqualTo(new[] { "bk-orders", "bk-users" }));
        });
    }

    [Test]
    public void BackupSetScheduleStatusResponse_round_trips_when_not_found()
    {
        var copy = RoundTrip(new BackupSetScheduleStatusResponse { Found = false });

        Assert.Multiple(() =>
        {
            Assert.That(copy.Found, Is.False);
            Assert.That(copy.Status, Is.Null);
        });
    }

    [Test]
    public void BackupSetScheduleNameRequestMessage_round_trips() =>
        Assert.That(RoundTrip(new BackupSetScheduleNameRequestMessage { Name = "nightly" }).Name, Is.EqualTo("nightly"));

    [Test]
    public void BackupSetScheduleRemoveRequestMessage_round_trips()
    {
        var copy = RoundTrip(new BackupSetScheduleRemoveRequestMessage { Name = "nightly", DeleteRetainedSets = true });

        Assert.That((copy.Name, copy.DeleteRetainedSets), Is.EqualTo(("nightly", true)));
    }

    [Test]
    public void BackupSetScheduleActionResponse_round_trips() =>
        Assert.That(RoundTrip(new BackupSetScheduleActionResponse { Result = true }).Result, Is.True);

    [Test]
    public void BackupSetScheduleListRequestMessage_round_trips() =>
        Assert.That(RoundTrip(new BackupSetScheduleListRequestMessage()), Is.Not.Null);

    [Test]
    public void BackupSetScheduleListResponse_round_trips_every_schedule()
    {
        var copy = RoundTrip(new BackupSetScheduleListResponse
        {
            Schedules = new[] { BackupSetScheduleTestData.Status("a"), BackupSetScheduleTestData.Status("b") },
        });

        Assert.That(copy.Schedules.Select(s => s.Name), Is.EqualTo(new[] { "a", "b" }));
    }
}
