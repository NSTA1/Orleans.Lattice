using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// The backup-set schedule tools (#6067), driven through their own invocation
/// delegates: each asserts on the value that reached the facade and on the
/// projected result, so a mis-bound argument cannot pass unnoticed.
/// </summary>
public sealed partial class BackupToolGroupInvocationTests
{
    [Test]
    public async Task Set_schedule_set_tool_delegate_forwards_an_explicit_schedule()
    {
        var schedule = await CallAsync<McpBackupSetSchedule>(
            "lattice_backup_set_schedule_set",
            ("name", "nightly"),
            ("intervalSeconds", 3600L),
            ("treeIds", new[] { "orders", "users" }),
            ("retentionKeepLast", 3));

        var sent = _control.LastSetScheduleRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.Membership, Is.EqualTo(BackupSetMembershipMode.Explicit));
            Assert.That(sent.Scopes.Select(s => s.TreeId), Is.EqualTo(new[] { "orders", "users" }));
            Assert.That(sent.Interval, Is.EqualTo(TimeSpan.FromHours(1)));
            Assert.That(sent.RetentionKeepLast, Is.EqualTo(3));
            Assert.That(sent.RetentionMaxAge, Is.Null);
            Assert.That(sent.CrossTreeConsistent, Is.True, "Cross-tree consistency defaults on.");
            Assert.That(schedule.Found, Is.True);
            Assert.That(schedule.Name, Is.EqualTo("nightly"));
            Assert.That(schedule.Membership, Is.EqualTo("Explicit"));
            Assert.That(schedule.TreeIds, Is.EqualTo(new[] { "orders", "users" }));
            Assert.That(schedule.IntervalSeconds, Is.EqualTo(3600));
            Assert.That(schedule.IsScheduled, Is.True);
        });
    }

    [Test]
    public async Task Set_schedule_set_tool_delegate_forwards_an_all_trees_schedule()
    {
        var schedule = await CallAsync<McpBackupSetSchedule>(
            "lattice_backup_set_schedule_set",
            ("name", "everything"),
            ("intervalSeconds", 600L),
            ("membership", "alltrees"),
            ("retentionMaxAgeSeconds", 86400L),
            ("crossTreeConsistent", false),
            ("includeNewTrees", false),
            ("excludedTreePrefixes", new[] { "tmp/" }));

        var sent = _control.LastSetScheduleRequest!;
        Assert.Multiple(() =>
        {
            Assert.That(sent.Membership, Is.EqualTo(BackupSetMembershipMode.AllTrees));
            Assert.That(sent.Scopes, Is.Empty);
            Assert.That(sent.RetentionMaxAge, Is.EqualTo(TimeSpan.FromDays(1)));
            Assert.That(sent.CrossTreeConsistent, Is.False);
            Assert.That(sent.IncludeNewTrees, Is.False);
            Assert.That(sent.ExcludedTreePrefixes, Is.EqualTo(new[] { "tmp/" }));
            Assert.That(schedule.Membership, Is.EqualTo("AllTrees"));
            Assert.That(schedule.RetentionMaxAgeSeconds, Is.EqualTo(86400));
            Assert.That(schedule.ExcludedTreePrefixes, Is.EqualTo(new[] { "tmp/" }));
        });
    }

    [TestCase("membership", "some-trees")]
    [TestCase("intervalSeconds", 0L)]
    [TestCase("retentionMaxAgeSeconds", -1L)]
    public void Set_schedule_set_tool_delegate_rejects_an_invalid_argument(string argument, object value)
    {
        var args = new List<(string, object?)> { ("name", "nightly"), ("intervalSeconds", 60L), ("treeIds", new[] { "orders" }), ("retentionKeepLast", 1) };
        args.RemoveAll(a => a.Item1 == argument);
        args.Add((argument, value));

        Assert.That(
            async () => await CallAsync<McpBackupSetSchedule>("lattice_backup_set_schedule_set", args.ToArray()),
            Throws.InstanceOf<ArgumentException>());
        Assert.That(_control.LastSetScheduleRequest, Is.Null, "An invalid argument must never reach the facade.");
    }

    [Test]
    public async Task Set_schedule_get_and_list_tool_delegates_project_the_status()
    {
        await CallAsync<McpBackupSetSchedule>(
            "lattice_backup_set_schedule_set",
            ("name", "b-set"), ("intervalSeconds", 60L), ("treeIds", new[] { "orders" }), ("retentionKeepLast", 1));
        await CallAsync<McpBackupSetSchedule>(
            "lattice_backup_set_schedule_set",
            ("name", "a-set"), ("intervalSeconds", 60L), ("treeIds", new[] { "users" }), ("retentionKeepLast", 1));

        var found = await CallAsync<McpBackupSetSchedule>("lattice_backup_set_schedule_get", ("name", "a-set"));
        var missing = await CallAsync<McpBackupSetSchedule>("lattice_backup_set_schedule_get", ("name", "absent"));
        var list = await CallAsync<McpBackupSetScheduleList>("lattice_backup_set_schedule_list");

        Assert.Multiple(() =>
        {
            Assert.That(found.Found, Is.True);
            Assert.That(found.MemberTreeIds, Is.EqualTo(new[] { "users" }));
            Assert.That(missing.Found, Is.False);
            Assert.That(list.Schedules.Select(s => s.Name), Is.EqualTo(new[] { "a-set", "b-set" }));
        });
    }

    [Test]
    public async Task Set_schedule_cancel_and_remove_tool_delegates_report_found_and_applied()
    {
        await CallAsync<McpBackupSetSchedule>(
            "lattice_backup_set_schedule_set",
            ("name", "nightly"), ("intervalSeconds", 60L), ("treeIds", new[] { "orders" }), ("retentionKeepLast", 1));

        var paused = await CallAsync<McpBackupSetScheduleActionResult>("lattice_backup_set_schedule_cancel", ("name", "nightly"));
        var status = await CallAsync<McpBackupSetSchedule>("lattice_backup_set_schedule_get", ("name", "nightly"));
        var removed = await CallAsync<McpBackupSetScheduleActionResult>(
            "lattice_backup_set_schedule_remove", ("name", "nightly"), ("deleteRetainedSets", true));
        var removedAgain = await CallAsync<McpBackupSetScheduleActionResult>("lattice_backup_set_schedule_remove", ("name", "nightly"));

        Assert.Multiple(() =>
        {
            Assert.That(paused, Is.EqualTo(new McpBackupSetScheduleActionResult { Name = "nightly", Found = true, Applied = true }));
            Assert.That(status.IsScheduled, Is.False, "Cancel pauses the schedule.");
            Assert.That(removed.Applied, Is.True);
            Assert.That(removedAgain.Found, Is.False);
            Assert.That(_control.LastRemoveDeleteRetainedSets, Is.False, "deleteRetainedSets defaults to false.");
        });
    }

    [Test]
    public async Task Set_schedule_remove_tool_delegate_forwards_delete_retained_sets()
    {
        await CallAsync<McpBackupSetScheduleActionResult>(
            "lattice_backup_set_schedule_remove", ("name", "nightly"), ("deleteRetainedSets", true));

        Assert.That(_control.LastRemoveDeleteRetainedSets, Is.True);
    }

    [Test]
    public async Task Set_schedule_run_now_tool_delegate_reports_started_in_flight_and_unknown()
    {
        await CallAsync<McpBackupSetSchedule>(
            "lattice_backup_set_schedule_set",
            ("name", "nightly"), ("intervalSeconds", 60L), ("treeIds", new[] { "orders" }), ("retentionKeepLast", 1));

        var started = await CallAsync<McpBackupSetScheduleActionResult>("lattice_backup_set_schedule_run_now", ("name", "nightly"));
        _control.RunNowResult = false;
        var inFlight = await CallAsync<McpBackupSetScheduleActionResult>("lattice_backup_set_schedule_run_now", ("name", "nightly"));
        var unknown = await CallAsync<McpBackupSetScheduleActionResult>("lattice_backup_set_schedule_run_now", ("name", "absent"));

        Assert.Multiple(() =>
        {
            Assert.That((started.Found, started.Applied), Is.EqualTo((true, true)));
            Assert.That((inFlight.Found, inFlight.Applied), Is.EqualTo((true, false)));
            Assert.That((unknown.Found, unknown.Applied), Is.EqualTo((false, false)),
                "An unknown schedule is reported as found=false, not raised.");
        });
    }

    [Test]
    public void Set_schedule_tool_delegates_surface_the_facades_fail_closed_denial()
    {
        _control.Authorized = false;

        Assert.That(
            async () => await CallAsync<McpBackupSetScheduleList>("lattice_backup_set_schedule_list"),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>());
    }
}
