namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Backup-set schedule holds on the per-scope <see cref="BackupSchedulerGrain"/>
/// (issue #6067): a member of a retained scheduled set is never doomed by the
/// scope's own retention, the hold is durable, and each schedule's hold is its
/// own.
/// </summary>
public sealed partial class BackupSchedulerGrainUnitTests
{
    [Test]
    public async Task PruneAsync_keeps_a_backup_a_set_schedule_holds()
    {
        var log = new List<string>();
        var old = RetentionManifest("old", 2);
        var newest = RetentionManifest("newest", 0);
        var catalog = new RecordingCatalog(log, old, newest);
        var sink = RecordingSink(log, () => [old, newest]);
        var grain = CreateGrain(catalog: catalog, sink: sink, options: KeepLastOne);
        await grain.SetSetScheduleHoldsAsync("set|nightly", ["old"]);

        var report = await grain.PruneAsync(TestScope);

        Assert.Multiple(() =>
        {
            Assert.That(report.PrunedBackupIds, Is.Empty);
            Assert.That(catalog.Ids, Is.EquivalentTo(new[] { "old", "newest" }));
        });
    }

    [Test]
    public async Task SetSetScheduleHoldsAsync_persists_the_hold_and_an_empty_list_lifts_it()
    {
        var state = new FakePersistentState<BackupSchedulerState>();
        var grain = CreateGrain(state: state);

        await grain.SetSetScheduleHoldsAsync("set|nightly", ["b", "a", "b"]);
        var held = state.State.SetScheduleHolds["set|nightly"].ToList();
        await grain.SetSetScheduleHoldsAsync("set|nightly", []);

        Assert.Multiple(() =>
        {
            Assert.That(held, Is.EqualTo(new[] { "a", "b" }), "held ids are deduplicated and sorted");
            Assert.That(state.State.SetScheduleHolds, Does.Not.ContainKey("set|nightly"));
            Assert.That(state.WriteCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task SetSetScheduleHoldsAsync_restores_the_previous_hold_when_the_write_fails()
    {
        var state = new FakePersistentState<BackupSchedulerState>();
        var grain = CreateGrain(state: state);
        await grain.SetSetScheduleHoldsAsync("set|nightly", ["a"]);
        state.ThrowOnWrite = new InvalidOperationException("storage unavailable");

        Assert.That(async () => await grain.SetSetScheduleHoldsAsync("set|nightly", ["b"]), Throws.Exception);
        await grain.SetSetScheduleHoldsAsync("set|nightly", ["b"]);

        Assert.That(state.State.SetScheduleHolds["set|nightly"], Is.EqualTo(new[] { "b" }),
            "a failed write must not short-circuit the retry");
    }

    [Test]
    public async Task IsHeldBySetScheduleAsync_ignores_the_asking_schedules_own_hold()
    {
        var grain = CreateGrain();
        await grain.SetSetScheduleHoldsAsync("set|a", ["shared", "only-a"]);
        await grain.SetSetScheduleHoldsAsync("set|b", ["shared"]);

        var sharedHeldByOther = await grain.IsHeldBySetScheduleAsync("shared", "set|a");
        var ownOnly = await grain.IsHeldBySetScheduleAsync("only-a", "set|a");
        var heldForOther = await grain.IsHeldBySetScheduleAsync("only-a", "set|b");

        Assert.Multiple(() =>
        {
            Assert.That(sharedHeldByOther, Is.True);
            Assert.That(ownOnly, Is.False);
            Assert.That(heldForOther, Is.True);
        });
    }
}
