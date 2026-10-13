using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;
using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// An unconstrained range delete tombstones only the rows live when it runs. A
/// write that lands at the source afterwards with an older HLC (a replicated or
/// skewed-clock write) is installed, because the key holds no tombstone, so the
/// source keeps it live at a version below the range delete's. The view's exact
/// lowering of the range must therefore spare a key the source still holds live,
/// whatever its version: deleting it on a replay removes a row the source never
/// removed, so a reader observes the row vanish (and, for a batch key, a partial
/// slice) until the write is re-applied.
/// </summary>
public partial class ViewMaintainerReplayFlushTests
{
    [Test]
    public async Task Range_confirmation_uses_one_row_observation_before_a_newer_tombstone_is_reclaimed()
    {
        var view = new FakeViewTree();
        view.Rows[Key] = [3];
        view.Rows[OtherKey] = [4];
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(h, [RangeDelete("a", "z", ticks: 20)]);
        h.SourceRows.Clear();
        h.SourceVersions[Key] = Clock(30);
        h.SourceVersions[OtherKey] = Clock(30);
        h.SourceRoot.GetRawEntriesAsync(Arg.Any<List<string>>())
            .Returns(call =>
            {
                // Compaction between independent liveness and version reads
                // loses the newer tombstone's version without changing the view.
                h.SourceVersions.Clear();
                return Task.FromResult(call.ArgAt<List<string>>(0).Select(_ => (LwwEntry?)null).ToList());
            });

        await h.Grain.DrainAsync();

        Assert.That(view.Snapshots.Where(s => !s.ContainsKey(Key) || !s.ContainsKey(OtherKey)), Is.Empty,
            "the range must spare both rows using the newer tombstones it observed, not a second read after reclamation");
        Assert.That(view.Rows.Keys, Is.EquivalentTo(new[] { Key, OtherKey }));
    }

    private static List<LatticeMutation> RangeThenOlderLateWrite(Guid? txId)
    {
        var log = new List<LatticeMutation> { RangeDelete("a", "z", ticks: 20) };
        if (txId is { } id)
        {
            log.Add(Prepared(id, Key, 3, ticks: 10, size: 2, index: 0));
            log.Add(Prepared(id, OtherKey, 4, ticks: 10, size: 2, index: 1));
            log.Add(Commit(id, ticks: 11));
        }
        else
        {
            log.Add(Ordinary(1, ticks: 10));
        }

        return log;
    }

    private static void SeedLateWriteSurvivedRange(Harness h, bool batch)
    {
        // The range delete ran while the key was absent, so the later-arriving older
        // write is the source's live row: the range never tombstoned it.
        h.SourceRows[Key] = [batch ? (byte)3 : (byte)1];
        h.SourceVersions[Key] = Clock(10);
        if (batch)
        {
            h.SourceRows[OtherKey] = [4];
            h.SourceVersions[OtherKey] = Clock(10);
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task A_replayed_range_delete_never_removes_a_row_the_source_still_holds_live_below_its_version(bool batch)
    {
        var view = new FakeViewTree();
        var log = RangeThenOlderLateWrite(batch ? Guid.NewGuid() : null);
        var first = Create(view, new FakePersistentState<ViewCheckpointState>
        {
            ThrowOnWrite = new InvalidOperationException("simulated crash before the checkpoint persists"),
        });
        ArmLog(first, log);
        SeedLateWriteSurvivedRange(first, batch);
        Assert.ThrowsAsync<InvalidOperationException>(() => first.Grain.DrainAsync());
        Assert.That(view.Rows.ContainsKey(Key), Is.True, "the first activation installed the late write");
        var snapshotsBeforeReplay = view.Snapshots.Count;

        var second = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(second, log);
        SeedLateWriteSurvivedRange(second, batch);
        await second.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Snapshots.Skip(snapshotsBeforeReplay).Where(s => !s.ContainsKey(Key)), Is.Empty,
                "the replay must never remove k: the source has held it live throughout, the range delete never tombstoned it");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { batch ? (byte)3 : (byte)1 }));
            if (batch)
            {
                Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 4 }));
            }
        });
    }

    [Test]
    public async Task A_single_pass_keeps_an_older_write_that_reached_the_source_after_the_range_delete()
    {
        // The pass applies writes in HLC order, so the late write (logged after the
        // range) is applied first and the range lowering then runs over it: without
        // the liveness spare the row is removed for good, not just transiently. (An
        // atomic late write is reinstated by the batch flush later in the same pass,
        // so only the replay test above covers the batch shape.)
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        ArmLog(h, RangeThenOlderLateWrite(null));
        SeedLateWriteSurvivedRange(h, batch: false);

        await h.Grain.DrainAsync();

        Assert.That(view.Rows.TryGetValue(Key, out var value) ? value : null, Is.EqualTo(new byte[] { 1 }));
    }
}
