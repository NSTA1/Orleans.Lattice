using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Views;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// Regression coverage for a completed atomic batch flushed with a key omitted
/// while the write that justified the omission had not reached the view (issue
/// #4869). A batch omits a prepared key when a newer write supersedes it - an
/// ordinary write recorded over the staged key, or a source mismatch confirmed
/// once the drain consumed the writes the read observed. Last-writer suppression
/// (#4885) can then drop that ordinary write because the source already holds a
/// still newer version the drain has not read yet, and an overlapping newer batch
/// that holds the replacement can flush after the omitting one. Either way the
/// view showed the batch's other keys with the omitted key absent or at a value
/// older than the batch: a partial batch, which atomic visibility forbids.
/// </summary>
public partial class ViewMaintainerReplayFlushTests
{
    [Test]
    public async Task Crossed_prepare_clock_dependencies_publish_the_complete_local_closure_atomically()
    {
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        List<LatticeMutation> log =
        [
            Prepared(first, Key, 2, ticks: 20, size: 2, index: 0),
            Prepared(first, OtherKey, 2, ticks: 40, size: 2, index: 1),
            Commit(first, ticks: 41),
            Prepared(second, Key, 3, ticks: 30, size: 2, index: 0),
            Prepared(second, OtherKey, 3, ticks: 10, size: 2, index: 1),
            Commit(second, ticks: 42),
        ];
        ArmLog(h, log);

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 2 }));
            Assert.That(view.Snapshots.Where(s =>
                s.ContainsKey(Key) != s.ContainsKey(OtherKey)), Is.Empty,
                "neither scalar transaction ordering can publish this crossed dependency cycle");
        });
    }

    private static LatticeMutation OrdinaryAt(string key, byte value, long ticks) => new()
    {
        TreeId = SourceTreeId,
        Kind = MutationKind.Set,
        Key = key,
        Value = [value],
        Timestamp = Clock(ticks),
        Category = MutationCategory.User,
    };

    /// <summary>
    /// The snapshots in which the batch's <c>j=2</c> is visible while <c>k</c> is
    /// absent or older than the batch's prepare: a partial batch.
    /// </summary>
    private static List<Dictionary<string, byte[]>> PartialBatchSnapshots(FakeViewTree view, params byte[] olderThanBatch) =>
        view.Snapshots
            .Where(s => s.TryGetValue(OtherKey, out var j) && j.SequenceEqual(new byte[] { 2 })
                && (!s.TryGetValue(Key, out var k) || olderThanBatch.Contains(k[0])))
            .ToList();

    [Test]
    public async Task A_batch_whose_omitted_key_relies_on_a_dropped_ordinary_write_waits_for_the_newer_source_write()
    {
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        var t1 = Guid.NewGuid();
        List<LatticeMutation> log =
        [
            OrdinaryAt(Key, 1, ticks: 10),
            Prepared(t1, Key, 2, ticks: 20, size: 2, index: 0),
            Prepared(t1, OtherKey, 2, ticks: 20, size: 2, index: 1),
            Commit(t1, ticks: 21),
            OrdinaryAt(Key, 3, ticks: 30),
        ];
        ArmLog(h, log);

        // The source already holds k=4@40, appended at offset 5 but not yet read: the
        // drain drops k=3@30 (and k=1@10) as superseded, and the batch omits k
        // because k=3@30 was recorded over its staged prepare.
        h.SourceRows[Key] = [4];
        h.SourceVersions[Key] = Clock(40);
        h.HeadOffset.Value = log.Count + 1;

        await h.Grain.DrainAsync();

        Assert.That(PartialBatchSnapshots(view, 1, 3), Is.Empty,
            "the batch must not become visible with k absent or older than the batch before k=4@40 is applied");

        log.Add(OrdinaryAt(Key, 4, ticks: 40));
        ArmLog(h, log);
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(PartialBatchSnapshots(view, 1, 3), Is.Empty,
                "no snapshot may show the batch's j without its k or a newer value");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 4 }), "k converges on the source last-writer");
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 2 }), "the deferred batch flushes once the newer write is applied");
        });
    }

    [TestCase(20)]
    [TestCase(100)]
    public async Task An_omitting_batch_flushes_after_the_overlapping_newer_batch_that_holds_the_replacement(long otherKeyTicks)
    {
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        var t1 = Guid.NewGuid();
        var t2 = Guid.NewGuid();
        List<LatticeMutation> log =
        [
            Prepared(t1, Key, 2, ticks: 20, size: 2, index: 0),
            Prepared(t1, OtherKey, 2, ticks: otherKeyTicks, size: 2, index: 1),
            Commit(t1, ticks: 21),
            OrdinaryAt(Key, 5, ticks: 25),
            Prepared(t2, Key, 3, ticks: 30, size: 1, index: 0),
            Commit(t2, ticks: 31),
        ];

        // Source last-writer: k=3@30 (t2), j=2@20 (t1). k=5@25 is dropped as
        // superseded, and t1 omits k because k=5@25 was recorded over its prepare;
        // the replacement k=3 is t2's, flushed in the same pass.
        ArmLog(h, log);

        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(PartialBatchSnapshots(view), Is.Empty,
                "t1's j must not be visible before t2 installs the k that replaces t1's omitted key");
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 2 }));
        });
    }

    [Test]
    public async Task A_batch_omitting_a_key_waits_for_an_unconfirmed_replacement_after_all_heads_are_consumed()
    {
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        var first = Guid.NewGuid();
        var replacement = Guid.NewGuid();
        List<LatticeMutation> log =
        [
            Prepared(first, Key, 2, ticks: 20, size: 2, index: 0),
            Prepared(first, OtherKey, 2, ticks: 20, size: 2, index: 1),
            Commit(first, ticks: 21),
            Prepared(replacement, Key, 3, ticks: 30, size: 2, index: 0),
            Prepared(replacement, "m", 3, ticks: 30, size: 2, index: 1),
            Commit(replacement, ticks: 31),
        ];
        ArmLog(h, log);
        h.HeadOffset.Value = log.Count;
        h.SourceRows.Remove("m");
        h.SourceVersions.Remove("m");

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(PartialBatchSnapshots(view), Is.Empty,
                "consumed heads do not release a replacement batch whose source row is still unconfirmed");
            Assert.That(view.Snapshots, Is.Empty);
            Assert.That(h.State.State.AppliedOffsets.GetValueOrDefault(0, -1), Is.EqualTo(-1));
        });

        h.SourceRows["m"] = [3];
        h.SourceVersions["m"] = Clock(30);
        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(PartialBatchSnapshots(view), Is.Empty);
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 2 }));
            Assert.That(view.Rows["m"], Is.EqualTo(new byte[] { 3 }));
            Assert.That(h.State.State.AppliedOffsets[0], Is.EqualTo(log.Count - 1));
        });
    }

    [Test]
    public async Task A_batch_omitting_a_key_that_a_deferred_newer_batch_replaces_waits_for_it()
    {
        var view = new FakeViewTree();
        var h = Create(view, new FakePersistentState<ViewCheckpointState>());
        var t1 = Guid.NewGuid();
        var t2 = Guid.NewGuid();
        List<LatticeMutation> log =
        [
            Prepared(t1, Key, 2, ticks: 20, size: 2, index: 0),
            Prepared(t1, OtherKey, 2, ticks: 20, size: 2, index: 1),
            Commit(t1, ticks: 21),
            Prepared(t2, Key, 3, ticks: 30, size: 2, index: 0),
            Prepared(t2, "m", 3, ticks: 30, size: 2, index: 1),
            Commit(t2, ticks: 31),
        ];
        ArmLog(h, log);

        // Source: k=3@30 (t2), j=2@20 (t1), but m already overwritten by a newer
        // write appended beyond the read. t2 is deferred by confirmation (m
        // mismatches, uncovered); t1 omits k once confirmation finds the source no
        // longer holds k=2, so it must wait for t2 rather than flush j alone.
        h.SourceRows["m"] = [9];
        h.SourceVersions["m"] = Clock(50);
        h.HeadOffset.Value = log.Count + 1;

        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.That(PartialBatchSnapshots(view), Is.Empty,
            "t1's j must not be visible while the k that replaces its omitted prepare is held in deferred t2");

        log.Add(OrdinaryAt("m", 9, ticks: 50));
        ArmLog(h, log);
        h.SourceRows["m"] = [9];
        h.SourceVersions["m"] = Clock(50);
        await h.Grain.DrainAsync();
        await h.Grain.DrainAsync();

        Assert.Multiple(() =>
        {
            Assert.That(PartialBatchSnapshots(view), Is.Empty);
            Assert.That(view.Rows[Key], Is.EqualTo(new byte[] { 3 }));
            Assert.That(view.Rows[OtherKey], Is.EqualTo(new byte[] { 2 }));
            Assert.That(view.Rows["m"], Is.EqualTo(new byte[] { 9 }));
        });
    }
}