using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue 1973: the snapshot's <b>online</b> per-shard
/// copy is work-bounded and resumes from a persisted key cursor, while the
/// <b>offline</b> copy stays deliberately atomic.
/// <para>
/// The offline path assembles the destination shard bottom-up through
/// <c>BulkLoadRawAsync</c>, which by contract refuses a shard that already has a
/// root node and needs the complete sorted entry set in one call - so there is
/// no intermediate position for a cursor to name, and the source is quiesced for
/// the copy's whole duration anyway. These tests pin both halves of that
/// decision so neither drifts.
/// </para>
/// </summary>
public partial class TreeSnapshotGrainTests
{
    private static Dictionary<string, byte[]> NumberedLiveEntries(int count)
    {
        var result = new Dictionary<string, byte[]>(count);
        for (var i = 0; i < count; i++)
            result[$"entry-{i:D2}"] = [(byte)i];
        return result;
    }

    /// <summary>
    /// Seeds a snapshot already in its Copy phase over a single source shard
    /// whose leaf chain is <paramref name="leafCount"/> leaves long, and records
    /// every key the destination accepts.
    /// </summary>
    private static (TreeSnapshotGrain Grain,
                    FakePersistentState<TreeSnapshotState> State,
                    IGrainFactory Factory,
                    IShardRootGrain SourceShard,
                    IShardRootGrain DestShard,
                    List<string> MergedKeys) CreateCopyingSnapshot(
        int leafCount, int leavesPerPass, SnapshotMode mode)
    {
        var options = new LatticeOptions { BackgroundDrainLeavesPerPass = leavesPerPass };
        var existing = new FakePersistentState<TreeSnapshotState>
        {
            State = new TreeSnapshotState
            {
                InProgress = true,
                Phase = SnapshotPhase.Copy,
                NextShardIndex = 0,
                DestinationTreeId = DestTreeId,
                Mode = mode,
                OperationId = "op-1",
                ShardCount = 1,
            },
        };

        var (grain, state, _, grainFactory, _) = CreateGrain(options, existing);

        var leafIds = new GrainId[leafCount];
        for (var i = 0; i < leafCount; i++)
            leafIds[i] = GrainId.Create("leaf", $"snap-src-leaf-{i}");

        SetupShardForSnapshot(grainFactory, SourceTreeId, 0, NumberedLiveEntries(leafCount), leafIds);

        var mergedKeys = new List<string>();
        var destShard = Substitute.For<IShardRootGrain>();
        grainFactory.GetGrain<IShardRootGrain>($"{DestTreeId}/0").Returns(destShard);
        destShard.MergeManyAsync(Arg.Any<Dictionary<string, LwwValue<byte[]>>>())
            .Returns(ci =>
            {
                mergedKeys.AddRange(((Dictionary<string, LwwValue<byte[]>>)ci[0]).Keys);
                return Task.FromResult<SplitResult?>(null);
            });
        destShard.BulkLoadRawAsync(Arg.Any<string>(), Arg.Any<List<LwwEntry>>())
            .Returns(ci =>
            {
                foreach (var e in (List<LwwEntry>)ci[1]) mergedKeys.Add(e.Key);
                return Task.CompletedTask;
            });

        return (grain, state, grainFactory,
            grainFactory.GetGrain<IShardRootGrain>($"{SourceTreeId}/0"), destShard, mergedKeys);
    }

    [Test]
    public async Task Online_copy_visits_at_most_the_configured_leaves_per_pass()
    {
        var h = CreateCopyingSnapshot(leafCount: 5, leavesPerPass: 2, SnapshotMode.Online);

        await h.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.MergedKeys, Has.Count.EqualTo(2));
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(0),
                "the shard cursor must not advance until the shard's leaf chain is copied");
            Assert.That(h.State.State.Phase, Is.EqualTo(SnapshotPhase.Copy));
            Assert.That(h.State.State.CopyCursorKey, Is.EqualTo(SnapshotLeafResumeKey(2)),
                "a yielded pass must persist where to resume");
        });
    }

    [Test]
    public async Task Online_copy_resumes_from_the_persisted_key_cursor_and_copies_every_entry_once()
    {
        var h = CreateCopyingSnapshot(leafCount: 5, leavesPerPass: 2, SnapshotMode.Online);

        await h.Grain.ProcessNextPhaseAsync();
        await h.Grain.ProcessNextPhaseAsync();
        await h.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.MergedKeys, Is.EquivalentTo(new[]
            {
                "entry-00", "entry-01", "entry-02", "entry-03", "entry-04",
            }), "every live entry must be copied exactly once across the resumed passes");
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1));
            Assert.That(h.State.State.CopyCursorKey, Is.Null,
                "the cursor must clear when the shard advances, so a stale key cannot re-descend into the next shard");
        });
    }

    /// <summary>
    /// The offline copy has no resumable intermediate state, so it must sweep
    /// the whole source shard in one turn regardless of the per-pass budget, and
    /// must never persist a cursor.
    /// </summary>
    [Test]
    public async Task Offline_copy_is_not_bounded_by_the_per_pass_budget_and_persists_no_cursor()
    {
        var h = CreateCopyingSnapshot(leafCount: 5, leavesPerPass: 1, SnapshotMode.Offline);

        await h.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.MergedKeys, Is.EquivalentTo(new[]
            {
                "entry-00", "entry-01", "entry-02", "entry-03", "entry-04",
            }), "the bulk-load path needs the complete sorted entry set, so the walk must not yield");
            Assert.That(h.State.State.CopyCursorKey, Is.Null);
            Assert.That(h.State.State.Phase, Is.EqualTo(SnapshotPhase.Unmark),
                "an offline copy advances straight to Unmark because it always completes in one turn");
        });
    }

    [Test]
    public async Task Offline_copy_bulk_loads_a_sorted_entry_set()
    {
        var h = CreateCopyingSnapshot(leafCount: 4, leavesPerPass: 1, SnapshotMode.Offline);

        await h.Grain.ProcessNextPhaseAsync();

        await h.DestShard.Received(1).BulkLoadRawAsync(
            Arg.Any<string>(),
            Arg.Is<List<LwwEntry>>(e => IsSortedByKey(e)));
    }

    private static bool IsSortedByKey(List<LwwEntry> entries)
    {
        for (var i = 1; i < entries.Count; i++)
            if (string.CompareOrdinal(entries[i - 1].Key, entries[i].Key) > 0) return false;
        return true;
    }

    /// <summary>
    /// The definition-of-done case for this grain: the source chain is
    /// structurally changed between two online passes. A leaf splits after the
    /// pass that parked before it, and the resumed copy must still carry both
    /// halves across.
    /// </summary>
    [Test]
    public async Task Online_copy_resumed_after_the_cursor_leaf_splits_copies_both_halves()
    {
        var h = CreateCopyingSnapshot(leafCount: 3, leavesPerPass: 1, SnapshotMode.Online);

        await h.Grain.ProcessNextPhaseAsync();
        Assert.That(h.State.State.CopyCursorKey, Is.EqualTo(SnapshotLeafResumeKey(1)));

        var leaf1 = (await h.SourceShard.GetLeafIdForKeyAsync(SnapshotLeafResumeKey(1)))!.Value;
        var leaf2 = (await h.SourceShard.GetLeafIdForKeyAsync(SnapshotLeafResumeKey(2)))!.Value;

        var rightHalf = GrainId.Create("leaf", "snap-src-leaf-1-right");
        var rightHalfLeaf = Substitute.For<IBPlusLeafGrain>();
        h.Factory.GetGrain<IBPlusLeafGrain>(rightHalf).Returns(rightHalfLeaf);
        rightHalfLeaf.GetLiveRawEntriesAsync().Returns(Task.FromResult(new List<LwwEntry>
        {
            new("entry-01-right", LwwValue<byte[]>.Create([9], default)),
        }));
        rightHalfLeaf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(leaf2));
        rightHalfLeaf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = "k0001m",
            HighKeyExclusive = SnapshotLeafResumeKey(2),
        }));

        var leftHalf = h.Factory.GetGrain<IBPlusLeafGrain>(leaf1);
        leftHalf.GetNextSiblingAsync().Returns(Task.FromResult<GrainId?>(rightHalf));
        leftHalf.GetKeyRangeAsync().Returns(Task.FromResult(new LeafKeyRange
        {
            LowKeyInclusive = SnapshotLeafResumeKey(1),
            HighKeyExclusive = "k0001m",
        }));
        h.SourceShard.GetLeafIdForKeyAsync("k0001m").Returns(Task.FromResult<GrainId?>(rightHalf));

        for (var i = 0; i < 5 && h.State.State.NextShardIndex == 0; i++)
            await h.Grain.ProcessNextPhaseAsync();

        Assert.Multiple(() =>
        {
            Assert.That(h.MergedKeys, Does.Contain("entry-01-right"),
                "the half grafted in between two passes must not be skipped");
            Assert.That(h.MergedKeys, Does.Contain("entry-02"),
                "the copy must continue past the split rather than truncate");
            Assert.That(h.State.State.NextShardIndex, Is.EqualTo(1));
        });
    }
}
