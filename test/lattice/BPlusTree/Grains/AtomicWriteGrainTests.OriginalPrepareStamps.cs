using System.Diagnostics.Metrics;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Lattice.Testing;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #4522 (PR2a): the coordinator reads each key's original prepare stamp P
/// back from the buckets that hold it and carries P on every committed terminal
/// delivery to a shard of the copy that minted it, so a leaf that holds no
/// bucket for a backstop key applies the saga's value under last-writer-wins at
/// P instead of over every later write. The read must account for every entry:
/// a fast pass that misses a key escalates to an exhaustive one, and a read that
/// faults or still misses a key fails the batch, so a saga never commits without
/// its stamps. Stamps never cross to another copy.
/// </summary>
public partial class AtomicWriteGrainTests
{
    private static readonly HybridLogicalClock StampP = new() { WallClockTicks = 5_000, Counter = 2 };

    private static void StubReadBack(IShardRootGrain shard, Dictionary<string, HybridLogicalClock?> fast, Dictionary<string, HybridLogicalClock?>? exhaustive = null)
    {
        shard.GetOriginalPrepareStampsAsync(Arg.Any<Guid>(), false).Returns(_ => Task.FromResult(new Dictionary<string, HybridLogicalClock?>(fast)));
        shard.GetOriginalPrepareStampsAsync(Arg.Any<Guid>(), true).Returns(_ => Task.FromResult(new Dictionary<string, HybridLogicalClock?>(exhaustive ?? fast)));
    }

    private static Dictionary<string, HybridLogicalClock>? CarriedStampsInContext() =>
        RequestContext.Get(LatticeEventConstants.OriginalPrepareStampsRequestContextKey) as Dictionary<string, HybridLogicalClock>;

    /// <summary>
    /// Records, per committed terminal that carries a committed-values backstop,
    /// the backstop keys and the stamps carried with them.
    /// </summary>
    private static List<(string[] Keys, Dictionary<string, HybridLogicalClock>? Stamps)> CaptureBackstopStamps(IShardRootGrain shard)
    {
        var captured = new List<(string[], Dictionary<string, HybridLogicalClock>?)>();
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), true, Arg.Is<IReadOnlyDictionary<string, byte[]>?>(v => v != null), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns(ci =>
            {
                var keys = ((IReadOnlyDictionary<string, byte[]>)ci[2]).Keys.ToArray();
                var stamps = CarriedStampsInContext();
                lock (captured) captured.Add((keys, stamps is null ? null : new Dictionary<string, HybridLogicalClock>(stamps)));
                return Task.FromResult<WalRecord?>(null);
            });
        return captured;
    }

    private sealed class SlowPathRecorder : IDisposable
    {
        private readonly MeterListener _listener;
        private readonly string _tree;
        public readonly List<string?> Reasons = [];

        public SlowPathRecorder(string tree)
        {
            _tree = tree;
            _listener = MeterListening.StartForInstrument(
                LatticeMetrics.AtomicWritePrepareStampReadBackSlowPath,
                l => l.SetMeasurementEventCallback<long>(OnLong));
        }

        private void OnLong(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            string? tree = null, reason = null;
            foreach (var tag in tags)
            {
                if (tag.Key == LatticeMetrics.TagTree) tree = tag.Value as string;
                else if (tag.Key == LatticeMetrics.TagReason) reason = tag.Value as string;
            }

            if (tree != _tree) return;
            lock (Reasons) Reasons.Add(reason);
        }

        public void Dispose() => _listener.Dispose();
    }

    [Test]
    public async Task A_commit_carries_each_backstop_key_original_prepare_stamp_to_the_copy_that_minted_it()
    {
        var (grain, state, _, _, shard) = CreateGrain();
        StubReadBack(shard, new() { ["a"] = StampP, ["b"] = null });
        var captured = CaptureBackstopStamps(shard);

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2])));

        Assert.That(captured, Is.Not.Empty, "PRECONDITION: the commit delivered a committed-values backstop");
        var withA = captured.Where(c => c.Keys.Contains("a")).ToList();
        Assert.That(withA, Is.Not.Empty, "PRECONDITION: a delivery carried 'a' in its backstop");
        Assert.That(withA.All(c => c.Stamps is { } s && s.Count == 1 && s["a"] == StampP), Is.True,
            "a delivery backstopping 'a' to the bound copy carries exactly its read-back stamp");
        Assert.That(captured.Where(c => !c.Keys.Contains("a")).All(c => c.Stamps is null), Is.True,
            "an unmarked key ('b', a CRDT delta in production) has no stamp to carry");
        Assert.That(state.State.OriginalPrepareStampsPhysicalTreeId, Is.EqualTo(TreeId));
        Assert.That(state.State.OriginalPrepareStamps!.Keys, Is.EquivalentTo(new[] { "a" }));
    }

    [Test]
    public async Task The_read_back_is_persisted_with_the_prepare_checkpoint_before_any_terminal()
    {
        var (grain, state, _, _, shard) = CreateGrain();
        StubReadBack(shard, new() { ["a"] = StampP });
        Dictionary<string, HybridLogicalClock>? persistedAtTerminal = null;
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns(_ =>
            {
                persistedAtTerminal ??= state.State.OriginalPrepareStamps;
                return Task.FromResult<WalRecord?>(null);
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(persistedAtTerminal, Is.Not.Null);
        Assert.That(persistedAtTerminal!["a"], Is.EqualTo(StampP),
            "a coordinator that reactivates after the decision still carries P: it is in the saga state");
    }

    [Test]
    public async Task A_key_the_fast_pass_misses_is_found_by_the_exhaustive_pass_and_carried()
    {
        // A shard root reactivated after the prepares no longer records the
        // leaves they reached, so its fast pass finds nothing; the exhaustive
        // pass reads every leaf, whose buckets are replayed from the log.
        var tree = $"stamp-exhaustive-{Guid.NewGuid():N}";
        using var recorder = new SlowPathRecorder(tree);
        var (grain, state, _, _, shard) = CreateGrain(treeId: tree);
        StubReadBack(shard, fast: new(), exhaustive: new() { ["a"] = StampP });
        var captured = CaptureBackstopStamps(shard);

        await grain.ExecuteAsync(tree, MakeEntries(("a", [1])));

        Assert.That(recorder.Reasons, Is.EqualTo(new[] { LatticeMetrics.PrepareStampReadBackExhaustive }));
        await shard.Received().GetOriginalPrepareStampsAsync(Arg.Any<Guid>(), true);
        Assert.That(captured, Is.Not.Empty);
        Assert.That(captured.All(c => c.Stamps is { } s && s["a"] == StampP), Is.True,
            "the stamp the exhaustive pass found is carried; no fallback to a dominating stamp");
        Assert.That(state.State.OriginalPrepareStamps!["a"], Is.EqualTo(StampP));
    }

    [Test]
    public async Task A_read_back_that_faults_fails_the_batch_and_the_saga_never_commits_without_its_stamps()
    {
        var tree = $"stamp-fault-{Guid.NewGuid():N}";
        using var recorder = new SlowPathRecorder(tree);
        var (grain, _, _, _, shard) = CreateGrain(treeId: tree);
        shard.GetOriginalPrepareStampsAsync(Arg.Any<Guid>(), Arg.Any<bool>())
            .ThrowsAsync(new TimeoutException("read-back timed out"));
        var captured = CaptureBackstopStamps(shard);

        Assert.CatchAsync(() => grain.ExecuteAsync(tree, MakeEntries(("a", [1]))));

        Assert.That(recorder.Reasons, Is.Not.Empty);
        Assert.That(recorder.Reasons, Has.All.EqualTo(LatticeMetrics.PrepareStampReadBackFailed));
        Assert.That(captured, Is.Empty, "no committed terminal was broadcast: the saga did not commit");
        await shard.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(), true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>());
    }

    [Test]
    public async Task A_key_no_pass_finds_fails_the_batch_and_the_saga_never_commits_without_its_stamps()
    {
        var tree = $"stamp-incomplete-{Guid.NewGuid():N}";
        using var recorder = new SlowPathRecorder(tree);
        var (grain, _, _, _, shard) = CreateGrain(treeId: tree);
        StubReadBack(shard, new() { ["a"] = StampP });

        Assert.CatchAsync(() => grain.ExecuteAsync(tree, MakeEntries(("a", [1]), ("b", [2]))));

        Assert.That(recorder.Reasons, Does.Contain(LatticeMetrics.PrepareStampReadBackExhaustive));
        Assert.That(recorder.Reasons, Does.Contain(LatticeMetrics.PrepareStampReadBackIncomplete));
        await shard.DidNotReceive().AppendTxTerminalAsync(
            Arg.Any<Guid>(), true, Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>());
    }

    [Test]
    public async Task A_complete_read_back_with_no_marked_prepare_records_the_copy_and_stays_on_the_fast_path()
    {
        var tree = $"stamp-unmarked-{Guid.NewGuid():N}";
        using var recorder = new SlowPathRecorder(tree);
        var (grain, state, _, _, shard) = CreateGrain(treeId: tree);

        await grain.ExecuteAsync(tree, MakeEntries(("a", [1])));

        Assert.That(recorder.Reasons, Is.Empty);
        Assert.That(state.State.OriginalPrepareStampsPhysicalTreeId, Is.EqualTo(tree),
            "the read-back completed, so a later broadcast does not redo it");
        Assert.That(state.State.OriginalPrepareStamps, Is.Null);
        await shard.DidNotReceive().GetOriginalPrepareStampsAsync(Arg.Any<Guid>(), true);
    }

    [Test]
    public async Task A_checkpoint_that_fails_after_the_read_back_restores_the_prior_stamps()
    {
        // Crash-before-checkpoint shape: the in-memory read-back is reverted
        // with the rest of the checkpoint, so a retry re-reads it rather than
        // committing on stamps that were never persisted.
        var state = new FakePersistentState<AtomicWriteState>();
        var (grain, _, _, _, shard) = CreateGrain(state);
        StubReadBack(shard, new() { ["a"] = StampP });
        var stampsAtFailedWrite = new List<string?>();
        state.OnWriteState = s =>
        {
            if (s.NextIndex > 0 && stampsAtFailedWrite.Count == 0)
            {
                stampsAtFailedWrite.Add(s.OriginalPrepareStampsPhysicalTreeId);
                throw new InvalidOperationException("checkpoint write failed");
            }
        };

        Assert.CatchAsync(() => grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]))));

        Assert.That(stampsAtFailedWrite, Is.EqualTo(new[] { TreeId }), "PRECONDITION: the checkpoint carried the read-back");
        Assert.That(state.State.OriginalPrepareStampsPhysicalTreeId, Is.Null,
            "the failed checkpoint reverts the read-back in memory");
        Assert.That(state.State.OriginalPrepareStamps, Is.Null);
    }

    [Test]
    public void The_lineage_guard_carries_stamps_only_to_the_copy_that_minted_them()
    {
        var (grain, state, _, _, _) = CreateGrain();
        state.State.OriginalPrepareStamps = new() { ["a"] = StampP, ["c"] = StampP };
        state.State.OriginalPrepareStampsPhysicalTreeId = TreeId;
        var values = new Dictionary<string, byte[]> { ["a"] = [1], ["b"] = [2] };

        var same = grain.CarriedOriginalStamps(TreeId, committed: true, values);
        Assert.That(same, Is.Not.Null);
        Assert.That(same!.Keys, Is.EquivalentTo(new[] { "a" }), "only the delivery's own backstop keys");

        Assert.That(grain.CarriedOriginalStamps(ResizedCopyId, committed: true, values), Is.Null,
            "a delivery to another copy (re-resolve, #4475 redirect) carries none");
        Assert.That(grain.CarriedOriginalStamps(TreeId, committed: false, values), Is.Null, "an abort carries none");
        Assert.That(grain.CarriedOriginalStamps(TreeId, committed: true, null), Is.Null, "no backstop, nothing to carry");
    }

    [Test]
    public async Task A_terminal_redelivered_to_the_resized_copy_after_a_purge_carries_no_stamps()
    {
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var purged = false;
        var resizedShard = Substitute.For<IShardRootGrain>();
        resizedShard.GetSplitForwardTargetsAsync().Returns(Task.FromResult(new List<int>()));
        var redelivered = new List<Dictionary<string, HybridLogicalClock>?>();
        resizedShard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns(_ =>
            {
                lock (redelivered) redelivered.Add(CarriedStampsInContext());
                return Task.FromResult<WalRecord?>(null);
            });
        var (grain, _, _, lattice, shard) = CreateGrain(configureFactory: f =>
        {
            f.GetGrain<ITreeDeletionGrain>(TreeId).IsDiscardedAsync().Returns(false);
            f.GetGrain<ITreeDeletionGrain>(TreeId).HoldsCompletedPurgeAsync().Returns(_ => Task.FromResult(purged));
            f.GetGrain<IShardRootGrain>(Arg.Is<string>(k => k.StartsWith(ResizedCopyId + "/", StringComparison.Ordinal)))
                .Returns(resizedShard);
        });
        lattice.GetRoutingAsync(Arg.Any<bool>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<RoutingInfo>(new RoutingInfo(purged ? ResizedCopyId : TreeId, map)));
        StubReadBack(shard, new() { ["a"] = StampP });
        shard.AppendTxTerminalAsync(Arg.Any<Guid>(), Arg.Any<bool>(), Arg.Any<IReadOnlyDictionary<string, byte[]>?>(), Arg.Any<CancellationToken>(), Arg.Any<bool>())
            .Returns<Task<WalRecord?>>(_ =>
            {
                purged = true;
                throw new LatticeTreePurgedException(TreeId);
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1])));

        Assert.That(redelivered, Is.Not.Empty, "PRECONDITION: the terminal was redelivered to the resized copy");
        Assert.That(redelivered, Has.All.Null, "P was minted on the purged copy and orders nothing on the resized one");
    }
}
