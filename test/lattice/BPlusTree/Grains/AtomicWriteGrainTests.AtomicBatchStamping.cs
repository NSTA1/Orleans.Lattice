using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for producer-side atomic-batch stamping. The saga must
/// capture the batch size once on the first <c>Prepare</c>, persist it on
/// <see cref="AtomicWriteState"/>, re-stamp it onto Orleans
/// <see cref="RequestContext"/> via <see cref="LatticeAtomicBatchContext"/>
/// at the head of every <c>RunSagaAsync</c> entry, and override the index
/// inside the per-key Execute / Compensate scopes so every per-key emit
/// observes the matching <c>(Size, Index)</c> ambient at the time it
/// reaches the leaf grain mutation publish helpers.
/// </summary>
public partial class AtomicWriteGrainTests
{
    [Test]
    public async Task ExecuteAsync_captures_batch_size_once_on_first_prepare()
    {
        var (grain, state, _, lattice, _) = CreateGrain();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2]), ("c", [3])));

        Assert.That(state.State.AtomicBatchSize, Is.EqualTo(3));
    }

    [Test]
    public async Task ExecuteAsync_stamps_batched_ambient_with_size_and_base_index_and_index_map()
    {
        // D1b: the saga dispatches a single SetManyAsync per batch and
        // stamps LatticeAtomicBatchContext with (Size, BaseIndex) plus
        // a key->globalIndex map. The map preserves the wire-level
        // per-entry AtomicBatchIndex contract through LatticeGrain's
        // shard-bucketing fan-out: bucket-local position no longer
        // equals saga-global position, so the leaf's CommitSetManyAsync
        // looks each entry's key up in the map to recover its true
        // saga-global index.
        var (grain, _, _, lattice, _) = CreateGrain();

        (int Size, int Index)? observedBatch = null;
        IReadOnlyDictionary<string, int>? observedMap = null;
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ =>
            {
                observedBatch = LatticeAtomicBatchContext.Current;
                observedMap = LatticeAtomicBatchContext.CurrentIndexMap;
                return Task.CompletedTask;
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2]), ("c", [3])));

        Assert.That(observedBatch, Is.EqualTo(((int Size, int Index)?)(3, 0)));
        Assert.That(observedMap, Is.Not.Null);
        Assert.That(observedMap!["a"], Is.EqualTo(0));
        Assert.That(observedMap!["b"], Is.EqualTo(1));
        Assert.That(observedMap!["c"], Is.EqualTo(2));
    }

    [Test]
    public async Task ExecuteAsync_does_not_stamp_atomic_batch_for_empty_batch()
    {
        // Empty batch is fast-success at the top of ExecuteAsync (no
        // saga work, no Prepare). AtomicBatchSize must remain 0 (the
        // not-in-a-saga sentinel). Sanity: a follow-up single-key
        // write outside the saga sees an unset ambient.
        var (grain, state, _, lattice, _) = CreateGrain();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        await grain.ExecuteAsync(TreeId, []);

        Assert.That(state.State.AtomicBatchSize, Is.EqualTo(0));
    }

    [Test]
    public async Task ReceiveReminder_resumes_execute_re_stamps_persisted_batch_size()
    {
        // Crash-replay regression: a saga that captured a batch size
        // on its original Prepare must re-stamp the persisted size on
        // every resumed per-key write so observers continue to see the
        // identical batch-wide AtomicBatchSize after silo restart.
        // Caller-side ambient context is deliberately *unset* here -
        // the value must come from persisted AtomicWriteState alone.
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Execute;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("a", [1]), ("b", [2]), ("c", [3]));
        state.State.PreValues = new List<AtomicPreValue>
        {
            new() { Key = "a", Value = null, Existed = false },
            new() { Key = "b", Value = null, Existed = false },
            new() { Key = "c", Value = null, Existed = false },
        };
        state.State.NextIndex = 1;
        state.State.AtomicBatchSize = 3;

        var (grain, _, _, lattice, _) = CreateGrain(state);

        (int Size, int Index)? observedBatch = null;
        IReadOnlyDictionary<string, int>? observedMap = null;
        lattice.SetManyAsync(Arg.Any<List<KeyValuePair<string, byte[]>>>())
            .Returns(_ =>
            {
                observedBatch = LatticeAtomicBatchContext.Current;
                observedMap = LatticeAtomicBatchContext.CurrentIndexMap;
                return Task.CompletedTask;
            });

        // Sanity: no ambient context outside the resume path.
        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);
        Assert.That(LatticeAtomicBatchContext.CurrentIndexMap, Is.Null);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        // D1b: resume from NextIndex=1 dispatches a single SetManyAsync
        // for the trailing (b, c) entries. The ambient is (Size=3,
        // BaseIndex=1) and the key->globalIndex map carries the
        // saga-global indices (b -> 1, c -> 2) so the leaf's commit
        // path can stamp the correct per-entry AtomicBatchIndex
        // regardless of how SetManyAsync routes entries to leaves.
        Assert.That(observedBatch, Is.EqualTo(((int Size, int Index)?)(3, 1)));
        Assert.That(observedMap, Is.Not.Null);
        Assert.That(observedMap!["b"], Is.EqualTo(1));
        Assert.That(observedMap!["c"], Is.EqualTo(2));
        Assert.That(observedMap, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task ExecuteAsync_does_not_overwrite_persisted_batch_size_on_replay()
    {
        // Reminder-driven replay must reuse the persisted size even if
        // a hypothetical activation-environment leak caused the
        // ambient context to disagree - capture-once is honoured.
        var state = new FakePersistentState<AtomicWriteState>();
        state.State.Phase = AtomicWritePhase.Execute;
        state.State.TreeId = TreeId;
        state.State.Entries = MakeEntries(("a", [1]));
        state.State.PreValues = new List<AtomicPreValue>
        {
            new() { Key = "a", Value = null, Existed = false },
        };
        state.State.NextIndex = 0;
        // Persisted size is the canonical truth; PrepareAsync's
        // capture-once block (guarded on `== 0`) must not re-fire.
        // The saga is in Execute phase so PrepareAsync is not
        // re-entered directly, but we still pin the invariant that
        // the persisted slot is the single source of truth post-capture.
        state.State.AtomicBatchSize = 7;

        var (grain, _, _, lattice, _) = CreateGrain(state);
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.That(state.State.AtomicBatchSize, Is.EqualTo(7));
    }
}