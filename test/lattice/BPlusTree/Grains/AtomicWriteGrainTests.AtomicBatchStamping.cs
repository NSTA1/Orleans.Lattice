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
/// Unit tests for R-095 producer-side atomic-batch stamping. The saga must
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
    public async Task ExecuteAsync_stamps_per_key_ambient_with_strictly_increasing_index_across_batch()
    {
        // Every per-key SetAsync the saga issues must observe a
        // (Size = N, Index = 0..N-1) ambient at the time it executes,
        // matching the producer-side stamping contract R-094 reserved
        // and R-095 implements.
        var (grain, _, _, lattice, _) = CreateGrain();

        var observed = new List<(int Size, int Index)?>();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(_ =>
            {
                observed.Add(LatticeAtomicBatchContext.Current);
                return Task.CompletedTask;
            });

        await grain.ExecuteAsync(TreeId, MakeEntries(("a", [1]), ("b", [2]), ("c", [3])));

        Assert.That(observed, Has.Count.EqualTo(3));
        Assert.That(observed[0], Is.EqualTo(((int Size, int Index)?)(3, 0)));
        Assert.That(observed[1], Is.EqualTo(((int Size, int Index)?)(3, 1)));
        Assert.That(observed[2], Is.EqualTo(((int Size, int Index)?)(3, 2)));
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
        // Caller-side ambient context is deliberately *unset* here —
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

        var observed = new List<(int Size, int Index)?>();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(_ =>
            {
                observed.Add(LatticeAtomicBatchContext.Current);
                return Task.CompletedTask;
            });

        // Sanity: no ambient context outside the resume path.
        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);

        await grain.ReceiveReminder("atomic-write-keepalive", new TickStatus());

        Assert.That(observed, Has.Count.EqualTo(2));
        // Resume from NextIndex=1 covers the trailing (b, c) entries:
        // indices 1 and 2 of the originally-captured Size=3 batch.
        Assert.That(observed[0], Is.EqualTo(((int Size, int Index)?)(3, 1)));
        Assert.That(observed[1], Is.EqualTo(((int Size, int Index)?)(3, 2)));
    }

    [Test]
    public async Task ExecuteAsync_does_not_overwrite_persisted_batch_size_on_replay()
    {
        // Reminder-driven replay must reuse the persisted size even if
        // a hypothetical activation-environment leak caused the
        // ambient context to disagree — capture-once is honoured.
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

    [Test]
    public async Task ExecuteApplyAsync_captures_batch_size_from_apply_entries_count()
    {
        // Apply-mode parity with the local-saga capture: PrepareAsync
        // is called with derivedEntries projected from applyEntries, so
        // derivedEntries.Count == applyEntries.Count and the persisted
        // AtomicBatchSize reflects the true sibling count of the
        // cross-cluster atomic batch. Pin the invariant directly so a
        // future refactor that changes the projection cannot silently
        // drift the size.
        var (grain, state, _, lattice, _) = CreateGrain();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>()).Returns(Task.CompletedTask);

        var applyEntries = MakeApplyEntries(
            ("a", [1], 100),
            ("b", [2], 101),
            ("c", [3], 102),
            ("d", [4], 103));

        var observed = new List<(int Size, int Index)?>();
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .Returns(_ =>
            {
                observed.Add(LatticeAtomicBatchContext.Current);
                return Task.CompletedTask;
            });

        var result = await grain.ExecuteApplyAsync(TreeId, applyEntries, "site-x");

        Assert.That(result.Outcome, Is.EqualTo(AtomicApplyOutcome.Committed));
        Assert.That(state.State.AtomicBatchSize, Is.EqualTo(4));
        Assert.That(observed, Has.Count.EqualTo(4));
        Assert.That(observed[0], Is.EqualTo(((int Size, int Index)?)(4, 0)));
        Assert.That(observed[1], Is.EqualTo(((int Size, int Index)?)(4, 1)));
        Assert.That(observed[2], Is.EqualTo(((int Size, int Index)?)(4, 2)));
        Assert.That(observed[3], Is.EqualTo(((int Size, int Index)?)(4, 3)));
    }
}