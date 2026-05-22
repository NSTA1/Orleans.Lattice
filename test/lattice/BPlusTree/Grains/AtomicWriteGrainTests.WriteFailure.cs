using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Class B "persisted / in-memory divergence on failing <c>WriteStateAsync</c>" regressions
/// for <see cref="Orleans.Lattice.BPlusTree.Grains.AtomicWriteGrain"/>. The saga coordinator
/// mutates many fields in place before each persist call - a transient storage failure
/// leaves the in-memory state ahead of disk. Several guards down the call path
/// short-circuit on the dirty in-memory values:
/// <list type="bullet">
///   <item><c>ExecuteAsync</c> Phase==NotStarted-only Prepare branch (line 168): a
///   PrepareAsync persist failure that mutated Phase to Prepare prevents the re-entry
///   from re-running Prepare on the same activation.</item>
///   <item><c>ExecuteAsync</c> Phase==Completed short-circuit (line 159): a CompleteSagaAsync
///   persist failure that flipped in-memory Phase to Completed reports success on retry
///   from the same activation even though the persisted state is still Execute.</item>
///   <item><c>RunSagaAsync</c> phase dispatch (lines 776/783/788/815): a compensate-pivot
///   persist failure that flipped Phase to Compensate makes the next dispatch enter the
///   compensation branch, but the persisted state still says Execute so a reactivation
///   re-runs the loop from the beginning.</item>
/// </list>
/// </summary>
public partial class AtomicWriteGrainTests
{
    [Test]
    public void Prepare_terminal_write_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Site 1 (line 393) - PrepareAsync mutates ~15 fields and persists. A failure
        // here leaves Phase=Prepare and Entries/PreValues/TouchedShards/KeyFingerprint/
        // TransactionId/AtomicBatchSize/SagaStartedAtTicks/TreeId/VectorClock dirty.
        // The ExecuteAsync L168 NotStarted-only Prepare branch then skips PrepareAsync
        // on every retry from the same activation, so the saga runs RunSagaAsync against
        // the dirty in-memory state while the disk says NotStarted.
        var (grain, state, _, _, _) = CreateGrain();

        var prevPhase = state.State.Phase;
        var prevTreeId = state.State.TreeId;
        var prevEntries = state.State.Entries;
        var prevPreValues = state.State.PreValues;
        var prevNextIndex = state.State.NextIndex;
        var prevRetries = state.State.RetriesOnCurrentStep;
        var prevFailureMessage = state.State.FailureMessage;
        var prevKeyFingerprint = state.State.KeyFingerprint;
        var prevTransactionId = state.State.TransactionId;
        var prevAtomicBatchSize = state.State.AtomicBatchSize;
        var prevTouchedShards = state.State.TouchedShards;
        var prevSagaStartedAtTicks = state.State.SagaStartedAtTicks;
        var prevVectorClock = state.State.VectorClock;
        var prevDelta = state.State.Delta;

        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(prevPhase),
                "Phase must revert to NotStarted so the ExecuteAsync NotStarted-only Prepare branch re-runs PrepareAsync on retry.");
            Assert.That(state.State.TreeId, Is.EqualTo(prevTreeId));
            Assert.That(state.State.Entries, Is.SameAs(prevEntries));
            Assert.That(state.State.PreValues, Is.SameAs(prevPreValues));
            Assert.That(state.State.NextIndex, Is.EqualTo(prevNextIndex));
            Assert.That(state.State.RetriesOnCurrentStep, Is.EqualTo(prevRetries));
            Assert.That(state.State.FailureMessage, Is.EqualTo(prevFailureMessage));
            Assert.That(state.State.KeyFingerprint, Is.EqualTo(prevKeyFingerprint));
            Assert.That(state.State.TransactionId, Is.EqualTo(prevTransactionId));
            Assert.That(state.State.AtomicBatchSize, Is.EqualTo(prevAtomicBatchSize));
            Assert.That(state.State.TouchedShards, Is.SameAs(prevTouchedShards));
            Assert.That(state.State.SagaStartedAtTicks, Is.EqualTo(prevSagaStartedAtTicks));
            Assert.That(state.State.VectorClock, Is.EqualTo(prevVectorClock));
            Assert.That(state.State.Delta, Is.EqualTo(prevDelta));
            Assert.That(state.WriteCount, Is.Zero,
                "Failed write must not be counted as a successful persist.");
        });
    }

    [Test]
    public void BroadcastTerminals_legacy_reconstruction_reverts_TouchedShards_when_WriteStateAsync_throws()
    {
        // Site 2 (line 549) - Legacy-reconstruction branch in BroadcastTerminalsAsync.
        // Triggered when persisted state has TouchedShards.Count==0 but Entries.Count>0
        // (legacy persisted state written by an earlier saga version). The grain
        // rebuilds the set from the routing map and persists it. A failure here leaves
        // TouchedShards dirty in memory; cross-grain calls within the same activation
        // would then iterate the dirty set and mis-route terminal broadcasts.
        var existing = new FakePersistentState<AtomicWriteState>();
        existing.State.Phase = AtomicWritePhase.Execute;
        existing.State.TreeId = TreeId;
        existing.State.Entries = MakeEntries(("k1", [1]));
        existing.State.PreValues = [new AtomicPreValue { Key = "k1" }];
        existing.State.NextIndex = 1;
        existing.State.TouchedShards = []; // legacy state - empty triggers reconstruction
        existing.State.TransactionId = Guid.NewGuid();
        existing.State.KeyFingerprint = ComputeFingerprint(("k1", [1]));
        existing.State.AtomicBatchSize = 1;
        existing.State.SagaStartedAtTicks = DateTimeOffset.UtcNow.UtcTicks;

        var (grain, state, _, _, _) = CreateGrain(existingState: existing);

        var prevTouchedShards = state.State.TouchedShards;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.TouchedShards, Is.SameAs(prevTouchedShards),
                "TouchedShards must revert to the empty legacy list so the next BroadcastTerminals pass re-builds it.");
            Assert.That(state.State.TouchedShards, Is.Empty);
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public void BroadcastTerminals_drift_correction_reverts_TouchedShards_when_WriteStateAsync_throws()
    {
        // Site 3 (line 590) - drift-correction pass in BroadcastTerminalsAsync.
        // TouchedShards was captured at PrepareAsync against the snapshot in effect
        // then, but the per-entry SetAsync in ExecutePhaseAsync may have routed
        // through a fresher snapshot if a reshard / resize / shard-split landed
        // mid-saga. The drift-correction pass re-resolves every entry against a
        // fresh routing snapshot and unions any new owners into TouchedShards.
        // A failure on this persist leaves the dirty union in memory; the rest of
        // the broadcast iterates the dirty set, double-fan-out to the union'd
        // shards via stale terminal append RPCs.
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        var routedShard = map.Resolve("k1");
        // Seed TouchedShards with a different-than-routed shard so the drift
        // correction observes a miss and adds the routed shard to the union.
        var staleShard = routedShard == 0 ? 1 : 0;

        var existing = new FakePersistentState<AtomicWriteState>();
        existing.State.Phase = AtomicWritePhase.Execute;
        existing.State.TreeId = TreeId;
        existing.State.Entries = MakeEntries(("k1", [1]));
        existing.State.PreValues = [new AtomicPreValue { Key = "k1" }];
        existing.State.NextIndex = 1;
        existing.State.TouchedShards = [staleShard];
        existing.State.TransactionId = Guid.NewGuid();
        existing.State.KeyFingerprint = ComputeFingerprint(("k1", [1]));
        existing.State.AtomicBatchSize = 1;
        existing.State.SagaStartedAtTicks = DateTimeOffset.UtcNow.UtcTicks;

        var (grain, state, _, _, _) = CreateGrain(existingState: existing);

        var prevTouchedShards = state.State.TouchedShards;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.TouchedShards, Is.SameAs(prevTouchedShards),
                "TouchedShards must revert to the pre-union value so the next pass re-derives the drift union.");
            Assert.That(state.State.TouchedShards, Does.Not.Contain(routedShard),
                "The drift-correction-added shard must not leak into the in-memory state on a failed union persist.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public void ExecutePhase_retry_persist_reverts_RetriesOnCurrentStep_when_WriteStateAsync_throws()
    {
        // Site 6 (line 922) - the catch-block retry persist in ExecutePhaseAsync.
        // Triggered when lattice.SetAsync throws and the RetriesOnCurrentStep budget
        // is not yet exhausted. The grain increments RetriesOnCurrentStep in memory
        // and persists. A failure here leaves the retry count dirty in memory;
        // subsequent retries on the same activation see an over-counted budget and
        // pivot to compensation prematurely.
        var existing = new FakePersistentState<AtomicWriteState>();
        existing.State.Phase = AtomicWritePhase.Execute;
        existing.State.TreeId = TreeId;
        existing.State.Entries = MakeEntries(("k1", [1]));
        existing.State.PreValues = [new AtomicPreValue { Key = "k1" }];
        existing.State.NextIndex = 0;
        existing.State.RetriesOnCurrentStep = 0;
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        existing.State.TouchedShards = [map.Resolve("k1")];
        existing.State.TransactionId = Guid.NewGuid();
        existing.State.KeyFingerprint = ComputeFingerprint(("k1", [1]));
        existing.State.AtomicBatchSize = 1;
        existing.State.SagaStartedAtTicks = DateTimeOffset.UtcNow.UtcTicks;

        var (grain, state, _, lattice, _) = CreateGrain(existingState: existing);
        // The first per-key SetAsync throws, so ExecutePhaseAsync enters the
        // catch retry path. With RetriesOnCurrentStep=0 < MaxRetriesPerStep=1
        // the catch increments and persists - this is the targeted write.
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .ThrowsAsync(new InvalidOperationException("simulated SetAsync failure"));

        var prevRetries = state.State.RetriesOnCurrentStep;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.RetriesOnCurrentStep, Is.EqualTo(prevRetries),
                "RetriesOnCurrentStep must revert so a subsequent retry observes the actual budget, not the dirtied value.");
            Assert.That(state.State.Phase, Is.EqualTo(AtomicWritePhase.Execute),
                "Phase must remain Execute - the retry path does not flip Phase.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public void ExecutePhase_compensate_pivot_reverts_in_memory_state_when_WriteStateAsync_throws()
    {
        // Site 7 (line 935) - the compensate-pivot persist in ExecutePhaseAsync.
        // Triggered when lattice.SetAsync throws and the RetriesOnCurrentStep budget
        // is exhausted. The grain flips Phase=Compensate, captures FailureMessage,
        // and resets RetriesOnCurrentStep before persisting. A failure here leaves
        // the in-memory saga in Compensate while disk still says Execute; the
        // RunSagaAsync dispatch at line 788 sees the dirty Phase==Compensate and
        // runs compensation, but on reactivation disk says Execute and the saga
        // re-runs the failing SetAsync from scratch (an unbounded retry loop).
        var existing = new FakePersistentState<AtomicWriteState>();
        existing.State.Phase = AtomicWritePhase.Execute;
        existing.State.TreeId = TreeId;
        existing.State.Entries = MakeEntries(("k1", [1]));
        existing.State.PreValues = [new AtomicPreValue { Key = "k1" }];
        existing.State.NextIndex = 0;
        existing.State.RetriesOnCurrentStep = 1; // MaxRetriesPerStep=1 - budget exhausted
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        existing.State.TouchedShards = [map.Resolve("k1")];
        existing.State.TransactionId = Guid.NewGuid();
        existing.State.KeyFingerprint = ComputeFingerprint(("k1", [1]));
        existing.State.AtomicBatchSize = 1;
        existing.State.SagaStartedAtTicks = DateTimeOffset.UtcNow.UtcTicks;

        var (grain, state, _, lattice, _) = CreateGrain(existingState: existing);
        lattice.SetAsync(Arg.Any<string>(), Arg.Any<byte[]>())
            .ThrowsAsync(new InvalidOperationException("simulated SetAsync failure"));

        var prevPhase = state.State.Phase;
        var prevFailureMessage = state.State.FailureMessage;
        var prevRetries = state.State.RetriesOnCurrentStep;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(prevPhase),
                "Phase must revert to Execute so the dispatch does not enter the Compensate branch with a dirty in-memory flag.");
            Assert.That(state.State.FailureMessage, Is.EqualTo(prevFailureMessage),
                "FailureMessage must revert - a persisted Compensate decision is the only legitimate cause of FailureMessage carrying a value.");
            Assert.That(state.State.RetriesOnCurrentStep, Is.EqualTo(prevRetries),
                "RetriesOnCurrentStep must revert to the budget-exhausted value.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    [Test]
    public void CompleteSaga_reverts_Phase_when_WriteStateAsync_throws()
    {
        // Site 8 (line 957) - the CompleteSagaAsync terminal persist. Flips
        // Phase=Completed and resets RetriesOnCurrentStep. A failure here leaves
        // in-memory Phase=Completed while disk says Execute. The ExecuteAsync L159
        // Phase==Completed short-circuit then reports success on every retry from
        // the same activation, but a reactivation finds disk at Execute and re-runs
        // the entire saga (re-applying every SetAsync, which is LWW-idempotent but
        // re-fires every observer / replication event).
        var existing = new FakePersistentState<AtomicWriteState>();
        existing.State.Phase = AtomicWritePhase.Execute;
        existing.State.TreeId = TreeId;
        existing.State.Entries = MakeEntries(("k1", [1]));
        existing.State.PreValues = [new AtomicPreValue { Key = "k1" }];
        existing.State.NextIndex = 1; // loop already past Entries.Count
        existing.State.RetriesOnCurrentStep = 3; // arbitrary non-zero - verify reset reverts
        var map = ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, LatticeConstants.DefaultShardCount);
        existing.State.TouchedShards = [map.Resolve("k1")];
        existing.State.TransactionId = Guid.NewGuid();
        existing.State.KeyFingerprint = ComputeFingerprint(("k1", [1]));
        existing.State.AtomicBatchSize = 1;
        existing.State.SagaStartedAtTicks = DateTimeOffset.UtcNow.UtcTicks;

        var (grain, state, _, _, _) = CreateGrain(existingState: existing);

        var prevPhase = state.State.Phase;
        var prevRetries = state.State.RetriesOnCurrentStep;
        state.ThrowOnWrite = new InvalidOperationException("storage transient");

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => grain.ExecuteAsync(TreeId, MakeEntries(("k1", [1]))));
        Assert.That(ex!.Message, Is.EqualTo("storage transient"));

        Assert.Multiple(() =>
        {
            Assert.That(state.State.Phase, Is.EqualTo(prevPhase),
                "Phase must revert to Execute so the ExecuteAsync Completed short-circuit does not report false success on retry.");
            Assert.That(state.State.RetriesOnCurrentStep, Is.EqualTo(prevRetries),
                "RetriesOnCurrentStep must revert so the reset is observable only after a successful CompleteSaga persist.");
            Assert.That(state.WriteCount, Is.Zero);
        });
    }

    /// <summary>
    /// Mirrors <c>AtomicWriteGrain.ComputeKeyFingerprint</c> so seeded state matches
    /// what the grain would compute on a re-entry. Keys are sorted, UTF-8 encoded
    /// with a 0x00 separator, and SHA-256 hashed.
    /// </summary>
    private static byte[] ComputeFingerprint(params (string, byte[])[] pairs)
    {
        var keys = new List<string>(pairs.Length);
        foreach (var (k, _) in pairs) keys.Add(k);
        keys.Sort(StringComparer.Ordinal);
        using var sha = System.Security.Cryptography.IncrementalHash.CreateHash(
            System.Security.Cryptography.HashAlgorithmName.SHA256);
        Span<byte> lenBuf = stackalloc byte[4];
        System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(lenBuf, keys.Count);
        sha.AppendData(lenBuf);
        foreach (var key in keys)
        {
            var bytes = System.Text.Encoding.UTF8.GetBytes(key);
            System.Buffers.Binary.BinaryPrimitives.WriteInt32LittleEndian(lenBuf, bytes.Length);
            sha.AppendData(lenBuf);
            sha.AppendData(bytes);
        }
        return sha.GetHashAndReset();
    }
}
