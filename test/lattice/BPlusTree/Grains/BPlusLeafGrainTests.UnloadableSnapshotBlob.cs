using System.Text;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #2725: a leaf that has never checkpointed any
/// partition persisted a snapshot blob that no reader would ever accept.
/// <para>
/// The entry condition of the widened capture branch and the refusal condition
/// of the load gate are the SAME condition, which is what makes this provable
/// rather than merely likely. Control reaches the branch only when
/// <c>IsPartitionProvenCheckpointed</c> is false for every partition, i.e.
/// <c>GetCurrentCheckpointForPartition(p) &lt; 0</c> for all <c>p</c>.
/// <c>BuildCheckpointCoverage</c> builds its array from exactly those accessor
/// reads, so every slot it can produce on that branch is negative and the scalar
/// normalises to <see langword="null"/>.
/// <c>LeafSnapshotStorageGrain.HasCapturedPrefix</c> returns false for precisely
/// that shape, so <c>LoadAsync</c> reports the blob absent on every subsequent
/// read.
/// </para>
/// <para>
/// The Half A fix for issue #2692 nonetheless wrote it, under a comment claiming
/// "the durability of this leaf's rows is earned by writing the blob". It is
/// not. The write cost storage for a row that <c>LoadAsync</c> discards, that
/// <c>GetSnapshotByteSizeAsync</c> reports as zero bytes (it gates on
/// <c>HasUsableSnapshot</c>, so real consumption is understated), and that
/// <c>ClearAsync</c> refuses to reclaim (it gates on <c>HasCapturedPrefix</c>
/// and short-circuits), leaving it in the provider permanently.
/// </para>
/// <para>
/// This file exists because the Half A fixtures could not have caught that.
/// They assert <c>SaveAsync</c> against an NSubstitute stub, and a stub has no
/// load gate - it records the call and answers <c>LoadAsync</c> with whatever it
/// was told to. The proof has to round-trip through the REAL
/// <see cref="LeafSnapshotStorageGrain"/>, which is what these tests do.
/// </para>
/// <para>
/// Declining loses no durability, and that is the load-bearing half of the
/// argument. The blob was already unreadable, so removing it cannot lower what
/// is recoverable: the rows remain in the WAL, the leaf's block pin is retained
/// (<c>ResolveDurablePinForPartition</c> computes
/// <c>min(checkpoint, covered) &lt; 0</c>), nothing authorises a trim, and WAL
/// replay rehydrates the leaf in full. Widening the load gate instead would have
/// been strictly worse: a blob covering nothing lets a rehydrating leaf skip no
/// WAL at all, so it would buy a cold-path blob read - the read that exhausts
/// the heap on a large leaf in issue #2364 - for zero saved replay, while
/// weakening the fail-closed gate the no-loss invariant of issue #1535 rests on.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    private const string UnloadableBlobTreeId = "tree-unloadable-blob";

    [Test]
    public async Task Never_checkpointed_leaf_persists_no_blob_that_the_load_gate_would_refuse()
    {
        // THE ISSUE #2725 REGRESSION, stated as an invariant rather than as a
        // count: whatever this capture persists must be readable back. Pre-fix
        // it persists one row and LoadAsync answers null, so the invariant is
        // violated in the "wrote something unreadable" direction; post-fix it
        // persists nothing, which satisfies the invariant honestly. Writing it
        // this way means the test cannot be satisfied by a future change that
        // merely writes a DIFFERENT unreadable blob.
        const int partitions = 8;
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        var snapshotState = new FakePersistentState<LeafSnapshotBlob>();
        var realStore = new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), snapshotState);

        var (leaf, leafState) = CreateResidualLeafWithSnapshotStore(partitions, realStore);
        leafState.State.TreeId = UnloadableBlobTreeId;

        // The never-checkpointed shape: no partition has absorbed a WAL entry,
        // yet the cache holds a foreground-written row. This is the population
        // Half A widened capture for.
        leafState.State.ProjectionCheckpointOffset = -1L;
        var projection = AsProjection(leaf);
        projection.Apply(BuildSet(dataKey, Encoding.UTF8.GetBytes("v"), hlcPhysical: 500, treeId: UnloadableBlobTreeId));

        for (var p = 0; p < partitions; p++)
        {
            Assert.That(leaf.GetCurrentCheckpointForPartition(p), Is.EqualTo(-1L),
                $"precondition: partition {p} has never checkpointed, so any coverage this capture could "
                + "derive is the -1 sentinel");
        }
        Assert.That(leaf.EntriesForTest, Is.Not.Empty,
            "precondition: the leaf holds live rows, so it is the widened branch that decides its fate "
            + "and not the empty-leaf decline that precedes it");
        Assert.That(snapshotState.WriteCount, Is.Zero, "precondition: nothing has been persisted yet");

        await leaf.CaptureSnapshotAsync();

        var loaded = await realStore.LoadAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(snapshotState.WriteCount, Is.Zero,
                "a capture that can make no coverage claim must persist NOTHING. Pre-fix this was 1: a "
                + "full row payload written through the storage provider that LoadAsync then discards on "
                + "every read, GetSnapshotByteSizeAsync reports as zero bytes, and ClearAsync refuses to "
                + "reclaim. The rows are not stranded by declining - they are in the WAL, the block pin "
                + "is retained, and WAL replay recovers them");
            Assert.That(loaded, Is.Null,
                "the load gate's verdict is unchanged and must stay unchanged: an all-sentinel coverage "
                + "claim authorises skipping no WAL, so accepting it would buy a cold-path blob read for "
                + "zero saved replay while weakening the gate the no-loss invariant of issue #1535 needs");
        });
    }

    [Test]
    public async Task Never_checkpointed_leaf_with_a_persisted_blob_does_not_rehydrate_from_it()
    {
        // The other half of "declining loses no durability". Even if such a blob
        // HAS been persisted - by a pre-fix build, since an upgraded deployment
        // will be carrying exactly these rows - a cold leaf does not and cannot
        // recover from it. So the blob was never a recovery path that declining
        // takes away; WAL replay was, and still is.
        //
        // This also guards the direction of any future "fix" that reaches for
        // the load gate: were HasCapturedPrefix widened to accept an
        // all-sentinel claim, this assertion inverts and says so.
        const int partitions = 8;
        var (dataKey, dataPartition) = FirstKeyInNonZeroPartition(partitions);

        var snapshotState = new FakePersistentState<LeafSnapshotBlob>();
        var realStore = new LeafSnapshotStorageGrain(Substitute.For<IGrainContext>(), snapshotState);

        // Persist the exact blob a pre-fix capture would have written: real
        // rows, and a coverage claim of -1 on every partition.
        var sentinelCoverage = new long[partitions];
        Array.Fill(sentinelCoverage, -1L);
        await realStore.SaveAsync(
            new LeafSnapshotBlob
            {
                SnapshotOffset = null,
                SnapshotOffsetsByPartition = sentinelCoverage,
                Rows = new[]
                {
                    new LeafSnapshotRow(
                        dataKey,
                        LwwValue<byte[]>.Create(
                            Encoding.UTF8.GetBytes("v"),
                            new HybridLogicalClock { WallClockTicks = 500L })),
                },
            },
            default);

        var (cold, coldState) = CreateResidualLeafWithSnapshotStore(partitions, realStore);
        coldState.State.TreeId = UnloadableBlobTreeId;
        coldState.State.ProjectionCheckpointOffset = -1L;

        var accepted = await cold.TryRehydrateFromSnapshotAsync(default);

        Assert.Multiple(() =>
        {
            Assert.That(accepted, Is.False,
                "a blob claiming coverage on no partition is refused by the load gate, so it can never "
                + "rehydrate a leaf. That is why writing it earned no durability, and why declining to "
                + "write it removes no recovery path");
            Assert.That(cold.EntriesForTest, Is.Empty,
                "and the cold leaf's cache stays empty, which is correct: its rows come back by replaying "
                + "the WAL prefix its retained block pin has kept, not from this blob");
            Assert.That(cold.DurableSnapshotCoverageForPartition(dataPartition), Is.EqualTo(-1L),
                "no coverage is adopted either, so the durable pin stays min(checkpoint, covered) = -1 "
                + "and nothing authorises trimming the prefix the rows actually live in");
        });
    }


    [Test]
    public async Task Replay_banking_by_a_leaf_with_no_persisted_checkpoint_still_writes_a_loadable_blob()
    {
        // THE NEGATIVE CONTROL for the issue #2725 decline: a leaf that ENTERS
        // activation with no persisted checkpoint on any partition, and must
        // still end it with a banked, loadable blob.
        //
        // It is worth stating exactly what this does and does not demonstrate,
        // because the first draft of this test asserted the wrong thing. The
        // decline does not intercept this leaf, and the reason is not the
        // coverage override: it is that the replay ADVANCES the checkpoint
        // before anything captures. TryFlushRecoveredCeilingAsync calls
        // SetCheckpointOffsetAsync(30) because 30 exceeds the -1 the leaf
        // started at, so by capture time partition 0 reads as checkpointed,
        // CaptureSnapshotCoreAsync never enters the never-checkpointed branch,
        // and the blob is stamped from real coverage. Perturbing the decline
        // leaves this test green for that reason, and that is the point of a
        // negative control - it pins that the decline does not over-reach into
        // a leaf whose rows DID earn a coverage claim during the activation.
        //
        // Note also that this leaf is not on the cold-replay banking arm at
        // all. That arm requires persistedCheckpoint > 0 to set
        // _cacheRebuiltFromWalStartThisActivation, so a leaf starting at the
        // sentinel banks through the warm pending-advance counterpart. The
        // cold arm and its coverage override are covered by the two
        // neighbouring fixtures, which seed 100/200.
        var noPersistedCheckpoint = new[] { -1L, -1L };

        var loaded = await RunCancelledColdReplayAndLoadBankedBlobAsync(noPersistedCheckpoint);

        Assert.That(loaded, Is.Not.Null,
            "a leaf that started with no checkpoint but advanced one during replay has earned a real "
            + "coverage claim, so its banked blob MUST still be written. The issue #2725 decline covers "
            + "leaves that can claim NOTHING, and must not reach this one");
        Assert.That(loaded!.SnapshotOffsetsByPartition, Is.Not.Null);
        Assert.That(loaded.SnapshotOffsetsByPartition![0], Is.EqualTo(ColdBankReReachedOffset),
            "and it banks the frontier the replay actually reached, unchanged by the issue #2725 decline");
    }
}