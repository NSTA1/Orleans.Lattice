using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4700: a purge marks each leaf it clears, and
/// recovery re-creates empty exactly the rowless leaves carrying that mark - the
/// purge's own leaves, however their clear was interrupted - and refuses any other
/// rowless leaf, whose row may have been lost.
/// </summary>
public partial class BPlusLeafGrainTests
{
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain Snapshot)
        CreateRowlessLeafWithSnapshotStore(FakeRowRecord rowRecord, LeafSnapshotBlob? snapshotBlob = null)
    {
        var snapshot = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(snapshotBlob));
        snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(LeafSnapshotSaveOutcome.Kept));
        snapshot.ClearAsync(Arg.Any<CancellationToken>()).Returns(Task.CompletedTask);
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();

        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = false };
        var grain = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid(), reporter: null, rowRecord);
        return (grain, state, snapshot);
    }

    [Test]
    public async Task Purge_clear_marks_the_row_record_before_the_row_is_cleared_and_keeps_it()
    {
        var events = new List<string>();
        var record = new FakeRowRecord(events) { Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId } };
        var (grain, state, _) = CreateRowlessLeafWithSnapshotStore(record);
        state.RecordExistsValue = true;
        state.State.TreeId = MaterialiserTreeId;
        state.State.RowRecorded = true;
        state.OnClearState = () => events.Add("row-clear");
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await grain.ClearGrainStateForPurgeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(events.IndexOf("record-purge-mark"), Is.GreaterThanOrEqualTo(0).And.LessThan(events.IndexOf("row-clear")),
                $"The purge's mark must be durable before the row is cleared. Events: [{string.Join(", ", events)}].");
            Assert.That(events, Does.Not.Contain("record-clear"), "A purge keeps the record for recovery to find.");
            Assert.That(record.Recorded?.PurgeCleared, Is.True);
        });
    }

    [Test]
    public async Task Plain_clear_deletes_the_row_record_and_leaves_no_purge_mark()
    {
        var events = new List<string>();
        var record = new FakeRowRecord(events) { Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId } };
        var (grain, state, _) = CreateRowlessLeafWithSnapshotStore(record);
        state.RecordExistsValue = true;
        state.State.TreeId = MaterialiserTreeId;
        state.State.RowRecorded = true;
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await grain.ClearGrainStateAsync();

        Assert.Multiple(() =>
        {
            Assert.That(events, Does.Not.Contain("record-purge-mark"), "A retirement, reclaim or repair is never recovered.");
            Assert.That(record.Recorded, Is.Null);
        });
    }

    [Test]
    public async Task Recovery_re_creates_a_purge_cleared_leaf_finishing_its_interrupted_clear()
    {
        // The purge's clear was interrupted after the row was gone: the snapshot
        // and the marked record survive. Recovery finishes the clear and re-creates
        // the leaf empty.
        var events = new List<string>();
        var record = new FakeRowRecord(events)
        {
            Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId, PurgeCleared = true },
        };
        var (grain, state, snapshot) = CreateRowlessLeafWithSnapshotStore(record, TrimmedPrefixSnapshot(5));
        state.OnWriteState = written => events.Add($"state-write:tree={written.TreeId}");
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await grain.RecoverBindingAsync(MaterialiserTreeId, 2);

        var firstWrite = events.FindIndex(e => e.StartsWith("state-write:", StringComparison.Ordinal));
        var reset = events.FindLastIndex(e => e.StartsWith("record:", StringComparison.Ordinal));
        Assert.Multiple(() =>
        {
            Assert.That(state.State.TreeId, Is.EqualTo(MaterialiserTreeId));
            Assert.That(state.State.ShardIndex, Is.EqualTo(2));
            Assert.That(record.Recorded?.PurgeCleared, Is.False, "the mark is reset once the leaf is re-created");
            Assert.That(firstWrite, Is.GreaterThanOrEqualTo(0));
            Assert.That(reset, Is.GreaterThan(firstWrite),
                $"The mark must outlive the first row write, or a crash between them strands the leaf. Events: [{string.Join(", ", events)}].");
        });
        await snapshot.Received().ClearAsync(Arg.Any<CancellationToken>());
        Assert.That(await grain.GetAsync("k0"), Is.Null, "the re-created leaf serves, empty");
    }

    [TestCase(false, TestName = "Recovery_refuses_a_rowless_leaf_whose_record_no_purge_marked")]
    [TestCase(true, TestName = "Recovery_refuses_a_rowless_leaf_with_no_record")]
    public async Task Recovery_refuses_a_rowless_leaf_no_purge_cleared(bool recordLost)
    {
        // The leaf the purge never reached, whose row was lost: re-creating it empty
        // would report its keys absent.
        var events = new List<string>();
        var record = new FakeRowRecord(events)
        {
            Recorded = recordLost ? null : new LeafRowRecordState { TreeId = MaterialiserTreeId },
        };
        var (grain, state, _) = CreateRowlessLeafWithSnapshotStore(record);
        state.OnWriteState = _ => events.Add("state-write");
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        var fault = Assert.ThrowsAsync<LeafStateRowLostException>(
            async () => await grain.RecoverBindingAsync(MaterialiserTreeId, 2));

        Assert.Multiple(() =>
        {
            Assert.That(fault!.Message, Does.Contain("#4654"));
            Assert.That(events, Does.Not.Contain("state-write"), "no row may be written for a leaf the purge did not clear");
        });
        Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.GetAsync("k0"));
    }

    [Test]
    public async Task Recovery_fails_on_a_record_it_cannot_read_rather_than_guess()
    {
        var record = new FakeRowRecord { ThrowOnGet = new TimeoutException("record store unreachable") };
        var (grain, _, _) = CreateRowlessLeafWithSnapshotStore(record);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.ThrowsAsync<TimeoutException>(async () => await grain.RecoverBindingAsync(MaterialiserTreeId, 2),
            "a fault reading the record propagates, so the recovery is retried");
    }

    [Test]
    public async Task Recovery_of_a_leaf_that_kept_its_row_resets_a_stale_purge_mark()
    {
        // A purge marked the leaf and was interrupted before clearing its row.
        // Recovery binds it as usual and resets the mark, so a later purge that
        // never reaches it cannot find a stale mark after its row is lost.
        var record = new FakeRowRecord
        {
            Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId, PurgeCleared = true },
        };
        var (grain, state, snapshot) = CreateRowlessLeafWithSnapshotStore(record);
        state.RecordExistsValue = true;
        state.State.TreeId = MaterialiserTreeId;
        state.State.ShardIndex = 2;
        state.State.RowRecorded = true;
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await grain.RecoverBindingAsync(MaterialiserTreeId, 2);

        Assert.That(record.Recorded?.PurgeCleared, Is.False);
        await snapshot.DidNotReceive().ClearAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task A_purge_cleared_leaf_without_a_recovery_or_create_path_fails_closed()
    {
        var record = new FakeRowRecord
        {
            Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId, PurgeCleared = true },
        };
        var (grain, _, _) = CreateRowlessLeafWithSnapshotStore(record);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.GetAsync("k0"),
            "a marked leaf is re-created only by recovery or a create path, never by a data operation");
    }

    [Test]
    public async Task Create_intent_re_creates_a_purge_cleared_leaf()
    {
        // A shard re-seeding the deterministic root leaf of a purged copy.
        var record = new FakeRowRecord
        {
            Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId, PurgeCleared = true },
        };
        var (grain, state, _) = CreateRowlessLeafWithSnapshotStore(record);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            await grain.SetTreeIdAsync(MaterialiserTreeId);
        }

        Assert.Multiple(() =>
        {
            Assert.That(state.State.TreeId, Is.EqualTo(MaterialiserTreeId));
            Assert.That(record.Recorded?.PurgeCleared, Is.False);
        });
    }
}
