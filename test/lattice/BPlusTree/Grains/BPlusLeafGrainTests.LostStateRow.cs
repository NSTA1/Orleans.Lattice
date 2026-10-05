using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4654: a leaf whose own state row has vanished
/// must not come up empty.
/// <para>
/// The row is the leaf's only link to its tree, key range, checkpoint and
/// kept-snapshot record, so a rowless activation has nothing to replay from. It
/// used to serve an empty cache, reporting every key the leaf held as absent. The
/// leaf now keeps a row record outside its row - written before its first state
/// write, deleted only after a deliberate clear - and a rowless, unbound
/// activation that finds it, or a snapshot that outlived the row, fails closed.
/// </para>
/// </summary>
public partial class BPlusLeafGrainTests
{
    /// <summary>In-memory row-record sidecar with fault switches and a shared event log.</summary>
    private sealed class FakeRowRecord(List<string>? events = null) : ILeafRowRecordGrain
    {
        public LeafRowRecordState? Recorded { get; set; }

        public Exception? ThrowOnGet { get; set; }

        public Exception? ThrowOnRecord { get; set; }

        public int Records { get; private set; }

        public Task<LeafRowRecordState?> GetAsync()
        {
            events?.Add("record-get");
            return ThrowOnGet is { } fault ? Task.FromException<LeafRowRecordState?>(fault) : Task.FromResult(Recorded);
        }

        public Task RecordAsync(string? treeId)
        {
            if (ThrowOnRecord is { } fault)
            {
                ThrowOnRecord = null;
                events?.Add("record-failed");
                return Task.FromException(fault);
            }

            Records++;
            events?.Add($"record:{treeId}");
            Recorded = new LeafRowRecordState { TreeId = treeId ?? Recorded?.TreeId };
            return Task.CompletedTask;
        }

        public Task ClearAsync()
        {
            events?.Add("record-clear");
            Recorded = null;
            return Task.CompletedTask;
        }
    }

    /// <summary>A leaf activation over a missing state row: no row, no tree id, nothing persisted.</summary>
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State, ILeafSnapshotStorageGrain Snapshot) CreateRowlessLeaf(
        FakeRowRecord rowRecord, LeafSnapshotBlob? snapshotBlob = null, ILeafCursorReporter? reporter = null)
    {
        var snapshot = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(snapshotBlob));
        snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(LeafSnapshotSaveOutcome.Kept));
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();

        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = false };
        var grain = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid(), reporter, rowRecord);
        return (grain, state, snapshot);
    }

    [Test]
    public async Task Rowless_activation_whose_row_record_survives_fails_the_replay_closed()
    {
        var record = new FakeRowRecord { Recorded = new LeafRowRecordState { TreeId = "tree-row-lost" } };
        var (grain, _, _) = CreateRowlessLeaf(record);

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.InstanceOf<LeafStateRowLostException>(),
                "The record proves the row was written; an empty activation would report every key it held absent.");
            Assert.That(fault, Is.InstanceOf<ILatticeLeafUnavailable>());
            Assert.That(((LeafStateRowLostException)fault!).TreeId, Is.EqualTo("tree-row-lost"));
            Assert.That(fault!.Message, Does.Contain("#4654").And.Contain("row record"));
        });
        Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.GetAsync("k0"),
            "Reads fail closed too, rather than reporting the key absent.");
    }

    [Test]
    public async Task Rowless_activation_whose_snapshot_outlived_the_row_fails_the_replay_closed()
    {
        // A leaf written before row records existed has none; a surviving snapshot
        // still proves its row was written.
        var (grain, _, _) = CreateRowlessLeaf(new FakeRowRecord(), TrimmedPrefixSnapshot(5));

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.That(fault, Is.InstanceOf<LeafStateRowLostException>());
        Assert.That(fault!.Message, Does.Contain("snapshot"));
    }

    [Test]
    public async Task Rowless_activation_whose_row_record_cannot_be_read_fails_the_replay_closed()
    {
        var record = new FakeRowRecord { ThrowOnGet = new TimeoutException("record store unreachable") };
        var (grain, _, _) = CreateRowlessLeaf(record);

        var fault = await ActivateCapturingFaultAsync(grain);

        Assert.Multiple(() =>
        {
            Assert.That(fault, Is.InstanceOf<LeafStateRowLostException>(),
                "A leaf that cannot tell a lost row from one never written must not guess.");
            Assert.That(fault?.InnerException, Is.InstanceOf<TimeoutException>());
        });
    }

    [Test]
    public async Task Rowless_activation_with_no_record_and_no_snapshot_comes_up_as_a_new_leaf()
    {
        // Every leaf's first activation looks like this; it must not be failed closed.
        var (grain, _, _) = CreateRowlessLeaf(new FakeRowRecord());

        Assert.That(await ActivateCapturingFaultAsync(grain), Is.Null);
        Assert.That(await grain.GetAsync("k0"), Is.Null);
    }

    [Test]
    public async Task Activation_with_a_state_row_never_reads_the_row_record()
    {
        var events = new List<string>();
        var record = new FakeRowRecord(events) { ThrowOnGet = new InvalidOperationException("must not be read") };
        var (grain, state, _) = CreateRowlessLeaf(record);
        state.RecordExistsValue = true;
        state.State.TreeId = MaterialiserTreeId;

        await ActivateCapturingFaultAsync(grain);

        Assert.That(events, Does.Not.Contain("record-get"), "The check costs nothing on an ordinary activation.");
    }

    [Test]
    public async Task First_state_write_records_the_row_before_it_lands_and_only_once()
    {
        var events = new List<string>();
        var record = new FakeRowRecord(events);
        var (grain, state, _) = CreateRowlessLeaf(record);
        state.OnWriteState = written => events.Add($"state-write:recorded={written.RowRecorded}");
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await grain.SetTreeIdAsync(MaterialiserTreeId);
        await grain.SetShardIndexAsync(1);

        var firstRecord = events.FindIndex(e => e.StartsWith("record:", StringComparison.Ordinal));
        var firstWrite = events.FindIndex(e => e.StartsWith("state-write:", StringComparison.Ordinal));
        Assert.Multiple(() =>
        {
            Assert.That(firstWrite, Is.GreaterThanOrEqualTo(0), "control: the birth seam must write the row.");
            Assert.That(firstRecord, Is.GreaterThanOrEqualTo(0).And.LessThan(firstWrite),
                $"The record must be durable before the row it vouches for. Events: [{string.Join(", ", events)}].");
            Assert.That(events[firstWrite], Is.EqualTo("state-write:recorded=True"),
                "The flag rides the first row write, so later writes skip the record.");
            Assert.That(record.Records, Is.EqualTo(1), "The record is written once per leaf, not once per write.");
            Assert.That(record.Recorded?.TreeId, Is.EqualTo(MaterialiserTreeId));
        });
    }

    [Test]
    public async Task Failed_row_record_write_fails_the_state_write()
    {
        var events = new List<string>();
        var record = new FakeRowRecord(events) { ThrowOnRecord = new TimeoutException("record store unreachable") };
        var (grain, state, _) = CreateRowlessLeaf(record);
        state.OnWriteState = _ => events.Add("state-write");
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.ThrowsAsync<TimeoutException>(async () => await grain.SetTreeIdAsync(MaterialiserTreeId));

        Assert.That(events, Does.Not.Contain("state-write"),
            "A row that lands without its record could later vanish and read as never written.");
    }

    [Test]
    public async Task Row_written_before_row_records_existed_is_recorded_on_its_next_write()
    {
        var record = new FakeRowRecord();
        var (grain, state, _) = CreateRowlessLeaf(record);
        state.RecordExistsValue = true;
        state.State.TreeId = MaterialiserTreeId;
        state.State.RowRecorded = false;
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await grain.SetShardIndexAsync(3);

        Assert.Multiple(() =>
        {
            Assert.That(record.Records, Is.EqualTo(1));
            Assert.That(state.State.RowRecorded, Is.True);
        });
    }

    [Test]
    public async Task Deliberate_clear_deletes_the_row_record()
    {
        var events = new List<string>();
        var record = new FakeRowRecord(events) { Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId } };
        var (grain, state, _) = CreateRowlessLeaf(record);
        state.RecordExistsValue = true;
        state.State.TreeId = MaterialiserTreeId;
        state.State.RowRecorded = true;
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await grain.ClearGrainStateAsync();

        Assert.That(record.Recorded, Is.Null,
            "A completed clear accepts the data's removal, so the next activation must not be failed closed.");
    }

    [Test]
    public async Task Failed_row_clear_keeps_the_row_record()
    {
        var record = new FakeRowRecord { Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId } };
        var (grain, state, _) = CreateRowlessLeaf(record);
        state.RecordExistsValue = true;
        state.State.TreeId = MaterialiserTreeId;
        state.State.RowRecorded = true;
        state.ThrowOnClear = new TimeoutException("row store unreachable");
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.ThrowsAsync<TimeoutException>(async () => await grain.ClearGrainStateAsync());

        Assert.That(record.Recorded, Is.Not.Null,
            "The row survived, so its record must too; deleting it first would leave a row that could vanish unnoticed.");
    }
}
