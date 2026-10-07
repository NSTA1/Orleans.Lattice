using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #4654: a leaf whose own state row has vanished
/// must not come up empty, and must not be re-created empty.
/// <para>
/// The row is the leaf's only link to its tree, key range, checkpoint and
/// kept-snapshot record, so a rowless activation has nothing to replay from. It
/// used to serve an empty cache, reporting every key the leaf held as absent, and
/// a call that bound it turned it into an empty bound leaf. A rowless activation is
/// now admitted - allowed to serve data and to write a first row - only under a
/// create intent naming it, set by the paths that create leaves, and only when no
/// row record or snapshot survives from an earlier row.
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

        public Task MarkPurgeClearedAsync()
        {
            events?.Add("record-purge-mark");
            Recorded = new LeafRowRecordState { TreeId = Recorded?.TreeId, PurgeCleared = true };
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
    private static (BPlusLeafGrain Grain, FakePersistentState<LeafNodeState> State) CreateRowlessLeaf(
        FakeRowRecord rowRecord, LeafSnapshotBlob? snapshotBlob = null)
    {
        var snapshot = Substitute.For<ILeafSnapshotStorageGrain>();
        snapshot.LoadAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(snapshotBlob));
        snapshot.SaveAsync(Arg.Any<LeafSnapshotBlob>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(LeafSnapshotSaveOutcome.Kept));
        var coordinator = Substitute.For<ILeafReplayCoordinatorGrain>();

        var state = new FakePersistentState<LeafNodeState> { RecordExistsValue = false };
        var grain = ActivateTrimmedPrefixLeafOver(state, snapshot, coordinator, Guid.NewGuid(), reporter: null, rowRecord);
        return (grain, state);
    }

    private static GrainId LeafIdOf(BPlusLeafGrain grain) => ((IGrainBase)grain).GrainContext.GrainId;

    [Test]
    public async Task Rowless_activation_without_a_create_intent_fails_every_data_operation_closed()
    {
        // The double loss: the row and its record are both gone, and no snapshot
        // survives. Nothing on the leaf can tell this from a leaf never created.
        var (grain, _) = CreateRowlessLeaf(new FakeRowRecord());
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        var read = Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.GetAsync("k0"));
        Assert.Multiple(() =>
        {
            Assert.That(read, Is.InstanceOf<ILatticeLeafUnavailable>());
            Assert.That(read!.Message, Does.Contain("#4654").And.Contain("not reached through a path creating it"));
        });
        Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.SetAsync("k0", [1]),
            "A write would otherwise be acknowledged into an empty leaf whose row was lost.");
    }

    [Test]
    public async Task Rowless_activation_refuses_a_birth_seam_without_a_create_intent()
    {
        // The #1744 re-bind reaches a leaf without an intent: binding a lost leaf
        // would turn it into an empty bound one for good.
        var events = new List<string>();
        var (grain, state) = CreateRowlessLeaf(new FakeRowRecord(events));
        state.OnWriteState = _ => events.Add("state-write");
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.SetTreeIdAsync(MaterialiserTreeId));

        Assert.That(events, Does.Not.Contain("state-write"), "No row may be written for a leaf not being created.");
    }

    [Test]
    public async Task Rowless_activation_with_a_create_intent_for_another_leaf_is_refused()
    {
        var (grain, _) = CreateRowlessLeaf(new FakeRowRecord());
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        using (LatticeNewLeafIntentContext.BeginScope(GrainId.Create("bplusleaf", Guid.NewGuid().ToString("N"))))
        {
            Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.SetTreeIdAsync(MaterialiserTreeId));
        }
    }

    [Test]
    public async Task Create_intent_admits_a_new_leaf_and_records_its_row_before_the_row_lands_once()
    {
        var events = new List<string>();
        var record = new FakeRowRecord(events);
        var (grain, state) = CreateRowlessLeaf(record);
        state.OnWriteState = written => events.Add($"state-write:recorded={written.RowRecorded}");
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            await grain.SetTreeIdAsync(MaterialiserTreeId);
        }

        await grain.SetShardIndexAsync(1);

        var firstRecord = events.FindIndex(e => e.StartsWith("record:", StringComparison.Ordinal));
        var firstWrite = events.FindIndex(e => e.StartsWith("state-write:", StringComparison.Ordinal));
        Assert.Multiple(() =>
        {
            Assert.That(firstWrite, Is.GreaterThanOrEqualTo(0), "control: the birth seam must write the row.");
            Assert.That(firstRecord, Is.GreaterThanOrEqualTo(0).And.LessThan(firstWrite),
                $"The record must be durable before the row it vouches for. Events: [{string.Join(", ", events)}].");
            Assert.That(events[firstWrite], Is.EqualTo("state-write:recorded=True"));
            Assert.That(record.Records, Is.EqualTo(1), "The record is written once per leaf, not once per write.");
        });
        Assert.That(await grain.GetAsync("k0"), Is.Null, "An admitted new leaf serves data.");
    }

    [Test]
    public async Task Create_intent_for_a_leaf_whose_row_record_survives_is_refused()
    {
        // A creator about to replace a lost row with an empty one.
        var record = new FakeRowRecord { Recorded = new LeafRowRecordState { TreeId = "tree-row-lost" } };
        var (grain, _) = CreateRowlessLeaf(record);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            var fault = Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.SetTreeIdAsync(MaterialiserTreeId));
            Assert.Multiple(() =>
            {
                Assert.That(fault!.TreeId, Is.EqualTo("tree-row-lost"));
                Assert.That(fault.Message, Does.Contain("row record"));
            });
            Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.GetAsync("k0"));
        }
    }

    [Test]
    public async Task Create_intent_for_a_leaf_whose_snapshot_survives_is_refused()
    {
        var (grain, _) = CreateRowlessLeaf(new FakeRowRecord(), TrimmedPrefixSnapshot(5));
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            var fault = Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.SetTreeIdAsync(MaterialiserTreeId));
            Assert.That(fault!.Message, Does.Contain("snapshot"));
        }
    }

    [Test]
    public async Task Create_intent_whose_row_record_cannot_be_read_is_refused()
    {
        var record = new FakeRowRecord { ThrowOnGet = new TimeoutException("record store unreachable") };
        var (grain, _) = CreateRowlessLeaf(record);
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            var fault = Assert.ThrowsAsync<LeafStateRowLostException>(async () => await grain.SetTreeIdAsync(MaterialiserTreeId));
            Assert.That(fault!.InnerException, Is.InstanceOf<TimeoutException>(),
                "A creator that cannot tell a lost row from one never written must not guess.");
        }
    }

    [Test]
    public async Task Activation_with_a_state_row_needs_no_intent_and_never_reads_the_row_record()
    {
        var events = new List<string>();
        var record = new FakeRowRecord(events) { ThrowOnGet = new InvalidOperationException("must not be read") };
        var (grain, state) = CreateRowlessLeaf(record);
        state.RecordExistsValue = true;
        state.State.TreeId = MaterialiserTreeId;
        state.State.RowRecorded = true;
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        Assert.That(await grain.GetAsync("k0"), Is.Null);
        await grain.SetShardIndexAsync(2);

        Assert.That(events, Does.Not.Contain("record-get"), "The rule costs nothing on an ordinary leaf.");
    }

    [Test]
    public async Task Failed_row_record_write_fails_the_state_write()
    {
        var events = new List<string>();
        var record = new FakeRowRecord(events) { ThrowOnRecord = new TimeoutException("record store unreachable") };
        var (grain, state) = CreateRowlessLeaf(record);
        state.OnWriteState = _ => events.Add("state-write");
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        using (LatticeNewLeafIntentContext.BeginScope(LeafIdOf(grain)))
        {
            Assert.ThrowsAsync<TimeoutException>(async () => await grain.SetTreeIdAsync(MaterialiserTreeId));
        }

        Assert.That(events, Does.Not.Contain("state-write"),
            "A row that lands without its record could later vanish and read as never written.");
    }

    [Test]
    public async Task Row_written_before_row_records_existed_is_recorded_on_its_next_write()
    {
        var record = new FakeRowRecord();
        var (grain, state) = CreateRowlessLeaf(record);
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
        var record = new FakeRowRecord { Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId } };
        var (grain, state) = CreateRowlessLeaf(record);
        state.RecordExistsValue = true;
        state.State.TreeId = MaterialiserTreeId;
        state.State.RowRecorded = true;
        await LeafActivationHarness.ActivateAsync(grain, CancellationToken.None);

        await grain.ClearGrainStateAsync();

        Assert.That(record.Recorded, Is.Null);
    }

    [Test]
    public async Task Failed_row_clear_keeps_the_row_record()
    {
        var record = new FakeRowRecord { Recorded = new LeafRowRecordState { TreeId = MaterialiserTreeId } };
        var (grain, state) = CreateRowlessLeaf(record);
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
