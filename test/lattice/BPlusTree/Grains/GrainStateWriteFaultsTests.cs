using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="GrainStateWriteFaults"/>, the shared classification
/// and translation of a failed saga state write (issue #3572).
/// </summary>
[TestFixture]
[Category("Unit")]
public sealed class GrainStateWriteFaultsTests
{
    [Test]
    public void IsConflict_finds_an_inconsistent_state_exception_anywhere_in_the_chain()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainStateWriteFaults.IsConflict(new InconsistentStateException("etag")), Is.True);
            Assert.That(GrainStateWriteFaults.IsConflict(
                new InvalidOperationException("wrap", new InconsistentStateException("etag"))), Is.True);
            Assert.That(GrainStateWriteFaults.IsConflict(new TimeoutException("blip")), Is.False);
        });
    }

    [Test]
    public void IsTranslatedConflict_matches_only_a_conflict_translation()
    {
        var conflict = new LatticeStateWriteFailedException("atomic-write", "k", new InconsistentStateException("etag"), conflict: true);
        var failure = new LatticeStateWriteFailedException("atomic-write", "k", new IOException("disk"), conflict: false);

        Assert.Multiple(() =>
        {
            Assert.That(GrainStateWriteFaults.IsTranslatedConflict(conflict), Is.True);
            Assert.That(GrainStateWriteFaults.IsTranslatedConflict(new AggregateException(conflict)), Is.True);
            Assert.That(GrainStateWriteFaults.IsTranslatedConflict(failure), Is.False);
            Assert.That(GrainStateWriteFaults.IsTranslatedConflict(new InconsistentStateException("etag")), Is.False);
        });
    }

    [Test]
    public void Translate_turns_a_conflict_into_an_attributed_conflict()
    {
        var translated = GrainStateWriteFaults.Translate("cross-tree-tx", "op-1", new InconsistentStateException("etag"));

        Assert.That(translated, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(translated!.Conflict, Is.True);
            Assert.That(translated.GrainType, Is.EqualTo("cross-tree-tx"));
            Assert.That(translated.GrainKey, Is.EqualTo("op-1"));
        });
    }

    [Test]
    public void Translate_turns_an_unloadable_provider_fault_into_a_non_conflict()
    {
        var translated = GrainStateWriteFaults.Translate(
            "atomic-write", "k", new InvalidOperationException("wrap", new ProviderOnlyException("down")));

        Assert.That(translated, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(translated!.Conflict, Is.False);
            Assert.That(translated.FaultType, Is.EqualTo(typeof(ProviderOnlyException).FullName));
        });
    }

    [Test]
    public void Translate_passes_client_loadable_faults_through()
    {
        Assert.Multiple(() =>
        {
            Assert.That(GrainStateWriteFaults.Translate("atomic-write", "k", new TimeoutException("blip")), Is.Null);
            Assert.That(GrainStateWriteFaults.Translate("atomic-write", "k", new LatticeShuttingDownException("stop")), Is.Null);
            Assert.That(GrainStateWriteFaults.Translate(
                "atomic-write", "k", new LatticeStateWriteFailedException("already translated")), Is.Null);
        });
    }

    [Test]
    public void ConflictedActivation_is_an_attributed_conflict()
    {
        var ex = GrainStateWriteFaults.ConflictedActivation("atomic-write", "tree/op");

        Assert.Multiple(() =>
        {
            Assert.That(ex.Conflict, Is.True);
            Assert.That(ex.GrainType, Is.EqualTo("atomic-write"));
            Assert.That(ex.GrainKey, Is.EqualTo("tree/op"));
            Assert.That(ex.FaultType, Is.EqualTo(typeof(InconsistentStateException).FullName));
            Assert.That(ex.Message, Does.Contain("tree/op"));
        });
    }

    [Test]
    public async Task ClearRecoveringConflictAsync_clears_directly_when_there_is_no_conflict()
    {
        var row = new DurableStateRow<Holder> { Value = new Holder { Terminal = true } };
        var state = new LandedConflictPersistentState<Holder>(row);

        await GrainStateWriteFaults.ClearRecoveringConflictAsync(state, static s => s.Terminal);

        Assert.That(row.Exists, Is.False);
        Assert.That(state.Reads, Is.Zero);
    }

    [Test]
    public async Task ClearRecoveringConflictAsync_clears_again_when_the_reread_state_is_still_clearable()
    {
        var row = new DurableStateRow<Holder> { Value = new Holder { Terminal = true } };
        var state = new LandedConflictPersistentState<Holder>(row);
        row.Etag++;

        await GrainStateWriteFaults.ClearRecoveringConflictAsync(state, static s => s.Terminal);

        Assert.That(row.Exists, Is.False);
        Assert.That(state.Reads, Is.EqualTo(1));
    }

    [Test]
    public void ClearRecoveringConflictAsync_propagates_a_non_conflict_fault()
    {
        var state = Substitute.For<IPersistentState<Holder>>();
        state.ClearStateAsync().Returns(Task.FromException(new TimeoutException("blip")));

        Assert.ThrowsAsync<TimeoutException>(() => GrainStateWriteFaults.ClearRecoveringConflictAsync(state, static _ => true));
    }

    [Test]
    public async Task TryConfirmLandedAsync_reloads_the_row_and_reports_whether_the_write_landed()
    {
        var row = new DurableStateRow<Holder>();
        var state = new LandedConflictPersistentState<Holder>(row);
        state.State.Terminal = true;
        state.LandThenConflictOnNextWrite();
        Assert.CatchAsync<InconsistentStateException>(() => state.WriteStateAsync());

        var landed = await GrainStateWriteFaults.TryConfirmLandedAsync(state, static s => s.Terminal);
        var mismatch = await GrainStateWriteFaults.TryConfirmLandedAsync(state, static s => !s.Terminal);

        Assert.Multiple(() =>
        {
            Assert.That(landed, Is.True);
            Assert.That(mismatch, Is.False);
            Assert.That(state.Reads, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task TryConfirmLandedAsync_is_false_when_the_row_is_absent_or_the_read_fails()
    {
        var row = new DurableStateRow<Holder>();
        var state = new LandedConflictPersistentState<Holder>(row);

        var absent = await GrainStateWriteFaults.TryConfirmLandedAsync(state, static _ => true);

        row.Value = new Holder { Terminal = true };
        state.FailNextReadWith = new TimeoutException("blip");
        var readFailed = await GrainStateWriteFaults.TryConfirmLandedAsync(state, static _ => true);
        var recovered = await GrainStateWriteFaults.TryConfirmLandedAsync(state, static s => s.Terminal);

        Assert.Multiple(() =>
        {
            Assert.That(absent, Is.False, "no row");
            Assert.That(readFailed, Is.False, "the read failed");
            Assert.That(recovered, Is.True);
        });
    }

    /// <summary>Minimal serializable state for the clear helper.</summary>
    [GenerateSerializer]
    public sealed class Holder
    {
        /// <summary>Whether the state may be cleared.</summary>
        [Id(0)] public bool Terminal { get; set; }
    }

    /// <summary>Stands in for a storage provider's exception type.</summary>
    private sealed class ProviderOnlyException(string message) : Exception(message);
}
