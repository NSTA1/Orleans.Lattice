using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Storage;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// Storage-fidelity tests for the process-scope test grain storage (issue #4196): a
/// write carrying a stale ETag must throw <see cref="InconsistentStateException"/> as a
/// durable provider would, and state must be copied on the way in and out so a
/// second activation never shares an object graph with the store or with another
/// activation.
/// </summary>
[TestFixture]
public sealed class ProcessScopeMemoryGrainStorageTests
{
    private const string StateName = "harness-state";

    private readonly ProcessScopeMemoryGrainStorage _storage = new();

    [Test]
    public async Task A_write_carrying_a_stale_etag_throws_InconsistentStateException_and_keeps_the_newer_state()
    {
        var grainId = NewGrainId();
        await _storage.WriteStateAsync(StateName, grainId, State(isRegistered: false));

        var first = await ReadAsync(grainId);
        var second = await ReadAsync(grainId);
        first.State.IsRegistered = true;
        await _storage.WriteStateAsync(StateName, grainId, first);

        second.State.IsDeleted = true;
        Assert.ThrowsAsync<InconsistentStateException>(() => _storage.WriteStateAsync(StateName, grainId, second));

        var stored = await ReadAsync(grainId);
        Assert.Multiple(() =>
        {
            Assert.That(stored.State.IsRegistered, Is.True, "The newer write survives.");
            Assert.That(stored.State.IsDeleted, Is.False, "The stale write is not applied.");
            Assert.That(stored.ETag, Is.EqualTo(first.ETag));
        });
    }

    [Test]
    public async Task A_write_carrying_an_etag_for_a_record_that_no_longer_exists_throws_InconsistentStateException()
    {
        var grainId = NewGrainId();
        await _storage.WriteStateAsync(StateName, grainId, State(isRegistered: true));
        var stale = await ReadAsync(grainId);
        var clearer = await ReadAsync(grainId);
        await _storage.ClearStateAsync(StateName, grainId, clearer);

        Assert.ThrowsAsync<InconsistentStateException>(() => _storage.WriteStateAsync(StateName, grainId, stale));
    }

    [Test]
    public async Task A_clear_carrying_a_stale_etag_throws_InconsistentStateException()
    {
        var grainId = NewGrainId();
        await _storage.WriteStateAsync(StateName, grainId, State(isRegistered: false));
        var stale = await ReadAsync(grainId);
        var current = await ReadAsync(grainId);
        await _storage.WriteStateAsync(StateName, grainId, current);

        Assert.ThrowsAsync<InconsistentStateException>(() => _storage.ClearStateAsync(StateName, grainId, stale));
    }

    [Test]
    public async Task State_is_copied_on_write_and_on_read()
    {
        var grainId = NewGrainId();
        var written = State(isRegistered: true);
        await _storage.WriteStateAsync(StateName, grainId, written);
        written.State.IsDeleted = true;

        var first = await ReadAsync(grainId);
        var second = await ReadAsync(grainId);
        first.State.RootIsLeaf = true;

        Assert.Multiple(() =>
        {
            Assert.That(first.State, Is.Not.SameAs(written.State), "A read must not hand back the written instance.");
            Assert.That(first.State, Is.Not.SameAs(second.State), "Two reads must not share an instance.");
            Assert.That(second.State.IsDeleted, Is.False, "Mutating the written instance after the write must not change the store.");
            Assert.That(second.State.RootIsLeaf, Is.False, "Mutating one read must not change another.");
        });
    }

    [Test]
    public async Task Corrupting_a_stored_root_is_leaf_flag_changes_the_stored_state_and_its_etag()
    {
        var grainId = NewGrainId();
        var internalRoot = NewGrainId();
        var written = new GrainState<ShardRootState> { State = new ShardRootState { RootNodeId = internalRoot, RootIsLeaf = false } };
        await _storage.WriteStateAsync(StateName, grainId, written);

        var corrupted = ProcessScopeMemoryGrainStorage.ForceRootIsLeafOverInternalRoot(internalRoot);

        var stored = await ReadAsync(grainId);
        Assert.Multiple(() =>
        {
            Assert.That(corrupted, Is.EqualTo(1));
            Assert.That(stored.State.RootIsLeaf, Is.True);
            Assert.That(stored.ETag, Is.Not.EqualTo(written.ETag), "An out-of-band edit changes the record's ETag.");
        });
    }

    private async Task<GrainState<ShardRootState>> ReadAsync(GrainId grainId)
    {
        var state = new GrainState<ShardRootState> { State = new ShardRootState() };
        await _storage.ReadStateAsync(StateName, grainId, state);
        return state;
    }

    private static GrainState<ShardRootState> State(bool isRegistered) =>
        new() { State = new ShardRootState { IsRegistered = isRegistered } };

    private static GrainId NewGrainId() => GrainId.Create("harness", Guid.NewGuid().ToString("N"));
}
