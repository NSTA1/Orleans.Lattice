using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Regression coverage for issue #5417. A purge records its registry removal
/// as owed, performs it, and only then clears the record. When the removal
/// lands but the record is not cleared - the write clearing it, or the loop
/// stop before it, throws - the keepalive re-drives the whole tail. By then
/// a write or a create may have registered the id again, and the re-drive
/// used to trim the new tree's log, clear its pins and unregister it. It must
/// recognise, from the lineage recorded with the purge's completion, that the
/// removal already landed, and only record it.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private static readonly Guid PurgedLineage = new("54170000-0000-0000-0000-000000000001");
    private static readonly Guid ReusedLineage = new("54170000-0000-0000-0000-000000000002");

    /// <summary>
    /// Purges the tree with its registry entry carrying
    /// <see cref="PurgedLineage"/>, letting the registry removal land and then
    /// failing the write that clears the owed record, and returns the harness
    /// with every call it observed so far cleared.
    /// </summary>
    private static async Task<DiscardHarness> PurgeWhoseRemovalLandedButStayedOwedAsync()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        var registry = RegistryOf(h.GrainFactory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(PurgedLineage)));
        await h.Grain.DeleteTreeAsync();

        registry.UnregisterAsync(TreeId).Returns(_ =>
        {
            // The removal lands; the write that would record it fails.
            registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(null));
            h.State.ThrowOnWrite = new InvalidOperationException("simulated storage failure");
            return Task.CompletedTask;
        });
        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.PurgeNowAsync());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.PurgeComplete, Is.True);
            Assert.That(h.State.State.RegistryUnregisterPending, Is.True, "the removal landed, but its record did not");
            Assert.That(h.State.State.PurgedRegistryLineageRecorded, Is.True);
            Assert.That(h.State.State.PurgedRegistryLineage, Is.EqualTo(PurgedLineage));
        });

        registry.ClearReceivedCalls();
        h.Wal.ClearReceivedCalls();
        h.Reporter.ClearReceivedCalls();
        return h;
    }

    private static TreeRegistryEntry PurgedEntry(Guid? lineage) =>
        new() { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = ShardCount, Lineage = lineage };

    [Test]
    public async Task A_redriven_purge_leaves_a_reused_tree_id_untouched()
    {
        var h = await PurgeWhoseRemovalLandedButStayedOwedAsync();
        var registry = RegistryOf(h.GrainFactory);

        // A write registers the id again: a new tree, with a new lineage.
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(ReusedLineage)));
        registry.ExistsAsync(TreeId).Returns(true);

        await h.Grain.ReceiveReminder("deletion-keepalive", default);

        await h.Wal.DidNotReceive().TrimAsync(TreeId, Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
        await h.Reporter.DidNotReceive().UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
        await registry.DidNotReceive().UnregisterAsync(TreeId);
        Assert.That(h.State.State.RegistryUnregisterPending, Is.False, "the landed removal is recorded");
    }

    [Test]
    public async Task A_reused_tree_id_is_recognised_once_a_redriven_purge_has_settled()
    {
        var h = await PurgeWhoseRemovalLandedButStayedOwedAsync();
        var registry = RegistryOf(h.GrainFactory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(ReusedLineage)));
        registry.ExistsAsync(TreeId).Returns(true);

        await h.Grain.ReceiveReminder("deletion-keepalive", default);

        Assert.That(await h.Grain.IsDeletedAsync(), Is.False,
            "with the record settled, the id reads as the new tree issue #3940 defines");
    }

    [Test]
    public async Task A_redriven_purge_whose_removal_landed_only_records_it()
    {
        var h = await PurgeWhoseRemovalLandedButStayedOwedAsync();
        var registry = RegistryOf(h.GrainFactory);

        await h.Grain.ReceiveReminder("deletion-keepalive", default);

        // The id is free and could be registered at any moment, so the steps
        // that act on it are not repeated: they all ran before the removal.
        await h.Wal.DidNotReceive().TrimAsync(TreeId, Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
        await h.Reporter.DidNotReceive().UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
        await registry.DidNotReceive().UnregisterAsync(TreeId);
        Assert.That(h.State.State.RegistryUnregisterPending, Is.False);
    }

    [Test]
    public async Task A_redriven_purge_whose_removal_never_landed_still_removes_the_purged_entry()
    {
        // The control: the entry still carries the purged lineage, so the
        // removal is owed and the re-drive performs the whole tail (#4265).
        var h = CreateDiscardHarness(4, 7, 9);
        var registry = RegistryOf(h.GrainFactory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(PurgedLineage)));
        await h.Grain.DeleteTreeAsync();
        registry.UnregisterAsync(TreeId).Returns(Task.FromException(new InvalidOperationException("registry down")));
        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.PurgeNowAsync());
        registry.UnregisterAsync(TreeId).Returns(Task.CompletedTask);
        registry.ClearReceivedCalls();
        h.Wal.ClearReceivedCalls();
        h.Reporter.ClearReceivedCalls();

        await h.Grain.ReceiveReminder("deletion-keepalive", default);

        await h.Wal.Received(1).TrimAsync(TreeId, 0, 4, Arg.Any<CancellationToken>());
        await h.Wal.Received(1).TrimAsync(TreeId, 2, 9, Arg.Any<CancellationToken>());
        await h.Reporter.Received(1).UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
        await registry.Received(1).UnregisterAsync(TreeId);
        Assert.That(h.State.State.RegistryUnregisterPending, Is.False);
    }

    [Test]
    public async Task A_redriven_purge_of_an_entry_without_a_lineage_recognises_a_reuse()
    {
        // A registration predating lineages carries none; a reuse always stamps
        // one, so it is still told apart from the purged entry.
        var h = CreateDiscardHarness(4, 7, 9);
        var registry = RegistryOf(h.GrainFactory);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(null)));
        await h.Grain.DeleteTreeAsync();
        registry.UnregisterAsync(TreeId).Returns(_ =>
        {
            h.State.ThrowOnWrite = new InvalidOperationException("simulated storage failure");
            return Task.CompletedTask;
        });
        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.PurgeNowAsync());
        Assert.That(h.State.State.PurgedRegistryLineageRecorded, Is.True);
        registry.GetEntryAsync(TreeId).Returns(Task.FromResult<TreeRegistryEntry?>(PurgedEntry(ReusedLineage)));
        registry.ClearReceivedCalls();
        h.Reporter.ClearReceivedCalls();

        await h.Grain.ReceiveReminder("deletion-keepalive", default);

        await h.Reporter.DidNotReceive().UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
        await registry.DidNotReceive().UnregisterAsync(TreeId);
    }
}
