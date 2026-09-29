using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit coverage for <see cref="TreeDeletionGrain.DiscardDerivedPhysicalTreeAsync"/>
/// and <see cref="TreeDeletionGrain.GetPhysicalRetentionAsync"/> (issue #3930):
/// a discarded copy releases its WAL retention at once - every leaf
/// materialiser pin retired and every WAL partition trimmed through its head -
/// while keeping the shard marks and the deferred purge a stale router relies on,
/// and it can never be recovered.
/// </summary>
public partial class TreeDeletionGrainTests
{
    private const int DiscardWalPartitions = 3;

    private sealed record DiscardHarness(
        TreeDeletionGrain Grain,
        FakePersistentState<TreeDeletionState> State,
        ILeafCursorReporter Reporter,
        IWalStorageProvider Wal,
        IGrainFactory GrainFactory,
        IReminderRegistry Reminders);

    /// <summary>
    /// A deletion grain whose options resolver resolves every WAL partition to
    /// one substitute provider, whose partition heads are <paramref name="heads"/>.
    /// </summary>
    private static DiscardHarness CreateDiscardHarness(params long[] heads) =>
        CreateDiscardHarnessFor(TreeId, heads);

    private static DiscardHarness CreateDiscardHarnessFor(string treeId, params long[] heads)
    {
        var reporter = Substitute.For<ILeafCursorReporter>();
        var services = new ServiceCollection().AddSingleton(reporter).BuildServiceProvider();

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("deletion", treeId));
        context.ActivationServices.Returns(services);

        var grainFactory = Substitute.For<IGrainFactory>();
        var reminderRegistry = Substitute.For<IReminderRegistry>();
        var options = new LatticeOptions
        {
            SoftDeleteDuration = TimeSpan.FromHours(72),
            WalPartitions = DiscardWalPartitions,
        };
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(options);

        for (int i = 0; i < ShardCount; i++)
        {
            var shardRoot = Substitute.For<IShardRootGrain>();
            grainFactory.GetGrain<IShardRootGrain>($"{treeId}/{i}").Returns(shardRoot);
            shardRoot.MarkDeletedAsync().Returns(Task.CompletedTask);
            shardRoot.PurgeAsync().Returns(Task.CompletedTask);
        }

        grainFactory.GetGrain<ITombstoneCompactionGrain>(treeId).Returns(Substitute.For<ITombstoneCompactionGrain>());
        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(treeId).Returns(treeId);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = ShardCount }));
        registry.GetWalPlacementAsync(Arg.Any<string>()).Returns(Task.FromResult(WalPlacementPin.Create()));

        var wal = Substitute.For<IWalStorageProvider>();
        for (var partition = 0; partition < heads.Length; partition++)
        {
            wal.GetHighestOffsetAsync(treeId, partition, Arg.Any<CancellationToken>())
                .Returns(Task.FromResult(heads[partition]));
        }

        var catalog = Substitute.For<IWalStorageProviderCatalog>();
        catalog.TryGet(Arg.Any<string>(), out _).ReturnsForAnyArgs(call =>
        {
            call[1] = wal;
            return true;
        });

        var resolver = new LatticeOptionsResolver(grainFactory, optionsMonitor, walProviderCatalog: catalog);
        var state = new FakePersistentState<TreeDeletionState>();
        var grain = new TreeDeletionGrain(
            context, grainFactory, reminderRegistry, optionsMonitor, resolver,
            new LoggerFactory().CreateLogger<TreeDeletionGrain>(), state);
        return new DiscardHarness(grain, state, reporter, wal, grainFactory, reminderRegistry);
    }

    [Test]
    public async Task Discard_marks_every_shard_deleted_and_keeps_the_deferred_purge()
    {
        var h = CreateDiscardHarness(4, 7, 9);

        await h.Grain.DiscardDerivedPhysicalTreeAsync();

        for (int i = 0; i < ShardCount; i++)
        {
            await h.GrainFactory.GetGrain<IShardRootGrain>($"{TreeId}/{i}").Received(1).MarkDeletedAsync();
        }

        // The purge stays deferred to the soft-delete window, so a router that
        // cached an alias to the copy keeps being refused by the shard marks.
        await h.Reminders.Received(1).RegisterOrUpdateReminder(
            Arg.Any<GrainId>(), "tree-deletion", Arg.Any<TimeSpan>(), Arg.Any<TimeSpan>());
        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.IsDeleted, Is.True);
            Assert.That(h.State.State.Discarded, Is.True);
            Assert.That(h.State.State.SuppressLifecycleEvents, Is.True, "a discard is physical maintenance and publishes no event");
            Assert.That(h.State.State.PurgeInProgress, Is.False);
            Assert.That(h.State.State.RetainsRegistryEntry, Is.False);
        });
    }

    [Test]
    public async Task Discard_retires_every_leaf_materialiser_pin_on_the_tree()
    {
        var h = CreateDiscardHarness(4, 7, 9);

        await h.Grain.DiscardDerivedPhysicalTreeAsync();

        await h.Reporter.Received(1).UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Discard_trims_every_wal_partition_through_its_head()
    {
        var h = CreateDiscardHarness(4, 7, 9);

        await h.Grain.DiscardDerivedPhysicalTreeAsync();

        await h.Wal.Received(1).TrimAsync(TreeId, 0, 4, Arg.Any<CancellationToken>());
        await h.Wal.Received(1).TrimAsync(TreeId, 1, 7, Arg.Any<CancellationToken>());
        await h.Wal.Received(1).TrimAsync(TreeId, 2, 9, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Discard_skips_a_partition_that_was_never_written()
    {
        var h = CreateDiscardHarness(4, -1, 9);

        await h.Grain.DiscardDerivedPhysicalTreeAsync();

        await h.Wal.DidNotReceive().TrimAsync(TreeId, 1, Arg.Any<long>(), Arg.Any<CancellationToken>());
        await h.Wal.Received(1).TrimAsync(TreeId, 2, 9, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Discard_trims_the_remaining_partitions_when_one_trim_fails()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        h.Wal.TrimAsync(TreeId, 0, Arg.Any<long>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("storage hiccup"));

        await h.Grain.DiscardDerivedPhysicalTreeAsync();

        await h.Wal.Received(1).TrimAsync(TreeId, 1, 7, Arg.Any<CancellationToken>());
        await h.Wal.Received(1).TrimAsync(TreeId, 2, 9, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Discard_retried_after_success_releases_again_without_remarking()
    {
        var h = CreateDiscardHarness(4, 7, 9);

        await h.Grain.DiscardDerivedPhysicalTreeAsync();
        await h.Grain.DiscardDerivedPhysicalTreeAsync();

        await h.GrainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").Received(1).MarkDeletedAsync();
        await h.Reporter.Received(2).UnregisterTreeAsync(TreeId, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Discard_rolls_back_the_record_when_its_first_write_fails()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        h.State.ThrowOnWrite = new InvalidOperationException("storage write failed");

        Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.DiscardDerivedPhysicalTreeAsync());

        Assert.Multiple(() =>
        {
            Assert.That(h.State.State.Discarded, Is.False);
            Assert.That(h.State.State.SuppressLifecycleEvents, Is.False);
        });
        await h.Reporter.DidNotReceive().UnregisterTreeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Recover_refuses_a_discarded_copy()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        await h.Grain.DiscardDerivedPhysicalTreeAsync();

        var ex = Assert.ThrowsAsync<InvalidOperationException>(() => h.Grain.RecoverPhysicalAsync());

        Assert.That(ex!.Message, Does.Contain("discarded"));
        Assert.That(h.State.State.IsDeleted, Is.True);
        await h.GrainFactory.GetGrain<IShardRootGrain>($"{TreeId}/0").DidNotReceive().UnmarkDeletedAsync();
    }

    [Test]
    public async Task Purging_a_discarded_copy_trims_its_wal_again_before_unregistering_it()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        await h.Grain.DiscardDerivedPhysicalTreeAsync();
        h.Wal.ClearReceivedCalls();

        await h.Grain.PurgeNowAsync();

        await h.Wal.Received(1).TrimAsync(TreeId, 0, 4, Arg.Any<CancellationToken>());
        Assert.That(h.State.State.PurgeComplete, Is.True);
    }

    [Test]
    public async Task Purging_a_discarded_copy_by_its_timer_trims_its_wal_again()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        await h.Grain.DiscardDerivedPhysicalTreeAsync();
        h.Wal.ClearReceivedCalls();

        await h.Grain.BeginPurgeStateAsync(startFromShard: 0);
        await h.Grain.CompletePurgeAsync();

        await h.Wal.Received(1).TrimAsync(TreeId, 2, 9, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Purging_an_ordinary_deleted_tree_does_not_trim_its_wal()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        await h.Grain.DeleteTreeAsync();

        await h.Grain.PurgeNowAsync();

        await h.Wal.DidNotReceive().TrimAsync(
            Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    // --- GetPhysicalRetentionAsync ---

    [Test]
    public async Task Physical_retention_is_live_for_a_tree_that_was_never_deleted()
    {
        var h = CreateDiscardHarness(4, 7, 9);

        Assert.That(await h.Grain.GetPhysicalRetentionAsync(), Is.EqualTo(PhysicalTreeRetention.Live));
    }

    [Test]
    public async Task Physical_retention_is_deleted_for_a_recoverable_deletion()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        await h.Grain.DeleteDerivedPhysicalTreeAsync();

        Assert.That(await h.Grain.GetPhysicalRetentionAsync(), Is.EqualTo(PhysicalTreeRetention.Deleted));
    }

    [Test]
    public async Task Physical_retention_is_deleted_for_a_delegated_deletion()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        h.State.State.Delegated = true;

        Assert.That(await h.Grain.GetPhysicalRetentionAsync(), Is.EqualTo(PhysicalTreeRetention.Deleted));
    }

    [Test]
    public async Task Physical_retention_is_discarded_after_a_discard()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        await h.Grain.DiscardDerivedPhysicalTreeAsync();

        Assert.That(await h.Grain.GetPhysicalRetentionAsync(), Is.EqualTo(PhysicalTreeRetention.Discarded));
    }

    [Test]
    public async Task Physical_retention_is_live_while_a_discard_record_has_no_shard_marks_yet()
    {
        // The record is written before the marks so a retry is still a
        // discard; until the marks land the copy is still readable and its
        // pins must keep protecting it.
        var h = CreateDiscardHarness(4, 7, 9);
        h.State.State.Discarded = true;

        Assert.That(await h.Grain.GetPhysicalRetentionAsync(), Is.EqualTo(PhysicalTreeRetention.Live));
    }

    [Test]
    public async Task Physical_retention_is_live_once_the_purge_has_completed()
    {
        // A later write under a purged id registers a new, live tree, which the
        // purged copy's record must not be reported against.
        var h = CreateDiscardHarness(4, 7, 9);
        await h.Grain.DiscardDerivedPhysicalTreeAsync();
        await h.Grain.PurgeNowAsync();

        Assert.That(await h.Grain.GetPhysicalRetentionAsync(), Is.EqualTo(PhysicalTreeRetention.Live));
    }

    // --- DiscardIfAbandonedDerivedCopyAsync ---

    private const string AbandonedCopy = "logical-tree/resized/0123456789abcdef";

    private static ITreeResizeGrain ResizeNaming(IGrainFactory factory, bool references)
    {
        var resize = Substitute.For<ITreeResizeGrain>();
        resize.ReferencesPhysicalTreeAsync(Arg.Any<string>()).Returns(Task.FromResult(references));
        factory.GetGrain<ITreeResizeGrain>("logical-tree").Returns(resize);
        return resize;
    }

    [Test]
    public async Task DiscardIfAbandoned_discards_a_retired_resize_copy_no_resize_names()
    {
        // The state a build predating the discard left an undone resize's
        // destination in: retired silently by DeleteDerivedPhysicalTreeAsync,
        // its coordinator reset by the undo.
        var h = CreateDiscardHarnessFor(AbandonedCopy, 4, 7, 9);
        var resize = ResizeNaming(h.GrainFactory, references: false);
        await h.Grain.DeleteDerivedPhysicalTreeAsync();

        var discarded = await h.Grain.DiscardIfAbandonedDerivedCopyAsync();

        Assert.That(discarded, Is.True);
        Assert.That(h.State.State.Discarded, Is.True);
        await resize.Received(1).ReferencesPhysicalTreeAsync(AbandonedCopy);
        await h.Reporter.Received(1).UnregisterTreeAsync(AbandonedCopy, Arg.Any<CancellationToken>());
        await h.Wal.Received(1).TrimAsync(AbandonedCopy, 2, 9, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DiscardIfAbandoned_keeps_a_retired_copy_an_undo_can_still_recover()
    {
        var h = CreateDiscardHarnessFor(AbandonedCopy, 4, 7, 9);
        ResizeNaming(h.GrainFactory, references: true);
        await h.Grain.DeleteDerivedPhysicalTreeAsync();

        Assert.That(await h.Grain.DiscardIfAbandonedDerivedCopyAsync(), Is.False);
        Assert.That(h.State.State.Discarded, Is.False);
        await h.Reporter.DidNotReceive().UnregisterTreeAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
        await h.Wal.DidNotReceive().TrimAsync(Arg.Any<string>(), Arg.Any<int>(), Arg.Any<long>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task DiscardIfAbandoned_keeps_a_tree_a_caller_deleted_and_may_recover()
    {
        // DeleteTreeAsync never suppresses lifecycle events, whatever the id.
        var h = CreateDiscardHarnessFor(AbandonedCopy, 4, 7, 9);
        var resize = ResizeNaming(h.GrainFactory, references: false);
        h.GrainFactory.GetLatticeRegistry().GetAliasesTargetingAsync(Arg.Any<string>()).Returns(Array.Empty<string>());
        await h.Grain.DeleteTreeAsync();

        Assert.That(h.State.State.IsDeleted, Is.True, "precondition: the caller's delete landed");
        Assert.That(await h.Grain.DiscardIfAbandonedDerivedCopyAsync(), Is.False);
        Assert.That(h.State.State.Discarded, Is.False);
        await resize.DidNotReceive().ReferencesPhysicalTreeAsync(Arg.Any<string>());
    }

    [Test]
    public async Task DiscardIfAbandoned_keeps_a_delegated_deletion()
    {
        var h = CreateDiscardHarnessFor(AbandonedCopy, 4, 7, 9);
        ResizeNaming(h.GrainFactory, references: false);
        await h.Grain.DeleteDelegatedAsync();

        Assert.That(h.State.State.IsDeleted, Is.True, "precondition: the delegated delete landed");
        Assert.That(await h.Grain.DiscardIfAbandonedDerivedCopyAsync(), Is.False);
        Assert.That(h.State.State.Discarded, Is.False);
    }

    [Test]
    public async Task DiscardIfAbandoned_leaves_a_live_copy_alone()
    {
        var h = CreateDiscardHarnessFor(AbandonedCopy, 4, 7, 9);
        var resize = ResizeNaming(h.GrainFactory, references: false);

        Assert.That(await h.Grain.DiscardIfAbandonedDerivedCopyAsync(), Is.False);
        await resize.DidNotReceive().ReferencesPhysicalTreeAsync(Arg.Any<string>());
        await h.GrainFactory.GetGrain<IShardRootGrain>($"{AbandonedCopy}/0").DidNotReceive().MarkDeletedAsync();
    }

    [Test]
    public async Task DiscardIfAbandoned_ignores_an_id_that_is_not_a_resize_copy()
    {
        var h = CreateDiscardHarness(4, 7, 9);
        await h.Grain.DeleteDerivedPhysicalTreeAsync();

        Assert.That(await h.Grain.DiscardIfAbandonedDerivedCopyAsync(), Is.False);
        Assert.That(h.State.State.Discarded, Is.False);
    }

    [Test]
    public async Task DiscardIfAbandoned_reports_an_already_discarded_copy_without_asking_the_resize()
    {
        var h = CreateDiscardHarnessFor(AbandonedCopy, 4, 7, 9);
        var resize = ResizeNaming(h.GrainFactory, references: true);
        await h.Grain.DiscardDerivedPhysicalTreeAsync();

        Assert.That(await h.Grain.DiscardIfAbandonedDerivedCopyAsync(), Is.True);
        await resize.DidNotReceive().ReferencesPhysicalTreeAsync(Arg.Any<string>());
    }

    [TestCase("t/resized/abc", "t")]
    [TestCase("a/b/resized/abc", "a/b")]
    [TestCase("t/resized/abc/resized/def", "t/resized/abc")]
    [TestCase("t", null)]
    [TestCase("/resized/abc", null)]
    [TestCase("t/resized/", null)]
    [TestCase("t/resized/abc/0", null)]
    public void ResizeLogicalTreeId_parses_only_a_resize_copy_id(string physicalTreeId, string? expected)
    {
        Assert.That(TreeDeletionGrain.ResizeLogicalTreeId(physicalTreeId), Is.EqualTo(expected));
    }
}
