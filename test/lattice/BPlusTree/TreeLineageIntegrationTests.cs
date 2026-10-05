using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// The content-lineage token and the soft-delete epoch a snapshot export reports
/// as its source generation (issue #4537). A receiver deletes a source-origin key
/// missing from an in-place re-bootstrap only when its copy is aligned with the
/// source's lineage and the generation did not move across the export, so the
/// lineage must change whenever a tree's content is swapped for content not
/// derived key-for-key from it, survive topology-only moves, and the epoch must
/// advance on every soft delete, logical or physical.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class TreeLineageIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder { Options = { InitialSilosCount = 1 } };
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private IGrainFactory Grains => _cluster.Client;

    private ILatticeRegistry Registry => Grains.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private async Task<Guid?> LineageAsync(string tree) => (await Registry.GetEntryAsync(tree))?.Lineage;

    private async Task SetAliasAsync(string logical, string physical)
    {
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SetAliasAsync(logical, physical);
        }
    }

    [Test]
    public async Task Registering_a_tree_stamps_a_lineage()
    {
        var tree = $"lineage-new-{Guid.NewGuid():N}";
        await Grains.GetGrain<ILattice>(tree).SetAsync("k", new byte[] { 1 });

        Assert.That(await LineageAsync(tree), Is.Not.Null.And.Not.EqualTo(Guid.Empty));
    }

    [Test]
    public async Task A_reshard_keeps_the_lineage()
    {
        var tree = $"lineage-reshard-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(tree, new TreeRegistryEntry { ShardCount = 2 });
        var before = await LineageAsync(tree);

        await Grains.GetGrain<ILattice>(tree).ReshardAsync(4);

        Assert.Multiple(async () =>
        {
            Assert.That(before, Is.Not.Null);
            Assert.That(await LineageAsync(tree), Is.EqualTo(before), "a reshard moves keys, it does not replace them");
        });
    }

    [Test]
    public async Task A_resize_keeps_the_lineage()
    {
        var tree = $"lineage-resize-{Guid.NewGuid():N}";
        await Grains.GetGrain<ILattice>(tree).SetAsync("k", new byte[] { 1 });
        var before = await LineageAsync(tree);

        var resize = Grains.GetGrain<ITreeResizeGrain>(tree);
        await resize.ResizeAsync(64, 64);
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(60);
        while (!await resize.IsIdleAsync())
        {
            if (DateTime.UtcNow > deadline) Assert.Fail("PRECONDITION: the resize did not complete");
            await Task.Delay(200);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await Registry.ResolveAsync(tree), Is.Not.EqualTo(tree), "PRECONDITION: the tree routes to the resized copy");
            Assert.That(await LineageAsync(tree), Is.EqualTo(before), "a resize copies every key, so the content lineage is unchanged");
        });
    }

    [Test]
    public async Task Pointing_the_alias_at_a_different_tree_restamps_and_re_setting_it_does_not()
    {
        var logical = $"lineage-alias-{Guid.NewGuid():N}";
        var other = $"{logical}-other";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 2 });
        await Registry.RegisterAsync(other, new TreeRegistryEntry { ShardCount = 2 });
        var before = await LineageAsync(logical);

        await SetAliasAsync(logical, other);
        var moved = await LineageAsync(logical);
        await SetAliasAsync(logical, other);

        Assert.Multiple(async () =>
        {
            Assert.That(moved, Is.Not.Null.And.Not.EqualTo(before), "the logical tree now serves different content");
            Assert.That(await LineageAsync(logical), Is.EqualTo(moved), "re-setting the same alias changes nothing");
        });
    }

    [Test]
    public async Task Removing_an_alias_restamps()
    {
        var logical = $"lineage-remove-{Guid.NewGuid():N}";
        var other = $"{logical}-other";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 2 });
        await Registry.RegisterAsync(other, new TreeRegistryEntry { ShardCount = 2 });
        await SetAliasAsync(logical, other);
        var aliased = await LineageAsync(logical);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.RemoveAliasAsync(logical);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await Registry.ResolveAsync(logical), Is.EqualTo(logical), "PRECONDITION: the tree serves its own shards");
            Assert.That(await LineageAsync(logical), Is.Not.Null.And.Not.EqualTo(aliased),
                "the tree now serves different content");
        });
    }

    [Test]
    public async Task An_explicit_alias_carry_to_a_different_tree_restamps()
    {
        var logical = $"lineage-carry-{Guid.NewGuid():N}";
        var other = $"{logical}-other";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 2 });
        await Registry.RegisterAsync(other, new TreeRegistryEntry { ShardCount = 3 });
        var before = await LineageAsync(logical);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, logical, other);
        }

        Assert.That(await LineageAsync(logical), Is.Not.Null.And.Not.EqualTo(before));
    }

    [Test]
    public async Task Every_soft_delete_advances_the_deletion_epoch_and_a_recover_keeps_it()
    {
        var tree = $"lineage-epoch-{Guid.NewGuid():N}";
        var lattice = Grains.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 1 });
        var deletion = Grains.GetGrain<ITreeDeletionGrain>(tree);
        var start = (await deletion.GetDeletionStatusAsync()).DeletionEpoch;

        await lattice.DeleteTreeAsync();
        var deleted = await deletion.GetDeletionStatusAsync();
        await lattice.RecoverTreeAsync();
        var recovered = await deletion.GetDeletionStatusAsync();
        await lattice.DeleteTreeAsync();
        var deletedAgain = await deletion.GetDeletionStatusAsync();
        await lattice.RecoverTreeAsync();

        Assert.Multiple(() =>
        {
            Assert.That(deleted.DeletionEpoch, Is.EqualTo(start + 1));
            Assert.That(deleted.IsDeleted, Is.True);
            Assert.That(recovered.DeletionEpoch, Is.EqualTo(start + 1), "a recover is not a new delete");
            Assert.That(recovered.IsDeleted, Is.False);
            Assert.That(deletedAgain.DeletionEpoch, Is.EqualTo(start + 2),
                "a second delete-and-recover must be distinguishable from the first");
        });
    }

    [Test]
    public async Task A_logical_delete_of_an_aliased_tree_advances_its_deletion_epoch()
    {
        var logical = $"lineage-logical-{Guid.NewGuid():N}";
        var physical = $"{logical}-physical";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 2 });
        await Registry.RegisterAsync(physical, new TreeRegistryEntry { ShardCount = 2, DerivedFrom = logical });
        await SetAliasAsync(logical, physical);
        var deletion = Grains.GetGrain<ITreeDeletionGrain>(logical);
        var start = (await deletion.GetDeletionStatusAsync()).DeletionEpoch;

        await Grains.GetGrain<ILattice>(logical).DeleteTreeAsync();
        var deleted = await deletion.GetDeletionStatusAsync();

        Assert.Multiple(() =>
        {
            Assert.That(deleted.IsDeleted, Is.True, "PRECONDITION: the logical tree is soft-deleted");
            Assert.That(deleted.DeletionEpoch, Is.EqualTo(start + 1));
        });
    }

    [Test]
    public async Task A_lineage_change_is_announced_before_it_is_persisted()
    {
        var logical = $"lineage-announce-{Guid.NewGuid():N}";
        var other = $"{logical}-other";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 2 });
        await Registry.RegisterAsync(other, new TreeRegistryEntry { ShardCount = 2 });
        var before = await LineageAsync(logical);

        await SetAliasAsync(logical, other);
        var after = await LineageAsync(logical);

        var change = RecordingLineageObserver.Changes.Single(c => c.TreeId == logical && c.Next == after);
        Assert.Multiple(() =>
        {
            Assert.That(change.Current, Is.EqualTo(before));
            Assert.That(change.PersistedAtNotification, Is.EqualTo(before),
                "the observer must hear of the change while the old lineage is still persisted");
        });
    }

    [Test]
    public async Task Registering_removing_an_alias_and_unregistering_are_all_announced()
    {
        var logical = $"lineage-lifecycle-{Guid.NewGuid():N}";
        var other = $"{logical}-other";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 2 });
        await Registry.RegisterAsync(other, new TreeRegistryEntry { ShardCount = 2 });
        var registered = await LineageAsync(logical);
        await SetAliasAsync(logical, other);
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.RemoveAliasAsync(logical);
        }

        var removed = await LineageAsync(logical);
        await Registry.UnregisterAsync(logical);

        var changes = RecordingLineageObserver.Changes.Where(c => c.TreeId == logical).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(changes.First().Current, Is.Null, "registration is announced");
            Assert.That(changes.First().Next, Is.EqualTo(registered));
            Assert.That(changes.Any(c => c.Next == removed && c.Current != removed), Is.True, "removing the alias is announced");
            Assert.That(changes.Last().Current, Is.EqualTo(removed), "unregistration is announced");
            Assert.That(changes.Last().Next, Is.Null);
        });
    }

    [Test]
    public async Task An_explicit_alias_carry_announces_the_re_stamp_before_the_alias_moves()
    {
        var logical = $"lineage-carry-order-{Guid.NewGuid():N}";
        var other = $"{logical}-other";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 2 });
        await Registry.RegisterAsync(other, new TreeRegistryEntry { ShardCount = 3 });
        var before = await LineageAsync(logical);

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await AliasCutoverShardMaps.CarryAcrossExplicitAliasAsync(Grains, logical, other);
        }

        var change = RecordingLineageObserver.Changes.Single(c => c.TreeId == logical && c.Current == before);
        Assert.That(change.ResolvedAtNotification, Is.EqualTo(logical),
            "the lineage moves before the alias, so the new contents are never served under the old lineage");
    }

    [Test]
    public async Task A_failing_lineage_observer_aborts_the_re_stamp()
    {
        var logical = $"lineage-refused-{Guid.NewGuid():N}";
        var other = $"{logical}-other";
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = 2 });
        await Registry.RegisterAsync(other, new TreeRegistryEntry { ShardCount = 2 });
        var before = await LineageAsync(logical);

        RecordingLineageObserver.FailFor.TryAdd(logical, 0);
        try
        {
            Assert.That(async () => await SetAliasAsync(logical, other), Throws.Exception);
        }
        finally
        {
            RecordingLineageObserver.FailFor.TryRemove(logical, out _);
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await LineageAsync(logical), Is.EqualTo(before), "fail closed: the lineage is not changed");
            Assert.That(await Registry.ResolveAsync(logical), Is.EqualTo(logical), "and neither is the alias written with it");
        });
    }

    [Test]
    public async Task A_purge_is_announced_before_the_contents_are_purged()
    {
        var tree = $"lineage-purge-{Guid.NewGuid():N}";
        var lattice = Grains.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 1 });
        var lineage = await LineageAsync(tree);
        await lattice.DeleteTreeAsync();

        await lattice.PurgeTreeAsync();

        var change = RecordingLineageObserver.Changes.First(c => c.TreeId == tree && c.Next is null);
        Assert.Multiple(async () =>
        {
            Assert.That(change.Current, Is.EqualTo(lineage));
            Assert.That(change.PersistedAtNotification, Is.EqualTo(lineage),
                "the purge is announced while the tree is still registered, before its row or contents go");
            Assert.That(await Registry.GetEntryAsync(tree), Is.Null, "PRECONDITION: the purge completed");
        });
    }

    [Test]
    public async Task A_failing_lineage_observer_stops_a_purge_from_starting()
    {
        var tree = $"lineage-purge-refused-{Guid.NewGuid():N}";
        var lattice = Grains.GetGrain<ILattice>(tree);
        await lattice.SetAsync("k", new byte[] { 7 });
        var lineage = await LineageAsync(tree);
        await lattice.DeleteTreeAsync();

        RecordingLineageObserver.FailFor.TryAdd(tree, 0);
        try
        {
            Assert.That(async () => await lattice.PurgeTreeAsync(), Throws.Exception);
        }
        finally
        {
            RecordingLineageObserver.FailFor.TryRemove(tree, out _);
        }

        await lattice.RecoverTreeAsync();
        Assert.Multiple(async () =>
        {
            Assert.That(await LineageAsync(tree), Is.EqualTo(lineage), "fail closed: the tree stays registered");
            Assert.That(await lattice.GetAsync("k"), Is.EqualTo(new byte[] { 7 }), "and its contents were not purged");
        });
    }

    internal sealed record LineageChange(string TreeId, Guid? Current, Guid? Next, Guid? PersistedAtNotification, string? ResolvedAtNotification);

    /// <summary>Records every announced lineage change, with what the registry held at that moment.</summary>
    internal sealed class RecordingLineageObserver(IGrainFactory grainFactory) : ITreeLineageObserver
    {
        public static ConcurrentQueue<LineageChange> Changes { get; } = new();

        public static ConcurrentDictionary<string, byte> FailFor { get; } = new();

        public async Task OnLineageChangingAsync(string treeId, Guid? currentLineage, Guid? nextLineage, CancellationToken cancellationToken = default)
        {
            if (FailFor.ContainsKey(treeId))
            {
                throw new InvalidOperationException($"observer refuses the lineage change of '{treeId}'");
            }

            var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            var persisted = (await registry.GetEntryAsync(treeId))?.Lineage;
            var resolved = currentLineage is null ? null : await registry.ResolveAsync(treeId);
            Changes.Enqueue(new LineageChange(treeId, currentLineage, nextLineage, persisted, resolved));
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<ITreeLineageObserver, RecordingLineageObserver>();
        }
    }
}
