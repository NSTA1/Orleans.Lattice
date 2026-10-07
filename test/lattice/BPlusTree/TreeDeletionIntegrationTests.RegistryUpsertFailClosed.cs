using System.Text;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression coverage for issue #4230: the registry's non-create verbs used to
/// upsert from an empty entry, so a late split, a leaf latch, a configuration
/// change or a WAL placement flip against a never-registered or purged id
/// created its row - a third route, after the two #4219 closed, to a deletion
/// that did not delete. Each verb family must now refuse and leave no row.
/// </summary>
public partial class TreeDeletionIntegrationTests
{
    public enum RegistryVerbFamily
    {
        ShardTopology,
        Configuration,
        DigestLatch,
        WalPlacement,
    }

    private static readonly RegistryVerbFamily[] VerbFamilies = Enum.GetValues<RegistryVerbFamily>();

    [TestCaseSource(nameof(VerbFamilies))]
    public async Task Non_create_registry_verbs_refuse_an_absent_id_and_leave_no_row(RegistryVerbFamily family)
    {
        var treeName = $"absent-{family}-{Guid.NewGuid():N}";
        await AssertFamilyRefusesWithoutCreatingAsync(family, treeName);
    }

    [TestCaseSource(nameof(VerbFamilies))]
    public async Task Non_create_registry_verbs_refuse_a_purged_id_and_leave_no_row(RegistryVerbFamily family)
    {
        var treeName = $"purged-{family}-{Guid.NewGuid():N}";
        await PurgeAsync(treeName);
        await AssertFamilyRefusesWithoutCreatingAsync(family, treeName);
        Assert.ThrowsAsync<InvalidOperationException>(
            () => _cluster.GrainFactory.GetGrain<ILattice>(treeName).RecoverTreeAsync(),
            "a refused verb must not make the purged tree recoverable");
    }

    /// <summary>
    /// The public configuration verbs on <see cref="ILattice"/> keep configuring a
    /// tree before its first write - the system-data initializers rely on it - by
    /// registering it explicitly, with its structural pins seeded.
    /// </summary>
    [Test]
    public async Task Lattice_configuration_verbs_register_a_never_created_tree_with_its_structural_pins()
    {
        var treeName = $"configure-first-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var registry = _cluster.GrainFactory.GetLatticeRegistry();

        await router.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, TimeSpan.FromHours(1));
        await router.SetPublishEventsEnabledAsync(true);

        var entry = await registry.GetEntryAsync(treeName);
        Assert.That(entry, Is.Not.Null);
        Assert.Multiple(() =>
        {
            Assert.That(entry!.HistoryRetentionMode, Is.EqualTo(HistoryRetentionMode.FullValue));
            Assert.That(entry.PublishEvents, Is.True);
            Assert.That(entry.MaxLeafKeys, Is.Not.Null, "the row must carry its structural pins");
            Assert.That(entry.MaxInternalChildren, Is.Not.Null);
            Assert.That(entry.ShardCount, Is.Not.Null);
        });
    }

    /// <summary>
    /// The same verbs on a purged id refuse with the not-found shape and do not
    /// register it: configuring a tree is not a write, so it does not reuse a
    /// purged id (issue #4219's guard).
    /// </summary>
    [Test]
    public async Task Lattice_configuration_verbs_refuse_a_purged_tree_and_leave_no_row()
    {
        var treeName = $"configure-purged-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        await PurgeAsync(treeName);

        Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(
            () => router.SetHistoryRetentionAsync(HistoryRetentionMode.FullValue, TimeSpan.FromHours(1)));
        Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(() => router.SetPublishEventsEnabledAsync(true));

        Assert.Multiple(async () =>
        {
            Assert.That(await registry.ExistsAsync(treeName), Is.False);
            Assert.That(await registry.GetEntryAsync(treeName), Is.Null);
        });
        Assert.ThrowsAsync<InvalidOperationException>(() => router.RecoverTreeAsync());
    }

    /// <summary>
    /// <see cref="ILatticeRegistry.SetAliasAsync"/> may create a logical row, but
    /// not for a purged id: the deletion grain's writability check refuses first.
    /// </summary>
    [Test]
    public async Task SetAlias_refuses_a_purged_logical_id_and_leaves_no_row()
    {
        var treeName = $"alias-purged-{Guid.NewGuid():N}";
        var targetName = $"alias-target-{Guid.NewGuid():N}";
        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        await _cluster.GrainFactory.GetGrain<ILattice>(targetName).SetAsync("k", Encoding.UTF8.GetBytes("v"));
        await PurgeAsync(treeName);

        Assert.ThrowsAsync<InvalidOperationException>(() => registry.SetAliasAsync(treeName, targetName));

        Assert.Multiple(async () =>
        {
            Assert.That(await registry.ExistsAsync(treeName), Is.False);
            Assert.That(await registry.GetEntryAsync(treeName), Is.Null);
        });
    }

    private async Task PurgeAsync(string treeName)
    {
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);
        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();
        Assert.That(await _cluster.GrainFactory.GetLatticeRegistry().ExistsAsync(treeName), Is.False,
            "precondition: the purge removed the row");
    }

    private async Task AssertFamilyRefusesWithoutCreatingAsync(RegistryVerbFamily family, string treeName)
    {
        var registry = _cluster.GrainFactory.GetLatticeRegistry();
        var calls = family switch
        {
            RegistryVerbFamily.ShardTopology => new Func<Task>[]
            {
                () => registry.SetShardMapAsync(treeName, ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2)),
                () => registry.ReassignSlotsAsync(treeName, [0], 1, ShardMap.CreateDefault(LatticeConstants.DefaultVirtualShardCount, 2)),
                () => registry.AllocateNextShardIndexAsync(treeName, 1),
            },
            RegistryVerbFamily.Configuration => new Func<Task>[]
            {
                () => registry.SetPublishEventsAsync(treeName, true),
                () => registry.SetHistoryRetentionAsync(treeName, HistoryRetentionMode.FullValue, TimeSpan.FromHours(1)),
                () => registry.SetMaintainProjectionDigestAsync(treeName, false),
                () => registry.SetMaxCacheValueBytesAsync(treeName, 4096),
                () => registry.SetWalMaxRetainedBytesAsync(treeName, 4096),
                () => registry.RaiseReplicationFloorEpochAsync(treeName, 1),
            },
            RegistryVerbFamily.DigestLatch => new Func<Task>[]
            {
                () => registry.LatchProjectionDigestPermanentlyDisabledAsync(treeName),
            },
            RegistryVerbFamily.WalPlacement => new Func<Task>[]
            {
                () => registry.UpdateWalPlacementAsync(treeName, 0, 0, "dedicated"),
                () => registry.UpdateWalPlacementAsync(treeName, 0, [(0, "dedicated")]),
                () => registry.RaiseWalMoveFencesAsync(treeName, 0, [0], "move-a", TimeSpan.FromMinutes(1), renew: false),
                () => registry.ReleaseWalMoveFenceAsync(treeName, 0, "move-a", onlyIfExpired: false),
                () => registry.FlipFencedWalPlacementAsync(treeName, 0, [(0, "dedicated")], "move-a"),
            },
            _ => throw new ArgumentOutOfRangeException(nameof(family)),
        };

        foreach (var call in calls)
        {
            var ex = Assert.ThrowsAsync<LatticeTreeNotRegisteredException>(() => call());
            Assert.That(ex!.TreeId, Is.EqualTo(treeName));
        }

        Assert.Multiple(async () =>
        {
            Assert.That(await registry.ExistsAsync(treeName), Is.False, $"a {family} verb created the row");
            Assert.That(await registry.GetEntryAsync(treeName), Is.Null, $"a {family} verb created the row");
        });
    }
}
