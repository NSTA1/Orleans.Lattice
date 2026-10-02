using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Issue #4250: routing reads the shard map under the logical tree id, while a
/// restore shadow is built by routing under its own map. A shadow-cutover restore
/// that swapped only the alias left the logical tree routing by the map of the tree
/// it replaced, so every key the shadow placed on a shard that map does not route it
/// to read back as absent. The cutover carries the shadow's map onto the logical
/// entry, as a resize does (#3880), and a revert carries the replaced tree's map back.
/// </summary>
[Category("Integration")]
public sealed class ShadowCutoverShardMapIntegrationTests
{
    private const int InitialShards = 2;
    private const int GrownShards = 4;
    private const int KeyCount = 64;

    private RestoreClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new RestoreClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeRegistry Registry => _fixture.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private static string Key(int i) => $"k-{i:D4}";

    [Test]
    public async Task RestoreAsync_shadow_cutover_of_a_resharded_tree_keeps_every_key_readable()
    {
        var target = await RegisterAndGrowAsync($"cutover-map-{Guid.NewGuid():N}");
        var backupId = await WriteAndCaptureAsync(target);

        await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(await MissingKeysAsync(target), Is.Empty,
            "after a shadow cutover the logical tree must route by the map the shadow was built under");
    }

    [Test]
    public async Task RevertRestoreAsync_of_a_resharded_tree_keeps_every_original_key_readable()
    {
        var target = await RegisterAndGrowAsync($"revert-map-{Guid.NewGuid():N}");
        var backupId = await WriteAndCaptureAsync(target);

        var restore = await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        await _fixture.Restore.RevertRestoreAsync(restore);

        Assert.That(await MissingKeysAsync(target), Is.Empty,
            "a revert must carry the replaced tree's map back with the alias");
    }

    [Test]
    public async Task RestoreAsync_shadow_cutover_of_a_resharded_aliased_tree_keeps_every_key_readable_and_reverts()
    {
        var (target, _) = await RegisterAliasedAndGrowAsync();
        var backupId = await WriteAndCaptureAsync(target);

        var restore = await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        Assert.That(await MissingKeysAsync(target), Is.Empty,
            "after a shadow cutover the logical tree must route by the map the shadow was built under");

        await _fixture.Restore.RevertRestoreAsync(restore);
        Assert.That(await MissingKeysAsync(target), Is.Empty,
            "a revert onto an aliased physical tree must carry its map back with the alias");
    }

    [Test]
    public async Task RestoreAsync_shadow_cutover_of_a_resharded_aliased_tree_arms_every_retained_shard()
    {
        var (target, physical) = await RegisterAliasedAndGrowAsync();
        var backupId = await WriteAndCaptureAsync($"source-{Guid.NewGuid():N}", keyCount: 4);

        await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(
            await UnarmedShardsAsync(physical, Enumerable.Range(0, GrownShards), logicalTreeId: target),
            Is.Empty,
            "the reshard of an aliased tree routes its physical tree by the logical map, so every shard of that map must be armed");
    }

    private async Task<string> RegisterAndGrowAsync(string treeId)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = InitialShards });
        await _fixture.GrainFactory.GetGrain<ILattice>(treeId).ReshardAsync(GrownShards);
        Assert.That(
            (await Registry.GetShardMapAsync(treeId))?.GetPhysicalShardIndices(),
            Has.Count.EqualTo(GrownShards),
            "precondition: the empty tree was re-pinned");
        return treeId;
    }

    /// <summary>
    /// Registers a logical tree aliased onto an empty physical tree pinned at
    /// <see cref="InitialShards"/> and stamped as a restore shadow of it, as a prior
    /// shadow-cutover restore leaves it, then reshards it through the logical id,
    /// which writes the grown map under the logical entry only.
    /// </summary>
    private async Task<(string Logical, string Physical)> RegisterAliasedAndGrowAsync()
    {
        var logical = $"aliased-{Guid.NewGuid():N}";
        var physical = $"{logical}-physical";
        await Registry.RegisterAsync(physical, new TreeRegistryEntry
        {
            ShardCount = InitialShards,
            RestoreShadowOfTreeId = logical,
            DerivedFrom = logical,
        });
        await Registry.RegisterAsync(logical, new TreeRegistryEntry { ShardCount = InitialShards });
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            await Registry.SetAliasAsync(logical, physical);
        }

        await RegisterAndGrowAsync(logical);
        Assert.That(await Registry.GetShardMapAsync(physical), Is.Null,
            "precondition: the reshard wrote no map under the physical id");
        return (logical, physical);
    }

    private Task<string> WriteAndCaptureAsync(string treeId) => WriteAndCaptureAsync(treeId, KeyCount);

    private async Task<string> WriteAndCaptureAsync(string treeId, int keyCount)
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync(Key(i), [(byte)i]);
        }

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest($"capture-{treeId}-{Guid.NewGuid():N}", BackupScopeSelector.WholeTree(treeId)));
        return backup.BackupId;
    }

    private async Task<List<string>> MissingKeysAsync(string treeId)
    {
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        _ = await tree.GetRoutingAsync(forceRefresh: true);
        var missing = new List<string>();
        for (var i = 0; i < KeyCount; i++)
        {
            if (await tree.GetAsync(Key(i)) is null)
            {
                missing.Add(Key(i));
            }
        }

        return missing;
    }

    private async Task<List<int>> UnarmedShardsAsync(string physicalTreeId, IEnumerable<int> shards, string logicalTreeId)
    {
        var unarmed = new List<int>();
        RequestContext.Set(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey, logicalTreeId);
        try
        {
            using (LatticeAccessGateContext.EnterSystemOrigin())
            {
                foreach (var index in shards)
                {
                    try
                    {
                        await _fixture.GrainFactory.GetGrain<IShardRootGrain>($"{physicalTreeId}/{index}").GetAsync("probe");
                        unarmed.Add(index);
                    }
                    catch (StaleTreeRoutingException)
                    {
                    }
                }
            }
        }
        finally
        {
            RequestContext.Remove(LatticeEventConstants.RoutedLogicalTreeIdRequestContextKey);
        }

        return unarmed;
    }
}
