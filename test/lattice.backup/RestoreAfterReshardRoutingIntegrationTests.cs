using System.Text;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// Issue #4206: the tree's <c>[StatelessWorker]</c> grain caches its routing per
/// activation, and a reshard never invalidates it. The restore engine enumerates a
/// target, retained or shadow tree's shards from that routing to apply records, arm
/// or clear redirects, or purge, so a map cached before the reshard sends records to
/// shards the live map no longer routes them to, or skips the shards the reshard
/// added.
/// </summary>
/// <remarks>
/// Each test warms the routing of the tree's only activation (a single silo, calls
/// made one at a time) while the tree is empty, re-pins it to more shards through
/// the empty-tree reshard fast path, and then drives the restore step through the
/// warmed activation.
/// </remarks>
[Category("Integration")]
public sealed class RestoreAfterReshardRoutingIntegrationTests
{
    private const int InitialShards = 2;
    private const int GrownShards = 4;

    private RestoreClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new RestoreClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private ILatticeCoordinatedRestoreEngine Engine => (ILatticeCoordinatedRestoreEngine)_fixture.Restore;

    private ILatticeRegistry Registry => _fixture.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);

    private static string Key(int i) => $"k-{i:D4}";

    [Test]
    public async Task RestoreAsync_in_place_after_a_reshard_applies_by_the_live_map()
    {
        var backupId = await CaptureAsync(keyCount: 64);
        var target = await WarmThenGrowAsync($"inplace-{Guid.NewGuid():N}");

        await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(backupId, target));

        var tree = _fixture.GrainFactory.GetGrain<ILattice>(target);
        _ = await tree.GetRoutingAsync(forceRefresh: true);
        var missing = new List<string>();
        for (var i = 0; i < 64; i++)
        {
            if (await tree.GetAsync(Key(i)) is null)
            {
                missing.Add(Key(i));
            }
        }

        Assert.That(missing, Is.Empty, "every restored record must be on the shard the live map routes it to");
    }

    [Test]
    public async Task RestoreAsync_shadow_cutover_after_a_reshard_arms_every_retained_shard()
    {
        var backupId = await CaptureAsync(keyCount: 4);
        var target = await WarmThenGrowAsync($"cutover-{Guid.NewGuid():N}");

        await _fixture.Restore.RestoreAsync(new LatticeRestoreRequest(
            backupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));

        Assert.That(
            await UnarmedShardsAsync(target, Enumerable.Range(0, GrownShards), logicalTreeId: target),
            Is.Empty,
            "the cutover must arm every shard of the retained tree's live map to redirect logical traffic");
    }

    [Test]
    public async Task RevertRestoreAsync_after_the_shadow_was_resharded_arms_every_shadow_shard()
    {
        var target = $"revert-{Guid.NewGuid():N}";
        var (shadow, shadowId, added) = await BuildThenGrowShadowAsync(target);
        await Engine.CommitShadowAsync(shadow);

        await _fixture.Restore.RevertRestoreAsync(shadow);

        Assert.That(
            await UnarmedShardsAsync(shadowId, added, logicalTreeId: target),
            Is.Empty,
            "the revert must arm the shards the reshard added to the shadow, which its live map routes to");
    }

    [Test]
    public async Task DeleteShadowAsync_after_the_shadow_was_resharded_purges_every_shard()
    {
        var target = $"abandon-{Guid.NewGuid():N}";
        var (_, shadowId, added) = await BuildThenGrowShadowAsync(target);

        // A record on each shard the reshard added, written straight to the shard
        // root, so a purge that misses the shard leaves it behind.
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            foreach (var index in added)
            {
                await _fixture.GrainFactory.GetGrain<IShardRootGrain>($"{shadowId}/{index}")
                    .SetAsync("orphan", Encoding.UTF8.GetBytes("left-behind"));
            }
        }

        await Engine.DeleteShadowAsync(shadowId);

        var survivors = new List<int>();
        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            foreach (var index in added)
            {
                if (await _fixture.GrainFactory.GetGrain<IShardRootGrain>($"{shadowId}/{index}").CountAsync() > 0)
                {
                    survivors.Add(index);
                }
            }
        }

        Assert.That(survivors, Is.Empty, "the purge must reach every shard of the shadow's live map");
    }

    private async Task<string> CaptureAsync(int keyCount)
    {
        var source = $"source-{Guid.NewGuid():N}";
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(source);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync(Key(i), [(byte)i]);
        }

        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest($"capture-{source}", BackupScopeSelector.WholeTree(source)));
        return backup.BackupId;
    }

    /// <summary>
    /// Registers an empty tree, warms its activation's routing at
    /// <see cref="InitialShards"/>, and re-pins it to <see cref="GrownShards"/>.
    /// </summary>
    private async Task<string> WarmThenGrowAsync(string treeId)
    {
        await Registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = InitialShards });
        var tree = _fixture.GrainFactory.GetGrain<ILattice>(treeId);
        var warmed = await tree.GetRoutingAsync();
        Assert.That(warmed.Map.GetPhysicalShardIndices(), Has.Count.EqualTo(InitialShards), "precondition: the warmed map");

        await tree.ReshardAsync(GrownShards);
        Assert.That(
            (await Registry.GetShardMapAsync(treeId))?.GetPhysicalShardIndices(),
            Has.Count.EqualTo(GrownShards),
            "precondition: the empty tree was re-pinned");
        return treeId;
    }

    /// <summary>
    /// Builds an empty restore shadow for <paramref name="target"/> (the build warms
    /// the shadow's activation), then re-pins the shadow to two more shards, as an
    /// adaptive split of a shadow under a long build would.
    /// </summary>
    private async Task<(LatticeRestoreResult Shadow, string ShadowId, IReadOnlyList<int> Added)> BuildThenGrowShadowAsync(
        string target)
    {
        var emptySource = $"empty-{Guid.NewGuid():N}";
        await Registry.RegisterAsync(emptySource);
        var backup = await _fixture.Capture.CaptureAsync(
            new LatticeBackupCaptureRequest($"capture-{emptySource}", BackupScopeSelector.WholeTree(emptySource)));

        var shadow = await Engine.BuildShadowAsync(new LatticeRestoreRequest(
            backup.BackupId, target, scope: null, mode: LatticeRestoreMode.ShadowCutover));
        var shadowId = shadow.ShadowPhysicalTreeId!;

        var shadowTree = _fixture.GrainFactory.GetGrain<ILattice>(shadowId);
        var warmed = (await shadowTree.GetRoutingAsync()).Map.GetPhysicalShardIndices();
        await shadowTree.ReshardAsync(warmed.Count + 2);
        var grown = (await Registry.GetShardMapAsync(shadowId))!.GetPhysicalShardIndices();
        Assert.That(grown, Has.Count.EqualTo(warmed.Count + 2), "precondition: the empty shadow was re-pinned");
        return (shadow, shadowId, grown.Except(warmed).ToList());
    }

    /// <summary>
    /// Returns the shards of <paramref name="physicalTreeId"/> that serve a read
    /// routed through <paramref name="logicalTreeId"/> instead of redirecting it.
    /// </summary>
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
