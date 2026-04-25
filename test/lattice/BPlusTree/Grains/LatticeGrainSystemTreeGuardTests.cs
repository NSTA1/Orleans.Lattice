using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the unbypassable public-surface guard that rejects any
/// <see cref="ILattice"/> call whose primary key starts with
/// <see cref="LatticeConstants.SystemTreePrefix"/>. Every read, write,
/// scan, cursor, and tree-lifecycle method on the public surface must
/// throw <see cref="InvalidOperationException"/> for both the registry
/// tree id (<see cref="LatticeConstants.RegistryTreeId"/>) and any
/// id beginning with the replog prefix
/// (<see cref="LatticeConstants.ReplogTreePrefix"/>). Internal callers
/// that legitimately bootstrap system trees go through
/// <see cref="ISystemLattice"/>, which is exercised by
/// <c>SystemLatticeIntegrationTests</c>.
/// </summary>
[TestFixture]
public class LatticeGrainSystemTreeGuardTests
{
    private static readonly string[] ReservedIds =
    [
        LatticeConstants.RegistryTreeId,
        "_lattice_replog_foo",
        "_lattice_custom",
    ];

    private static (LatticeGrain grain, IGrainFactory factory) CreateGrain(string treeId)
    {
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("lattice", treeId));

        var grainFactory = Substitute.For<IGrainFactory>();
        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions());

        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.ResolveAsync(Arg.Any<string>()).Returns(c => Task.FromResult(c.Arg<string>()));
        registry.GetShardMapAsync(Arg.Any<string>()).Returns(Task.FromResult<ShardMap?>(null));
        registry.GetEntryAsync(Arg.Any<string>()).Returns(Task.FromResult<TreeRegistryEntry?>(
            new TreeRegistryEntry { MaxLeafKeys = 128, MaxInternalChildren = 128, ShardCount = 4 }));

        var optionsResolver = TestOptionsResolver.ForFactory(grainFactory);
        var services = Substitute.For<IServiceProvider>();
        var grain = new LatticeGrain(context, grainFactory, optionsMonitor, optionsResolver, services, NullLogger<LatticeGrain>.Instance);
        return (grain, grainFactory);
    }

    private static ILattice CreateGrainFor(string treeId) => CreateGrain(treeId).grain;

    [TestCaseSource(nameof(ReservedIds))]
    public void GetAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).GetAsync("k"));

    [TestCaseSource(nameof(ReservedIds))]
    public void GetWithVersionAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).GetWithVersionAsync("k"));

    [TestCaseSource(nameof(ReservedIds))]
    public void ExistsAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).ExistsAsync("k"));

    [TestCaseSource(nameof(ReservedIds))]
    public void GetManyAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).GetManyAsync(["k"]));

    [TestCaseSource(nameof(ReservedIds))]
    public void SetAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).SetAsync("k", [1]));

    [TestCaseSource(nameof(ReservedIds))]
    public void SetAsync_ttl_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).SetAsync("k", [1], TimeSpan.FromMinutes(1)));

    [TestCaseSource(nameof(ReservedIds))]
    public void SetIfVersionAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).SetIfVersionAsync("k", [1], HybridLogicalClock.Zero));

    [TestCaseSource(nameof(ReservedIds))]
    public void GetOrSetAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).GetOrSetAsync("k", [1]));

    [TestCaseSource(nameof(ReservedIds))]
    public void SetManyAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).SetManyAsync([new("k", [1])]));

    [TestCaseSource(nameof(ReservedIds))]
    public void SetManyAtomicAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).SetManyAtomicAsync([new("k", [1])]));

    [TestCaseSource(nameof(ReservedIds))]
    public void SetManyAtomicAsync_with_operation_id_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).SetManyAtomicAsync([new("k", [1])], "op-1"));

    [TestCaseSource(nameof(ReservedIds))]
    public void DeleteAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).DeleteAsync("k"));

    [TestCaseSource(nameof(ReservedIds))]
    public void DeleteRangeAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).DeleteRangeAsync("a", "z"));

    [TestCaseSource(nameof(ReservedIds))]
    public void CountAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).CountAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void CountPerShardAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).CountPerShardAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void DiagnoseAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).DiagnoseAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void KeysAsync_rejects_reserved_id(string treeId)
    {
        // KeysAsync returns IAsyncEnumerable synchronously via the public
        // forwarder, so the guard throws on the call, not on first MoveNext.
        var grain = CreateGrainFor(treeId);
        Assert.Throws<InvalidOperationException>(() => grain.KeysAsync());
    }

    [TestCaseSource(nameof(ReservedIds))]
    public void EntriesAsync_rejects_reserved_id(string treeId)
    {
        // EntriesAsync returns IAsyncEnumerable synchronously via the public
        // forwarder, so the guard throws on the call, not on first MoveNext.
        var grain = CreateGrainFor(treeId);
        Assert.Throws<InvalidOperationException>(() => grain.EntriesAsync());
    }

    [TestCaseSource(nameof(ReservedIds))]
    public void GetRoutingAsync_does_not_throw_for_reserved_id(string treeId)
    {
        // GetRoutingAsync is intentionally NOT guarded — it is called by
        // internal coordinator grains for system trees before dispatching
        // further internal calls. It does not read or mutate user data;
        // shard grains enforce the real boundary on reads/writes.
        var grain = CreateGrainFor(treeId);
        Assert.DoesNotThrowAsync(() => grain.GetRoutingAsync());
    }

    [TestCaseSource(nameof(ReservedIds))]
    public void OpenKeyCursorAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).OpenKeyCursorAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void OpenEntryCursorAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).OpenEntryCursorAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void OpenDeleteRangeCursorAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).OpenDeleteRangeCursorAsync("a", "z"));

    [TestCaseSource(nameof(ReservedIds))]
    public void NextKeysAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).NextKeysAsync("c", 10));

    [TestCaseSource(nameof(ReservedIds))]
    public void NextEntriesAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).NextEntriesAsync("c", 10));

    [TestCaseSource(nameof(ReservedIds))]
    public void DeleteRangeStepAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).DeleteRangeStepAsync("c", 10));

    [TestCaseSource(nameof(ReservedIds))]
    public void CloseCursorAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).CloseCursorAsync("c"));

    [TestCaseSource(nameof(ReservedIds))]
    public void BulkLoadAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).BulkLoadAsync([new("k", [1])]));

    [TestCaseSource(nameof(ReservedIds))]
    public void DeleteTreeAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).DeleteTreeAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void RecoverTreeAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).RecoverTreeAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void PurgeTreeAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).PurgeTreeAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void ResizeAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).ResizeAsync(64, 64));

    [TestCaseSource(nameof(ReservedIds))]
    public void UndoResizeAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).UndoResizeAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void SnapshotAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).SnapshotAsync("dest", SnapshotMode.Offline));

    [TestCaseSource(nameof(ReservedIds))]
    public void TreeExistsAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).TreeExistsAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void GetAllTreeIdsAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).GetAllTreeIdsAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void SetPublishEventsEnabledAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).SetPublishEventsEnabledAsync(true));

    [TestCaseSource(nameof(ReservedIds))]
    public void MergeAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).MergeAsync("other"));

    [TestCaseSource(nameof(ReservedIds))]
    public void IsMergeCompleteAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).IsMergeCompleteAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void IsSnapshotCompleteAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).IsSnapshotCompleteAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void IsResizeCompleteAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).IsResizeCompleteAsync());

    [TestCaseSource(nameof(ReservedIds))]
    public void ReshardAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).ReshardAsync(8));

    [TestCaseSource(nameof(ReservedIds))]
    public void IsReshardCompleteAsync_rejects_reserved_id(string treeId)
        => Assert.ThrowsAsync<InvalidOperationException>(
            () => CreateGrainFor(treeId).IsReshardCompleteAsync());

    [Test]
    public void Guard_allows_tree_ids_that_merely_embed_system_prefix()
    {
        // Only a leading match is rejected; embedded prefixes are fine.
        var (grain, factory) = CreateGrain("my_lattice_table");
        var shardRoot = Substitute.For<IShardRootGrain>();
        factory.GetGrain<IShardRootGrain>(Arg.Any<string>(), Arg.Any<string>())
            .Returns(shardRoot);

        Assert.DoesNotThrowAsync(() => grain.GetAsync("k"));
    }
}
