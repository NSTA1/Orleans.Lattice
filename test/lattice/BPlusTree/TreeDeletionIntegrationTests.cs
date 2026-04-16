using Orleans.Lattice.BPlusTree;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

[Collection(SmallLeafClusterCollection.Name)]
public class TreeDeletionIntegrationTests(SmallLeafClusterFixture fixture)
{
    private readonly TestCluster _cluster = fixture.Cluster;

    [Fact]
    public async Task DeleteTree_makes_tree_inaccessible()
    {
        var treeName = $"del-test-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        // Write some data.
        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        Assert.NotNull(await router.GetAsync("a"));

        // Delete the tree.
        await router.DeleteTreeAsync();

        // All operations should throw.
        await Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("a"));
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            router.SetAsync("c", Encoding.UTF8.GetBytes("3")));
        await Assert.ThrowsAsync<InvalidOperationException>(() => router.DeleteAsync("a"));
    }

    [Fact]
    public async Task DeleteTree_is_idempotent()
    {
        var treeName = $"del-idem-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("x", Encoding.UTF8.GetBytes("val"));
        await router.DeleteTreeAsync();
        await router.DeleteTreeAsync(); // Should not throw.

        await Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("x"));
    }

    [Fact]
    public async Task DeleteTree_blocks_bulk_load()
    {
        var treeName = $"del-bulk-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.DeleteTreeAsync();

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            KeyValuePair.Create("a", Encoding.UTF8.GetBytes("1")),
        };
        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            router.BulkLoadAsync(entries));
    }

    [Fact]
    public async Task DeleteTree_blocks_key_scan()
    {
        var treeName = $"del-keys-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in router.KeysAsync()) { }
        });
    }

    [Fact]
    public async Task DeleteTree_on_empty_tree_succeeds()
    {
        var treeName = $"del-empty-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.DeleteTreeAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("x"));
    }

    [Fact]
    public async Task IsDeleted_returns_correct_state()
    {
        var treeName = $"del-isdeleted-{Guid.NewGuid():N}";
        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeName);

        Assert.False(await deletion.IsDeletedAsync());

        await deletion.DeleteTreeAsync();

        Assert.True(await deletion.IsDeletedAsync());
    }

    // --- RecoverTreeAsync ---

    [Fact]
    public async Task RecoverTree_restores_access_to_data()
    {
        var treeName = $"rec-test-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.SetAsync("b", Encoding.UTF8.GetBytes("2"));
        await router.DeleteTreeAsync();

        // Tree is inaccessible.
        await Assert.ThrowsAsync<InvalidOperationException>(() => router.GetAsync("a"));

        // Recover.
        await router.RecoverTreeAsync();

        // Data is accessible again.
        Assert.Equal("1", Encoding.UTF8.GetString((await router.GetAsync("a"))!));
        Assert.Equal("2", Encoding.UTF8.GetString((await router.GetAsync("b"))!));
    }

    [Fact]
    public async Task RecoverTree_allows_new_writes_after_recovery()
    {
        var treeName = $"rec-write-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("x", Encoding.UTF8.GetBytes("old"));
        await router.DeleteTreeAsync();
        await router.RecoverTreeAsync();

        await router.SetAsync("y", Encoding.UTF8.GetBytes("new"));
        Assert.Equal("new", Encoding.UTF8.GetString((await router.GetAsync("y"))!));
    }

    [Fact]
    public async Task RecoverTree_throws_if_not_deleted()
    {
        var treeName = $"rec-notdel-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => router.RecoverTreeAsync());
    }

    [Fact]
    public async Task RecoverTree_throws_after_purge()
    {
        var treeName = $"rec-purged-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => router.RecoverTreeAsync());
    }

    // --- PurgeTreeAsync ---

    [Fact]
    public async Task PurgeTree_immediately_destroys_data()
    {
        var treeName = $"purge-test-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();

        // Deletion grain should show purge complete.
        var deletion = _cluster.GrainFactory.GetGrain<ITreeDeletionGrain>(treeName);
        Assert.True(await deletion.IsDeletedAsync());
    }

    [Fact]
    public async Task PurgeTree_throws_if_not_deleted()
    {
        var treeName = $"purge-notdel-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.SetAsync("a", Encoding.UTF8.GetBytes("1"));
        await Assert.ThrowsAsync<InvalidOperationException>(() => router.PurgeTreeAsync());
    }

    [Fact]
    public async Task PurgeTree_throws_if_already_purged()
    {
        var treeName = $"purge-twice-{Guid.NewGuid():N}";
        var router = _cluster.GrainFactory.GetGrain<ILattice>(treeName);

        await router.DeleteTreeAsync();
        await router.PurgeTreeAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(() => router.PurgeTreeAsync());
    }
}
