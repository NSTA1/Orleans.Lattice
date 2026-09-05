namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── TreeExistsAsync ─────────────────────────────────────────────────

    [Test]
    public async Task TreeExistsAsync_returns_false_for_unwritten_tree()
    {
        var tree = Tree("pac-tree-notexists");
        Assert.That(await tree.TreeExistsAsync(), Is.False);
    }

    [Test]
    public async Task TreeExistsAsync_returns_true_after_first_write()
    {
        var tree = Tree("pac-tree-exists");
        await tree.SetAsync("k", Bytes("v"));
        Assert.That(await tree.TreeExistsAsync(), Is.True);
    }

    // ── GetAllTreeIdsAsync ──────────────────────────────────────────────

    [Test]
    public async Task GetAllTreeIdsAsync_includes_user_tree_ids_after_writes()
    {
        var treeId = "pac-tree-listed-" + Guid.NewGuid().ToString("N")[..8];
        var tree = Tree(treeId);
        await tree.SetAsync("k", Bytes("v"));

        var allIds = await tree.GetAllTreeIdsAsync();
        Assert.That(allIds, Contains.Item(treeId));
    }

    [Test]
    public async Task GetAllTreeIdsAsync_excludes_system_internal_trees()
    {
        var treeId = "pac-tree-no-internal-" + Guid.NewGuid().ToString("N")[..8];
        var tree = Tree(treeId);
        await tree.SetAsync("k", Bytes("v"));

        var allIds = await tree.GetAllTreeIdsAsync();
        Assert.That(allIds, Has.None.StartsWith("_lattice_"));
    }

    [Test]
    public async Task GetAllTreeIdsAsync_returns_ids_in_sorted_order()
    {
        // Use a deterministic shared prefix so this test only asserts ordering
        // among trees this test owns (other tests in the suite create unrelated ids).
        var prefix = "pac-tree-sorted-" + Guid.NewGuid().ToString("N")[..8] + "-";
        await Tree(prefix + "c").SetAsync("k", Bytes("v"));
        await Tree(prefix + "a").SetAsync("k", Bytes("v"));
        await Tree(prefix + "b").SetAsync("k", Bytes("v"));

        var allIds = await Tree(prefix + "a").GetAllTreeIdsAsync();
        var subset = allIds.Where(id => id.StartsWith(prefix, StringComparison.Ordinal)).ToList();
        Assert.That(subset, Is.EqualTo(new[] { prefix + "a", prefix + "b", prefix + "c" }));
    }

    // ── DeleteTreeAsync / RecoverTreeAsync / PurgeTreeAsync ─────────────

    [Test]
    public async Task DeleteTreeAsync_makes_subsequent_reads_throw()
    {
        var tree = Tree("pac-tree-delete-then-read");
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteTreeAsync();
        Assert.That(
            async () => await tree.GetAsync("k"),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task DeleteTreeAsync_makes_subsequent_writes_throw()
    {
        var tree = Tree("pac-tree-delete-then-write");
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteTreeAsync();
        Assert.That(
            async () => await tree.SetAsync("k2", Bytes("v2")),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task DeleteTreeAsync_is_idempotent_on_already_deleted_tree()
    {
        var tree = Tree("pac-tree-delete-twice");
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteTreeAsync();
        await tree.DeleteTreeAsync();

        // Idempotent means the repeat delete left the tree deleted, not merely
        // that it did not throw: a second delete that silently resurrected the
        // tree would also pass a bare not-throws check.
        Assert.That(
            async () => await tree.SetAsync("k2", Bytes("v2")),
            Throws.InstanceOf<InvalidOperationException>(),
            "a twice-deleted tree must stay deleted");
    }

    [Test]
    public async Task RecoverTreeAsync_restores_a_soft_deleted_tree()
    {
        var tree = Tree("pac-tree-recover");
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteTreeAsync();
        await tree.RecoverTreeAsync();

        Assert.That(Str(await tree.GetAsync("k")), Is.EqualTo("v"));
    }

    [Test]
    public void RecoverTreeAsync_on_live_tree_throws()
    {
        var tree = Tree("pac-tree-recover-live");
        Assert.That(
            async () => await tree.RecoverTreeAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task PurgeTreeAsync_immediately_purges_a_soft_deleted_tree()
    {
        var tree = Tree("pac-tree-purge");
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();

        // After purge, the tree no longer exists in the registry.
        Assert.That(await tree.TreeExistsAsync(), Is.False);
    }

    [Test]
    public void PurgeTreeAsync_on_live_tree_throws()
    {
        var tree = Tree("pac-tree-purge-live");
        Assert.That(
            async () => await tree.PurgeTreeAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }

    [Test]
    public async Task RecoverTreeAsync_after_purge_throws()
    {
        var tree = Tree("pac-tree-recover-after-purge");
        await tree.SetAsync("k", Bytes("v"));
        await tree.DeleteTreeAsync();
        await tree.PurgeTreeAsync();
        Assert.That(
            async () => await tree.RecoverTreeAsync(),
            Throws.InstanceOf<InvalidOperationException>());
    }
}
