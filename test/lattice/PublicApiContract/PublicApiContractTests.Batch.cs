namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── GetManyAsync ────────────────────────────────────────────────────

    [Test]
    public async Task GetManyAsync_returns_present_keys_and_omits_missing()
    {
        var tree = Tree("pac-batch-getmany-mixed");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("c", Bytes("3"));

        var result = await tree.GetManyAsync(["a", "b", "c"]);
        Assert.That(result.Keys, Is.EquivalentTo(new[] { "a", "c" }));
        Assert.That(Str(result["a"]), Is.EqualTo("1"));
        Assert.That(Str(result["c"]), Is.EqualTo("3"));
    }

    [Test]
    public async Task GetManyAsync_with_empty_input_returns_empty_dictionary()
    {
        var tree = Tree("pac-batch-getmany-empty");
        var result = await tree.GetManyAsync([]);
        Assert.That(result, Is.Empty);
    }

    [Test]
    public async Task GetManyAsync_omits_tombstoned_keys()
    {
        var tree = Tree("pac-batch-getmany-tombstone");
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("2"));
        await tree.DeleteAsync("a");

        var result = await tree.GetManyAsync(["a", "b"]);
        Assert.That(result.Keys, Is.EquivalentTo(new[] { "b" }));
    }

    // ── SetManyAsync (non-atomic) ───────────────────────────────────────

    [Test]
    public async Task SetManyAsync_writes_every_entry()
    {
        var tree = Tree("pac-batch-setmany");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"),
        };
        await tree.SetManyAsync(entries);

        Assert.That(Str(await tree.GetAsync("a")), Is.EqualTo("1"));
        Assert.That(Str(await tree.GetAsync("b")), Is.EqualTo("2"));
        Assert.That(Str(await tree.GetAsync("c")), Is.EqualTo("3"));
    }

    [Test]
    public async Task SetManyAsync_overwrites_existing_keys()
    {
        var tree = Tree("pac-batch-setmany-overwrite");
        await tree.SetAsync("a", Bytes("old"));
        await tree.SetManyAsync([Kvp("a", "new"), Kvp("b", "fresh")]);

        Assert.That(Str(await tree.GetAsync("a")), Is.EqualTo("new"));
        Assert.That(Str(await tree.GetAsync("b")), Is.EqualTo("fresh"));
    }

    [Test]
    public async Task SetManyAsync_with_empty_list_is_a_noop()
    {
        var tree = Tree("pac-batch-setmany-empty");
        await tree.SetManyAsync([]);
        Assert.That(await tree.CountAsync(), Is.EqualTo(0));
    }

    // ── SetManyAtomicAsync (saga) ───────────────────────────────────────

    [Test]
    public async Task SetManyAtomicAsync_writes_every_entry()
    {
        var tree = Tree("pac-batch-atomic");
        await tree.SetManyAtomicAsync([Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);

        Assert.That(Str(await tree.GetAsync("a")), Is.EqualTo("1"));
        Assert.That(Str(await tree.GetAsync("b")), Is.EqualTo("2"));
        Assert.That(Str(await tree.GetAsync("c")), Is.EqualTo("3"));
    }

    [Test]
    public void SetManyAtomicAsync_with_duplicate_keys_throws()
    {
        var tree = Tree("pac-batch-atomic-duplicate");
        Assert.That(
            async () => await tree.SetManyAtomicAsync(
                [Kvp("a", "1"), Kvp("a", "2")]),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task SetManyAtomicAsync_with_operationId_is_idempotent_on_replay()
    {
        var tree = Tree("pac-batch-atomic-opid");
        var opId = "op-" + Guid.NewGuid().ToString("N");
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            Kvp("a", "1"), Kvp("b", "2"),
        };
        await tree.SetManyAtomicAsync(entries, opId);
        // Second call with the same opId attaches to the original saga
        // and returns immediately (no new commits, no new mutations).
        await tree.SetManyAtomicAsync(entries, opId);

        Assert.That(Str(await tree.GetAsync("a")), Is.EqualTo("1"));
        Assert.That(Str(await tree.GetAsync("b")), Is.EqualTo("2"));
    }

    [Test]
    public void SetManyAtomicAsync_with_empty_operationId_throws()
    {
        var tree = Tree("pac-batch-atomic-empty-opid");
        Assert.That(
            async () => await tree.SetManyAtomicAsync([Kvp("a", "1")], operationId: string.Empty),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public void SetManyAtomicAsync_with_slash_in_operationId_throws()
    {
        var tree = Tree("pac-batch-atomic-slash-opid");
        Assert.That(
            async () => await tree.SetManyAtomicAsync([Kvp("a", "1")], operationId: "bad/id"),
            Throws.InstanceOf<ArgumentException>());
    }

    [Test]
    public async Task SetManyAtomicAsync_with_same_opId_and_different_keyset_throws()
    {
        var tree = Tree("pac-batch-atomic-keyset-mismatch");
        var opId = "op-" + Guid.NewGuid().ToString("N");
        await tree.SetManyAtomicAsync([Kvp("a", "1"), Kvp("b", "2")], opId);
        Assert.That(
            async () => await tree.SetManyAtomicAsync([Kvp("a", "1"), Kvp("c", "3")], opId),
            Throws.InstanceOf<InvalidOperationException>());
    }

    // ── DeleteRangeAsync ────────────────────────────────────────────────

    [Test]
    public async Task DeleteRangeAsync_tombstones_keys_in_range_and_returns_count()
    {
        var tree = Tree("pac-batch-deleterange");
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4")]);

        var count = await tree.DeleteRangeAsync("b", "d");
        Assert.That(count, Is.EqualTo(2));

        Assert.That(await tree.GetAsync("a"), Is.Not.Null);
        Assert.That(await tree.GetAsync("b"), Is.Null);
        Assert.That(await tree.GetAsync("c"), Is.Null);
        Assert.That(Str(await tree.GetAsync("d")), Is.EqualTo("4"));
    }

    [Test]
    public async Task DeleteRangeAsync_with_no_matching_keys_returns_zero()
    {
        var tree = Tree("pac-batch-deleterange-empty");
        await tree.SetAsync("a", Bytes("1"));
        var count = await tree.DeleteRangeAsync("x", "z");
        Assert.That(count, Is.EqualTo(0));
    }

    [Test]
    public async Task DeleteRangeAsync_treats_endExclusive_as_exclusive()
    {
        var tree = Tree("pac-batch-deleterange-exclusive");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2")]);
        await tree.DeleteRangeAsync("a", "b");
        Assert.That(await tree.GetAsync("a"), Is.Null);
        Assert.That(Str(await tree.GetAsync("b")), Is.EqualTo("2"));
    }

    // ── CountAsync / CountPerShardAsync ─────────────────────────────────

    [Test]
    public async Task CountAsync_returns_zero_for_empty_tree()
    {
        var tree = Tree("pac-batch-count-empty");
        Assert.That(await tree.CountAsync(), Is.EqualTo(0));
    }

    [Test]
    public async Task CountAsync_counts_only_live_keys()
    {
        var tree = Tree("pac-batch-count-live");
        await tree.SetManyAsync([Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3")]);
        await tree.DeleteAsync("b");
        Assert.That(await tree.CountAsync(), Is.EqualTo(2));
    }

    [Test]
    public async Task CountPerShardAsync_returns_one_entry_per_shard()
    {
        var tree = await _fixture.CreateSmallTreeAsync(
            "pac-batch-countpershard", shardCount: 4);
        await tree.SetManyAsync(
            [Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"), Kvp("d", "4")]);

        var perShard = await tree.CountPerShardAsync();
        Assert.That(perShard.Count, Is.EqualTo(4));
        Assert.That(perShard.Sum(), Is.EqualTo(4));
    }

    [Test]
    public async Task CountPerShardAsync_returns_zero_per_shard_for_empty_tree()
    {
        var tree = await _fixture.CreateSmallTreeAsync(
            "pac-batch-countpershard-empty", shardCount: 4);

        var perShard = await tree.CountPerShardAsync();
        Assert.That(perShard.Count, Is.EqualTo(4));
        Assert.That(perShard, Is.All.EqualTo(0));
    }
}
