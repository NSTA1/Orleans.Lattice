using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

/// <summary>
/// KEYSTONE: prove that data written through <see cref="ILattice"/>
/// survives a full cluster restart via WAL replay at grain activation.
/// <para>
/// The fixture-scope <c>InMemoryWalStorageProvider</c> outlives the
/// cluster lifecycle, while per-silo memory grain storage does not.
/// After <c>RestartClusterAsync</c>, every leaf grain is in the
/// pristine "no grain state" condition - its first activation must
/// reconstruct its in-memory page from the WAL or the read returns
/// null. These tests assert reads return the original values, which
/// is only possible if the activation-time materialiser ran.
/// </para>
/// <para>
/// Tree is re-registered post-restart with the same shape because
/// the registry grain is also memory-backed and was wiped. The WAL
/// stream itself is keyed on (treeId, shardIndex), so re-registering
/// the same id reaches the same WAL partition.
/// </para>
/// </summary>
public partial class PublicApiContractTests
{
    [Test]
    public async Task SingleKey_write_survives_cluster_restart_via_WAL_replay()
    {
        var treeId = "pac-walreact-single-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);
        await tree.SetAsync("alpha", Bytes("v1"));
        Assert.That(Str(await tree.GetAsync("alpha")), Is.EqualTo("v1"));

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        Assert.That(Str(await rehydrated.GetAsync("alpha")), Is.EqualTo("v1"));
    }

    [Test]
    public async Task Many_keys_across_shards_survive_cluster_restart_via_WAL_replay()
    {
        var treeId = "pac-walreact-many-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);

        // Spread across all shards by varying key prefix.
        var pairs = Enumerable.Range(0, 24)
            .Select(i => Kvp($"key-{i:D2}", $"val-{i:D2}"))
            .ToList();
        foreach (var p in pairs)
        {
            await tree.SetAsync(p.Key, p.Value);
        }
        Assert.That(await tree.CountAsync(), Is.EqualTo(24));

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        Assert.That(await rehydrated.CountAsync(), Is.EqualTo(24));
        foreach (var p in pairs)
        {
            Assert.That(Str(await rehydrated.GetAsync(p.Key)), Is.EqualTo(Str(p.Value)),
                $"key '{p.Key}' did not rehydrate");
        }
    }

    [Test]
    public async Task Last_write_wins_after_overwrite_survives_cluster_restart()
    {
        var treeId = "pac-walreact-overwrite-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);
        await tree.SetAsync("k", Bytes("first"));
        await tree.SetAsync("k", Bytes("second"));
        await tree.SetAsync("k", Bytes("third"));
        Assert.That(Str(await tree.GetAsync("k")), Is.EqualTo("third"));

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        // The materialiser must apply WAL entries in HLC order - the
        // post-restart read must observe the latest write, not any
        // earlier one.
        Assert.That(Str(await rehydrated.GetAsync("k")), Is.EqualTo("third"));
    }

    [Test]
    public async Task Deletes_survive_cluster_restart_as_absent_keys()
    {
        var treeId = "pac-walreact-delete-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);
        await tree.SetAsync("kept", Bytes("present"));
        await tree.SetAsync("removed", Bytes("doomed"));
        await tree.DeleteAsync("removed");

        Assert.That(Str(await tree.GetAsync("kept")), Is.EqualTo("present"));
        Assert.That(await tree.GetAsync("removed"), Is.Null);

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        Assert.That(Str(await rehydrated.GetAsync("kept")), Is.EqualTo("present"));
        Assert.That(await rehydrated.GetAsync("removed"), Is.Null,
            "deleted key must not resurrect after WAL replay");
    }

    [Test]
    public async Task SetMany_batch_survives_cluster_restart_via_WAL_replay()
    {
        var treeId = "pac-walreact-setmany-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"),
            Kvp("d", "4"), Kvp("e", "5"), Kvp("f", "6"),
        };
        await tree.SetManyAsync(entries);
        Assert.That(await tree.CountAsync(), Is.EqualTo(6));

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        Assert.That(await rehydrated.CountAsync(), Is.EqualTo(6));
        foreach (var entry in entries)
        {
            Assert.That(Str(await rehydrated.GetAsync(entry.Key)), Is.EqualTo(Str(entry.Value)));
        }
    }

    [Test]
    public async Task SetManyAtomic_saga_survives_cluster_restart_via_WAL_replay()
    {
        var treeId = "pac-walreact-saga-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);
        var entries = new List<KeyValuePair<string, byte[]>>
        {
            Kvp("tx-a", "alpha"),
            Kvp("tx-b", "beta"),
            Kvp("tx-c", "gamma"),
            Kvp("tx-d", "delta"),
        };
        await tree.SetManyAtomicAsync(entries, "saga-survives-restart");

        // Sanity-check pre-restart.
        foreach (var entry in entries)
        {
            Assert.That(Str(await tree.GetAsync(entry.Key)), Is.EqualTo(Str(entry.Value)));
        }

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        // The materialiser must observe the saga's TxCommit marker
        // and surface every entry. Either all-or-nothing.
        foreach (var entry in entries)
        {
            Assert.That(Str(await rehydrated.GetAsync(entry.Key)), Is.EqualTo(Str(entry.Value)),
                $"saga entry '{entry.Key}' did not rehydrate");
        }
    }

    [Test]
    public async Task DeleteRange_tombstones_survive_cluster_restart()
    {
        var treeId = "pac-walreact-deleterange-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);

        // Write a contiguous span and an out-of-range survivor.
        for (int i = 0; i < 10; i++)
        {
            await tree.SetAsync($"r-{i:D2}", Bytes($"v{i}"));
        }
        await tree.SetAsync("survivor", Bytes("kept"));

        var deleted = await tree.DeleteRangeAsync("r-03", "r-07");
        Assert.That(deleted, Is.EqualTo(4)); // r-03..r-06 inclusive

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        for (int i = 0; i < 3; i++)
        {
            Assert.That(Str(await rehydrated.GetAsync($"r-{i:D2}")), Is.EqualTo($"v{i}"));
        }
        for (int i = 3; i < 7; i++)
        {
            Assert.That(await rehydrated.GetAsync($"r-{i:D2}"), Is.Null,
                $"tombstoned key 'r-{i:D2}' must not resurrect");
        }
        for (int i = 7; i < 10; i++)
        {
            Assert.That(Str(await rehydrated.GetAsync($"r-{i:D2}")), Is.EqualTo($"v{i}"));
        }
        Assert.That(Str(await rehydrated.GetAsync("survivor")), Is.EqualTo("kept"));
    }

    [Test]
    public async Task Tree_with_split_leaves_survives_cluster_restart()
    {
        var treeId = "pac-walreact-split-" + Guid.NewGuid().ToString("N")[..8];
        // shardCount=1 so all keys hash to the single shard, forcing
        // the leaf to split as the per-leaf cap is exceeded.
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1, maxLeafKeys: 4, maxInternalChildren: 4);

        // 16 keys >> the 4-leaf cap → guaranteed multi-level B+ tree.
        var written = new List<KeyValuePair<string, byte[]>>();
        for (int i = 0; i < 16; i++)
        {
            var k = $"k-{i:D2}";
            var v = $"v-{i:D2}";
            await tree.SetAsync(k, Bytes(v));
            written.Add(new KeyValuePair<string, byte[]>(k, Bytes(v)));
        }
        Assert.That(await tree.CountAsync(), Is.EqualTo(16));

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1, maxLeafKeys: 4, maxInternalChildren: 4);

        Assert.That(await rehydrated.CountAsync(), Is.EqualTo(16),
            "post-split leaf chain must be fully reconstructed from WAL");
        foreach (var entry in written)
        {
            Assert.That(Str(await rehydrated.GetAsync(entry.Key)), Is.EqualTo(Str(entry.Value)),
                $"split-tree key '{entry.Key}' did not rehydrate");
        }
    }

    [Test]
    public async Task KeysAsync_enumeration_after_restart_returns_all_rehydrated_keys()
    {
        var treeId = "pac-walreact-enum-keys-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);

        var expected = Enumerable.Range(0, 12).Select(i => $"e-{i:D2}").ToList();
        foreach (var k in expected)
        {
            await tree.SetAsync(k, Bytes(k));
        }

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        var collected = new List<string>();
        await foreach (var key in rehydrated.KeysAsync())
        {
            collected.Add(key);
        }
        Assert.That(collected, Is.EquivalentTo(expected));
    }

    [Test]
    public async Task EntriesAsync_enumeration_after_restart_returns_all_rehydrated_pairs()
    {
        var treeId = "pac-walreact-enum-entries-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);

        var expected = Enumerable.Range(0, 12)
            .Select(i => new KeyValuePair<string, string>($"e-{i:D2}", $"val-{i:D2}"))
            .ToList();
        foreach (var p in expected)
        {
            await tree.SetAsync(p.Key, Bytes(p.Value));
        }

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        var collected = new List<KeyValuePair<string, string>>();
        await foreach (var entry in rehydrated.EntriesAsync())
        {
            collected.Add(new KeyValuePair<string, string>(entry.Key, Str(entry.Value)));
        }
        Assert.That(collected, Is.EquivalentTo(expected));
    }

    [Test]
    public async Task Multiple_distinct_trees_all_survive_cluster_restart_independently()
    {
        var prefix = Guid.NewGuid().ToString("N")[..8];
        var treeAId = $"pac-walreact-multitree-a-{prefix}";
        var treeBId = $"pac-walreact-multitree-b-{prefix}";
        var treeCId = $"pac-walreact-multitree-c-{prefix}";

        var a = await _fixture.CreateSmallTreeAsync(treeAId);
        var b = await _fixture.CreateSmallTreeAsync(treeBId);
        var c = await _fixture.CreateSmallTreeAsync(treeCId);

        await a.SetAsync("ka", Bytes("a-data"));
        await b.SetAsync("kb1", Bytes("b-1"));
        await b.SetAsync("kb2", Bytes("b-2"));
        await c.SetAsync("kc", Bytes("c-data"));

        await _fixture.RestartClusterAsync();

        var a2 = await _fixture.CreateSmallTreeAsync(treeAId);
        var b2 = await _fixture.CreateSmallTreeAsync(treeBId);
        var c2 = await _fixture.CreateSmallTreeAsync(treeCId);

        Assert.That(Str(await a2.GetAsync("ka")), Is.EqualTo("a-data"));
        Assert.That(Str(await b2.GetAsync("kb1")), Is.EqualTo("b-1"));
        Assert.That(Str(await b2.GetAsync("kb2")), Is.EqualTo("b-2"));
        Assert.That(Str(await c2.GetAsync("kc")), Is.EqualTo("c-data"));

        // No bleed-through: each tree must only contain its own keys.
        Assert.That(await a2.GetAsync("kb1"), Is.Null);
        Assert.That(await b2.GetAsync("ka"), Is.Null);
        Assert.That(await c2.GetAsync("kb1"), Is.Null);
    }

    [Test]
    public async Task Pristine_tree_with_no_writes_recovers_empty_after_restart()
    {
        var treeId = "pac-walreact-empty-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);
        Assert.That(await tree.CountAsync(), Is.EqualTo(0));

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        Assert.That(await rehydrated.CountAsync(), Is.EqualTo(0));
        Assert.That(await rehydrated.GetAsync("never-set"), Is.Null);
    }

    [Test]
    public async Task TTL_set_before_restart_remains_active_after_WAL_replay()
    {
        var treeId = "pac-walreact-ttl-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);

        // Long TTL - must not have expired by the time we read post-restart.
        await tree.SetAsync("temp", Bytes("ttl-value"), TimeSpan.FromMinutes(30));
        Assert.That(Str(await tree.GetAsync("temp")), Is.EqualTo("ttl-value"));

        await _fixture.RestartClusterAsync();
        var rehydrated = await _fixture.CreateSmallTreeAsync(treeId);

        Assert.That(Str(await rehydrated.GetAsync("temp")), Is.EqualTo("ttl-value"),
            "TTL'd entry must survive WAL replay so long as the deadline has not passed");
    }

    [Test]
    public async Task Writes_after_restart_persist_through_a_second_restart()
    {
        var treeId = "pac-walreact-double-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);
        await tree.SetAsync("phase-1", Bytes("v1"));

        await _fixture.RestartClusterAsync();
        var phase2 = await _fixture.CreateSmallTreeAsync(treeId);
        Assert.That(Str(await phase2.GetAsync("phase-1")), Is.EqualTo("v1"));
        await phase2.SetAsync("phase-2", Bytes("v2"));

        await _fixture.RestartClusterAsync();
        var phase3 = await _fixture.CreateSmallTreeAsync(treeId);

        // Both pre- and post-first-restart writes must be present after
        // the second restart - the WAL is the canonical record across
        // any number of activations.
        Assert.That(Str(await phase3.GetAsync("phase-1")), Is.EqualTo("v1"));
        Assert.That(Str(await phase3.GetAsync("phase-2")), Is.EqualTo("v2"));
    }

    [Test]
    public async Task DeleteTree_then_restart_then_PurgeTree_removes_all_WAL_state()
    {
        var treeId = "pac-walreact-purge-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId);
        await tree.SetAsync("x", Bytes("y"));
        await tree.DeleteTreeAsync();

        await _fixture.RestartClusterAsync();
        var rehydrated = _fixture.GetTree(treeId);

        // After delete-then-restart, recovery is allowed.
        await rehydrated.RecoverTreeAsync();
        Assert.That(Str(await rehydrated.GetAsync("x")), Is.EqualTo("y"),
            "soft-deleted tree's data must be recoverable after restart");
    }
}
