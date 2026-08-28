using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.PublicApiContract;

public partial class PublicApiContractTests
{
    // ── Basic firing ────────────────────────────────────────────────────

    [Test]
    public async Task IMutationObserver_fires_on_SetAsync()
    {
        var treeId = "pac-obs-set-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        await tree.SetAsync("k", Bytes("v"));

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        Assert.That(captured.Any(m => m.Kind == MutationKind.Set && m.Key == "k"), Is.True);
    }

    [Test]
    public async Task IMutationObserver_fires_on_DeleteAsync()
    {
        var treeId = "pac-obs-delete-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));
        PublicApiContractClusterFixture.DrainObserverEvents();

        await tree.DeleteAsync("k");

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        Assert.That(captured.Any(m => m.Kind == MutationKind.Delete && m.Key == "k"), Is.True);
    }

    [Test]
    public async Task IMutationObserver_fires_on_DeleteRangeAsync_per_shard()
    {
        var treeId = "pac-obs-deleterange-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 2);
        await tree.SetAsync("a", Bytes("1"));
        await tree.SetAsync("b", Bytes("2"));
        PublicApiContractClusterFixture.DrainObserverEvents();

        await tree.DeleteRangeAsync("a", "z");

        // DeleteRange fires once per shard (regardless of whether the shard had keys).
        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 2, kind: MutationKind.DeleteRange);
        Assert.That(captured.All(m => m.Kind == MutationKind.DeleteRange), Is.True);
        Assert.That(captured.All(m => m.Key == "a" && m.EndExclusiveKey == "z"), Is.True);
    }

    [Test]
    public async Task IMutationObserver_carries_TreeId_and_value_for_Set()
    {
        var treeId = "pac-obs-set-shape-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        await tree.SetAsync("k", Bytes("hello"));

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        var setEvent = captured.First(m => m.Kind == MutationKind.Set);
        Assert.That(setEvent.TreeId, Is.EqualTo(treeId));
        Assert.That(setEvent.Key, Is.EqualTo("k"));
        Assert.That(setEvent.Value, Is.Not.Null);
        Assert.That(Str(setEvent.Value), Is.EqualTo("hello"));
        Assert.That(setEvent.IsTombstone, Is.False);
    }

    [Test]
    public async Task IMutationObserver_carries_IsTombstone_true_for_Delete()
    {
        var treeId = "pac-obs-tomb-flag-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        await tree.SetAsync("k", Bytes("v"));
        PublicApiContractClusterFixture.DrainObserverEvents();

        await tree.DeleteAsync("k");

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        var del = captured.First(m => m.Kind == MutationKind.Delete);
        Assert.That(del.IsTombstone, Is.True);
        Assert.That(del.Value, Is.Null);
    }

    [Test]
    public async Task IMutationObserver_categorises_user_writes_as_User()
    {
        var treeId = "pac-obs-category-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        await tree.SetAsync("k", Bytes("v"));

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        Assert.That(
            captured.Where(m => m.Kind == MutationKind.Set).Select(m => m.Category),
            Is.All.EqualTo(MutationCategory.User));
    }

    // ── Atomic-batch slots ──────────────────────────────────────────────

    [Test]
    public async Task SetManyAtomicAsync_stamps_AtomicBatchSize_on_every_emit()
    {
        var treeId = "pac-obs-atomic-size-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            Kvp("a", "1"), Kvp("b", "2"), Kvp("c", "3"),
        };
        await tree.SetManyAtomicAsync(entries);

        // Wait for all three saga emits.
        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 3, kind: MutationKind.Set);

        // Filter to Set events for our keys.
        var saga = captured
            .Where(m => m.Kind == MutationKind.Set && (m.Key == "a" || m.Key == "b" || m.Key == "c"))
            .ToList();

        Assert.That(saga, Has.Count.GreaterThanOrEqualTo(3));
        Assert.That(saga.All(m => m.AtomicBatchSize == 3), Is.True);

        // Indexes 0..2 are each present exactly once.
        var indexes = saga.Select(m => m.AtomicBatchIndex).Distinct().OrderBy(i => i).ToList();
        Assert.That(indexes, Is.EqualTo(new[] { 0, 1, 2 }));
    }

    [Test]
    public async Task SetManyAtomicAsync_shares_TransactionId_across_emits()
    {
        var treeId = "pac-obs-atomic-txid-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        var entries = new List<KeyValuePair<string, byte[]>> { Kvp("a", "1"), Kvp("b", "2") };
        await tree.SetManyAtomicAsync(entries);

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 2, kind: MutationKind.Set);
        var saga = captured.Where(m => m.Kind == MutationKind.Set && (m.Key == "a" || m.Key == "b")).ToList();
        var distinctTx = saga.Select(m => m.TransactionId).Distinct().ToList();

        Assert.That(distinctTx, Has.Count.EqualTo(1));
        Assert.That(distinctTx[0], Is.Not.EqualTo(Guid.Empty));
    }

    [Test]
    public async Task Single_key_writes_stamp_zero_AtomicBatchSize()
    {
        var treeId = "pac-obs-single-batchsize-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        await tree.SetAsync("k", Bytes("v"));

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        var setEvent = captured.First(m => m.Kind == MutationKind.Set);
        Assert.That(setEvent.AtomicBatchSize, Is.EqualTo(0));
        Assert.That(setEvent.AtomicBatchIndex, Is.EqualTo(0));
    }

    // ── LatticeOriginContext ────────────────────────────────────────────

    [Test]
    public async Task LatticeOriginContext_With_stamps_OriginClusterId_on_mutation()
    {
        var treeId = "pac-obs-origin-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        using (LatticeOriginContext.With("remote-cluster-A"))
        {
            await tree.SetAsync("k", Bytes("v"));
        }

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        var setEvent = captured.First(m => m.Kind == MutationKind.Set);
        Assert.That(setEvent.OriginClusterId, Is.EqualTo("remote-cluster-A"));
    }

    [Test]
    public async Task LatticeOriginContext_outside_scope_yields_null_origin()
    {
        var treeId = "pac-obs-noorigin-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        await tree.SetAsync("k", Bytes("v"));

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        var setEvent = captured.First(m => m.Kind == MutationKind.Set);
        Assert.That(setEvent.OriginClusterId, Is.Null);
    }

    [Test]
    public void LatticeOriginContext_With_restores_previous_value_on_dispose()
    {
        // Pure ambient-context test - no grain calls needed.
        Assert.That(LatticeOriginContext.Current, Is.Null);

        using (LatticeOriginContext.With("outer"))
        {
            Assert.That(LatticeOriginContext.Current, Is.EqualTo("outer"));
            using (LatticeOriginContext.With("inner"))
            {
                Assert.That(LatticeOriginContext.Current, Is.EqualTo("inner"));
            }
            Assert.That(LatticeOriginContext.Current, Is.EqualTo("outer"));
        }
        Assert.That(LatticeOriginContext.Current, Is.Null);
    }

    [Test]
    public void LatticeOriginContext_With_null_clears_ambient()
    {
        using (LatticeOriginContext.With("first"))
        {
            using (LatticeOriginContext.With(null))
            {
                Assert.That(LatticeOriginContext.Current, Is.Null);
            }
            Assert.That(LatticeOriginContext.Current, Is.EqualTo("first"));
        }
    }

    // ── LatticeVectorClockContext ───────────────────────────────────────

    [Test]
    public async Task LatticeVectorClockContext_With_stamps_VectorClock_on_mutation()
    {
        var treeId = "pac-obs-vc-" + Guid.NewGuid().ToString("N")[..8];
        var tree = await _fixture.CreateSmallTreeAsync(treeId, shardCount: 1);
        PublicApiContractClusterFixture.DrainObserverEvents();

        var vc = new VersionVector();
        vc.Tick("r1");

        using (LatticeVectorClockContext.With(vc))
        {
            await tree.SetAsync("k", Bytes("v"));
        }

        var captured = await CaptureMutationsForTreeAsync(treeId, expectedMin: 1);
        var setEvent = captured.First(m => m.Kind == MutationKind.Set);
        Assert.That(setEvent.VectorClock, Is.Not.Null);
        Assert.That(setEvent.VectorClock!.Entries.ContainsKey("r1"), Is.True);
    }

    [Test]
    public void LatticeVectorClockContext_With_restores_previous_on_dispose()
    {
        Assert.That(LatticeVectorClockContext.Current, Is.Null);

        var vc = new VersionVector();
        vc.Tick("r1");
        using (LatticeVectorClockContext.With(vc))
        {
            VectorClockAssert.SameFrontier(LatticeVectorClockContext.Current, vc);
        }
        Assert.That(LatticeVectorClockContext.Current, Is.Null);
    }

    // ── LatticeHlcOverrideContext ───────────────────────────────────────

    [Test]
    public void LatticeHlcOverrideContext_With_restores_previous_on_dispose()
    {
        Assert.That(LatticeHlcOverrideContext.Current, Is.Null);

        var hlc = new HybridLogicalClock { WallClockTicks = 123, Counter = 456 };
        using (LatticeHlcOverrideContext.With(hlc))
        {
            Assert.That(LatticeHlcOverrideContext.Current, Is.EqualTo(hlc));
        }
        Assert.That(LatticeHlcOverrideContext.Current, Is.Null);
    }

    [Test]
    public void LatticeHlcOverrideContext_With_null_clears_ambient()
    {
        var hlc = new HybridLogicalClock { WallClockTicks = 123, Counter = 456 };
        using (LatticeHlcOverrideContext.With(hlc))
        {
            using (LatticeHlcOverrideContext.With(null))
            {
                Assert.That(LatticeHlcOverrideContext.Current, Is.Null);
            }
            Assert.That(LatticeHlcOverrideContext.Current, Is.EqualTo(hlc));
        }
    }

    // ── LatticeAtomicBatchContext ───────────────────────────────────────

    [Test]
    public void LatticeAtomicBatchContext_With_restores_previous_on_dispose()
    {
        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);

        using (LatticeAtomicBatchContext.With((Size: 5, Index: 2)))
        {
            Assert.That(LatticeAtomicBatchContext.Current, Is.Not.Null);
            Assert.That(LatticeAtomicBatchContext.Current!.Value.Size, Is.EqualTo(5));
            Assert.That(LatticeAtomicBatchContext.Current!.Value.Index, Is.EqualTo(2));
        }
        Assert.That(LatticeAtomicBatchContext.Current, Is.Null);
    }

    // ── Helper: drain observer queue for a specific tree ────────────────

    private static async Task<List<LatticeMutation>> CaptureMutationsForTreeAsync(
        string treeId, int expectedMin, MutationKind? kind = null, TimeSpan? timeout = null)
    {
        var t = timeout ?? TimeSpan.FromSeconds(5);
        var deadline = DateTime.UtcNow + t;
        var collected = new List<LatticeMutation>();

        while (DateTime.UtcNow < deadline)
        {
            foreach (var m in PublicApiContractClusterFixture.DrainObserverEvents())
            {
                if (m.TreeId != treeId)
                {
                    continue;
                }
                if (kind is not null && m.Kind != kind)
                {
                    continue;
                }
                collected.Add(m);
            }

            if (collected.Count >= expectedMin)
            {
                return collected;
            }

            await Task.Delay(50);
        }

        return collected;
    }
}
