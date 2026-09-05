using System.Text;
using NUnit.Framework;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration tests covering the <see cref="IMutationObserver"/>
/// hook through the full <c>ILattice</c> → <c>LatticeGrain</c> →
/// <c>ShardRootGrain</c> → <c>BPlusLeafGrain</c> pipeline.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed partial class MutationObserverIntegrationTests
{
    private MutationObserverClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _fixture = new MutationObserverClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task TearDown() => await _fixture.DisposeAsync();

    [SetUp]
    public void BeforeEach() => MutationObserverClusterFixture.Drain();

    /// <summary>
    /// Waits up to <paramref name="timeout"/> for the predicate to match at
    /// least one captured mutation, polling the process-global sink.
    /// </summary>
    private static async Task<LatticeMutation> WaitForAsync(
        Func<LatticeMutation, bool> predicate,
        TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (DateTime.UtcNow < deadline)
        {
            foreach (var m in MutationObserverClusterFixture.Captured)
            {
                if (predicate(m)) return m;
            }
            await Task.Delay(25);
        }
        Assert.Fail(
            "Timed out. Observed: " +
            string.Join(", ", MutationObserverClusterFixture.Captured.Select(m => $"{m.Kind}:{m.Key}")));
        throw new InvalidOperationException("unreachable");
    }

    /// <summary>
    /// Snapshots every captured mutation matching <paramref name="predicate"/>,
    /// in capture order. The sink is a <c>ConcurrentQueue</c>, so enumeration is
    /// safe while publishes are still arriving.
    /// </summary>
    private static List<LatticeMutation> Matching(Func<LatticeMutation, bool> predicate) =>
        MutationObserverClusterFixture.Captured.Where(predicate).ToList();

    /// <summary>
    /// Waits until at least <paramref name="atLeast"/> captured mutations match
    /// <paramref name="predicate"/> and returns them in capture order, failing
    /// with the full observed set when the deadline passes first.
    /// </summary>
    /// <remarks>
    /// Replaces the hand-rolled <c>while (DateTime.UtcNow &lt; deadline)</c>
    /// loops that fell through on timeout into a bare count assertion, which
    /// reported "expected 3 but was 1" with no indication of what the pipeline
    /// actually published.
    /// </remarks>
    private static async Task<List<LatticeMutation>> WaitForAtLeastAsync(
        Func<LatticeMutation, bool> predicate,
        int atLeast,
        TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        List<LatticeMutation> matched;
        while (true)
        {
            matched = Matching(predicate);
            if (matched.Count >= atLeast) return matched;
            if (DateTime.UtcNow >= deadline) break;
            await Task.Delay(25);
        }

        Assert.Fail(
            $"Timed out waiting for {atLeast} matching mutation(s); saw {matched.Count}. Observed: " +
            string.Join(", ", MutationObserverClusterFixture.Captured.Select(m => $"{m.Kind}:{m.TreeId}:{m.Key}")));
        throw new InvalidOperationException("unreachable");
    }

    /// <summary>
    /// Waits for at least <paramref name="atLeast"/> matching mutations and then
    /// for the matching set to stop growing, so an upper-bound or
    /// all-emits-agree assertion sees the complete per-shard fan-out.
    /// </summary>
    /// <remarks>
    /// Supersedes the fixed <c>Task.Delay(250)</c> tail drains. A fixed sleep is
    /// wrong in both directions: on a loaded agent a late shard publish lands
    /// after the window and is silently excluded (so an "every emit agrees"
    /// assertion can pass without ever seeing the disagreeing emit - a false
    /// green), while on an idle agent the full 250 ms is paid even though the
    /// fan-out completed in a few milliseconds. Quiescence is the property those
    /// assertions actually depend on, so it is what is waited for.
    /// </remarks>
    private static async Task<List<LatticeMutation>> WaitForFanOutToSettleAsync(
        Func<LatticeMutation, bool> predicate,
        int atLeast,
        TimeSpan? timeout = null)
    {
        var settled = await WaitForAtLeastAsync(predicate, atLeast, timeout);

        // Quiet period at least as long as the sleep it replaces, so the tail
        // window is never narrower than before - only adaptive.
        var quietPolls = 10;
        var stable = 0;
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(5));
        while (stable < quietPolls && DateTime.UtcNow < deadline)
        {
            await Task.Delay(25);
            var next = Matching(predicate);
            stable = next.Count == settled.Count ? stable + 1 : 0;
            settled = next;
        }

        return settled;
    }

    [Test]
    public async Task SetAsync_publishes_set_mutation_through_the_full_pipeline()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-set");
        await tree.SetAsync("user/42", Encoding.UTF8.GetBytes("alice"));

        var m = await WaitForAsync(m => m.Kind == MutationKind.Set && m.Key == "user/42");
        Assert.That(m.TreeId, Is.EqualTo("obs-e2e-set"));
        Assert.That(m.IsTombstone, Is.False);
        Assert.That(m.Value, Is.EqualTo(Encoding.UTF8.GetBytes("alice")));
    }

    [Test]
    public async Task DeleteAsync_publishes_delete_mutation_through_the_full_pipeline()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-del");
        await tree.SetAsync("k", [1]);
        await tree.DeleteAsync("k");

        var m = await WaitForAsync(m => m.Kind == MutationKind.Delete && m.Key == "k");
        Assert.That(m.TreeId, Is.EqualTo("obs-e2e-del"));
        Assert.That(m.IsTombstone, Is.True);
        Assert.That(m.Value, Is.Null);
    }

    [Test]
    public async Task DeleteRangeAsync_publishes_single_range_mutation_even_when_nothing_matched()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-range-empty");
        var deleted = await tree.DeleteRangeAsync("a", "z");

        Assert.That(deleted, Is.Zero);
        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.DeleteRange && m.TreeId == "obs-e2e-range-empty");
        Assert.That(m.Key, Is.EqualTo("a"));
        Assert.That(m.EndExclusiveKey, Is.EqualTo("z"));
        Assert.That(m.IsTombstone, Is.True);
    }

    [Test]
    public async Task DeleteRangeAsync_publishes_range_mutation_after_populated_range()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-range-populated");
        await tree.SetAsync("a1", [1]);
        await tree.SetAsync("a2", [2]);
        await tree.SetAsync("b1", [3]);

        var deleted = await tree.DeleteRangeAsync("a", "b");
        Assert.That(deleted, Is.EqualTo(2));

        // DeleteRange fires once per shard (not per user call). With a 4-shard
        // tree, we expect between 1 and ShardCount identical-payload events,
        // all with the same Key / EndExclusiveKey - this is the documented
        // per-shard fan-out contract that replication consumers dedup on.
        var ranges = await WaitForFanOutToSettleAsync(
            m => m.Kind == MutationKind.DeleteRange && m.TreeId == "obs-e2e-range-populated",
            atLeast: 1);

        Assert.That(ranges, Has.Count.GreaterThanOrEqualTo(1));
        Assert.That(ranges, Has.Count.LessThanOrEqualTo(MutationObserverClusterFixture.TestShardCount));
        Assert.That(ranges.All(r => r.Key == "a"), Is.True);
        Assert.That(ranges.All(r => r.EndExclusiveKey == "b"), Is.True);
        Assert.That(ranges.All(r => r.IsTombstone), Is.True);
    }

    [Test]
    public async Task Two_successive_sets_on_same_key_publish_two_mutations_with_monotonic_hlc()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-hlc");
        await tree.SetAsync("k", [1]);
        await tree.SetAsync("k", [2]);

        var mine = await WaitForAtLeastAsync(
            m => m.Kind == MutationKind.Set && m.Key == "k" && m.TreeId == "obs-e2e-hlc",
            atLeast: 2);

        Assert.That(mine, Has.Count.EqualTo(2));
        Assert.That(
            mine[1].Timestamp.CompareTo(mine[0].Timestamp),
            Is.GreaterThan(0),
            "HLC must be strictly monotonic across successive sets of the same key.");
        Assert.That(mine[0].Value, Is.EqualTo(new byte[] { 1 }));
        Assert.That(mine[1].Value, Is.EqualTo(new byte[] { 2 }));
    }

    [Test]
    public async Task SetAsync_stamps_OriginClusterId_end_to_end_through_pipeline()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-origin");

        using (LatticeOriginContext.With("cluster-peer"))
        {
            await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        }

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.Key == "k" && m.TreeId == "obs-e2e-origin");
        Assert.That(m.OriginClusterId, Is.EqualTo("cluster-peer"));
    }

    [Test]
    public async Task SetAsync_publishes_null_origin_when_context_unset()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-origin-null");

        await tree.SetAsync("k", [1]);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.Key == "k" && m.TreeId == "obs-e2e-origin-null");
        Assert.That(m.OriginClusterId, Is.Null);
    }

    [Test]
    public async Task SetAsync_stamps_VectorClock_end_to_end_through_pipeline()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-vc");

        var vc = new Orleans.Lattice.VersionVector();
        vc.Tick("cluster-peer");

        using (LatticeVectorClockContext.With(vc))
        {
            await tree.SetAsync("k", Encoding.UTF8.GetBytes("v"));
        }

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.Key == "k" && m.TreeId == "obs-e2e-vc");
        Assert.That(m.VectorClock, Is.Not.Null);
        Assert.That(m.VectorClock!.Entries.ContainsKey("cluster-peer"), Is.True);
    }

    [Test]
    public async Task SetAsync_publishes_null_VectorClock_when_context_unset()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-vc-null");

        await tree.SetAsync("k", [1]);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.Key == "k" && m.TreeId == "obs-e2e-vc-null");
        Assert.That(m.VectorClock, Is.Null);
    }

    [Test]
    public async Task SetAsync_publishes_non_empty_TransactionId_per_call()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-tx-single");

        await tree.SetAsync("k", [1]);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.Key == "k" && m.TreeId == "obs-e2e-tx-single");
        Assert.That(m.TransactionId, Is.Not.EqualTo(Guid.Empty),
            "Public ILattice.SetAsync must mint a fresh transaction id when the caller did not supply one.");
    }

    [Test]
    public async Task Two_successive_SetAsync_calls_produce_distinct_TransactionIds()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-tx-distinct");

        await tree.SetAsync("k1", [1]);
        await tree.SetAsync("k2", [2]);

        var m1 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.Key == "k1" && m.TreeId == "obs-e2e-tx-distinct");
        var m2 = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.Key == "k2" && m.TreeId == "obs-e2e-tx-distinct");

        Assert.That(m1.TransactionId, Is.Not.EqualTo(Guid.Empty));
        Assert.That(m2.TransactionId, Is.Not.EqualTo(Guid.Empty));
        Assert.That(m1.TransactionId, Is.Not.EqualTo(m2.TransactionId),
            "Single-key writes must each get a fresh transaction id; ids must not be shared across separate user calls.");
    }

    [Test]
    public async Task DeleteRangeAsync_per_shard_emits_share_one_TransactionId()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-tx-range");
        await tree.SetAsync("a1", [1]);
        await tree.SetAsync("a2", [2]);
        await tree.SetAsync("b1", [3]);

        var deleted = await tree.DeleteRangeAsync("a", "b");
        Assert.That(deleted, Is.EqualTo(2));

        // Wait for at least one range emit and then for the per-shard fan-out
        // to settle, so the "every emit shares one id" assertion below is made
        // against the complete set rather than whichever shards happened to
        // land inside a fixed window.
        var ranges = await WaitForFanOutToSettleAsync(
            m => m.Kind == MutationKind.DeleteRange && m.TreeId == "obs-e2e-tx-range",
            atLeast: 1);

        Assert.That(ranges, Is.Not.Empty);
        var first = ranges[0].TransactionId;
        Assert.That(first, Is.Not.EqualTo(Guid.Empty));
        Assert.That(ranges.All(r => r.TransactionId == first), Is.True,
            "Every per-shard DeleteRange emit produced by one user DeleteRangeAsync call must share the same transaction id.");
    }

    [Test]
    public async Task SetManyAtomicAsync_emits_share_a_single_TransactionId()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-tx-saga");

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", [1]),
            new("k2", [2]),
            new("k3", [3]),
        };
        await tree.SetManyAtomicAsync(entries);

        // Wait until all three Set events are captured.
        var mine = await WaitForAtLeastAsync(
            m => m.Kind == MutationKind.Set && m.TreeId == "obs-e2e-tx-saga",
            atLeast: 3);

        Assert.That(mine, Has.Count.EqualTo(3));
        var first = mine[0].TransactionId;
        Assert.That(first, Is.Not.EqualTo(Guid.Empty));
        Assert.That(mine.All(m => m.TransactionId == first), Is.True,
            "Every per-key Set emit produced by one SetManyAtomicAsync saga must share the same transaction id.");
    }

    [Test]
    public async Task SetManyAsync_emits_share_a_single_TransactionId()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-tx-many");

        var entries = new List<KeyValuePair<string, byte[]>>
        {
            new("k1", [1]),
            new("k2", [2]),
            new("k3", [3]),
        };
        await tree.SetManyAsync(entries);

        var mine = await WaitForAtLeastAsync(
            m => m.Kind == MutationKind.Set && m.TreeId == "obs-e2e-tx-many",
            atLeast: 3);

        Assert.That(mine, Has.Count.EqualTo(3));
        var first = mine[0].TransactionId;
        Assert.That(first, Is.Not.EqualTo(Guid.Empty));
        Assert.That(mine.All(m => m.TransactionId == first), Is.True,
            "Every per-key Set emit produced by a single SetManyAsync user call must share the same transaction id.");
    }

    [Test]
    public async Task SetAsync_publishes_User_category_through_full_pipeline()
    {
        var tree = await _fixture.CreateTreeAsync("obs-e2e-category");
        await tree.SetAsync("k", [1]);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.Set && m.Key == "k" && m.TreeId == "obs-e2e-category");
        Assert.That(m.Category, Is.EqualTo(MutationCategory.User),
            "Public ILattice write paths must default to User category - no internal site should be wrapping them in a maintenance scope.");
    }

    // ------------------------------------------------------------------
    // Replication apply seam preserves VectorClock end-to-end
    // ------------------------------------------------------------------
    //
    // Set/Delete apply paths route through IShardRootGrain.MergeManyAsync.
    // That merge path now publishes per-key Set/Delete observer events (so a
    // replicated reserved-tree write drives the receiver's snapshot rebuild),
    // but VectorClock preservation for Set/Delete is still asserted in
    // LatticeGrainReplicationApplyTests via the raw LwwEntry. DeleteRange
    // walks the leaf chain and fires a per-shard observer event, so we verify
    // VectorClock preservation here against the captured payload.

    [Test]
    public async Task ApplyDeleteRangeAsync_with_source_vector_clock_stamps_per_shard_observer_payload()
    {
        const string treeId = "obs-e2e-rapply-range-vc";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("a", [1]);
        await tree.SetAsync("m", [2]);

        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeId);
        var vc = new VersionVector();
        vc.Tick("site-x");
        vc.Tick("site-y");

        // Drain prior captures so we observe only the range publish.
        MutationObserverClusterFixture.Drain();

        await apply.ApplyDeleteRangeAsync("a", "z", HybridLogicalClock.Zero, "site-x", sourceVectorClock: vc);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.DeleteRange && m.TreeId == treeId);
        Assert.Multiple(() =>
        {
            Assert.That(m.OriginClusterId, Is.EqualTo("site-x"));
            Assert.That(m.VectorClock, Is.Not.Null);
            Assert.That(m.VectorClock!.GetClock("site-x"), Is.EqualTo(vc.GetClock("site-x")));
            Assert.That(m.VectorClock!.GetClock("site-y"), Is.EqualTo(vc.GetClock("site-y")));
        });
    }

    [Test]
    public async Task ApplyDeleteRangeAsync_with_null_vector_clock_emits_null_on_observer_payload()
    {
        const string treeId = "obs-e2e-rapply-range-vc-null";
        var tree = await _fixture.CreateTreeAsync(treeId);
        await tree.SetAsync("a", [1]);

        var apply = _fixture.Cluster.Client.GetGrain<IReplicationApplyGrain>(treeId);

        MutationObserverClusterFixture.Drain();

        await apply.ApplyDeleteRangeAsync("a", "z", HybridLogicalClock.Zero, "site-x", sourceVectorClock: null);

        var m = await WaitForAsync(m =>
            m.Kind == MutationKind.DeleteRange && m.TreeId == treeId);
        Assert.That(m.VectorClock, Is.Null);
    }
}
