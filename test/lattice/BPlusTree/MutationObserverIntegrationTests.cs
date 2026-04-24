using System.Text;
using NUnit.Framework;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// End-to-end integration tests covering the <see cref="IMutationObserver"/>
/// hook through the full <c>ILattice</c> → <c>LatticeGrain</c> →
/// <c>ShardRootGrain</c> → <c>BPlusLeafGrain</c> pipeline.
/// </summary>
[TestFixture]
public sealed class MutationObserverIntegrationTests
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
        // all with the same Key / EndExclusiveKey — this is the documented
        // per-shard fan-out contract that replication consumers dedup on.
        LatticeMutation[] ranges = [];
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (DateTime.UtcNow < deadline)
        {
            ranges = MutationObserverClusterFixture.Captured
                .Where(m => m.Kind == MutationKind.DeleteRange && m.TreeId == "obs-e2e-range-populated")
                .ToArray();
            if (ranges.Length >= 1) break;
            await Task.Delay(25);
        }

        // Drain remaining shards for another brief window so the upper-bound
        // assertion sees every publish that would arrive.
        await Task.Delay(250);
        ranges = MutationObserverClusterFixture.Captured
            .Where(m => m.Kind == MutationKind.DeleteRange && m.TreeId == "obs-e2e-range-populated")
            .ToArray();

        Assert.That(ranges.Length, Is.GreaterThanOrEqualTo(1));
        Assert.That(ranges.Length, Is.LessThanOrEqualTo(MutationObserverClusterFixture.TestShardCount));
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

        var deadline = DateTime.UtcNow.AddSeconds(5);
        List<LatticeMutation> mine;
        while (true)
        {
            mine = MutationObserverClusterFixture.Captured
                .Where(m => m.Kind == MutationKind.Set && m.Key == "k" && m.TreeId == "obs-e2e-hlc")
                .ToList();
            if (mine.Count >= 2 || DateTime.UtcNow >= deadline) break;
            await Task.Delay(25);
        }

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
}
