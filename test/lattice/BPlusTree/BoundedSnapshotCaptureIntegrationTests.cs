using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.TestingHost;
using System.Text;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Verifies that bounding the snapshot-capture fan-out
/// (<see cref="LatticeOptions.MaxConcurrentSnapshotCaptures"/>, issue #1054)
/// does not change what a snapshot cursor returns. The cluster is pinned to
/// <c>MaxConcurrentSnapshotCaptures = 1</c> so a multi-shard open captures its
/// per-shard baselines strictly serially (in waves of one), the most extreme
/// bounding. The point-in-time result set must still be complete and correct.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BoundedSnapshotCaptureIntegrationTests
{
    private const int ShardCount = 4;
    private const int MaxLeafKeys = 4;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder();
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private async Task<ILattice> CreateTreeAsync(string treeId)
    {
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = MaxLeafKeys,
            ShardCount = ShardCount,
        });
        return _cluster.Client.GetGrain<ILattice>(treeId);
    }

    [Test]
    public async Task Serially_bounded_capture_returns_the_full_snapshot()
    {
        var tree = await CreateTreeAsync($"bounded-snap-{Guid.NewGuid():N}");

        // Enough keys to spread across all four shards and span multiple
        // leaves/pages, so the bounded (one-shard-at-a-time) capture and the
        // subsequent k-way merge are both exercised.
        var expected = Enumerable.Range(0, 200)
            .Select(i => $"key-{i:D4}")
            .ToArray();
        foreach (var k in expected) await tree.SetAsync(k, Bytes(k));

        var cursorId = await tree.OpenSnapshotEntryCursorAsync();
        var collected = new List<KeyValuePair<string, byte[]>>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 16);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        Assert.That(collected.Select(kv => kv.Key), Is.EquivalentTo(expected));
        foreach (var kv in collected)
        {
            Assert.That(Encoding.UTF8.GetString(kv.Value), Is.EqualTo(kv.Key),
                "Bounded capture must still return the value present at open time.");
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(o =>
            {
                o.DigestCoalescingWindowMs = 0;
                // The invariant under test: bound the snapshot-capture
                // fan-out to a single shard at a time.
                o.MaxConcurrentSnapshotCaptures = 1;
            });
            siloBuilder.UseInMemoryReminderService();
        }
    }
}
