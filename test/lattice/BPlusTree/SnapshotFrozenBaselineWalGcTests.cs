using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Wal;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Regression for issue #913: a snapshot-isolated scan
/// (<see cref="ILattice.OpenSnapshotEntryCursorAsync"/>) must return the
/// complete, correct point-in-time key set even after
/// <see cref="ILatticeWalGc"/> has trimmed the committed WAL prefix the old
/// from-zero snapshot replay depended on.
/// <para>
/// Before the frozen-baseline fix, <c>SnapshotLeafGrain.ReplayWalAsync</c>
/// rebuilt the snapshot projection by replaying each partition's WAL from
/// offset 0 to the captured head. A snapshot scan is an ephemeral reader, not
/// a registered WAL cursor, so once every leaf checkpointed and reported its
/// cursor at the head the GC was free to trim the entire prefix. The next
/// snapshot open then read an empty / short slice and silently served an
/// empty or partial result - dropping plain LWW keys whose only write was in
/// the trimmed prefix, and, worse, mis-folding a CRDT counter (the increment
/// records were gone, so the fold restarted from zero).
/// </para>
/// <para>
/// This test drives the full public pipeline: it seeds a single shard with
/// enough keys to force in-shard B+ leaf splitting (a multi-leaf chain),
/// includes a PN-counter CRDT key with several increments and a couple of
/// LWW keys that are overwritten, waits for the leaves to checkpoint, trims
/// the WAL via <see cref="ILatticeWalGc.RunOnceAsync"/>, and only then opens
/// the snapshot cursor. The assertions cover the full key set, the latest LWW
/// values, and the exact folded counter total.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SnapshotFrozenBaselineWalGcTests
{
    private SnapshotFrozenBaselineWalGcClusterFixture _fixture = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _fixture = new SnapshotFrozenBaselineWalGcClusterFixture();
        await _fixture.InitializeAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown() => await _fixture.DisposeAsync();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    [Test]
    public async Task Snapshot_cursor_returns_full_key_set_and_counter_after_wal_prefix_trimmed()
    {
        var treeId = $"snap-frozen-walgc-{Guid.NewGuid():N}";
        var tree = await _fixture.CreateTreeAsync(treeId);

        // Seed enough keys to force the single shard to split into a chain of
        // several B+ leaves (MaxLeafKeys is pinned small by the fixture), so
        // the capture must walk a multi-leaf chain - not a single leaf.
        const int keyCount = 60;
        var expected = new Dictionary<string, string>(keyCount);
        for (var i = 0; i < keyCount; i++)
        {
            var key = $"k-{i:D4}";
            var value = $"v-{i}";
            await tree.SetAsync(key, Bytes(value));
            expected[key] = value;
        }

        // Overwrite a couple of keys so the snapshot must reflect the LATEST
        // LWW value, not the first write (which is the record most likely to
        // be trimmed away from the WAL prefix).
        await tree.SetAsync("k-0000", Bytes("v-0000-updated"));
        await tree.SetAsync("k-0030", Bytes("v-0030-updated"));
        expected["k-0000"] = "v-0000-updated";
        expected["k-0030"] = "v-0030-updated";

        // A PN-counter CRDT key whose increments land across several WAL
        // records. The fold is non-idempotent, so a trimmed prefix would make
        // the old from-zero replay restart the counter from zero.
        const string counterKey = "counter-hits";
        var counter = tree.PnCounter(counterKey);
        await counter.IncrementAsync("r1", 3);
        await counter.IncrementAsync("r1", 4);
        await counter.IncrementAsync("r2", 5);
        const long expectedCounter = 12;

        // Wait for the leaves to checkpoint and report their WAL cursors so
        // the GC is allowed to trim the committed prefix, then trim it.
        var trimmed = await _fixture.AdvanceAndTrimWalAsync(treeId);
        Assert.That(trimmed, Is.GreaterThan(0),
            "The WAL GC must trim a non-empty prefix for this regression to exercise the bug; "
            + "if nothing was trimmed the leaves never checkpointed and the test is vacuous.");

        // Open the snapshot AFTER the trim. The frozen-baseline capture must
        // produce the complete projection without depending on the trimmed
        // prefix.
        var cursorId = await tree.OpenSnapshotEntryCursorAsync();
        var collected = new List<KeyValuePair<string, byte[]>>();
        while (true)
        {
            var page = await tree.NextEntriesAsync(cursorId, 25);
            collected.AddRange(page.Entries);
            if (!page.HasMore) break;
        }
        await tree.CloseCursorAsync(cursorId);

        var collectedByKey = collected.ToDictionary(kv => kv.Key, kv => kv.Value);

        Assert.Multiple(() =>
        {
            // Every plain LWW key must be present exactly once with its latest
            // value - the core "empty / partial after trim" regression.
            var lwwKeys = collected.Select(kv => kv.Key).Where(k => k != counterKey).ToList();
            Assert.That(lwwKeys, Is.Unique,
                "Snapshot entry cursor must not surface duplicate keys.");
            Assert.That(lwwKeys, Is.EquivalentTo(expected.Keys),
                "Snapshot entry cursor must surface every seeded LWW key after the WAL prefix was trimmed.");

            foreach (var (key, value) in expected)
            {
                Assert.That(collectedByKey.ContainsKey(key), Is.True, $"Snapshot dropped key '{key}'.");
                Assert.That(Encoding.UTF8.GetString(collectedByKey[key]), Is.EqualTo(value),
                    $"Snapshot returned a stale or wrong value for '{key}'.");
            }

            // The CRDT counter must fold to the exact total - not a partial
            // sum and not zero.
            Assert.That(collectedByKey.ContainsKey(counterKey), Is.True,
                "Snapshot dropped the CRDT counter key entirely after the WAL prefix was trimmed.");
            var foldedCounter = JsonLatticeSerializer<PnCounter>.Default
                .Deserialize(collectedByKey[counterKey]).Value;
            Assert.That(foldedCounter, Is.EqualTo(expectedCounter),
                "Snapshot must fold the CRDT counter to its full total; a trimmed prefix must not "
                + "restart the fold from zero.");
        });
    }
}

/// <summary>
/// Single-silo cluster fixture for <see cref="SnapshotFrozenBaselineWalGcTests"/>.
/// Pins every tree to a single physical shard with a small leaf fan-out (so a
/// modest key count forces in-shard B+ leaf splitting), wires the in-core WAL
/// cursor registry and GC, and captures the silo's
/// <see cref="IServiceProvider"/> so the test can drive
/// <see cref="ILatticeWalGc.RunOnceAsync"/> directly.
/// </summary>
public sealed class SnapshotFrozenBaselineWalGcClusterFixture
{
    public const int SmallMaxLeafKeys = 4;

    public TestCluster Cluster { get; private set; } = null!;

    public async Task InitializeAsync()
    {
        SiloServiceProviderCaptureForWalTests.Reset();
        // Single silo so the captured IServiceProvider hosts the same WAL
        // storage / cursor-registry singletons the leaves write through.
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        Cluster = builder.Build();
        await Cluster.DeployAsync();
    }

    public async Task DisposeAsync()
    {
        await Cluster.StopAllSilosAsync();
        await Cluster.DisposeAsync();
        SiloServiceProviderCaptureForWalTests.Reset();
    }

    /// <summary>
    /// Registers <paramref name="treeId"/> with a single shard and a small
    /// leaf fan-out, then returns a grain reference. The single-shard layout
    /// keeps the whole key set in one shard so the capture must walk that
    /// shard's multi-leaf chain.
    /// </summary>
    public async Task<ILattice> CreateTreeAsync(string treeId)
    {
        ArgumentNullException.ThrowIfNull(treeId);
        var registry = Cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry
        {
            MaxLeafKeys = SmallMaxLeafKeys,
            ShardCount = 1,
        });
        return Cluster.Client.GetGrain<ILattice>(treeId);
    }

    /// <summary>
    /// Waits for the tree's leaves to checkpoint and report their WAL cursors,
    /// then runs the WAL GC once and returns the number of entries trimmed.
    /// Polls until the reported min cursor stops advancing so the trim covers
    /// as much of the committed prefix as the leaves have durably absorbed.
    /// </summary>
    public async Task<long> AdvanceAndTrimWalAsync(string treeId)
    {
        var services = SiloServiceProviderCaptureForWalTests.Captured
            ?? throw new InvalidOperationException("Silo IServiceProvider was not captured by the fixture.");
        var registry = services.GetRequiredService<IWalCursorRegistry>();

        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
        HybridLogicalClock? last = null;
        var stableObservations = 0;
        while (DateTime.UtcNow < deadline)
        {
            var min = await registry.GetMinCursorAsync(treeId);
            if (min is { } floor && floor.CompareTo(HybridLogicalClock.Zero) > 0)
            {
                if (last is { } prev && floor.CompareTo(prev) == 0)
                {
                    // The cursor has stopped advancing across two polls; the
                    // leaves have caught up, so the prefix is trim-eligible.
                    if (++stableObservations >= 2) break;
                }
                else
                {
                    stableObservations = 0;
                }
                last = floor;
            }
            await Task.Delay(100);
        }

        var gc = services.GetRequiredService<ILatticeWalGc>();
        var report = await gc.RunOnceAsync(treeId);
        return report.EntriesTrimmed;
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(o =>
            {
                o.TombstoneGracePeriod = TimeSpan.Zero;
                o.DigestCoalescingWindowMs = 0;
                // Every-write checkpoint mode: each leaf flushes its
                // projection checkpoint and reports its WAL cursor after every
                // applied write, so the committed prefix becomes trim-eligible
                // deterministically (no 5s coalescing wait) and the GC can
                // reclaim it before the snapshot is opened.
                o.MaterialiserCheckpointInterval = TimeSpan.Zero;
            });
            siloBuilder.UseInMemoryReminderService();

            siloBuilder.Services.AddSingleton<SiloServiceProviderCaptureForWalTests>();
            siloBuilder.Services.AddHostedService(
                sp => sp.GetRequiredService<SiloServiceProviderCaptureForWalTests>());
        }
    }
}
