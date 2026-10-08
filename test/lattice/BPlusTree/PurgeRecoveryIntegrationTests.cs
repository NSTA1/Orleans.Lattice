using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Tests.Wal;
using Orleans.Runtime;
using Orleans.Storage;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Real-grain detectors for issue #4700: recovering a tree whose purge was
/// interrupted must re-create, empty, exactly the leaves the purge cleared - every
/// one of them, however the purge was interrupted and however many there are - and
/// no leaf the purge never reached.
/// <para>
/// Each scenario deletes a tree and runs the shard's real <c>PurgeAsync</c>,
/// interrupting it with a call filter that fails one call, exactly as a grain-call
/// timeout or a storage fault would. Recovery then runs as an operator would run it.
/// </para>
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class PurgeRecoveryIntegrationTests
{
    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(1);
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

    [TearDown]
    public void TearDown()
    {
        PurgeInterrupter.Disarm();
        RowClearFaultStorage.Disarm();
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static IServiceProvider Services => SiloServiceProviderCaptureForWalTests.Captured
        ?? throw new InvalidOperationException("Silo IServiceProvider was not captured by the fixture.");

    private async Task<(ILattice Tree, IShardRootGrain Shard, List<GrainId> Leaves, Dictionary<GrainId, List<string>> KeysByLeaf)>
        CreateMultiLeafTreeAsync(string prefix, int keyCount)
    {
        var treeId = $"{prefix}-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 4 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        for (var i = 0; i < keyCount; i++)
        {
            await tree.SetAsync($"k-{i:D3}", Bytes($"v-{i}"));
        }

        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{physical}/0");
        var (leaves, keys) = await ChainAsync(shard);
        Assert.That(leaves, Has.Count.GreaterThanOrEqualTo(3), "precondition: the shard holds several leaves");
        return (tree, shard, leaves, keys);
    }

    private async Task<(List<GrainId> Leaves, Dictionary<GrainId, List<string>> KeysByLeaf)> ChainAsync(IShardRootGrain shard)
    {
        var leaves = new List<GrainId>();
        var keys = new Dictionary<GrainId, List<string>>();
        var next = await shard.GetLeftmostLeafIdAsync();
        while (next is { } id)
        {
            var leaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(id);
            leaves.Add(id);
            keys[id] = await leaf.GetKeysAsync();
            next = await leaf.GetNextSiblingAsync();
        }

        return (leaves, keys);
    }

    private static async Task DeleteLeafRowAsync(GrainId leafId)
    {
        var storage = Services.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);
        var row = new GrainState<LeafNodeState>();
        await storage.ReadStateAsync("leaf", leafId, row);
        Assert.That(row.RecordExists, Is.True, "precondition: the leaf's row exists before the fault takes it");
        await storage.ClearStateAsync("leaf", leafId, row);
    }

    private static async Task<(byte[]? Read, Exception? Failure)> TryGetAsync(ILattice tree, string key)
    {
        try
        {
            return (await tree.GetAsync(key), null);
        }
        catch (Exception ex)
        {
            return (null, ex);
        }
    }

    /// <summary>
    /// Runs the shard's real purge with the interrupter armed, and asserts it was
    /// interrupted, so the scenario is the one under test and not a completed purge.
    /// </summary>
    private static void PurgeInterrupted(IShardRootGrain shard)
    {
        var fault = Assert.CatchAsync(async () => await shard.PurgeAsync());
        Assert.That(PurgeInterrupter.Fired, Is.True, $"precondition: the purge was interrupted by the test; it threw {fault}");
    }

    /// <summary>
    /// Part two of #4700. The purge clears the first leaf and is interrupted before
    /// it reaches the second; then the second leaf loses its row and its row record
    /// to a storage fault. Recovery must not re-create that leaf empty - the purge
    /// never cleared it, so its keys are not the operator's to lose - while the leaf
    /// the purge did clear comes back empty and writable.
    /// </summary>
    [Test]
    public async Task A_leaf_the_purge_never_reached_is_not_re_created_empty_by_recovery()
    {
        var (tree, shard, leaves, keys) = await CreateMultiLeafTreeAsync("purge-unreached", 16);
        var cleared = leaves[0];
        var unreached = leaves[1];

        await tree.DeleteTreeAsync();
        PurgeInterrupter.FailLeafClear(unreached);
        PurgeInterrupted(shard);

        // The fault: the unreached leaf goes cold and loses its row and its row
        // record (it holds no snapshot - the one its deactivation captured goes too).
        var unreachedLeaf = _cluster.Client.GetGrain<IBPlusLeafGrain>(unreached);
        await unreachedLeaf.ForceDeactivateAsync();
        await Task.Delay(200);
        await DeleteLeafRowAsync(unreached);
        await _cluster.Client.GetGrain<ILeafRowRecordGrain>(unreached.GetGuidKey()).ClearAsync();
        await _cluster.Client.GetGrain<ILeafSnapshotStorageGrain>(unreached.GetGuidKey()).ClearAsync(CancellationToken.None);

        await tree.RecoverTreeAsync();

        Assert.That(await unreachedLeaf.GetTreeIdAsync(), Is.Null,
            "recovery re-created, empty, a leaf the purge never cleared: once the WAL is trimmed past it its "
            + "acknowledged keys read as absent");

        var unreachedKey = keys[unreached][0];
        var (read, failure) = await TryGetAsync(tree, unreachedKey);
        TestContext.Out.WriteLine($"unreached {unreachedKey}: read={(read is null ? "null" : Encoding.UTF8.GetString(read))}; failure={failure?.GetType().Name}: {failure?.Message}");
        Assert.That(read is not null || failure is not null, Is.True,
            $"{unreachedKey} was acknowledged on a leaf the purge never reached: recovery must not re-create that leaf "
            + "empty and report the key absent");
        if (failure is not null)
        {
            Assert.That(failure, Is.InstanceOf<ILatticeLeafUnavailable>());
        }

        var clearedKey = keys[cleared][0];
        Assert.That(await tree.GetAsync(clearedKey), Is.Null, "the leaf the purge cleared comes back empty");
        await tree.SetAsync(clearedKey, Bytes("again"));
        Assert.That(await tree.GetAsync(clearedKey), Is.EqualTo(Bytes("again")), "and writable");
    }

    /// <summary>
    /// Part one of #4700, the refused re-create. The purge's clear of the first
    /// leaf is interrupted after its row is gone but before the rest of the leaf's
    /// state is - here at the snapshot clear. Recovery must re-create that leaf: it
    /// is the purge's own, half-cleared leaf, and leaving it failed closed would
    /// strand its key range for good.
    /// </summary>
    [Test]
    public async Task A_leaf_whose_purge_clear_was_interrupted_part_way_is_re_created_by_recovery()
    {
        var (tree, shard, leaves, keys) = await CreateMultiLeafTreeAsync("purge-halfcleared", 16);
        var halfCleared = leaves[0];

        await tree.DeleteTreeAsync();
        PurgeInterrupter.FailSnapshotClear(halfCleared);
        PurgeInterrupted(shard);

        await tree.RecoverTreeAsync();

        var key = keys[halfCleared][0];
        var (read, failure) = await TryGetAsync(tree, key);
        TestContext.Out.WriteLine($"half-cleared {key}: read={(read is null ? "null" : Encoding.UTF8.GetString(read))}; failure={failure?.GetType().Name}: {failure?.Message}");
        Assert.That(failure, Is.Null, "the purge's own half-cleared leaf must be re-created, not left failed closed");
        Assert.That(read, Is.Null, "its data was discarded by the purge");
        await tree.SetAsync(key, Bytes("again"));
        Assert.That(await tree.GetAsync(key), Is.EqualTo(Bytes("again")));

        // A leaf the purge never reached keeps its data.
        var untouched = leaves[^1];
        Assert.That(await tree.GetAsync(keys[untouched][0]), Is.Not.Null, "a leaf the purge did not reach keeps its data");
    }

    /// <summary>
    /// Part one of #4700, the truncated walk. A shard with more routed leaves than
    /// one recovery pass handles is purged almost completely, then recovered. Every
    /// leaf the purge cleared must come back - including those beyond the first
    /// pass - rather than fail closed for good.
    /// </summary>
    [Test]
    public async Task Every_leaf_a_purge_cleared_is_re_created_even_beyond_one_recovery_pass()
    {
        const int leafCount = 4200;
        var treeId = $"purge-wide-{Guid.NewGuid():N}";
        var registry = _cluster.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(treeId, new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 2 });
        var tree = _cluster.Client.GetGrain<ILattice>(treeId);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        var shard = _cluster.Client.GetGrain<IShardRootGrain>($"{physical}/0");

        var entries = Enumerable.Range(0, leafCount * 2)
            .Select(i => new KeyValuePair<string, byte[]>($"w-{i:D6}", Bytes($"v-{i}")))
            .ToList();
        await shard.BulkLoadAsync("purge-wide", entries);

        // The bulk-loaded leaves, in key order, read from the routing the
        // recovery will walk (the sibling chain of a bulk load is complete too).
        var last = entries[^1].Key;
        Assert.That(await tree.GetAsync(last), Is.Not.Null, "precondition: the bulk load is readable");
        var lastLeaf = (await shard.GetLeafIdForKeyAsync(last))!.Value;

        await tree.DeleteTreeAsync();

        // Interrupt the purge at its very last leaf, so it clears every other one,
        // most of them beyond the first recovery pass.
        PurgeInterrupter.FailLeafClear(lastLeaf);
        PurgeInterrupted(shard);

        await tree.RecoverTreeAsync();

        var probes = new[] { entries[0].Key, entries[entries.Count / 2].Key, entries[^3].Key };
        foreach (var key in probes)
        {
            var (read, failure) = await TryGetAsync(tree, key);
            TestContext.Out.WriteLine($"{key}: read={(read is null ? "null" : Encoding.UTF8.GetString(read))}; failure={failure?.GetType().Name}");
            Assert.That(failure, Is.Null, $"{key}'s leaf was cleared by the purge, and recovery must re-create it");
            Assert.That(read, Is.Null, $"{key}'s data was discarded by the purge");
        }

        Assert.That(await tree.GetAsync(last), Is.Not.Null, "the leaf the purge never reached keeps its data");
    }

    /// <summary>
    /// Mixed versions (#4700). A purge begun on a silo built before per-leaf purge
    /// marks clears its leaves with the plain clear, which deletes the row record and
    /// leaves no mark, and records only the retired shard-wide flag. Recovery on a
    /// current silo must not re-create such a leaf empty: it has no evidence the purge
    /// cleared it, so it fails closed, while the leaves that purge never reached keep
    /// their data.
    /// </summary>
    [Test]
    public async Task Recovery_after_a_purge_begun_by_a_silo_without_purge_marks_fails_closed_on_the_leaves_it_cleared()
    {
        var (tree, shard, leaves, keys) = await CreateMultiLeafTreeAsync("purge-mixed", 16);
        var cleared = leaves[0];

        await tree.DeleteTreeAsync();

        // The older silo's purge clear: the row, the snapshot and the record go, no mark.
        await _cluster.Client.GetGrain<IBPlusLeafGrain>(cleared).ClearGrainStateAsync();
        Assert.That(await _cluster.Client.GetGrain<ILeafRowRecordGrain>(cleared.GetGuidKey()).GetAsync(), Is.Null,
            "precondition: the pre-#4700 clear leaves no row record, so no mark");

        await tree.RecoverTreeAsync();

        var key = keys[cleared][0];
        var (read, failure) = await TryGetAsync(tree, key);
        TestContext.Out.WriteLine($"{key}: read={(read is null ? "null" : Encoding.UTF8.GetString(read))}; failure={failure?.GetType().Name}");
        Assert.That(failure, Is.InstanceOf<ILatticeLeafUnavailable>(),
            "a leaf an older silo's purge cleared carries no mark, so recovery must fail it closed, not re-create it empty");
        Assert.That(await tree.GetAsync(keys[leaves[^1]][0]), Is.Not.Null, "a leaf that purge never reached keeps its data");

        // The documented way out: delete and purge again, which marks and clears every leaf.
        await tree.DeleteTreeAsync();
        Assert.DoesNotThrowAsync(async () => await shard.PurgeAsync(), "a fresh purge completes over the unmarked cleared leaf");
    }

    /// <summary>
    /// The purge clear's ordering (#4700, confirmed against the WAL model). The purge
    /// marks a leaf and is interrupted before the leaf's row is cleared, so the leaf
    /// keeps its data and recovery hands it back. The leaf's materialiser pins must
    /// still be standing at that point: had the clear retired them first, the WAL GC
    /// would trim, floored only by the tree's other leaves, past the leaf's durable
    /// checkpoint, and the recovered leaf would latch
    /// <see cref="LeafProjectionStaleException"/> on its next activation.
    /// <para>
    /// This is end-to-end supporting evidence, not the detector of the order: the WAL
    /// GC really trims past the leaf's checkpoint here, and the recovered leaf must
    /// still serve its keys. In process, the interrupted activation's own deactivation
    /// flush and the leaf's covering snapshot also close the window, so this test
    /// stays green when the pins are retired first; that order needs a silo crash
    /// between the two steps to latch. The ordering itself is pinned by the
    /// <c>ClearGrainStateForPurge_*</c> leaf-grain tests, which are red under it.
    /// </para>
    /// </summary>
    [Test]
    public async Task A_marked_leaf_whose_purge_was_interrupted_before_its_row_clear_recovers_without_latching_stale()
    {
        var (tree, shard, leaves, keys) = await CreateMultiLeafTreeAsync("purge-pinorder", 16);
        var physical = (await tree.GetRoutingAsync()).PhysicalTreeId;
        var target = leaves[0];

        // Every other leaf applies a write after the target's last one, so each of
        // their cursors moves past the target's checkpoint.
        foreach (var other in leaves.Skip(1))
        {
            await tree.SetAsync(keys[other][^1] + "~", Bytes("after"));
        }

        // Every other leaf's durable pin advances past the target's checkpoint: a
        // deactivation captures a snapshot covering the leaf's checkpoint, and the
        // next write's checkpoint flush publishes the pin that coverage licenses.
        // The target's own pin is then all that floors the WAL GC at its checkpoint.
        foreach (var other in leaves.Skip(1))
        {
            await _cluster.Client.GetGrain<IBPlusLeafGrain>(other).ForceDeactivateAsync();
        }

        await Task.Delay(200);
        foreach (var other in leaves.Skip(1))
        {
            await tree.SetAsync(keys[other][^1] + "~~", Bytes("after"));
        }

        await tree.DeleteTreeAsync();
        RowClearFaultStorage.FailRowClear(target);
        var fault = Assert.CatchAsync(async () => await shard.PurgeAsync());
        Assert.That(RowClearFaultStorage.Fired, Is.True, $"precondition: the purge was interrupted at the target's row clear; it threw {fault}");

        var trimmed = await TrimWalAsync(physical);
        TestContext.Out.WriteLine($"trimmed after the interrupted purge: {trimmed}");

        await tree.RecoverTreeAsync();

        foreach (var key in keys[target])
        {
            var (read, failure) = await TryGetAsync(tree, key);
            TestContext.Out.WriteLine($"{key}: read={(read is null ? "null" : Encoding.UTF8.GetString(read))}; failure={failure?.GetType().Name}: {failure?.Message}");
            Assert.That(failure, Is.Null,
                $"{key}: the purge never cleared this leaf's row, so recovery must hand its data back intact; it "
                + "fails instead, because the clear retired the leaf's pins before its row and the WAL GC trimmed "
                + "past its checkpoint");
            Assert.That(read, Is.Not.Null, $"{key} keeps its value");
        }
    }

    private static async Task<long> TrimWalAsync(string treeId)
    {
        var registry = Services.GetRequiredService<IWalCursorRegistry>();
        var deadline = Environment.TickCount64 + 15_000;
        HybridLogicalClock? last = null;
        var stable = 0;
        while (Environment.TickCount64 < deadline)
        {
            var min = await registry.GetMinCursorAsync(treeId);
            if (min is { } floor && floor.CompareTo(HybridLogicalClock.Zero) > 0)
            {
                if (last is { } prev && floor.CompareTo(prev) == 0)
                {
                    if (++stable >= 2) break;
                }
                else
                {
                    stable = 0;
                }

                last = floor;
            }

            await Task.Delay(100);
        }

        var report = await Services.GetRequiredService<ILatticeWalGc>().RunOnceAsync(treeId);
        TestContext.Out.WriteLine($"gc: {report}");
        return report.EntriesTrimmed;
    }

    /// <summary>
    /// The fixture's grain storage: in memory, failing one leaf's row clear once
    /// when armed, as a storage fault would interrupt a purge mid-clear.
    /// </summary>
    private sealed class RowClearFaultStorage : IGrainStorage
    {
        private static GrainId? _target;
        private static int _fired;
        private readonly EnumerableMemoryGrainStorage _inner = new();

        public static bool Fired => Volatile.Read(ref _fired) != 0;

        public static void FailRowClear(GrainId leaf)
        {
            Disarm();
            _target = leaf;
        }

        public static void Disarm()
        {
            _target = null;
            Volatile.Write(ref _fired, 0);
        }

        public Task ReadStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
            => _inner.ReadStateAsync(stateName, grainId, grainState);

        public Task WriteStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
            => _inner.WriteStateAsync(stateName, grainId, grainState);

        public Task ClearStateAsync<T>(string stateName, GrainId grainId, IGrainState<T> grainState)
        {
            if (_target is { } target
                && grainId == target
                && stateName == "leaf"
                && Interlocked.Exchange(ref _fired, 1) == 0)
            {
                throw new TimeoutException("test: the purge was interrupted at this leaf's row clear");
            }

            return _inner.ClearStateAsync(stateName, grainId, grainState);
        }
    }

    /// <summary>
    /// Fails one call the shard's purge makes, once, so the purge is interrupted
    /// exactly where a test needs it.
    /// </summary>
    private sealed class PurgeInterrupter : IOutgoingGrainCallFilter
    {
        private static GrainId? _leafClearTarget;
        private static Guid? _snapshotClearTarget;
        private static int _fired;

        public static bool Fired => Volatile.Read(ref _fired) != 0;

        public static void FailLeafClear(GrainId leaf)
        {
            Disarm();
            _leafClearTarget = leaf;
        }

        public static void FailSnapshotClear(GrainId leaf)
        {
            Disarm();
            _snapshotClearTarget = leaf.GetGuidKey();
        }

        public static void Disarm()
        {
            _leafClearTarget = null;
            _snapshotClearTarget = null;
            Volatile.Write(ref _fired, 0);
        }

        public Task Invoke(IOutgoingGrainCallContext context)
        {
            var method = context.InterfaceMethod?.Name ?? string.Empty;
            if (_leafClearTarget is { } leaf
                && context.TargetId == leaf
                && method.StartsWith("ClearGrainState", StringComparison.Ordinal)
                && Interlocked.Exchange(ref _fired, 1) == 0)
            {
                throw new TimeoutException("test: the purge was interrupted at this leaf's clear");
            }

            if (_snapshotClearTarget is { } snapshotLeaf
                && method == nameof(ILeafSnapshotStorageGrain.ClearAsync)
                && context.TargetId.TryGetGuidKey(out var key, out _)
                && key == snapshotLeaf
                && context.TargetId.Type.ToString()?.Contains("snapshot", StringComparison.OrdinalIgnoreCase) == true
                && Interlocked.Exchange(ref _fired, 1) == 0)
            {
                throw new TimeoutException("test: the purge was interrupted at this leaf's snapshot clear");
            }

            return context.Invoke();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) =>
                silo.Services.AddKeyedSingleton<IGrainStorage>(name, (_, _) => new RowClearFaultStorage()));
            siloBuilder.AddWalCursorRegistry();
            siloBuilder.AddLatticeWalGc();
            siloBuilder.ConfigureLattice(o =>
            {
                o.TombstoneGracePeriod = TimeSpan.Zero;
                o.DigestCoalescingWindowMs = 0;
                o.MaterialiserCheckpointInterval = TimeSpan.Zero;
                // One WAL partition, so every leaf's pin covers the whole log and no
                // unwritten partition holds a blocking pin.
                o.WalPartitions = 1;
            });
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.Services.AddSingleton<IOutgoingGrainCallFilter, PurgeInterrupter>();
            siloBuilder.Services.AddSingleton<SiloServiceProviderCaptureForWalTests>();
            siloBuilder.Services.AddHostedService(sp => sp.GetRequiredService<SiloServiceProviderCaptureForWalTests>());
        }
    }
}
