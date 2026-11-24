using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Issue #6164: the per-chunk cost of a sorted bulk-append preseed must not grow
/// with the size of the tree or of the write-ahead log behind it. The cost is
/// measured deterministically, as the WAL offsets leaf replays scan through the
/// replay coordinator while each chunk lands.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class BulkAppendChunkCostIntegrationTests
{
    private const string Tree = "bulk-append-chunk-cost";
    private const int ChunkSize = 1_024;
    private const int Chunks = 64;
    private const int SettleChunks = 8;
    private const string ReactivationTree = "bulk-append-reactivation-cost";
    private const int ReactivationChunks = 64;
    private const string ColdLeafTree = "bulk-append-cold-leaf-cost";
    private const int ReplayGateWidth = 8;

    /// <summary>
    /// The leaves a run's right edge filled with live writes, about one per fence of the
    /// run, whose latest advance is the live one, so the fast stall check leaves them to
    /// the coverage-lag full pass, minutes away; and the few a loaded host's timer share
    /// has not yet reached, which back off after a refusal (#3575).
    /// </summary>
    private const int MaxLiveFilledLaggards = ReactivationChunks / ShardRootGrain.BulkSwitchesPerFence * 2;

    /// <summary>
    /// The fewest leaves, of every 100, a reactivation must keep within two switch runs'
    /// span. The rest replay to the head: the first chunk's leaves, which a never-written
    /// shard gives no birth hint (#5812, B2), and a leaf a close holds while its range's
    /// unstamped watermark lies above its checkpoint, which no close-hold catch-up may
    /// pass (#5851, W4). Replaying to the head, about a quarter do.
    /// </summary>
    private const int MinLeavesWithinRunSpanPercent = 80;

    /// <summary>
    /// The most a leaf reads past its birth to its own switch's close: a run of
    /// <see cref="ShardRootGrain.BulkSwitchesPerFence"/> switches and its own chunk, twice,
    /// since a fence folded into the next run's begin records the close a run later.
    /// </summary>
    private const long RunSpan = 2 * (ShardRootGrain.BulkSwitchesPerFence + 1L) * ChunkSize;

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        // The replay gate is process-wide and sized once, from the first options read or
        // the host's cores: a fixed width keeps the settle bounds below from depending on
        // the host or on a fixture that ran first.
        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(Tree, new TreeRegistryEntry { ShardCount = 2, MaxLeafKeys = 128, WalPartitions = 1 });
        await registry.RegisterAsync(ReactivationTree, new TreeRegistryEntry { ShardCount = 2, MaxLeafKeys = 128, WalPartitions = 1 });
        await registry.RegisterAsync(ColdLeafTree, new TreeRegistryEntry { ShardCount = 2, MaxLeafKeys = 128, WalPartitions = 2 });
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        ReplayScanMeter.Disarm();
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }

        BPlusLeafGrain.ResetReplayConcurrencyGateForTest();
    }

    [Test]
    public async Task Replay_scan_per_bulk_append_chunk_does_not_grow_with_the_chunk_index()
    {
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(Tree);
        var scanned = new long[Chunks];
        ReplayScanMeter.Arm(Tree);
        for (var c = 0; c < Chunks; c++)
        {
            var chunk = new List<KeyValuePair<string, byte[]>>(ChunkSize);
            for (var i = 0; i < ChunkSize; i++)
            {
                var n = c * ChunkSize + i;
                chunk.Add(new KeyValuePair<string, byte[]>($"k{n:D9}", Encoding.UTF8.GetBytes($"v{n}")));
            }

            var before = ReplayScanMeter.Scanned;
            await lattice.BulkAppendChunkAsync($"chunk-{c}", chunk);
            scanned[c] = ReplayScanMeter.Scanned - before;
            if (c == SettleChunks - 1)
            {
                // A leaf the first chunk created on a never-written shard holds no birth
                // hint (#5812, B2), so its first drive reads the log from the start: once,
                // whenever a sweep or a stall check reaches it. Paid here, between chunks.
                foreach (var leaf in await ListLeavesAsync(Tree))
                {
                    await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).DriveStarvedCheckpointAsync();
                }
            }
        }

        ReplayScanMeter.Disarm();
        TestContext.Out.WriteLine("per-chunk replay scan: " + string.Join(", ", scanned));

        // A chunk legitimately re-reads the few chunks of WAL behind the shard's
        // right edge, so its steady-state scan is a small constant multiple of
        // the chunk size. A leaf that replays from the start of the log instead
        // scans a span proportional to the chunk index, which is what #6164 was.
        // The first chunks are excluded: they create the leaves the drive above settles.
        var steadyState = scanned.Skip(SettleChunks).ToArray();
        Assert.That(steadyState.Sum(), Is.LessThanOrEqualTo(4L * ChunkSize * steadyState.Length),
            $"per-chunk replay scan grew with the chunk index: {string.Join(", ", scanned)}");
    }

    [Test]
    public async Task A_bulk_appended_leaf_behind_the_right_edge_does_not_rescan_the_log_appended_after_it()
    {
        // Every replay-coordinator read is metered from the first chunk on: the seed, the
        // stall checks that follow it, and a reactivation of every leaf.
        ReplayScanMeter.Arm(ReactivationTree);
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(ReactivationTree);
        var leaves = await SeedAsync(ReactivationTree);

        // The stall checks bring the durable checkpoint of every leaf behind the right edge
        // to the head: by replay, or by taking the head without one.
        var head = await _cluster.GrainFactory.GetGrain<ILeafReplayCoordinatorGrain>($"{ReactivationTree}/0").GetHeadOffsetAsync();
        var behind = new List<long>();
        var settled = await TestPoll.TryUntilAsync(
            async () =>
            {
                behind.Clear();
                foreach (var leaf in leaves)
                {
                    var page = await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).GetProjectionCheckpointAndNextSiblingAsync();
                    if (page.CheckpointOffset < head - 1 - ChunkSize)
                    {
                        behind.Add(page.CheckpointOffset);
                    }
                }

                return behind.Count <= MaxLiveFilledLaggards;
            },
            // Bounded by drive throughput, not by a per-leaf cost: every leaf behind the
            // edge takes a capped catch-up and a cut through the timer's share of the
            // replay gate, which a loaded host serves slowly.
            timeout: TimeSpan.FromSeconds(180),
            cadence: TimeSpan.FromMilliseconds(250));
        Assert.That(settled, Is.True,
            $"{behind.Count} of {leaves.Count} leaves' durable checkpoints stayed behind the head {head}: {string.Join(", ", behind.Take(20))}");

        await ReactivateAsync(leaves);
        Assert.That(await lattice.GetAsync("k000000000"), Is.Not.Null, "the seed is still readable");
        var scanned = ReplayScanMeter.Scanned;
        var perLeafScans = ReplayScanMeter.PerLeaf();
        ReplayScanMeter.Disarm();

        // A leaf a sorted append left behind its right edge owns nothing appended after
        // its birth. A leaf that replays to the head to learn that re-reads every chunk
        // appended after it, so the total over the chain grows with the square of the
        // seed (#6166). One that takes the head from the consistent cut reads only to its
        // own switch's close: a span that does not grow with the seed.
        var within = leaves.Count(l => perLeafScans.GetValueOrDefault(l) <= RunSpan);
        TestContext.Out.WriteLine($"leaves={leaves.Count} head={head} replay scan total={scanned} mean={(double)scanned / leaves.Count:F1} withinRunSpan={within}");
        Assert.That(within * 100, Is.GreaterThanOrEqualTo(leaves.Count * MinLeavesWithinRunSpanPercent),
            $"only {within} of {leaves.Count} leaves read within two switch runs' span; the rest re-read the log appended after them");
    }

    [Test]
    public async Task A_bulk_born_leaf_reactivated_at_its_birth_checkpoint_does_not_replay_the_seed_appended_after_it()
    {
        // The rig's 3M read wedge (#6166): a leaf born by a sorted append and deactivated
        // before any stall drive advanced it holds its birth checkpoint, so its first read
        // after the seed replayed every chunk appended after its birth, behind a replay
        // permit. The stall checks are off here, so only the activation can pass that span.
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(ColdLeafTree);
        var leaves = await SeedAsync(ColdLeafTree);

        // Past the idle close, so no switch is open over any leaf at its reactivation.
        await Task.Delay(ShardRootGrain.BulkSwitchIdleClose + TimeSpan.FromSeconds(2));
        foreach (var leaf in leaves)
        {
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).ForceDeactivateAsync();
        }

        ReplayScanMeter.Arm(ColdLeafTree);
        await ReactivateAsync(leaves);
        var scanned = ReplayScanMeter.Scanned;
        ReplayScanMeter.Disarm();
        Assert.That(await lattice.GetAsync("k000000000"), Is.Not.Null, "the seed's first key is still readable");
        Assert.That(await lattice.GetAsync($"k{(ReactivationChunks * ChunkSize) - 1:D9}"), Is.Not.Null, "the seed's last key is still readable");

        var perLeaf = (double)scanned / leaves.Count;
        TestContext.Out.WriteLine($"leaves={leaves.Count} replay scan total={scanned} perLeaf={perLeaf:F1}");

        // Read to its own switch's close at most, as a stall drive's cut reads: a span that
        // does not grow with the seed. Replaying to the head reads half the seed per leaf.
        Assert.That(perLeaf, Is.LessThanOrEqualTo((ShardRootGrain.BulkSwitchesPerFence + 1.0) * ChunkSize),
            $"reactivated leaves re-read the seed appended after them: total {scanned} over {leaves.Count} leaves");
    }

    /// <summary>Appends <see cref="ReactivationChunks"/> sorted chunks to <paramref name="tree"/> and returns its leaves.</summary>
    private async Task<List<GrainId>> SeedAsync(string tree)
    {
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(tree);
        for (var c = 0; c < ReactivationChunks; c++)
        {
            var chunk = new List<KeyValuePair<string, byte[]>>(ChunkSize);
            for (var i = 0; i < ChunkSize; i++)
            {
                var n = c * ChunkSize + i;
                chunk.Add(new KeyValuePair<string, byte[]>($"k{n:D9}", Encoding.UTF8.GetBytes($"v{n}")));
            }

            await lattice.BulkAppendChunkAsync($"chunk-{c}", chunk);
        }

        var leaves = await ListLeavesAsync(tree);
        Assert.That(leaves, Has.Count.GreaterThan(ReactivationChunks), "precondition: the seed created many leaves");
        return leaves;
    }

    /// <summary>Every leaf of <paramref name="tree"/>, by each shard's chain.</summary>
    private async Task<List<GrainId>> ListLeavesAsync(string tree)
    {
        var leaves = new List<GrainId>();
        for (var s = 0; s < 2; s++)
        {
            var shard = _cluster.GrainFactory.GetGrain<IShardRootGrain>($"{tree}/{s}");
            GrainId? id = await shard.GetLeftmostLeafIdAsync();
            while (id is { } current)
            {
                leaves.Add(current);
                id = (await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(current.GetGuidKey()).GetKeyRangeAndNextSiblingAsync()).NextSibling;
            }
        }

        return leaves;
    }

    /// <summary>Deactivates every one of <paramref name="leaves"/>, then activates each again.</summary>
    private async Task ReactivateAsync(List<GrainId> leaves)
    {
        foreach (var leaf in leaves)
        {
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).ForceDeactivateAsync();
        }

        await Task.Delay(500);
        foreach (var leaf in leaves)
        {
            await _cluster.GrainFactory.GetGrain<IBPlusLeafGrain>(leaf.GetGuidKey()).GetKeyRangeAndNextSiblingAsync();
        }
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(options => options.WalMaterialiserMaxConcurrentReplays = ReplayGateWidth);
            siloBuilder.ConfigureLattice(Tree, options => options.WalPartitions = 1);
            siloBuilder.ConfigureLattice(ReactivationTree, options =>
            {
                options.WalPartitions = 1;
                options.LeafCheckpointStallCheckIntervalSeconds = 1;
            });
            siloBuilder.ConfigureLattice(ColdLeafTree, options =>
            {
                options.WalPartitions = 2;
                options.LeafCheckpointStallCheckIntervalSeconds = 3_600;
            });
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, ReplayScanFilter>();
        }
    }

    /// <summary>
    /// Totals the WAL offsets each replay-coordinator slice read of one tree advanced over;
    /// another fixture tree's background drives are not counted. Static because the
    /// TestingHost silo runs in-process.
    /// </summary>
    private static class ReplayScanMeter
    {
        private static int _armed;
        private static long _scanned;
        private static string _treePrefix = string.Empty;
        private static readonly System.Collections.Concurrent.ConcurrentDictionary<GrainId, long> PerSource = new();

        internal static long Scanned => Interlocked.Read(ref _scanned);

        internal static bool Armed => Volatile.Read(ref _armed) == 1;

        internal static bool Meters(string coordinatorKey) => coordinatorKey.StartsWith(Volatile.Read(ref _treePrefix), StringComparison.Ordinal);

        internal static void Arm(string tree)
        {
            Volatile.Write(ref _treePrefix, tree + "/");
            Interlocked.Exchange(ref _scanned, 0);
            PerSource.Clear();
            Volatile.Write(ref _armed, 1);
        }

        internal static void Disarm() => Volatile.Write(ref _armed, 0);

        internal static void Add(GrainId? source, long span)
        {
            Interlocked.Add(ref _scanned, span);
            if (source is { } leaf)
            {
                PerSource.AddOrUpdate(leaf, span, (_, total) => total + span);
            }
        }

        /// <summary>The offsets each reading leaf scanned since the meter was armed.</summary>
        internal static Dictionary<GrainId, long> PerLeaf() => new(PerSource);
    }

    private sealed class ReplayScanFilter : IIncomingGrainCallFilter
    {
        public async Task Invoke(IIncomingGrainCallContext context)
        {
            ArgumentNullException.ThrowIfNull(context);
            await context.Invoke();

            if (!ReplayScanMeter.Armed
                || context.InterfaceMethod?.DeclaringType != typeof(ILeafReplayCoordinatorGrain)
                || context.InterfaceMethod.Name != nameof(ILeafReplayCoordinatorGrain.ReadSliceAsync)
                || context.Result is not IReadOnlyList<CommitLogSliceEntry> { Count: > 0 } slice
                || context.Request.GetArgument(0) is not long fromExclusive
                || !ReplayScanMeter.Meters(context.TargetId.Key.ToString()!))
            {
                return;
            }

            ReplayScanMeter.Add(context.SourceId, slice[^1].Offset - fromExclusive);
        }
    }
}
