using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
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

    private TestCluster _cluster = null!;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();

        var registry = _cluster.GrainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        await registry.RegisterAsync(Tree, new TreeRegistryEntry { ShardCount = 2, MaxLeafKeys = 128, WalPartitions = 1 });
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
    }

    [Test]
    public async Task Replay_scan_per_bulk_append_chunk_does_not_grow_with_the_chunk_index()
    {
        var lattice = _cluster.GrainFactory.GetGrain<ILattice>(Tree);
        var scanned = new long[Chunks];
        ReplayScanMeter.Arm();
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
        }

        ReplayScanMeter.Disarm();
        TestContext.Out.WriteLine("per-chunk replay scan: " + string.Join(", ", scanned));

        // A chunk legitimately re-reads the few chunks of WAL behind the shard's
        // right edge, so its steady-state scan is a small constant multiple of
        // the chunk size. A leaf that replays from the start of the log instead
        // scans a span proportional to the chunk index, which is what #6164 was.
        // The first chunks are excluded: a held, uncovered birth hint is
        // re-read once before its covering capture lands.
        var steadyState = scanned.Skip(SettleChunks).ToArray();
        Assert.That(steadyState, Has.All.LessThanOrEqualTo(4L * ChunkSize),
            $"per-chunk replay scan grew with the chunk index: {string.Join(", ", scanned)}");
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.ConfigureLattice(Tree, options => options.WalPartitions = 1);
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter, ReplayScanFilter>();
        }
    }

    /// <summary>
    /// Totals the WAL offsets each replay-coordinator slice read advanced over.
    /// Static because the TestingHost silo runs in-process.
    /// </summary>
    private static class ReplayScanMeter
    {
        private static int _armed;
        private static long _scanned;

        internal static long Scanned => Interlocked.Read(ref _scanned);

        internal static bool Armed => Volatile.Read(ref _armed) == 1;

        internal static void Arm()
        {
            Interlocked.Exchange(ref _scanned, 0);
            Volatile.Write(ref _armed, 1);
        }

        internal static void Disarm() => Volatile.Write(ref _armed, 0);

        internal static void Add(long span) => Interlocked.Add(ref _scanned, span);
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
                || context.Request.GetArgument(0) is not long fromExclusive)
            {
                return;
            }

            ReplayScanMeter.Add(slice[^1].Offset - fromExclusive);
        }
    }
}
