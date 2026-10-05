using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Wal;
using Orleans.Lattice.Views;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.Views;

/// <summary>
/// A history view's timeline is bounded by WAL retention: a retention trim that
/// overtakes the view's read position is a legitimate retention event, and the
/// view must detect the fall-off, log it, and rebuild from current source state,
/// which collapses the timeline to one revision per key. It must never tail on
/// across the gap with a silently missing revision. Runs the real view maintainer,
/// leaf and WAL shard grains and the WAL GC's retention ceiling.
/// </summary>
[TestFixture]
[Category("Integration")]
public class HistoryViewWalFallOffTests
{
    private const string SourceTreeId = "hist-falloff-src";
    private const string ViewName = "hist-falloff-view";

    private static readonly WarningCapturingLoggerProvider Logs = new();

    private TestCluster _cluster = null!;

    private IServiceProvider SiloServices
        => ((InProcessSiloHandle)_cluster.Primary).SiloHost.Services;

    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        var builder = new TestClusterBuilder(initialSilosCount: 1);
        builder.AddSiloBuilderConfigurator<SiloConfigurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        if (_cluster is not null)
        {
            await _cluster.StopAllSilosAsync();
            await _cluster.DisposeAsync();
        }
    }

    [Test]
    public async Task A_history_view_a_retention_trim_overtakes_logs_the_fall_off_and_collapses_to_current_state()
    {
        var client = _cluster.Client;
        await client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).RegisterAsync(
            SourceTreeId,
            new TreeRegistryEntry { ShardCount = 1, MaxLeafKeys = 64, MaxInternalChildren = 4 });
        var source = client.GetGrain<ILattice>(SourceTreeId);
        SiloServices.GetRequiredService<ILatticeViewFactory>()
            .Create(source, ViewName, LatticeHistoryView.Definition(ViewName, SiloServices));
        var maintainer = client.GetGrain<IViewMaintainerGrain>(ViewName);
        await maintainer.EnsureActiveAsync();

        // Two revisions the view records.
        await source.SetAsync("k", [1]);
        await source.SetAsync("k", [2]);
        await DrainToZeroAsync(maintainer);
        Assert.That(await ReadHistoryAsync(maintainer), Has.Count.EqualTo(2), "the view records both revisions");

        // A third revision the view does not read before retention trims it.
        await source.SetAsync("k", [3]);
        var partitions = await SiloServices.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(SourceTreeId);
        var q = WalPartitionHash.Compute("k", partitions);
        var walShard = client.GetGrain<IWalShardGrain>($"{SourceTreeId}/{q}");
        var unreadOffset = (await walShard.ReadAsync(0, 8, CancellationToken.None)).Entries.Max(e => e.Sequence);
        await CheckpointLeavesPastAsync(q, unreadOffset, source);
        Assert.That(await maintainer.GetLagAsync(), Is.GreaterThan(0), "the view has not read the third revision");

        var ttlGc = new LatticeWalGc(
            SiloServices,
            SiloServices.GetRequiredService<IWalCursorRegistry>(),
            new FixedLatticeOptionsMonitor(new LatticeOptions
            {
                WalRetention = TimeSpan.FromMilliseconds(1),
                WalDurabilityHoldCeilingBytes = 0,
            }));
        await Task.Delay(5);
        await ttlGc.RunOnceAsync(SourceTreeId);
        var retained = await walShard.ReadAsync(0, 8, CancellationToken.None);
        Assert.That(retained.Entries.Select(e => e.Sequence), Does.Not.Contain(unreadOffset),
            "the retention ceiling trims past the view's read position");

        var generationBefore = await maintainer.GetActiveTreeIdAsync();
        await DrainToZeroAsync(maintainer);

        var rows = await ReadHistoryAsync(maintainer);
        Assert.Multiple(async () =>
        {
            Assert.That(await maintainer.GetActiveTreeIdAsync(), Is.Not.EqualTo(generationBefore),
                "the view detected that it fell off the log and rebuilt");
            Assert.That(Logs.Messages, Has.Some.Contains($"View '{ViewName}' fell off the WAL on source '{SourceTreeId}'; rebuilding."),
                "the collapse is observable as a warning");
            Assert.That(rows, Has.Count.EqualTo(1),
                "the rebuild re-derives the timeline from current source state, one revision per key");
            Assert.That(rows.Single().SourceKey, Is.EqualTo("k"));
            Assert.That(rows.Single().ValueLength, Is.EqualTo(1));
        });
    }

    private static async Task DrainToZeroAsync(IViewMaintainerGrain maintainer)
    {
        await TestPoll.UntilAsync(
            async () =>
            {
                await maintainer.DrainAsync();
                return await maintainer.GetLagAsync() == 0;
            },
            "the history view to catch up to the source head",
            TimeSpan.FromSeconds(30));
    }

    private async Task<IReadOnlyList<HistoryRow>> ReadHistoryAsync(IViewMaintainerGrain maintainer)
    {
        var codec = SiloServices.GetRequiredService<HistoryRowCodec>();
        var viewTree = _cluster.Client.GetGrain<ILattice>(await maintainer.GetActiveTreeIdAsync());
        var rows = new List<HistoryRow>();
        using var scope = ViewReadContext.BeginScope();
        await foreach (var entry in viewTree.ScanEntriesAsync())
        {
            rows.Add(codec.Decode(entry.Value));
        }

        return rows;
    }

    /// <summary>
    /// Deactivates and re-reads the tree's leaves until one of them has a durable
    /// checkpoint in <paramref name="partition"/> at or past <paramref name="offset"/>,
    /// so neither the materialiser offset floor nor a block pin holds the entry.
    /// </summary>
    private async Task CheckpointLeavesPastAsync(int partition, long offset, ILattice source)
    {
        var client = _cluster.Client;
        var shards = WalMaterialiserPinRouting.ResolveShardCount(SiloServices.GetService<IOptionsMonitor<LatticeOptions>>());
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(SourceTreeId, shards);

        await TestPoll.UntilAsync(
            async () =>
            {
                var leaves = new HashSet<Guid>();
                var checkpointed = false;
                var blocked = false;
                foreach (var pinKey in pinKeys)
                {
                    var pinGrain = client.GetGrain<IWalMaterialiserPinGrain>(pinKey);
                    var offsets = await pinGrain.GetPinOffsetsAsync();
                    foreach (var (consumerId, pin) in await pinGrain.GetPinsAsync())
                    {
                        // A per-partition pin id ends in "_<partition>"; a
                        // single-partition log's pin id ends at the leaf guid.
                        var start = consumerId.IndexOf("bplusleaf/", StringComparison.Ordinal);
                        var end = consumerId.LastIndexOf('_');
                        var attributed = end > start;
                        if (!attributed)
                        {
                            end = consumerId.Length;
                        }

                        var forPartition = !attributed || consumerId.EndsWith("_" + partition, StringComparison.Ordinal);
                        checkpointed |= forPartition && offsets.GetValueOrDefault(consumerId, -1) >= offset;
                        blocked |= pin <= HybridLogicalClock.Zero && offsets.GetValueOrDefault(consumerId, -1) < 0;
                        if (start >= 0 && end > start + 10
                            && Guid.TryParseExact(consumerId[(start + 10)..end], "N", out var leaf))
                        {
                            leaves.Add(leaf);
                        }
                    }
                }

                if (checkpointed && !blocked)
                {
                    return true;
                }

                foreach (var leaf in leaves)
                {
                    await client.GetGrain<IBPlusLeafGrain>(leaf).ForceDeactivateAsync();
                }

                await Task.Delay(250);
                await source.GetAsync("k");
                return false;
            },
            $"a leaf to checkpoint partition {partition} through offset {offset}",
            TimeSpan.FromSeconds(60),
            TimeSpan.FromMilliseconds(100));
    }

    private sealed class SiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeWalGc();
            // The test drives the GC pass itself.
            siloBuilder.ConfigureLattice(o => o.WalGcInterval = TimeSpan.Zero);
            siloBuilder.AddLatticeViews();
            // The view drains only when the test asks it to.
            siloBuilder.Services.ConfigureAll<LatticeViewOptions>(o => o.CoalesceWindow = TimeSpan.FromHours(1));
            siloBuilder.Services.AddSingleton<ILoggerProvider>(Logs);
        }
    }

    /// <summary>Records every formatted warning-or-worse message the silo logs.</summary>
    private sealed class WarningCapturingLoggerProvider : ILoggerProvider
    {
        private readonly List<string> _messages = [];

        internal IReadOnlyList<string> Messages
        {
            get
            {
                lock (_messages)
                {
                    return _messages.ToArray();
                }
            }
        }

        public ILogger CreateLogger(string categoryName) => new WarningCapturingLogger(_messages);

        public void Dispose()
        {
        }

        private sealed class WarningCapturingLogger(List<string> messages) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel >= LogLevel.Warning;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                if (!IsEnabled(logLevel))
                {
                    return;
                }

                lock (messages)
                {
                    messages.Add(formatter(state, exception));
                }
            }
        }
    }
}
