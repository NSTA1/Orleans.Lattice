using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// End-to-end coverage for a receiver that fell behind a source WAL trim.
/// </summary>
[TestFixture]
[Category("Integration")]
public sealed class SourceWalTrimFallOffIntegrationTests
{
    private const string Tree = "source-wal-trim-falloff";
    private const string SiteAClusterId = "swf-site-a";
    private const string SiteBClusterId = "swf-site-b";
    private static readonly TimeSpan PollBudget = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan PollCadence = TimeSpan.FromMilliseconds(100);

    private static readonly ConcurrentDictionary<string, IRemoteSnapshotTransport> SnapshotTransports = new();
    private static ReseedDeliveringTransport? ActiveTransport;

    private TestCluster _siteA = null!;
    private TestCluster _siteB = null!;
    private ReseedDeliveringTransport _transport = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        _transport = new ReseedDeliveringTransport();
        ActiveTransport = _transport;

        var aBuilder = new TestClusterBuilder(initialSilosCount: 1);
        aBuilder.UseSharedInMemoryWal();
        aBuilder.AddSiloBuilderConfigurator<SiteASiloConfigurator>();
        _siteA = aBuilder.Build();
        await _siteA.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_siteA);
        await WarmUpReminderServiceAsync(_siteA);

        var siteAProvider = new LatticeSnapshotProvider(
            _siteA.Client,
            new InMemoryWalCursorRegistry(),
            LatticeSnapshotProviderUnitTests.TestOptions());
        SnapshotTransports[SiteAClusterId] = new LatticeRemoteSnapshotService(
            siteAProvider,
            new StubReplicationContext(SiteAClusterId, LatticeMergeMode.LwwRegister),
            NullLogger<LatticeRemoteSnapshotService>.Instance);

        var bBuilder = new TestClusterBuilder(initialSilosCount: 1);
        bBuilder.UseSharedInMemoryWal();
        bBuilder.AddSiloBuilderConfigurator<SiteBSiloConfigurator>();
        _siteB = bBuilder.Build();
        await _siteB.DeployAsync();
        await SharedInMemoryWal.AssertAllSilosShareOneWalAsync(_siteB);
        await WarmUpReminderServiceAsync(_siteB);
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        if (_siteB is not null)
        {
            await _siteB.StopAllSilosAsync();
            await _siteB.DisposeAsync();
        }

        if (_siteA is not null)
        {
            await _siteA.StopAllSilosAsync();
            await _siteA.DisposeAsync();
        }

        SnapshotTransports.TryRemove(SiteAClusterId, out _);
        ActiveTransport = null;
    }

    [SetUp]
    public void ResetTransport()
    {
        _transport.Reset();
        _transport.Reachable = true;
    }

    [Test]
    public async Task Receiver_behind_a_source_wal_trim_is_re_seeded_and_converges()
    {
        var siteA = _siteA.Client.GetGrain<ILattice>(Tree);
        var siteB = _siteB.Client.GetGrain<ILattice>(Tree);
        var shipper = _siteA.Client.GetGrain<IReplicationShipperGrain>($"{Tree}/{SiteBClusterId}");
        await shipper.EnsureActiveAsync(CancellationToken.None);

        await siteA.SetAsync("stable", [0x01]);
        await siteA.SetAsync("overwritten", [0x02]);
        await siteA.SetAsync("deleted", [0x03]);

        await WaitForValueAsync(siteB, "stable", [0x01], "initial stable value must ship before the outage");
        await WaitForValueAsync(siteB, "overwritten", [0x02], "initial overwritten value must ship before the outage");
        await WaitForValueAsync(siteB, "deleted", [0x03], "initial deleted value must ship before the outage");

        _transport.Reachable = false;

        await siteA.SetAsync("stable", [0x11]);
        await siteA.SetAsync("overwritten", [0x22]);
        await siteA.SetAsync("new-while-behind", [0x33]);
        await siteA.DeleteAsync("deleted");

        await TrimPastUnshippedEntriesAsync();

        await siteA.SetAsync("after-trim-trigger", [0x44]);
        _transport.Reachable = true;

        await TestPoll.UntilAsync(
            () => _transport.ReseedRequests.Count > 0,
            "the first retained send after the trim must carry a re-seed request",
            PollBudget,
            PollCadence);

        var requestedEpoch = _transport.ReseedRequests.Single();
        Assert.That(requestedEpoch, Is.Not.Null, "the shipper must persist and stamp ReseedRequiredEpoch");

        var coordinator = _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree);
        await TestPoll.UntilAsync(
            async () => await coordinator.GetStateAsync(CancellationToken.None) == LatticeBootstrapState.LiveIncremental,
            "the receiver must complete the responder-started bootstrap",
            PollBudget,
            PollCadence);

        await siteA.SetAsync("clear-trigger", [0x55]);

        await TestPoll.UntilAsync(
            () => _transport.BootstrapEpochEchoes.Any(e => e > requestedEpoch),
            "the receiver must echo a completed export epoch after the requested marker",
            PollBudget,
            PollCadence);

        await TestPoll.UntilAsync(
            () => _transport.SentAfterClear > 0,
            "the shipper must clear the marker and send a later batch without ReseedAfterEpoch",
            PollBudget,
            PollCadence);

        await AssertConvergedAsync(siteA, siteB, "stable");
        await AssertConvergedAsync(siteA, siteB, "overwritten");
        await AssertConvergedAsync(siteA, siteB, "new-while-behind");
        await AssertConvergedAsync(siteA, siteB, "after-trim-trigger");
        await AssertConvergedAsync(siteA, siteB, "clear-trigger");
        await AssertConvergedAsync(siteA, siteB, "deleted");
    }

    private async Task TrimPastUnshippedEntriesAsync()
    {
        var gc = SiloServices(_siteA).GetRequiredService<ILatticeWalGc>();
        LatticeWalGcReport latest = default;
        await TestPoll.UntilAsync(
            async () =>
            {
                // A leaf that has not yet checkpointed holds its partitions with a
                // durable block pin, against the retention ceiling too (issue
                // #4622), so the source leaves make their writes durable first:
                // a graceful deactivation checkpoints and captures.
                await DeactivateSourceLeavesAsync();
                await Task.Delay(TimeSpan.FromMilliseconds(25));
                latest = await gc.RunOnceAsync(Tree, CancellationToken.None);
                return latest.EntriesTrimmed > 0;
            },
            "the source WAL GC must trim entries while the receiver is unreachable",
            PollBudget,
            PollCadence);

        Assert.That(latest.EntriesTrimmed, Is.GreaterThan(0), "the trim pass must remove entries the shipper has not sent");
    }

    private async Task DeactivateSourceLeavesAsync()
    {
        var pinKeys = WalMaterialiserPinRouting.EnumerateReadKeys(
            Tree,
            WalMaterialiserPinRouting.ResolveShardCount(
                SiloServices(_siteA).GetService<Microsoft.Extensions.Options.IOptionsMonitor<LatticeOptions>>()));
        foreach (var pinKey in pinKeys)
        {
            foreach (var (consumerId, pin) in await _siteA.Client.GetGrain<IWalMaterialiserPinGrain>(pinKey).GetPinsAsync())
            {
                // A per-partition pin id ends in "_<partition>"; a single-partition
                // log's pin id ends at the leaf guid.
                var start = consumerId.IndexOf("bplusleaf/", StringComparison.Ordinal);
                var end = consumerId.LastIndexOf('_');
                if (end <= start)
                {
                    end = consumerId.Length;
                }

                if (pin <= HybridLogicalClock.Zero
                    && start >= 0 && end > start + 10
                    && Guid.TryParseExact(consumerId[(start + 10)..end], "N", out var leaf))
                {
                    await _siteA.Client.GetGrain<Orleans.Lattice.BPlusTree.IBPlusLeafGrain>(leaf).ForceDeactivateAsync();
                }
            }
        }

        await Task.Delay(TimeSpan.FromMilliseconds(250));
        await _siteA.Client.GetGrain<ILattice>(Tree).GetAsync("stable");
    }

    private static IServiceProvider SiloServices(TestCluster cluster) =>
        cluster.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;

    private static async Task WarmUpReminderServiceAsync(TestCluster cluster)
    {
        var deadline = Environment.TickCount64 + (long)TimeSpan.FromSeconds(60).TotalMilliseconds;
        var tree = cluster.Client.GetGrain<ILattice>($"source-wal-trim-warmup-{Guid.NewGuid():N}");
        while (true)
        {
            try
            {
                await tree.SetAsync("warmup", [0]);
                return;
            }
            catch (OrleansException ex) when (
                ex.Message.Contains("Reminder Service is still initializing", StringComparison.Ordinal)
                && Environment.TickCount64 < deadline)
            {
                await Task.Delay(TimeSpan.FromMilliseconds(250));
            }
        }
    }

    private static async Task WaitForValueAsync(ILattice lattice, string key, byte[] expected, string because)
    {
        await TestPoll.UntilAsync(
            async () => (await lattice.GetAsync(key)) is { } value && value.SequenceEqual(expected),
            because,
            PollBudget,
            PollCadence);
    }

    private static async Task AssertConvergedAsync(ILattice source, ILattice receiver, string key)
    {
        var expected = await source.GetAsync(key);
        await TestPoll.UntilAsync(
            async () =>
            {
                var actual = await receiver.GetAsync(key);
                return expected is null ? actual is null : actual is not null && actual.SequenceEqual(expected);
            },
            $"receiver value for '{key}' must converge to source value",
            PollBudget,
            PollCadence);

        var actual = await receiver.GetAsync(key);
        Assert.That(actual, Is.EqualTo(expected), $"key '{key}' must match the source");
    }

    private static void ConfigureCommon(ISiloBuilder siloBuilder, string clusterId)
    {
        siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
        siloBuilder.UseInMemoryReminderService();
        siloBuilder.Services.Configure<LatticeOptions>(Tree, opts =>
        {
            opts.WalPartitions = 1;
            opts.WalRetention = TimeSpan.FromMilliseconds(1);
        });
        siloBuilder.AddLatticeReplication(opts =>
        {
            opts.ClusterId = clusterId;
            opts.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal)
            {
                [Tree] = LatticeMergeMode.LwwRegister,
            };
            opts.ReplogPartitions = 1;
            opts.ShipBatchSize = 8;
            opts.ShipCursorWriteInterval = 1;
            opts.ShipPhaseTimerPeriod = TimeSpan.FromMilliseconds(25);
            opts.ShipBackoffInitial = TimeSpan.FromMilliseconds(10);
            opts.ShipBackoffMax = TimeSpan.FromMilliseconds(25);
            opts.ShipBackoffJitter = 0;
            opts.MaintenanceGcInterval = TimeSpan.FromMinutes(30);
            opts.AllowWalRetentionWithoutAntiEntropy = true;
            opts.WalRetention = TimeSpan.FromMilliseconds(1);
        });
        siloBuilder.Services.AddSingleton<ILatticeMergeModeResolver, AllowAllLwwRegisterResolver>();
    }

    private sealed class SiteASiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            ConfigureCommon(siloBuilder, SiteAClusterId);
            siloBuilder.Services.Configure<LatticeReplicationOptions>(opts =>
                opts.ReplicationPeers = new[] { SiteBClusterId });
            siloBuilder.Services.AddSingleton<IReplicationTransport>(_ =>
                ActiveTransport ?? throw new InvalidOperationException("Transport was not initialised."));
            siloBuilder.Services.AddSingleton(new ClusterServiceLocatorRegistration(SiteAClusterId));
            siloBuilder.Services.AddHostedService<ClusterServiceProviderRegistrar>();
        }
    }

    private sealed class SiteBSiloConfigurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            ConfigureCommon(siloBuilder, SiteBClusterId);
            if (SnapshotTransports.TryGetValue(SiteAClusterId, out var transport))
            {
                siloBuilder.Services.AddSingleton(transport);
            }

            siloBuilder.Services.AddSingleton(new ClusterServiceLocatorRegistration(SiteBClusterId));
            siloBuilder.Services.AddHostedService<ClusterServiceProviderRegistrar>();
        }
    }

    private sealed class AllowAllLwwRegisterResolver : ILatticeMergeModeResolver
    {
        public LatticeMergeMode? Resolve(string treeId) => LatticeMergeMode.LwwRegister;
    }

    private sealed record ClusterServiceLocatorRegistration(string ClusterId);

    private sealed class ClusterServiceProviderRegistrar(
        ClusterServiceLocatorRegistration registration,
        IServiceProvider services) : IHostedService
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            ReseedDeliveringTransport.RegisterCluster(registration.ClusterId, services);
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            ReseedDeliveringTransport.UnregisterCluster(registration.ClusterId);
            return Task.CompletedTask;
        }
    }

    private sealed class ReseedDeliveringTransport : IReplicationTransport
    {
        private static readonly ConcurrentDictionary<string, IServiceProvider> ClusterServices = new();
        private readonly ConcurrentQueue<long?> _reseedRequests = new();
        private readonly ConcurrentQueue<long?> _bootstrapEpochEchoes = new();
        private int _sentAfterClear;

        public bool Reachable { get; set; } = true;
        public IReadOnlyCollection<long?> ReseedRequests => _reseedRequests.ToArray();
        public IReadOnlyCollection<long?> BootstrapEpochEchoes => _bootstrapEpochEchoes.ToArray();
        public int SentAfterClear => Volatile.Read(ref _sentAfterClear);

        public static void RegisterCluster(string clusterId, IServiceProvider services) =>
            ClusterServices[clusterId] = services;

        public static void UnregisterCluster(string clusterId) =>
            ClusterServices.TryRemove(clusterId, out _);

        public void Reset()
        {
            while (_reseedRequests.TryDequeue(out _)) { }
            while (_bootstrapEpochEchoes.TryDequeue(out _)) { }
            Interlocked.Exchange(ref _sentAfterClear, 0);
        }

        public async Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            if (!Reachable)
            {
                throw new TimeoutException("Synthetic receiver outage.");
            }

            if (!ClusterServices.TryGetValue(batch.TargetClusterId, out var dest))
            {
                throw new InvalidOperationException($"No silo registered for cluster id '{batch.TargetClusterId}'.");
            }

            long? bootstrapEpoch = null;
            if (batch.ReseedAfterEpoch is { } reseedAfter)
            {
                _reseedRequests.Enqueue(reseedAfter);
                bootstrapEpoch = await Task.Run(
                    () => ReplicationReseedResponder.RespondAsync(
                        dest.GetRequiredService<IGrainFactory>(),
                        batch.TreeName,
                        batch.OriginClusterId,
                        reseedAfter,
                        autoBootstrap: true,
                        NullLogger.Instance),
                    cancellationToken).ConfigureAwait(false);
                _bootstrapEpochEchoes.Enqueue(bootstrapEpoch);
            }
            else
            {
                Interlocked.Increment(ref _sentAfterClear);
            }

            if (batch.Payload.IsEmpty && batch.EncodedEnvelope is null)
            {
                return new ReplicationAck
                {
                    Accepted = true,
                    HighestAppliedHlc = HybridLogicalClock.Zero,
                    BootstrapEpoch = bootstrapEpoch,
                };
            }

            var encoded = batch.EncodedEnvelope!.Value;
            var walEncoder = dest.GetRequiredService<IWalRecordEncoder>();
            var segments = encoded.EncodedEntries.Span;
            var decoded = new WalRecord[segments.Length];
            for (var i = 0; i < segments.Length; i++)
            {
                decoded[i] = walEncoder.Decode(segments[i].AsSpan(), batch.TreeName, encoded.Header.Mode);
            }

            var applier = dest.GetRequiredService<IReplicationApplier>();
            var result = await Task.Run(
                () => applier.ApplyBatchAsync(decoded, cancellationToken),
                cancellationToken).ConfigureAwait(false);

            return new ReplicationAck
            {
                Accepted = !result.Deferred,
                HighestAppliedHlc = result.HighWaterMark,
                BootstrapEpoch = bootstrapEpoch,
            };
        }
    }
}
