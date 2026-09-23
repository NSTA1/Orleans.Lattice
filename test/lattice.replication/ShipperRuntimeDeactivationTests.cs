using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Runtime;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>Exercises the production shipper's deactivation through the Orleans runtime.</summary>
[TestFixture]
[Category("Integration")]
public sealed class ShipperRuntimeDeactivationTests
{
    private const string Tree = "deactivation-cursor";
    private const string Local = "deactivation-source";
    private const string Peer = "deactivation-peer";
    private const string DeactivateRequest = "shipper-test-deactivate";
    private TestCluster _cluster = null!;
    private Probe _probe = null!;

    [OneTimeSetUp]
    public async Task SetUp()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<Configurator>();
        _cluster = builder.Build();
        await _cluster.DeployAsync();
        _probe = _cluster.Silos.OfType<InProcessSiloHandle>().Single()
            .SiloHost.Services.GetRequiredService<Probe>();
    }

    [OneTimeTearDown]
    public async Task TearDown()
    {
        await _cluster.StopAllSilosAsync();
        await _cluster.DisposeAsync();
    }

    [Test]
    public async Task Runtime_deactivation_preserves_pending_cursor_for_next_activation()
    {
        var wal = _cluster.GrainFactory.GetGrain<IWalShardGrain>($"{Tree}/0");
        for (var i = 1; i <= 3; i++)
        {
            await wal.AppendAsync(Entry(i), CancellationToken.None);
        }
        var shipper = _cluster.GrainFactory.GetGrain<IReplicationShipperGrain>($"{Tree}/{Peer}");
        RequestContext.Set(DeactivateRequest, true);
        try
        {
            await shipper.IsShippingPausedAsync();
        }
        finally
        {
            RequestContext.Remove(DeactivateRequest);
        }
        await _probe.Deactivated.WaitAsync(TimeSpan.FromSeconds(15));
        var firstActivation = _probe.Activation;
        Assert.That(_probe.Sent.ToArray(), Is.EqualTo(new long[] { 1, 2, 3 }));

        await wal.AppendAsync(Entry(4), CancellationToken.None);
        await shipper.IsShippingPausedAsync();

        Assert.Multiple(() =>
        {
            Assert.That(_probe.Activation, Is.Not.SameAs(firstActivation),
                "The second call must reach a new runtime activation.");
            Assert.That(_probe.Sent.ToArray(), Is.EqualTo(new long[] { 1, 2, 3, 4 }),
                "The successor must resume after acknowledged entries, not re-ship them.");
        });
    }

    private static WalRecord Entry(int index) => new()
    {
        TreeId = Tree,
        Key = $"k{index}",
        Value = [1],
        Op = MutationKind.Set,
        OriginClusterId = Local,
        Timestamp = new HybridLogicalClock { WallClockTicks = index },
    };

    private sealed class Probe(IWalRecordEncoder encoder) : IIncomingGrainCallFilter, IReplicationTransport
    {
        public ConcurrentQueue<long> Sent { get; } = new();
        public object? Activation { get; private set; }
        public Task Deactivated { get; private set; } = Task.CompletedTask;

        public async Task Invoke(IIncomingGrainCallContext context)
        {
            await context.Invoke();
            if (context.Grain is not ReplicationShipperGrain shipper
                || context.InterfaceMethod.Name != nameof(IReplicationShipperGrain.IsShippingPausedAsync))
            {
                return;
            }

            // Run the ordinary pump within the real grain turn; only lifecycle
            // dispatch is under test, so no timer sleeps or private-field setup.
            await shipper.PumpForTestingAsync();
            Activation = shipper;
            if (RequestContext.Get(DeactivateRequest) is true)
            {
                var grainContext = ((IGrainBase)shipper).GrainContext;
                Deactivated = grainContext.Deactivated;
                grainContext.Deactivate(new DeactivationReason(
                    DeactivationReasonCode.ApplicationRequested, "cursor-runtime-test"));
            }
        }

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            foreach (var entry in batch.EncodedEnvelope!.Value.EncodedEntries.Span)
            {
                Sent.Enqueue(encoder.Decode(entry).Timestamp.WallClockTicks);
            }
            return Task.FromResult(new ReplicationAck { Accepted = true });
        }
    }

    private sealed class Configurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(options =>
            {
                options.ClusterId = Local;
                options.ReplogPartitions = 1;
                options.ShipBatchSize = 1;
                options.AdaptiveBatchSizingEnabled = false;
                options.ShipCursorWriteInterval = 100;
                options.ShipCursorWriteMaxDelay = Timeout.InfiniteTimeSpan;
                options.ShipPhaseTimerPeriod = TimeSpan.FromHours(1);
                options.LivenessProbeInterval = Timeout.InfiniteTimeSpan;
            });
            siloBuilder.Services.AddSingleton<Probe>();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter>(sp => sp.GetRequiredService<Probe>());
            siloBuilder.Services.AddSingleton<IReplicationTransport>(sp => sp.GetRequiredService<Probe>());
        }
    }
}
