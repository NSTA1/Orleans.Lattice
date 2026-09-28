using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
[Category("Integration")]
public sealed class ObserverAliasShippingIntegrationTests
{
    private const string Tree = "observer-shipping";
    private const string Peer = "observer-peer";

    [Test]
    public async Task Resized_tree_keeps_nudging_and_shipping_under_logical_identity()
    {
        var builder = new TestClusterBuilder(1);
        builder.AddSiloBuilderConfigurator<Configurator>();
        await using var cluster = builder.Build();
        await cluster.DeployAsync();
        try
        {
            var services = cluster.Silos.OfType<InProcessSiloHandle>().Single().SiloHost.Services;
            var probe = services.GetRequiredService<Probe>();
            var tree = cluster.GrainFactory.GetGrain<ILattice>(Tree);
            var shipper = cluster.GrainFactory.GetGrain<IReplicationShipperGrain>($"{Tree}/{Peer}");
            await tree.SetAsync("before", [1]);
            await shipper.IsShippingPausedAsync();
            Assert.That(probe.Sent.Select(r => r.Key), Does.Contain("before"));

            var resize = cluster.GrainFactory.GetGrain<ITreeResizeGrain>(Tree);
            await resize.ResizeAsync(64, 64);
            await resize.RunResizePassAsync();
            var physical = await cluster.GrainFactory.GetLatticeRegistry().ResolveAsync(Tree);
            Assert.That(physical, Is.Not.EqualTo(Tree));
            probe.Nudges.Clear();
            probe.Sent.Clear();

            await tree.SetAsync("after", [2]);
            Assert.That(probe.Nudges.ToArray(), Is.EqualTo(new[] { Tree }),
                "Enrollment includes only the logical tree; a physical-id callback loses its doorbell.");
            await shipper.IsShippingPausedAsync();
            var shipped = probe.Sent.Single(r => r.Key == "after");
            Assert.That(shipped.TreeId, Is.EqualTo(Tree));
            Assert.That(shipped.Value, Is.EqualTo(new byte[] { 2 }));
            Assert.That(shipped.OriginClusterId, Is.EqualTo("observer-source"));

            var wal = cluster.GrainFactory.GetGrain<IWalShardGrain>($"{physical}/0");
            var durable = await wal.ReadAsync(0, 100, CancellationToken.None);
            Assert.That(durable.Entries.Single(e => e.Entry.Key == "after").Entry.TreeId, Is.EqualTo(physical),
                "Observer identity must not move the durability record out of the physical WAL.");
        }
        finally
        {
            await cluster.StopAllSilosAsync();
        }
    }

    private sealed class Probe(IWalRecordEncoder encoder) : IIncomingGrainCallFilter, IReplicationTransport, IReplogSink
    {
        public ConcurrentQueue<WalRecord> Sent { get; } = new();
        public ConcurrentQueue<string> Nudges { get; } = new();

        public async Task Invoke(IIncomingGrainCallContext context)
        {
            await context.Invoke();
            if (context.Grain is ReplicationShipperGrain shipper
                && context.InterfaceMethod.Name == nameof(IReplicationShipperGrain.IsShippingPausedAsync))
            {
                await shipper.PumpForTestingAsync();
            }
        }

        public Task WriteAsync(string treeId, CancellationToken cancellationToken)
        {
            Nudges.Enqueue(treeId);
            return Task.CompletedTask;
        }

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            Assert.That(batch.TreeName, Is.EqualTo(Tree));
            foreach (var entry in batch.EncodedEnvelope!.Value.EncodedEntries.Span)
            {
                Sent.Enqueue(encoder.Decode(entry, batch.TreeName));
            }
            return Task.FromResult(new ReplicationAck { Accepted = true });
        }
    }

    private sealed class Configurator : ISiloConfigurator
    {
        public void Configure(ISiloBuilder siloBuilder)
        {
            siloBuilder.AddLattice((silo, name) => silo.AddMemoryGrainStorage(name));
            siloBuilder.ConfigureLattice(options => options.WalPartitions = 1);
            siloBuilder.UseInMemoryReminderService();
            siloBuilder.AddLatticeReplication(options =>
            {
                options.ClusterId = "observer-source";
                options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode> { [Tree] = LatticeMergeMode.LwwRegister };
                options.ReplicationPeers = [Peer];
                options.ReplogPartitions = 1;
                options.ShipPhaseTimerPeriod = TimeSpan.FromHours(1);
                options.ShipSourceIdentityBackstopInterval = TimeSpan.FromHours(1);
                options.LivenessProbeInterval = Timeout.InfiniteTimeSpan;
            });
            siloBuilder.Services.AddSingleton<Probe>();
            siloBuilder.Services.AddSingleton<IIncomingGrainCallFilter>(services => services.GetRequiredService<Probe>());
            siloBuilder.Services.AddSingleton<IReplicationTransport>(services => services.GetRequiredService<Probe>());
            siloBuilder.Services.AddSingleton<IReplogSink>(services => services.GetRequiredService<Probe>());
        }
    }
}
