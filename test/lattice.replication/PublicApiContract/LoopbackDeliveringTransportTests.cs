using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Grains;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>Receiver feedback on the in-process transport used by replication integration tests.</summary>
[TestFixture]
public class LoopbackDeliveringTransportTests
{
    [Test]
    public async Task Send_completed_reseed_echoes_the_export_epoch_on_an_idle_link()
    {
        const string tree = "loopback-completed-reseed";
        const string source = "loopback-reseed-source";
        const string target = "loopback-reseed-target";
        var coordinator = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        coordinator.GetCompletedExportEpochAsync(source).Returns(Task.FromResult<long?>(8));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree).Returns(coordinator);
        using var services = CreateServices(factory);
        LoopbackDeliveringTransport.RegisterCluster(target, services);
        try
        {
            var ack = await new LoopbackDeliveringTransport().SendAsync(
                Probe(tree, source, target, reseedAfter: 7), CancellationToken.None);

            Assert.Multiple(() =>
            {
                Assert.That(ack.Accepted, Is.True);
                Assert.That(ack.BootstrapEpoch, Is.EqualTo(8),
                    "an idle sender must be able to retire its durable re-seed marker");
            });
            await coordinator.DidNotReceiveWithAnyArgs()
                .BootstrapForReseedAsync(default!, default, default, default);
        }
        finally
        {
            LoopbackDeliveringTransport.UnregisterCluster(target);
        }
    }

    [TestCase(true, TestName = "Send_unsettled_reseed_records_the_request_with_automatic_bootstrap_enabled")]
    [TestCase(false, TestName = "Send_unsettled_reseed_records_the_request_with_automatic_bootstrap_disabled")]
    public async Task Send_unsettled_reseed_records_the_request_and_honours_automatic_bootstrap(bool autoBootstrap)
    {
        const string tree = "loopback-pending-reseed";
        const string source = "loopback-pending-source";
        const string target = "loopback-pending-target";
        var coordinator = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        coordinator.GetCompletedExportEpochAsync(source).Returns(Task.FromResult<long?>(7));
        coordinator.GetStatusAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new BootstrapCoordinatorStatus(LatticeBootstrapState.Idle, null)));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree).Returns(coordinator);
        using var services = CreateServices(factory, autoBootstrap);
        LoopbackDeliveringTransport.RegisterCluster(target, services);
        try
        {
            var ack = await new LoopbackDeliveringTransport().SendAsync(
                Probe(tree, source, target, reseedAfter: 7), CancellationToken.None);

            Assert.That(ack.BootstrapEpoch, Is.EqualTo(7),
                "an equal epoch does not settle the sender's marker");
            await coordinator.Received(1)
                .BootstrapForReseedAsync(source, 7, autoBootstrap, Arg.Any<CancellationToken>());
        }
        finally
        {
            LoopbackDeliveringTransport.UnregisterCluster(target);
        }
    }

    [Test]
    public async Task Send_unenrolled_tree_does_not_activate_a_bootstrap_coordinator()
    {
        const string tree = "loopback-unenrolled-reseed";
        const string source = "loopback-unenrolled-source";
        const string target = "loopback-unenrolled-target";
        var factory = Substitute.For<IGrainFactory>();
        using var services = CreateServices(factory, enrolled: false);
        LoopbackDeliveringTransport.RegisterCluster(target, services);
        try
        {
            var ack = await new LoopbackDeliveringTransport().SendAsync(
                Probe(tree, source, target, reseedAfter: 7), CancellationToken.None);

            Assert.That(ack.BootstrapEpoch, Is.Null);
            Assert.That(factory.ReceivedCalls(), Is.Empty,
                "wire tree names must not start a bootstrap outside local enrollment");
        }
        finally
        {
            LoopbackDeliveringTransport.UnregisterCluster(target);
        }
    }

    [Test]
    public async Task Send_pending_reseed_refuses_a_straggling_prepare_before_apply()
    {
        const string tree = "loopback-straggler-reseed";
        const string source = "loopback-straggler-source";
        const string target = "loopback-straggler-target";
        var coordinator = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        coordinator.IsReseedPendingAsync(source).Returns(Task.FromResult(true));
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(tree).Returns(coordinator);
        var encoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var entry = encoder.EncodeToBytes(new WalRecord
        {
            TreeId = tree,
            OriginClusterId = source,
            Op = MutationKind.Set,
            Key = "pending",
            IsPrepared = true,
            TransactionId = Guid.NewGuid(),
        });
        using var services = CreateServices(factory, encoder: encoder);
        LoopbackDeliveringTransport.RegisterCluster(target, services);
        try
        {
            var probe = Probe(tree, source, target, reseedAfter: 7);
            var batch = probe with
            {
                ReseedAfterEpoch = null,
                EncodedEnvelope = probe.EncodedEnvelope!.Value with
                {
                    Header = probe.EncodedEnvelope.Value.Header with { EntryCount = 1 },
                    EncodedEntries = new ArraySegment<byte>[] { new(entry) },
                },
            };
            var ack = await new LoopbackDeliveringTransport().SendAsync(batch, CancellationToken.None);

            Assert.That(ack.Accepted, Is.False,
                "a prepare sent before the marker must not restage a bucket the re-seed is clearing");
            await services.GetRequiredService<IReplicationApplier>().DidNotReceive()
                .ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>());
        }
        finally
        {
            LoopbackDeliveringTransport.UnregisterCluster(target);
        }
    }

    private static ServiceProvider CreateServices(
        IGrainFactory factory,
        bool autoBootstrap = true,
        bool enrolled = true,
        IWalRecordEncoder? encoder = null)
    {
        var options = Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        options.Get(Arg.Any<string>()).Returns(new LatticeReplicationOptions { AutoBootstrapOnFallOffLog = autoBootstrap });
        var context = Substitute.For<ILatticeReplicationContext>();
        context.ResolveMergeMode(Arg.Any<string>()).Returns(enrolled ? LatticeMergeMode.LwwRegister : null);
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new ApplyResult()));
        return new ServiceCollection()
            .AddLogging()
            .AddSingleton(factory)
            .AddSingleton(options)
            .AddSingleton(context)
            .AddSingleton(applier)
            .AddSingleton(encoder ?? Substitute.For<IWalRecordEncoder>())
            .BuildServiceProvider();
    }

    private static ReplicationBatch Probe(string tree, string source, string target, long reseedAfter) => new()
    {
        TreeName = tree,
        OriginClusterId = source,
        TargetClusterId = target,
        ReseedAfterEpoch = reseedAfter,
        EncodedEnvelope = new ReplicationBatchEncodedEnvelope
        {
            Header = new EncodedBatchHeader
            {
                Magic = EncodedBatchHeader.MagicValue,
                WireVersion = EncodedBatchHeader.CurrentWireVersion,
                Mode = LatticeMergeMode.LwwRegister,
            },
            EncodedEntries = ReadOnlyMemory<ArraySegment<byte>>.Empty,
        },
    };
}
