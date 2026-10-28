using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.PublicApiContract;
using Orleans.Lattice.Testing;
using Orleans.TestingHost;

namespace Orleans.Lattice.Replication.Tests;

public partial class CrossTreeSiblingBoundaryIntegrationTests
{
    [Test]
    public async Task Seeded_trees_with_an_empty_reverse_binding_import_and_ship_drain_updates_in_both_directions()
    {
        const string tree = "xtsb-startup-tree";
        const string sibling = "xtsb-startup-sibling";
        const string statusKey = "tenant-status";
        const string eastWriteKey = "east-write";
        const string westWriteKey = "west-write";
        const string draining = "Draining";
        const string offline = "Offline";
        const string removed = "Removed";
        var samples = new List<string>();
        var services = _siteB.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        var logger = Substitute.For<ILogger>();
        logger.IsEnabled(Arg.Any<LogLevel>()).Returns(true);
        var provider = Substitute.For<ILoggerProvider>();
        provider.CreateLogger(Arg.Any<string>()).Returns(call =>
            call.Arg<string>() == typeof(ReplicationShipperGrain).FullName ? logger : NullLogger.Instance);
        services.GetRequiredService<ILoggerFactory>().AddProvider(provider);

        try
        {
            await WriteAsync(tree, sibling);
            LatticeBootstrapCoordinatorGrain.SiblingBoundaryReseedAfter = TimeSpan.FromSeconds(10);

            var registry = _siteB.Client.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
            Assert.That(await registry.GetEntryAsync(sibling), Is.Null,
                "the control must start before site B has registered or imported the sibling");
            await registry.RegisterAsync(sibling);
            var before = await registry.GetEntryAsync(sibling);
            Assert.That(before, Is.Not.Null);
            Assert.That(before!.Lineage, Is.Not.Null.And.Not.EqualTo(Guid.Empty));
            var oldPhysical = before.PhysicalTreeId ?? sibling;
            var oldHeads = await CaptureStartupLogAsync("before-binding", sibling, before);
            Assert.That(oldHeads, Is.Not.Empty.And.All.Zero,
                "no record may prime the reverse binding before the real bootstrap");

            var shipper = _siteB.Client.GetGrain<IReplicationShipperGrain>($"{sibling}/{SiteAClusterId}");
            var consumer = shipper.AsReference<IWalOffsetConsumer>();
            await shipper.NotifySourceIdentityChangedAsync(oldPhysical, CancellationToken.None);
            await shipper.EnsureActiveAsync(CancellationToken.None);
            await TestPoll.UntilAsync(async () =>
                    (await _siteB.Client.GetGrain<IWalOffsetConsumerRegistryGrain>(oldPhysical).GetConsumersAsync())
                    .Contains(shipper.GetGrainId())
                    && await consumer.GetDurableReadPositionsAsync(oldPhysical) is { Length: > 0 } positions
                    && positions.All(position => position == 0)
                    && await consumer.GetDurableReadPositionsAsync(oldPhysical + "-not-bound") is null,
                "the exact reverse shipper to bind and register the empty pre-import physical log",
                TimeSpan.FromSeconds(10));
            Assert.That(await CaptureStartupLogAsync("bound-empty", sibling, before), Is.EqualTo(oldHeads));
            Assert.That(await consumer.GetDurableReadPositionsAsync(oldPhysical),
                Is.EqualTo(new long[oldHeads.Length]));
            TestContext.WriteLine(
                $"pre-binding:physical={oldPhysical};positions="
                + $"{string.Join(",", (await consumer.GetDurableReadPositionsAsync(oldPhysical))!)}");
            WriteStartupWarnings(logger, sibling);
            Assert.That(await _siteB.Client.GetGrain<ILatticeBootstrapCoordinatorGrain>(sibling)
                .GetStateAsync(CancellationToken.None), Is.EqualTo(LatticeBootstrapState.Idle));

            var bootstrapTasks = new[]
            {
                Coordinator(tree).BootstrapAsync(SiteAClusterId, CancellationToken.None),
                Coordinator(sibling).BootstrapAsync(SiteAClusterId, CancellationToken.None),
            };
            var recovered = await WaitForCoordinatorPhasesAsync(
                tree, sibling, TimeSpan.FromSeconds(30), samples, LatticeBootstrapState.LiveIncremental);
            if (recovered)
            {
                await Task.WhenAll(bootstrapTasks);
            }

            Assert.That(recovered, Is.True,
                $"Both imports should leave the mutual sibling hold within two minutes. Samples:{Environment.NewLine}{string.Join(Environment.NewLine, samples)}");

            var after = await registry.GetEntryAsync(sibling);
            Assert.That(after, Is.Not.Null);
            var newPhysical = after!.PhysicalTreeId ?? sibling;
            await CaptureStartupLogAsync("after-cutover-before-observation", sibling, after);
            TestContext.WriteLine(
                $"cutover:oldPhysical={oldPhysical};newPhysical={newPhysical};"
                + $"oldLineage={before.Lineage};newLineage={after.Lineage}");
            Assert.Multiple(() =>
            {
                Assert.That(newPhysical, Is.Not.EqualTo(oldPhysical), "the real bootstrap must publish its shadow copy");
                Assert.That(after.Lineage, Is.EqualTo(before.Lineage),
                    "ordinary bootstrap SwapAlias preserves the logical lineage; no synthetic mutation is allowed");
            });
            await shipper.NotifySourceIdentityChangedAsync(newPhysical, CancellationToken.None);
            await TestPoll.UntilAsync(async () =>
                    (await _siteB.Client.GetGrain<IWalOffsetConsumerRegistryGrain>(newPhysical).GetConsumersAsync())
                    .Contains(shipper.GetGrainId())
                    && await consumer.GetDurableReadPositionsAsync(newPhysical) is { Length: > 0 }
                    && await consumer.GetDurableReadPositionsAsync(oldPhysical) is null,
                "the pre-import reverse shipper to observe and bind the actual bootstrap cutover",
                TimeSpan.FromSeconds(10));
            await CaptureStartupLogAsync("after-authoritative-observation", sibling, after);
            TestContext.WriteLine(
                $"post-binding:physical={newPhysical};positions="
                + $"{string.Join(",", (await consumer.GetDurableReadPositionsAsync(newPhysical))!)}");
            TestContext.WriteLine("post-binding-health:" + string.Join(" | ", ReadOutboundHealth(sibling)));
            WriteStartupWarnings(logger, sibling);

            foreach (var treeName in new[] { tree, sibling })
            {
                await Task.WhenAll(
                    _siteA.Client.GetGrain<IReplicationShipperGrain>($"{treeName}/{SiteBClusterId}")
                        .EnsureActiveAsync(CancellationToken.None),
                    _siteB.Client.GetGrain<IReplicationShipperGrain>($"{treeName}/{SiteAClusterId}")
                        .EnsureActiveAsync(CancellationToken.None));
            }

            await _siteA.Client.GetGrain<ILattice>(tree).SetAsync(statusKey, System.Text.Encoding.UTF8.GetBytes(draining));
            await SignalShipperAsync(_siteA.Client, tree, SiteBClusterId);
            var drainingArrived = await WaitForValueAsync(_siteB.Client, tree, statusKey, draining, TimeSpan.FromSeconds(40));

            await _siteB.Client.GetGrain<ILattice>(tree).SetAsync(statusKey, System.Text.Encoding.UTF8.GetBytes(offline));
            await SignalShipperAsync(_siteB.Client, tree, SiteAClusterId);
            var offlineReturned = await WaitForValueAsync(_siteA.Client, tree, statusKey, offline, TimeSpan.FromSeconds(40));

            await _siteB.Client.GetGrain<ILattice>(tree).SetAsync(statusKey, System.Text.Encoding.UTF8.GetBytes(removed));
            await SignalShipperAsync(_siteB.Client, tree, SiteAClusterId);
            var removedReturned = await WaitForValueAsync(_siteA.Client, tree, statusKey, removed, TimeSpan.FromSeconds(40));

            await _siteA.Client.GetGrain<ILattice>(sibling).SetAsync(eastWriteKey, [1]);
            await SignalShipperAsync(_siteA.Client, sibling, SiteBClusterId);
            var eastWriteArrived = await WaitForValueAsync(_siteB.Client, sibling, eastWriteKey, [1], TimeSpan.FromSeconds(40));

            await _siteB.Client.GetGrain<ILattice>(sibling).SetAsync(westWriteKey, [2]);
            await SignalShipperAsync(_siteB.Client, sibling, SiteAClusterId);
            var westWriteArrived = await WaitForValueAsync(_siteA.Client, sibling, westWriteKey, [2], TimeSpan.FromSeconds(40));

            var stable = await CaptureDrainStateAsync(tree, sibling);
            var healthyLinks = ReadOutboundHealth(tree, sibling);
            TestContext.WriteLine("final-health:" + string.Join(" | ", healthyLinks));
            TestContext.WriteLine("final-bootstrap:" + stable);
            await CaptureStartupLogAsync("after-values", sibling, after);
            TestContext.WriteLine("reseed-requests:" + string.Join(" | ", LoopbackDeliveringTransport.Sent
                .Where(batch => batch.TreeName == sibling && batch.OriginClusterId == SiteBClusterId)
                .Select(batch => $"{batch.OriginClusterId}->{batch.TargetClusterId}:marker={batch.ReseedAfterEpoch}")));

            Assert.Multiple(() =>
            {
                Assert.That(drainingArrived, Is.True, "the origin's Draining write should reach the drained region");
                Assert.That(offlineReturned, Is.True, "the drained region's Offline write should reach the origin");
                Assert.That(removedReturned, Is.True, "the drained region's Removed write should reach the origin");
                Assert.That(eastWriteArrived, Is.True, "the origin-to-peer write should converge on the sibling tree");
                Assert.That(westWriteArrived, Is.True, "the peer-to-origin write should converge on the sibling tree");
                Assert.That(stable, Does.Contain($"{tree}:A=Idle,B=LiveIncremental"));
                Assert.That(stable, Does.Contain($"{sibling}:A=Idle,B=LiveIncremental"));
                Assert.That(healthyLinks, Has.Length.EqualTo(4), string.Join(Environment.NewLine, healthyLinks));
                Assert.That(healthyLinks, Is.All.Contains("healthy=True"), string.Join(Environment.NewLine, healthyLinks));
            });
        }
        finally
        {
            WriteStartupWarnings(logger, sibling);
            logger.IsEnabled(Arg.Any<LogLevel>()).Returns(false);
        }
    }

    private async Task<long[]> CaptureStartupLogAsync(string boundary, string logical, TreeRegistryEntry entry)
    {
        var physical = entry.PhysicalTreeId ?? logical;
        var services = _siteB.Silos.OfType<InProcessSiloHandle>().First().SiloHost.Services;
        var partitions = await services.GetRequiredService<LatticeOptionsResolver>().GetWalPartitionsAsync(physical);
        var heads = new long[partitions];
        var watermarks = new long?[partitions];
        for (var partition = 0; partition < partitions; partition++)
        {
            var wal = _siteB.Client.GetGrain<IWalShardGrain>($"{physical}/{partition}");
            heads[partition] = await wal.GetNextSequenceAsync(CancellationToken.None);
            watermarks[partition] = await wal.GetTrimWatermarkAsync(CancellationToken.None);
        }

        TestContext.WriteLine(
            $"{boundary}:logical={logical};physical={physical};lineage={entry.Lineage};"
            + $"heads={string.Join(",", heads)};watermarks={string.Join(",", watermarks)}");
        return heads;
    }

    private static void WriteStartupWarnings(ILogger logger, string sibling)
    {
        var warnings = logger.ReceivedCalls()
            .Where(call => call.GetMethodInfo().Name == nameof(ILogger.Log)
                && (LogLevel)call.GetArguments()[0]! >= LogLevel.Warning)
            .Select(call => call.GetArguments()[2]!.ToString())
            .Where(message => message!.Contains(sibling, StringComparison.Ordinal))
            .ToArray();
        TestContext.WriteLine("actual-shipper-warnings:" + string.Join(" | ", warnings));
    }
}
