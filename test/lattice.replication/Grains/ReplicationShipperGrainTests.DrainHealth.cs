using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Replication.Tests.PublicApiContract;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Replication.Tests.Grains;

public partial class ReplicationShipperGrainTests
{
    [TestCase(false, false, false, TestName = "DrainHealth_unchanged_source_delivers_without_a_reseed")]
    [TestCase(true, false, false, TestName = "DrainHealth_physical_move_with_same_lineage_delivers_without_a_reseed")]
    [TestCase(true, true, false, TestName = "DrainHealth_lineage_change_requires_a_later_export_even_after_plain_write_delivery")]
    [TestCase(false, false, true, TestName = "DrainHealth_trim_requires_a_later_export_even_after_plain_write_delivery")]
    public async Task DrainHealth_controlled_source_transition_preserves_the_reseed_boundary(
        bool movePhysical, bool changeLineage, bool trim)
    {
        const string physical = "drain-health-rebound";
        const long marker = 7;
        var target = "drain-health-" + movePhysical + "-" + changeLineage + "-" + trim;
        var options = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ReplogPartitions = 1,
            ShipMaxInFlight = 1,
            ShipCursorWriteInterval = 1,
            WireVersionNegotiationEnabled = false,
            LivenessProbeInterval = TimeSpan.Zero,
            AutoBootstrapOnFallOffLog = false,
        };
        var monitor = Monitor(options);
        var walEncoder = new StubWalRecordEncoder();
        var original = new StubReplogShardGrain(walEncoder);
        var replacement = new StubReplogShardGrain(walEncoder);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalShardGrain>($"{Tree}/0").Returns(original);
        factory.GetGrain<IWalShardGrain>($"{physical}/0").Returns(replacement);
        TreeRegistryEntry entry = new() { Lineage = Guid.NewGuid() };
        var registry = Substitute.For<ILatticeRegistry>();
        registry.GetEntryAsync(Tree).Returns(_ => Task.FromResult<TreeRegistryEntry?>(entry));
        registry.ResolveAsync(Tree).Returns(_ => Task.FromResult(entry.PhysicalTreeId ?? Tree));
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        factory.GetGrain<IReplicationExportEpochGrain>(Tree).GetAsync().Returns(Task.FromResult(marker));
        long? completed = marker;
        var coordinator = Substitute.For<ILatticeBootstrapCoordinatorGrain>();
        coordinator.GetCompletedExportEpochAsync(LocalCluster).Returns(_ => Task.FromResult(completed));
        coordinator.GetStatusAsync(Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new BootstrapCoordinatorStatus(LatticeBootstrapState.Idle, null)));
        factory.GetGrain<ILatticeBootstrapCoordinatorGrain>(Tree).Returns(coordinator);
        var receiverContext = Substitute.For<ILatticeReplicationContext>();
        receiverContext.ResolveMergeMode(Tree).Returns(LatticeMergeMode.LwwRegister);
        var delivered = new ConcurrentQueue<WalRecord>();
        var applier = Substitute.For<IReplicationApplier>();
        applier.ApplyBatchAsync(Arg.Any<IReadOnlyList<WalRecord>>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var records = call.Arg<IReadOnlyList<WalRecord>>();
                foreach (var record in records) delivered.Enqueue(record);
                return Task.FromResult(new ApplyResult
                {
                    HighWaterMark = records.Count == 0 ? HybridLogicalClock.Zero : records[^1].Timestamp,
                });
            });
        using var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(factory)
            .AddSingleton(monitor)
            .AddSingleton(receiverContext)
            .AddSingleton(applier)
            .AddSingleton<IWalRecordEncoder>(walEncoder)
            .BuildServiceProvider();
        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("shipper", $"{Tree}/{target}"));
        var state = new FakePersistentState<ReplicationShipperState>();
        var stats = new ReplicationPeerStats();
        var logger = Substitute.For<ILogger<ReplicationShipperGrain>>();
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns(LatticeMergeMode.LwwRegister);
        var shipper = new ReplicationShipperGrain(
            context, Substitute.For<IReminderRegistry>(), logger,
            monitor, new LoopbackDeliveringTransport(), new TestEncoder(), walEncoder,
            Substitute.For<IWalCursorRegistry>(), factory, state, stats, resolver,
            new WireVersionNegotiationState(), new NoOpReplicationDigestProbeTransport());
        shipper.InitializeForTesting(Tree, target);
        LoopbackDeliveringTransport.RegisterCluster(target, services);
        try
        {
            original.Append(MakeEntry("before", ticks: 10));
            await shipper.PumpForTestingAsync();
            Assert.That(delivered.Any(record => record.Key == "before"), Is.True,
                "establish a real acknowledged binding before changing the source");
            Assert.Multiple(() =>
            {
                Assert.That(state.State.BoundPhysicalTreeId, Is.EqualTo(Tree));
                Assert.That(state.State.BoundSourceLineage, Is.EqualTo(entry.Lineage));
                Assert.That(state.State.PartitionCursors[0], Is.EqualTo(1));
            });
            Assert.That(await shipper.GetDurableReadPositionsAsync(Tree), Is.EqualTo(new long[] { 1 }));
            TestContext.WriteLine(
                $"before:physical={state.State.BoundPhysicalTreeId};lineage={state.State.BoundSourceLineage};"
                + $"cursor={state.State.PartitionCursors[0]};trimmedThrough={await original.GetTrimWatermarkAsync(CancellationToken.None)}");

            var active = original;
            if (movePhysical)
            {
                entry = entry with { PhysicalTreeId = physical };
                if (changeLineage) entry = entry with { Lineage = Guid.NewGuid() };
                await shipper.NotifySourceIdentityChangedAsync(physical, CancellationToken.None);
                active = replacement;
            }
            if (trim)
            {
                original.Append(MakeEntry("lost-unshipped", ticks: 15));
                original.TrimmedThrough = 2;
            }

            active.Append(MakeEntry("west-write", ticks: 20) with { Value = [2] });
            await shipper.PumpForTestingAsync();
            var west = delivered.Single(record => record.Key == "west-write");
            var needsReseed = changeLineage || trim;
            var expectedCursor = movePhysical ? 1L : trim ? 3L : 2L;
            var expectedRetain = changeLineage ? 0L : 2L;
            var retained = await active.ReadShippingAsync(0, 1, CancellationToken.None);
            var row = stats.ReadStatusPage(new ReplicationPeerStatusReadRequest
            {
                TreeId = Tree, Peer = target, Limit = 10,
            }).Single(record => record.Direction == ReplicationContactDirection.Outbound);
            var warnings = logger.ReceivedCalls()
                .Where(call => call.GetMethodInfo().Name == nameof(ILogger.Log)
                    && (LogLevel)call.GetArguments()[0]! == LogLevel.Warning)
                .Select(call => call.GetArguments()[2]!.ToString())
                .ToArray();
            Assert.Multiple(() =>
            {
                Assert.That(west.Value, Is.EqualTo(new byte[] { 2 }));
                Assert.That(state.State.BoundPhysicalTreeId, Is.EqualTo(movePhysical ? physical : Tree));
                Assert.That(state.State.BoundSourceLineage, Is.EqualTo(entry.Lineage));
                Assert.That(state.State.PartitionCursors[0], Is.EqualTo(expectedCursor));
                Assert.That(retained.Entries[0].Sequence, Is.EqualTo(trim ? 2 : 0));
                Assert.That(shipper.ReseedRequired, Is.EqualTo(needsReseed),
                    "plain-write delivery is not evidence that a lineage replacement or trimmed gap was recovered");
                Assert.That(row.ReseedRequiredSeconds is not null, Is.EqualTo(needsReseed));
                Assert.That(state.State.ReseedRequiredEpoch, Is.EqualTo(needsReseed ? marker : (long?)null));
                Assert.That(state.State.ReseedRetainFrom,
                    Is.EqualTo(needsReseed ? new[] { expectedRetain } : null),
                    "the reseed keeps its rewind floor even after the plain write advances the acknowledged cursor");
                Assert.That(row.ConsecutiveErrors, Is.Zero);
                Assert.That(row.InFlight, Is.Zero);
            });
            Assert.That(await shipper.GetDurableReadPositionsAsync(movePhysical ? physical : Tree),
                Is.EqualTo(new[] { needsReseed ? expectedRetain : expectedCursor }));
            Assert.That(await active.GetTrimWatermarkAsync(CancellationToken.None), Is.EqualTo(trim ? 1 : -1));
            Assert.That(warnings.Any(warning => warning!.Contains("source tree's lineage changed", StringComparison.Ordinal)),
                Is.EqualTo(changeLineage), "attribute a lineage marker to the production source-binding warning");
            Assert.That(warnings.Any(warning => warning!.Contains("trimmed past the unshipped cursor", StringComparison.Ordinal)),
                Is.EqualTo(trim), "attribute a trim marker to the production WAL-gap warning");
            TestContext.WriteLine(
                $"physical={state.State.BoundPhysicalTreeId};lineage={state.State.BoundSourceLineage};"
                + $"cursor={state.State.PartitionCursors[0]};firstRetained={retained.Entries[0].Sequence};"
                + $"reseedRetainFrom={string.Join(",", state.State.ReseedRetainFrom ?? [])};"
                + $"trimmedThrough={await active.GetTrimWatermarkAsync(CancellationToken.None)};"
                + $"marker={state.State.ReseedRequiredEpoch};warnings={string.Join(" | ", warnings)}");
            if (!needsReseed) return;

            completed = null;
            await shipper.PumpForTestingAsync();
            Assert.That(shipper.ReseedRequired, Is.True, "an unknown completion must not retire the marker");
            completed = marker;
            await shipper.PumpForTestingAsync();
            Assert.That(shipper.ReseedRequired, Is.True, "an equal export epoch must not retire the marker");
            completed = marker + 1;
            for (var tick = 0; tick < 8 && shipper.ReseedRequired; tick++)
                await shipper.PumpForTestingAsync();
            Assert.That(shipper.ReseedRequired, Is.False,
                "only a strictly later export echoed on an idle pump may complete recovery; no extra write carries it");
            Assert.That(stats.ReadStatusPage(new ReplicationPeerStatusReadRequest
            {
                TreeId = Tree, Peer = target, Limit = 10,
            }).Single(record => record.Direction == ReplicationContactDirection.Outbound).ReseedRequiredSeconds, Is.Null);
        }
        finally
        {
            LoopbackDeliveringTransport.UnregisterCluster(target);
        }
    }
}
