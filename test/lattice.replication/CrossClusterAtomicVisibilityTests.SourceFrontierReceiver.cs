using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4586 part 2b, sender and receiver together: the real shipper over
/// stub WAL partitions ships to the real receiver tree frontier, origin
/// frontier and high-water-mark grains, with each batch's frontier observed as
/// the gRPC receive path observes it and the receiver's epoch returned on the
/// acknowledgements. These are the model's receiver-restore detectors: a
/// replacement of the receiver's contents is a forced gap at the sender, the
/// origin's watermark stays at its cap until the re-seed completes and the
/// sender re-covers the tree, and a dependent of a write the new contents lack
/// is never released on coverage from before the replacement.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    private sealed class FrontierReceiver
    {
        public const string Tree = "ccv-frontier-receiver";

        public Dictionary<string, ReplicationOriginFrontierGrain> Origins { get; } = new(StringComparer.Ordinal);
        public ITreeLineageSource Lineage { get; } = Substitute.For<ITreeLineageSource>();
        public ReplicationTreeFrontierGrain Frontier { get; }
        public ReplicationHighWaterMarkGrain Hwm { get; }

        private int _observed;

        public FrontierReceiver(string tree, Guid? lineage)
        {
            var factory = HighWaterMarkTestGrains.FrontierFactory(Origins);
            Hwm = HighWaterMarkTestGrains.Real(grainFactory: factory, treeId: tree);
            factory.GetGrain<IReplicationHighWaterMarkGrain>(tree, Arg.Any<string?>()).Returns(Hwm);
            Lineage.GetLineageAsync(tree, Arg.Any<CancellationToken>()).Returns(lineage);
            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(GrainId.Create("replication-tree-frontier", tree));
            Frontier = new ReplicationTreeFrontierGrain(
                context, factory, Lineage, NullLogger<ReplicationTreeFrontierGrain>.Instance,
                new FakePersistentState<ReplicationTreeFrontierState>());
        }

        public Task<HybridLogicalClock> OriginWatermarkAsync() =>
            Origins.TryGetValue(TwoSiteClusterFixture.SiteAClusterId, out var origin)
                ? origin.GetLowWatermarkAsync(CancellationToken.None)
                : Task.FromResult(HybridLogicalClock.Zero);

        public async Task<CausalDependencyVerdict> CheckAsync(HybridLogicalClock dependency)
        {
            var vector = new VersionVector();
            vector.Entries[TwoSiteClusterFixture.SiteAClusterId] = dependency;
            return (await Hwm.CheckDependenciesAsync([vector], CancellationToken.None))[0];
        }

        /// <summary>
        /// Pumps the shipper once per tick, acknowledging each batch with the
        /// receiver's epoch at that moment and recording the frontier it carried,
        /// as the gRPC receive path does.
        /// </summary>
        public async Task ExchangeAsync(ReplicationShipperGrain shipper, FrontierTransport transport, int ticks)
        {
            for (var i = 0; i < ticks; i++)
            {
                transport.Lineage = (await Frontier.GetAsync(CancellationToken.None)).Epoch;
                await PumpAsync(shipper, ticks: 1);
                for (; _observed < transport.Batches.Count; _observed++)
                {
                    await Frontier.ObserveAsync(
                        TwoSiteClusterFixture.SiteAClusterId,
                        transport.Batches[_observed].SourceFrontier,
                        CancellationToken.None);
                }
            }
        }
    }

    [Test]
    public async Task A_receiver_re_stamp_holds_the_origin_watermark_at_its_cap_until_the_re_seed_completes_and_the_sender_re_covers_the_tree()
    {
        const string tree = FrontierReceiver.Tree + "-cap";
        var ticks = DateTime.UtcNow.Ticks;
        var floor = Hlc(ticks, 50);
        var (shipper, feeds, transport, _) = FrontierShipper(tree, floor);
        var receiver = new FrontierReceiver(tree, Guid.NewGuid());
        await receiver.Frontier.ObserveAsync(TwoSiteClusterFixture.SiteAClusterId, null, CancellationToken.None);

        feeds[0].Append(LocalSet(tree, "a", Hlc(ticks, 10)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 1);
        feeds[0].Append(LocalSet(tree, "b", Hlc(ticks, 60)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 2);
        var vouched = await receiver.OriginWatermarkAsync();

        // The receiver restores the tree: the registry re-stamps its lineage.
        await receiver.Frontier.OnLineageChangingAsync(Guid.NewGuid(), CancellationToken.None);
        feeds[1].Append(LocalSet(tree, "c", Hlc(ticks, 61)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 2);
        var reseedForced = shipper.ReseedRequired;
        var whileAwaiting = await receiver.OriginWatermarkAsync();

        // The re-seed completes: the bootstrap pins the export, the sender sees the
        // echo, rewinds and re-covers the tree under the new epoch.
        var epoch = (await receiver.Frontier.GetAsync(CancellationToken.None)).Epoch;
        Assert.That(await receiver.Frontier.PinAsync(epoch, new Dictionary<string, HybridLogicalClock>(), new Dictionary<string, HybridLogicalClock[]>(), CancellationToken.None), Is.True);
        transport.Echo = 1;
        feeds[1].Append(LocalSet(tree, "d", Hlc(ticks, 62)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 6);
        feeds[0].Append(LocalSet(tree, "e", Hlc(ticks, 63)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 2);
        var recovered = await receiver.OriginWatermarkAsync();

        Assert.Multiple(() =>
        {
            Assert.That(vouched, Is.EqualTo(floor), "precondition: the receiver accepted the sender's watermark");
            Assert.That(reseedForced, Is.True, "the receiver's new epoch is a forced gap at the sender");
            Assert.That(whileAwaiting, Is.EqualTo(HybridLogicalClock.Zero), "the restored contents may lack what the old watermark covered");
            Assert.That(recovered, Is.EqualTo(floor), "the sender re-covered the tree under the new epoch");
        });
    }

    [Test]
    public async Task A_dependent_of_a_write_the_restored_receiver_lacks_waits_although_the_origin_had_vouched_past_it()
    {
        const string tree = FrontierReceiver.Tree + "-dependent";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport, _) = FrontierShipper(tree, Hlc(ticks, 50));
        var receiver = new FrontierReceiver(tree, Guid.NewGuid());
        await receiver.Frontier.ObserveAsync(TwoSiteClusterFixture.SiteAClusterId, null, CancellationToken.None);
        var w = Hlc(ticks, 10);

        feeds[0].Append(LocalSet(tree, "w", w));
        await receiver.ExchangeAsync(shipper, transport, ticks: 1);
        feeds[0].Append(LocalSet(tree, "x", Hlc(ticks, 60)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 2);
        var beforeRestore = await receiver.CheckAsync(w);

        // Restored to a copy taken before w landed.
        await receiver.Frontier.OnLineageChangingAsync(Guid.NewGuid(), CancellationToken.None);
        feeds[1].Append(LocalSet(tree, "y", Hlc(ticks, 61)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 2);
        var afterRestore = await receiver.CheckAsync(w);

        Assert.Multiple(() =>
        {
            Assert.That(beforeRestore, Is.EqualTo(CausalDependencyVerdict.Met), "precondition: the origin's watermark covered w");
            Assert.That(afterRestore, Is.EqualTo(CausalDependencyVerdict.Unmet),
                "the watermark from before the restore must not release a dependent of a write the new contents lack");
        });
    }

    [Test]
    public async Task A_first_receiver_lineage_after_unvouched_acknowledgements_forces_a_re_seed_and_releases_nothing()
    {
        const string tree = FrontierReceiver.Tree + "-first";
        var ticks = DateTime.UtcNow.Ticks;
        var (shipper, feeds, transport, _) = FrontierShipper(tree, Hlc(ticks, 50));

        // A legacy receiver row: no lineage, so the frontier is degraded and the
        // acknowledgements carry an empty epoch. A restore before the upgrade may
        // already have dropped w.
        var receiver = new FrontierReceiver(tree, lineage: null);
        var w = Hlc(ticks, 10);
        feeds[0].Append(LocalSet(tree, "w", w));
        feeds[0].Append(LocalSet(tree, "x", Hlc(ticks, 60)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 3);
        var degradedEpoch = (await receiver.Frontier.GetAsync(CancellationToken.None)).Epoch;

        // The registry stamps the tree's first lineage.
        await receiver.Frontier.OnLineageChangingAsync(Guid.NewGuid(), CancellationToken.None);
        feeds[1].Append(LocalSet(tree, "y", Hlc(ticks, 61)));
        await receiver.ExchangeAsync(shipper, transport, ticks: 3);

        var verdict = await receiver.CheckAsync(w);
        var watermark = await receiver.OriginWatermarkAsync();
        Assert.Multiple(() =>
        {
            Assert.That(degradedEpoch, Is.EqualTo(Guid.Empty), "precondition: the legacy receiver tracked no lineage");
            Assert.That(shipper.ReseedRequired, Is.True,
                "acknowledgements from before the first lineage cannot vouch for the stamped contents, so the first lineage is a forced gap");
            Assert.That(watermark, Is.EqualTo(HybridLogicalClock.Zero));
            Assert.That(verdict, Is.EqualTo(CausalDependencyVerdict.Unmet));
        });
    }

    [Test]
    public async Task A_dependent_of_a_write_an_uncoordinated_source_restore_destroyed_is_released_not_parked_forever()
    {
        // The source wrote w, a peer that learned it wrote d depending on it,
        // and the source then restored its tree to before w outside a
        // coordinated restore, so w never ships here. After the re-seed rewind
        // the source's watermark from its new log passes w: d is released
        // without w rather than parked for a write no relay can deliver. That is
        // the divergence a unilateral source restore accepts by contract; the
        // source counts the restore as uncoordinated.
        const string tree = FrontierReceiver.Tree + "-source-restore";
        var receiver = new FrontierReceiver(tree, Guid.NewGuid());
        var epoch = await receiver.Frontier.ObserveAsync(TwoSiteClusterFixture.SiteAClusterId, null, CancellationToken.None);
        var ticks = DateTime.UtcNow.Ticks;
        var w = Hlc(ticks, 10);
        var afterRewind = new ReplicationSourceFrontier
        {
            ReceiverLineage = epoch,
            TreeLowWatermark = Hlc(ticks, 50),
            OriginLowWatermark = Hlc(ticks, 50),
            OriginGeneration = 1,
        };

        var before = await receiver.CheckAsync(w);
        await receiver.Frontier.ObserveAsync(TwoSiteClusterFixture.SiteAClusterId, afterRewind, CancellationToken.None);
        var after = await receiver.CheckAsync(w);

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.EqualTo(CausalDependencyVerdict.Unmet), "precondition: w never arrived here");
            Assert.That(after, Is.EqualTo(CausalDependencyVerdict.Met), "released on the source's watermark, not parked for ever");
        });
    }
}
