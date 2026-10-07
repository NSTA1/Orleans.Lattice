using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Replication.Tests.Grains;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Issue #4534: a shipper that took its peer off the log clears the re-seed
/// marker once any ack - a push from a pipelined window, or a liveness probe on
/// a quiet link - echoes a later export epoch, and only then rewinds, after the
/// tick's batches have folded their cursors. Runs the real shipper.
/// </summary>
public partial class CrossClusterAtomicVisibilityTests
{
    [Test]
    public async Task Pipelined_shipper_clears_the_reseed_once_the_peer_echoes_a_later_epoch()
    {
        const string tree = "ccv-reseed-pipelined";
        var (feeds, walEncoder, txid, ticks) = TrimmedSagaFeeds(tree);
        long? echo = null;
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => echo);
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var shipper = CreateShipper(
            tree, feeds, walEncoder, transport,
            configureFactory: factory => factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry),
            configureOptions: o => o.ShipMaxInFlight = 2);

        await PumpAsync(shipper, ticks: 2);
        Assert.That(shipper.ReseedRequired, Is.True, "precondition: the trim took the peer off the log");

        echo = 1;
        feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain",
            Value = new byte[] { 3 },
            Timestamp = Hlc(ticks, 9),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        await PumpAsync(shipper, ticks: 4);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.False, "a pipelined push that echoes a later epoch clears the marker");
            Assert.That(shipped.Count(r => r.IsPrepared && r.TransactionId == txid), Is.EqualTo(1),
                "after the re-seed the retained saga record is re-shipped from the lowest retained entry");
        });
    }

    [Test]
    public async Task Quiet_shipper_clears_the_reseed_from_a_liveness_probe_echo()
    {
        const string tree = "ccv-reseed-probe";
        var (feeds, walEncoder, txid, _) = TrimmedSagaFeeds(tree);
        long? echo = null;
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => echo);
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var shipper = CreateShipper(
            tree, feeds, walEncoder, transport,
            configureFactory: factory => factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry),
            configureOptions: o => o.LivenessProbeInterval = TimeSpan.FromMilliseconds(1));

        await PumpAsync(shipper, ticks: 2);
        Assert.That(shipper.ReseedRequired, Is.True, "precondition: the trim took the peer off the log");

        // No further writes: only liveness probes reach the peer.
        echo = 1;
        for (var i = 0; i < 6 && shipper.ReseedRequired; i++)
        {
            await Task.Delay(5);
            await PumpAsync(shipper, ticks: 1);
        }

        var clearedByProbe = !shipper.ReseedRequired;
        await PumpAsync(shipper, ticks: 2);

        Assert.Multiple(() =>
        {
            Assert.That(clearedByProbe, Is.True, "a liveness probe that echoes a later epoch clears the marker on a quiet link");
            Assert.That(shipped.Count(r => r.IsPrepared && r.TransactionId == txid), Is.EqualTo(1),
                "and the retained saga record is re-shipped");
        });
    }

    [Test]
    public async Task Shipper_stays_off_the_log_while_a_silo_predates_the_purge_hold()
    {
        // The replay after the rewind is exact only while every registry
        // honours the replay's purge hold (#4533).
        const string tree = "ccv-reseed-preguard";
        var (feeds, walEncoder, txid, ticks) = TrimmedSagaFeeds(tree);
        long? echo = null;
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => echo);
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var supported = false;
        var shipper = CreateShipper(tree, feeds, walEncoder, transport,
            configureFactory: factory => factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));
        shipper.PurgeHoldSupportForTesting = () => supported;

        await PumpAsync(shipper, ticks: 2);
        echo = 1;
        feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain",
            Value = new byte[] { 3 },
            Timestamp = Hlc(ticks, 9),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        await PumpAsync(shipper, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.True, "an old silo keeps the peer off the log despite the echo");
            Assert.That(shipped.Any(r => r.TransactionId == txid), Is.False, "no saga record ships meanwhile");
            Assert.That(shipped.Any(r => r.Key == "plain"), Is.True, "plain writes keep shipping");
        });

        supported = true;
        await PumpAsync(shipper, ticks: 1);

        // Only an export opened after every silo honours the hold settles the
        // re-seed (#4664).
        echo = 2;
        feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain-2",
            Value = new byte[] { 4 },
            Timestamp = Hlc(ticks, 10),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        await PumpAsync(shipper, ticks: 3);

        Assert.That(shipper.ReseedRequired, Is.False, "once every silo honours the hold a later export completes the re-seed");
    }

    [Test]
    public async Task A_reseed_deferred_for_a_pre_hold_silo_does_not_settle_on_an_export_drained_before_the_upgrade()
    {
        // #4664: while a silo predated the purge hold, its registry could purge
        // a decision the export drained in that window still needed. The echo
        // of that export must not rewind once the upgrade completes; only an
        // export opened after every silo honours the hold settles the re-seed.
        const string tree = "ccv-reseed-preguard-window";
        var (feeds, walEncoder, txid, ticks) = TrimmedSagaFeeds(tree);
        long? echo = null;
        long exportEpoch = 0;
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => echo);
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var state = new FakePersistentState<ReplicationShipperState>();
        var supported = false;
        var epochGrain = Substitute.For<IReplicationExportEpochGrain>();
        epochGrain.GetAsync().Returns(_ => Task.FromResult(exportEpoch));
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state,
            configureFactory: factory =>
            {
                factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry);
                factory.GetGrain<IReplicationExportEpochGrain>(tree).Returns(epochGrain);
            });
        shipper.PurgeHoldSupportForTesting = () => supported;
        var plain = 0;
        void AppendPlain() => feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = $"plain-{plain}",
            Value = new byte[] { (byte)plain },
            Timestamp = Hlc(ticks, 20 + plain++),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });

        await PumpAsync(shipper, ticks: 2);
        Assert.That(shipper.ReseedRequired, Is.True, "precondition: the trim took the peer off the log at epoch 0");

        // The peer drains export 1 while a silo still predates the hold.
        exportEpoch = 1;
        echo = 1;
        AppendPlain();
        await PumpAsync(shipper, ticks: 3);
        Assert.That(shipper.ReseedRequired, Is.True, "precondition: the old silo keeps the peer off the log");

        // The upgrade completes; the peer keeps echoing the export it drained.
        supported = true;
        AppendPlain();
        await PumpAsync(shipper, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.True,
                "an export drained while a silo predated the purge hold must not settle the re-seed");
            Assert.That(state.State.ReseedRequiredEpoch, Is.GreaterThanOrEqualTo(1L),
                "the marker is raised to the export epoch current when every silo first honours the hold");
            Assert.That(shipped.Any(r => r.TransactionId == txid), Is.False, "no saga record ships meanwhile");
        });

        // A fresh export, opened after every silo honours the hold, settles it.
        exportEpoch = 2;
        echo = 2;
        AppendPlain();
        await PumpAsync(shipper, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.False, "an export opened after the upgrade settles the re-seed");
            Assert.That(shipped.Count(r => r.IsPrepared && r.TransactionId == txid), Is.EqualTo(1),
                "and the retained saga record is re-shipped");
        });
    }

    [Test]
    public async Task Replay_takes_the_peer_off_the_log_when_a_silo_that_predates_the_purge_hold_joins()
    {
        const string tree = "ccv-replay-preguard-join";
        var (feeds, walEncoder, txid, _) = TrimmedSagaFeeds(tree);
        feeds[0].TrimmedThrough = 0;
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => null);
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var state = new FakePersistentState<ReplicationShipperState>();
        state.State.ReplayFilterHorizon = [5, 5];
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state,
            configureFactory: factory => factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));
        shipper.PurgeHoldSupportForTesting = () => false;

        await PumpAsync(shipper, ticks: 1);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.True, "a replay whose verdicts are no longer exact takes the peer off the log");
            Assert.That(shipped.Any(r => r.TransactionId == txid), Is.False, "no saga record ships");
        });
    }

    [Test]
    public async Task Saga_records_withheld_for_a_reseed_stay_retained_for_its_rewind()
    {
        // Withheld records are consumed and the cursor passes them, but the
        // published read position must not, or the GC trims what the rewind
        // has to re-ship (#4533).
        const string tree = "ccv-reseed-retain";
        var (feeds, walEncoder, txid, ticks) = TrimmedSagaFeeds(tree);
        feeds[0].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain-after",
            Value = new byte[] { 5 },
            Timestamp = Hlc(ticks, 11),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => null);
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var state = new FakePersistentState<ReplicationShipperState>();
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state,
            configureFactory: factory => factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));

        await PumpAsync(shipper, ticks: 3);
        var positions = await shipper.GetDurableReadPositionsAsync(tree);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.True, "precondition: the trim took the peer off the log");
            Assert.That(shipped.Any(r => r.Key == "plain-after"), Is.True, "precondition: the plain write past the withheld record shipped");
            Assert.That(state.State.PartitionCursors[0], Is.GreaterThan(1L), "precondition: the cursor passed the withheld saga record");
            Assert.That(positions, Is.Not.Null);
            Assert.That(positions![0], Is.LessThanOrEqualTo(1L),
                "the published read position must keep the withheld saga record (sequence 1) from the GC");
        });
    }

    [Test]
    public async Task A_trim_past_withheld_records_takes_the_peer_off_the_log_again_instead_of_rewinding()
    {
        const string tree = "ccv-reseed-retrim";
        var (feeds, walEncoder, txid, ticks) = TrimmedSagaFeeds(tree);
        long? echo = null;
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => echo);
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var state = new FakePersistentState<ReplicationShipperState>();
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state,
            configureFactory: factory => factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));

        await PumpAsync(shipper, ticks: 2);
        Assert.That(shipper.ReseedRequired, Is.True, "precondition: the trim took the peer off the log");

        // The retention ceiling trims the withheld saga record too, then the peer
        // echoes a re-seed from an export that may predate its saga's decision.
        feeds[0].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain-p0",
            Value = new byte[] { 6 },
            Timestamp = Hlc(ticks, 8),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        feeds[0].TrimmedThrough = 2;
        echo = 1;
        feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain",
            Value = new byte[] { 3 },
            Timestamp = Hlc(ticks, 9),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        await PumpAsync(shipper, ticks: 3);

        Assert.Multiple(() =>
        {
            Assert.That(shipper.ReseedRequired, Is.True,
                "a rewind that could not re-ship a withheld record must not resume saga records");
            Assert.That(shipped.Any(r => r.TransactionId == txid), Is.False, "no record of the saga ships");
        });
    }

    [Test]
    public async Task Detached_shipper_does_not_rewind_when_its_peer_echoes_a_later_epoch()
    {
        // #4652 (epic #4430 review finding S2): a detached shipper has left the
        // log's offset consumers and released its purge holds, so the GC may
        // already have trimmed a prepare it never read. A later-epoch echo must
        // therefore not clear its re-seed or rewind it: only re-attaching, which
        // re-marks the re-seed, makes it eligible again.
        const string tree = "ccv-reseed-detached";
        var (feeds, walEncoder, txid, ticks) = TrimmedSagaFeeds(tree);
        long? echo = null;
        var shipped = new List<WalRecord>();
        var transport = RecordingTransport(walEncoder, shipped, () => echo);
        var registry = ReplayRegistry(Guid.NewGuid(), Guid.NewGuid(), txid);
        var state = new FakePersistentState<ReplicationShipperState>();
        var shipper = CreateShipper(tree, feeds, walEncoder, transport, state: state,
            configureFactory: factory => factory.GetGrain<ITxRegistryGrain>(Arg.Any<string>(), Arg.Any<string?>()).Returns(registry));

        await PumpAsync(shipper, ticks: 2);
        Assert.That(shipper.ReseedRequired, Is.True, "precondition: the trim took the peer off the log");

        await shipper.DetachFromLogAsync(CancellationToken.None);
        Assert.That(state.State.DetachedFromLog, Is.True, "precondition: the peer's removal detached the shipper");
        var markedCursor = state.State.PartitionCursors.TryGetValue(0, out var c) ? c : 0L;

        echo = 1;
        feeds[1].Append(new WalRecord
        {
            TreeId = tree,
            Op = MutationKind.Set,
            Key = "plain",
            Value = new byte[] { 3 },
            Timestamp = Hlc(ticks, 9),
            OriginClusterId = TwoSiteClusterFixture.SiteAClusterId,
        });
        await PumpAsync(shipper, ticks: 4);

        Assert.Multiple(() =>
        {
            Assert.That(shipped.Any(r => r.Key == "plain"), Is.True,
                "precondition: the detached shipper still ships plain writes, so the peer's echo was seen");
            Assert.That(shipper.ReseedRequired, Is.True,
                "a detached shipper must not clear its re-seed on an echo: the GC no longer holds the log for it");
            Assert.That(state.State.PartitionCursors.TryGetValue(0, out var after) ? after : 0L, Is.GreaterThanOrEqualTo(markedCursor),
                "a detached shipper must not rewind");
            Assert.That(shipped.Any(r => r.TransactionId == txid), Is.False, "no saga record ships while detached");
        });
    }

    private static (ReplicationShipperGrainTests.StubReplogShardGrain[] Feeds, ReplicationShipperGrainTests.StubWalRecordEncoder Encoder, Guid Txid, long Ticks)
        TrimmedSagaFeeds(string tree)
    {
        var txid = Guid.NewGuid();
        var ticks = DateTime.UtcNow.Ticks;
        var walEncoder = new ReplicationShipperGrainTests.StubWalRecordEncoder();
        var feeds = new[]
        {
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
            new ReplicationShipperGrainTests.StubReplogShardGrain(walEncoder),
        };
        feeds[0].Append(PreparedSet(tree, "trimmed", 2, txid, Hlc(ticks, 2), index: 1));
        feeds[0].Append(PreparedSet(tree, "retained", 1, txid, Hlc(ticks, 1), index: 0));
        feeds[0].TrimmedThrough = 1;
        return (feeds, walEncoder, txid, ticks);
    }
}
