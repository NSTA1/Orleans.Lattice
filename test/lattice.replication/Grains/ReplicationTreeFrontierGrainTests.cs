using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// The receiver's per-tree causal frontier (issue #4586 part 2b): the epoch it
/// acknowledges, the shipped watermarks it accepts, and the forced gap it opens
/// on every possible replacement of the tree's contents.
/// </summary>
[TestFixture]
public sealed class ReplicationTreeFrontierGrainTests
{
    private const string Tree = "tf-tree";
    private const string OriginA = "site-a";
    private const string OriginB = "site-b";

    private static readonly Guid RegistryLineage = Guid.Parse("11111111-2222-3333-4444-555555555555");

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private sealed class Harness
    {
        public Dictionary<string, ReplicationOriginFrontierGrain> Origins { get; } = new(StringComparer.Ordinal);
        public IGrainFactory Factory { get; }
        public IReplicationHighWaterMarkGrain Hwm { get; } = Substitute.For<IReplicationHighWaterMarkGrain>();
        public ITreeLineageSource Lineage { get; } = Substitute.For<ITreeLineageSource>();
        public FakePersistentState<ReplicationTreeFrontierState> State { get; } = new();
        public IGrainContext Context { get; } = Substitute.For<IGrainContext>();

        public Harness(Guid? lineage)
        {
            Factory = HighWaterMarkTestGrains.FrontierFactory(Origins);
            Factory.GetGrain<IReplicationHighWaterMarkGrain>(Tree, Arg.Any<string?>()).Returns(Hwm);
            Lineage.GetLineageAsync(Tree, Arg.Any<CancellationToken>()).Returns(lineage);
            Context.GrainId.Returns(GrainId.Create("replication-tree-frontier", Tree));
        }

        public ReplicationTreeFrontierGrain Activate() => new(
            Context, Factory, Lineage, NullLogger<ReplicationTreeFrontierGrain>.Instance, State);

        public ReplicationOriginFrontierGrain Origin(string origin) => (ReplicationOriginFrontierGrain)Factory.GetGrain<IReplicationOriginFrontierGrain>(origin);
    }

    private static ReplicationSourceFrontier Shipped(Guid epoch, long tree, long origin, long generation = 0) => new()
    {
        ReceiverLineage = epoch,
        TreeLowWatermark = Hlc(tree),
        OriginLowWatermark = Hlc(origin),
        OriginGeneration = generation,
    };

    [Test]
    public async Task Without_a_registry_lineage_the_tree_is_degraded_and_accepts_nothing()
    {
        var h = new Harness(lineage: null);
        var grain = h.Activate();

        var epoch = await grain.ObserveAsync(OriginA, Shipped(Guid.NewGuid(), 50, 40), CancellationToken.None);
        var snapshot = await grain.GetAsync(CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(epoch, Is.EqualTo(Guid.Empty));
            Assert.That(snapshot.LowWatermarks, Is.Empty);
            Assert.That(snapshot.RegistryLineage, Is.Null);
            Assert.That(await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task A_watermark_tagged_with_the_current_epoch_is_accepted_and_any_other_is_ignored()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        var epoch = await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);

        await grain.ObserveAsync(OriginA, Shipped(Guid.NewGuid(), 90, 90), CancellationToken.None);
        await grain.ObserveAsync(OriginA, Shipped(epoch, 50, 40), CancellationToken.None);
        var snapshot = await grain.GetAsync(CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(epoch, Is.Not.EqualTo(Guid.Empty));
            Assert.That(snapshot.LowWatermarks[OriginA], Is.EqualTo(Hlc(50)));
            Assert.That(snapshot.RegistryLineage, Is.EqualTo(RegistryLineage));
            Assert.That(await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(Hlc(40)));
        });
    }

    [Test]
    public async Task A_replacement_re_mints_the_epoch_zeroes_and_caps_every_origin_and_forgets_identities()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        var before = await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        await grain.ObserveAsync(OriginA, Shipped(before, 50, 40), CancellationToken.None);
        h.Hwm.ClearReceivedCalls();

        await grain.OnContentsReplacingAsync(CancellationToken.None);
        var after = await grain.ObserveAsync(OriginA, Shipped(before, 80, 80), CancellationToken.None);
        await grain.ObserveAsync(OriginA, Shipped(after, 80, 80), CancellationToken.None);
        var snapshot = await grain.GetAsync(CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(after, Is.Not.EqualTo(before).And.Not.EqualTo(Guid.Empty));
            Assert.That(snapshot.LowWatermarks, Is.Empty, "even a watermark tagged with the new epoch waits for the re-seed");
            Assert.That(await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(HybridLogicalClock.Zero),
                "the origin's aggregate may still count the lost coverage, so the tree caps it");
            await h.Hwm.Received(1).ResetAppliedIdentitiesAsync(Arg.Any<CancellationToken>());
        });
    }

    [Test]
    public async Task A_pin_installs_the_export_watermark_and_keeps_the_cap_until_the_origin_re_covers_the_tree()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        var before = await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        await grain.ObserveAsync(OriginA, Shipped(before, 50, 50, generation: 1), CancellationToken.None);
        await grain.OnContentsReplacingAsync(CancellationToken.None);
        var epoch = (await grain.GetAsync(CancellationToken.None)).Epoch;

        var pinned = await grain.PinAsync(epoch, new Dictionary<string, HybridLogicalClock> { [OriginA] = Hlc(30) }, new Dictionary<string, HybridLogicalClock[]>(), CancellationToken.None);
        var afterPin = await grain.GetAsync(CancellationToken.None);
        var cappedAt = await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None);

        // A late batch from before the re-cover cannot raise the aggregate past the cap.
        await h.Origin(OriginA).RecordLowWatermarkAsync(Hlc(70), generation: 1, CancellationToken.None);
        var stillCapped = await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None);

        await grain.ObserveAsync(OriginA, Shipped(epoch, 45, 40, generation: 2), CancellationToken.None);
        var lifted = await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None);
        await h.Origin(OriginA).RecordLowWatermarkAsync(Hlc(99), generation: 1, CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(pinned, Is.True);
            Assert.That(afterPin.LowWatermarks[OriginA], Is.EqualTo(Hlc(30)));
            Assert.That(cappedAt, Is.EqualTo(Hlc(30)), "the cap rises to the export's watermark at the pin");
            Assert.That(stillCapped, Is.EqualTo(Hlc(30)), "the cap rose to the export's watermark, not past it");
            Assert.That(lifted, Is.EqualTo(Hlc(40)));
            Assert.That(await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(Hlc(40)),
                "a generation older than the re-cover is ignored for good");
            Assert.That((await grain.GetAsync(CancellationToken.None)).LowWatermarks[OriginA], Is.EqualTo(Hlc(45)));
        });
    }

    [Test]
    public async Task A_pin_begun_before_a_replacement_is_refused()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        var begunUnder = (await grain.GetAsync(CancellationToken.None)).Epoch;
        await grain.OnContentsReplacingAsync(CancellationToken.None);

        var pinned = await grain.PinAsync(begunUnder, new Dictionary<string, HybridLogicalClock> { [OriginA] = Hlc(30) }, new Dictionary<string, HybridLogicalClock[]>(), CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(pinned, Is.False);
            Assert.That((await grain.GetAsync(CancellationToken.None)).LowWatermarks, Is.Empty);
        });
    }

    [Test]
    public async Task A_pin_publishes_the_writes_the_source_held_before_any_watermark()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        var epoch = (await grain.GetAsync(CancellationToken.None)).Epoch;
        await h.Origin(OriginA).RecordLowWatermarkAsync(Hlc(100), generation: 0, CancellationToken.None);
        h.Hwm.HasAppliedAsync(OriginA, Hlc(12)).Returns(false);

        await grain.PinAsync(
            epoch,
            new Dictionary<string, HybridLogicalClock> { [OriginA] = Hlc(30) },
            new Dictionary<string, HybridLogicalClock[]> { [OriginA] = [Hlc(12)] },
            CancellationToken.None);
        var verdicts = await h.Origin(OriginA).CheckAsync([Hlc(12), Hlc(13)], CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(verdicts[0], Is.EqualTo(CausalDependencyVerdict.Unmet), "the export lacked it");
            Assert.That(verdicts[1], Is.EqualTo(CausalDependencyVerdict.Met));
        });
    }

    [Test]
    public async Task An_activation_that_finds_a_different_registry_lineage_re_stamps()
    {
        var h = new Harness(RegistryLineage);
        var first = h.Activate();
        var before = await first.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        await first.ObserveAsync(OriginA, Shipped(before, 50, 40), CancellationToken.None);
        await first.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);

        // Replaced while no activation was listening.
        h.Lineage.GetLineageAsync(Tree, Arg.Any<CancellationToken>()).Returns(Guid.NewGuid());
        var second = h.Activate();
        var after = await second.ObserveAsync(OriginA, Shipped(before, 60, 60), CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(after, Is.Not.EqualTo(before));
            Assert.That((await second.GetAsync(CancellationToken.None)).LowWatermarks, Is.Empty);
            Assert.That(await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task An_announced_replacement_mints_once_however_the_registry_lineage_settles()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        await grain.OnContentsReplacingAsync(CancellationToken.None);
        var announced = (await grain.GetAsync(CancellationToken.None)).Epoch;

        h.Lineage.GetLineageAsync(Tree, Arg.Any<CancellationToken>()).Returns(Guid.NewGuid());
        var settled = await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);

        Assert.That(settled, Is.EqualTo(announced), "the announcement already forced the gap; settling only records the lineage");
    }

    [Test]
    public async Task A_cap_the_reloaded_state_does_not_know_about_is_lifted_by_the_first_accepted_watermark()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        var epoch = await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        await h.Origin(OriginA).SetTreeCapAsync(Tree, HybridLogicalClock.Zero, CancellationToken.None);

        await grain.ObserveAsync(OriginA, Shipped(epoch, 50, 40, generation: 3), CancellationToken.None);

        Assert.That(await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(Hlc(40)));
    }

    [Test]
    public async Task A_replacement_whose_cap_cannot_land_fails_without_changing_the_epoch()
    {
        var h = new Harness(RegistryLineage);
        var failing = Substitute.For<IReplicationOriginFrontierGrain>();
        failing.SetTreeCapAsync(Tree, Arg.Any<HybridLogicalClock>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("frontier down"));
        h.Factory.GetGrain<IReplicationOriginFrontierGrain>(OriginB, Arg.Any<string?>()).Returns(failing);
        var grain = h.Activate();
        var before = await grain.ObserveAsync(OriginB, shipped: null, CancellationToken.None);

        Assert.That(() => grain.OnContentsReplacingAsync(CancellationToken.None), Throws.InstanceOf<InvalidOperationException>());
        Assert.That(h.State.State.Epoch, Is.EqualTo(before));
    }

    [Test]
    public async Task A_replacement_whose_own_write_fails_deactivates_rather_than_serve_unsaved_state()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        h.State.ThrowOnWrite = new InvalidOperationException("store down");

        Assert.That(() => grain.OnContentsReplacingAsync(CancellationToken.None), Throws.InstanceOf<InvalidOperationException>());
        h.Context.Received().Deactivate(Arg.Any<DeactivationReason>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task The_frontier_origins_gauge_follows_each_pair_through_its_modes_and_is_withdrawn_at_deactivation()
    {
        const string tree = "tf-gauge-tree";
        var h = new Harness(RegistryLineage);
        h.Context.GrainId.Returns(GrainId.Create("replication-tree-frontier", tree));
        h.Lineage.GetLineageAsync(tree, Arg.Any<CancellationToken>()).Returns(RegistryLineage);
        h.Factory.GetGrain<IReplicationHighWaterMarkGrain>(tree, Arg.Any<string?>()).Returns(h.Hwm);
        var net = new Dictionary<string, long>(StringComparer.Ordinal);
        using var listener = MeterListening.StartForInstrument(
            LatticeReplicationMetrics.CausalFrontierOrigins,
            l => l.SetMeasurementEventCallback<long>((_, value, tags, _) =>
            {
                string? mode = null;
                string? taggedTree = null;
                foreach (var tag in tags)
                {
                    if (tag.Key == LatticeReplicationMetrics.TagMode) mode = tag.Value?.ToString();
                    if (tag.Key == LatticeReplicationMetrics.TagTree) taggedTree = tag.Value?.ToString();
                }

                if (taggedTree == tree && mode is not null)
                {
                    lock (net)
                    {
                        net[mode] = net.GetValueOrDefault(mode) + value;
                    }
                }
            }));
        Dictionary<string, long> Snapshot()
        {
            lock (net)
            {
                return net.Where(kv => kv.Value != 0).ToDictionary(kv => kv.Key, kv => kv.Value);
            }
        }

        var grain = h.Activate();
        var epoch = await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        var pending = Snapshot();
        await grain.ObserveAsync(OriginA, Shipped(epoch, 50, 40), CancellationToken.None);
        var exact = Snapshot();
        await grain.OnContentsReplacingAsync(CancellationToken.None);
        var awaiting = Snapshot();
        await grain.OnDeactivateAsync(new DeactivationReason(DeactivationReasonCode.ApplicationRequested, "test"), CancellationToken.None);
        var withdrawn = Snapshot();

        Assert.Multiple(() =>
        {
            Assert.That(pending, Is.EqualTo(new Dictionary<string, long> { ["pending"] = 1 }));
            Assert.That(exact, Is.EqualTo(new Dictionary<string, long> { ["exact"] = 1 }));
            Assert.That(awaiting, Is.EqualTo(new Dictionary<string, long> { ["awaiting_reseed"] = 1 }));
            Assert.That(withdrawn, Is.Empty);
        });
    }

    [Test]
    public void A_tree_with_no_registry_lineage_reports_its_origins_as_degraded()
    {
        Assert.Multiple(() =>
        {
            Assert.That(ReplicationTreeFrontierGrain.ModeOf(Guid.Empty, new ReplicationTreeOriginFrontier { LowWatermark = Hlc(5) }), Is.EqualTo("degraded"));
            Assert.That(ReplicationTreeFrontierGrain.ModeOf(Guid.NewGuid(), new ReplicationTreeOriginFrontier { AwaitingPin = true }), Is.EqualTo("awaiting_reseed"));
            Assert.That(ReplicationTreeFrontierGrain.ModeOf(Guid.NewGuid(), new ReplicationTreeOriginFrontier()), Is.EqualTo("pending"));
            Assert.That(ReplicationTreeFrontierGrain.ModeOf(Guid.NewGuid(), new ReplicationTreeOriginFrontier { LowWatermark = Hlc(5) }), Is.EqualTo("exact"));
        });
    }

    [Test]
    public async Task A_registry_lineage_change_forces_one_gap_and_records_the_lineage_it_produces()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        var before = await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);
        var next = Guid.NewGuid();

        await grain.OnLineageChangingAsync(next, CancellationToken.None);
        var during = (await grain.GetAsync(CancellationToken.None)).Epoch;

        // The registry persisted it; a later activation settles without a second gap.
        h.Lineage.GetLineageAsync(Tree, Arg.Any<CancellationToken>()).Returns(next);
        var reactivated = await h.Activate().ObserveAsync(OriginA, shipped: null, CancellationToken.None);

        await Assert.MultipleAsync(async () =>
        {
            Assert.That(during, Is.Not.EqualTo(before).And.Not.EqualTo(Guid.Empty));
            Assert.That(reactivated, Is.EqualTo(during));
            Assert.That(h.State.State.ObservedRegistryLineage, Is.EqualTo(next));
            Assert.That(await h.Origin(OriginA).GetLowWatermarkAsync(CancellationToken.None), Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task An_unregistration_or_purge_puts_the_frontier_in_degraded_mode()
    {
        var h = new Harness(RegistryLineage);
        var grain = h.Activate();
        await grain.ObserveAsync(OriginA, shipped: null, CancellationToken.None);

        await grain.OnLineageChangingAsync(nextLineage: null, CancellationToken.None);

        Assert.That((await grain.GetAsync(CancellationToken.None)).Epoch, Is.EqualTo(Guid.Empty));
    }

    [Test]
    public void Arguments_are_validated()
    {
        var grain = new Harness(RegistryLineage).Activate();

        Assert.Multiple(() =>
        {
            Assert.That(() => grain.ObserveAsync("", null, CancellationToken.None), Throws.InstanceOf<ArgumentException>());
            Assert.That(() => grain.PinAsync(Guid.NewGuid(), null!, new Dictionary<string, HybridLogicalClock[]>(), CancellationToken.None), Throws.ArgumentNullException);
            Assert.That(() => grain.PinAsync(Guid.NewGuid(), new Dictionary<string, HybridLogicalClock>(), null!, CancellationToken.None), Throws.ArgumentNullException);
        });
    }
}
