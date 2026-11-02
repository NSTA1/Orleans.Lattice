using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Testing;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Unit tests for <see cref="WalCopyLogHoldGrain"/>, the bounded hold a copy
/// builder keeps on its physical copy's WAL (issue #5471). Without it, the log of
/// a remediation or resize copy was trimmed while it was built, the replication
/// shipper that rebound to the copy found a gap on its first read, and every peer
/// had to be re-seeded. The tests pin the bounds: an abandoned or crashed build's
/// hold lapses with its lease, and a published copy's hold ends once the source's
/// consumers registered on the copy, or after its grace at the latest.
/// </summary>
[TestFixture]
public sealed class WalCopyLogHoldGrainTests
{
    private const string Source = "orders";
    private const string Copy = "orders/remediated/abc";

    private static readonly TimeSpan Lease = TimeSpan.FromMinutes(30);
    private static readonly TimeSpan Grace = TimeSpan.FromMinutes(10);
    private static readonly GrainId HoldId = GrainId.Create("walcopyloghold", Copy);
    private static readonly GrainId Shipper = GrainId.Create("replicationshipper", "orders/east");
    private static readonly GrainId RetiredShipper = GrainId.Create("replicationshipper", "orders/west");

    [Test]
    public async Task A_copy_whose_source_has_a_live_offset_consumer_is_held_from_its_start()
    {
        var rig = Rig.Create();

        Assert.That(await rig.Grain.ArmAsync(Source, Lease), Is.True);

        Assert.Multiple(async () =>
        {
            Assert.That(rig.CopyRegistry.Consumers, Does.Contain(HoldId), "the hold registers on the copy's consumers");
            Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Not.Null.And.Empty,
                "an empty answer is position 0 in every partition: the GC keeps the whole log");
            Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Source), Is.Null, "the hold answers only for its copy");
        });
    }

    [Test]
    public async Task A_copy_whose_source_no_consumer_reads_is_not_held()
    {
        var rig = Rig.Create(shipperReadsSource: false);

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Grain.ArmAsync(Source, Lease), Is.False);
            Assert.That(rig.CopyRegistry.Consumers, Is.Empty, "nothing registers");
            Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Null);
        });
    }

    [Test]
    public async Task A_source_consumer_that_cannot_be_asked_is_waited_for()
    {
        var rig = Rig.Create();
        rig.ShipperGrain.GetDurableReadPositionsAsync(Source)
            .Returns<Task<long[]?>>(_ => throw new InvalidOperationException("unreachable"));

        Assert.That(await rig.Grain.ArmAsync(Source, Lease), Is.True, "unknown is not 'not reading'");
    }

    [Test]
    public async Task Another_copy_s_hold_on_the_source_is_not_waited_for()
    {
        var rig = Rig.Create();
        rig.SourceRegistry.Consumers.Clear();
        rig.SourceRegistry.Consumers.Add(GrainId.Create("walcopyloghold", Source));

        Assert.That(await rig.Grain.ArmAsync(Source, Lease), Is.False, "a hold never registers on another copy");
    }

    [Test]
    public async Task An_abandoned_build_s_hold_lapses_with_its_lease()
    {
        var rig = Rig.Create();
        await rig.Grain.ArmAsync(Source, Lease);

        rig.Clock.Advance(Lease + TimeSpan.FromSeconds(1));

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Null, "a lapsed hold keeps nothing");
            Assert.That(await rig.Grain.WithdrawIfEndedAsync(), Is.True);
            Assert.That(rig.CopyRegistry.Consumers, Does.Not.Contain(HoldId), "a lapsed hold withdraws itself");
        });
    }

    [Test]
    public async Task A_build_that_keeps_renewing_keeps_its_hold_and_writes_only_once_half_the_lease_is_spent()
    {
        var rig = Rig.Create();
        await rig.Grain.ArmAsync(Source, Lease);
        var writes = rig.State.WriteCount;

        rig.Clock.Advance(TimeSpan.FromMinutes(10));
        await rig.Grain.ArmAsync(Source, Lease);
        Assert.That(rig.State.WriteCount, Is.EqualTo(writes), "a renewal with most of the lease left writes nothing");

        rig.Clock.Advance(TimeSpan.FromMinutes(10));
        await rig.Grain.ArmAsync(Source, Lease);
        Assert.That(rig.State.WriteCount, Is.EqualTo(writes + 1), "past half the lease the renewal is persisted");

        rig.Clock.Advance(TimeSpan.FromMinutes(25));
        Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Not.Null, "the renewed hold is still in force");
    }

    [Test]
    public async Task A_withdrawal_queued_behind_a_renewal_does_not_undo_it()
    {
        var rig = Rig.Create();
        await rig.Grain.ArmAsync(Source, Lease);
        rig.Clock.Advance(Lease + TimeSpan.FromSeconds(1));
        Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Null, "precondition: the hold lapsed");

        await rig.Grain.ArmAsync(Source, Lease);

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Grain.WithdrawIfEndedAsync(), Is.False);
            Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Not.Null);
        });
    }

    [Test]
    public async Task A_published_copy_s_hold_ends_once_every_source_consumer_registered_on_the_copy()
    {
        var rig = Rig.Create();
        await rig.Grain.ArmAsync(Source, Lease);
        await rig.Grain.PublishAsync(Grace);
        Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Not.Null,
            "precondition: the shipper has not rebound yet, so the copy is still held");

        rig.CopyRegistry.Consumers.Add(Shipper);

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Null,
                "the rebound shipper holds the log itself from here");
            Assert.That(await rig.Grain.WithdrawIfEndedAsync(), Is.True);
            Assert.That(rig.CopyRegistry.Consumers, Is.EqualTo(new[] { Shipper }));
        });
    }

    [Test]
    public async Task A_published_copy_s_hold_lapses_after_its_grace_when_a_consumer_never_rebinds()
    {
        var rig = Rig.Create();
        await rig.Grain.ArmAsync(Source, Lease);
        await rig.Grain.PublishAsync(Grace);

        rig.Clock.Advance(Grace - TimeSpan.FromSeconds(1));
        Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Not.Null, "within the grace the copy is held");

        rig.Clock.Advance(TimeSpan.FromSeconds(2));
        Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Null, "the grace bounds the hold");
    }

    [Test]
    public async Task A_publish_shortens_a_long_build_lease_to_the_grace()
    {
        var rig = Rig.Create();
        await rig.Grain.ArmAsync(Source, Lease);
        await rig.Grain.PublishAsync(Grace);

        await rig.Grain.ArmAsync(Source, Lease);
        rig.Clock.Advance(Grace + TimeSpan.FromSeconds(1));

        Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Null, "an arm after the publish renews nothing");
    }

    [Test]
    public async Task A_discarded_copy_s_hold_is_released_at_once()
    {
        var rig = Rig.Create();
        await rig.Grain.ArmAsync(Source, Lease);

        await rig.Grain.ReleaseAsync();
        await rig.Grain.ReleaseAsync();

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Grain.GetDurableReadPositionsAsync(Copy), Is.Null);
            Assert.That(rig.CopyRegistry.Consumers, Is.Empty);
            Assert.That(rig.State.State.Holding, Is.False);
        });
    }

    [Test]
    public async Task A_publish_or_withdrawal_of_a_hold_never_armed_does_nothing()
    {
        var rig = Rig.Create();

        await rig.Grain.PublishAsync(Grace);

        Assert.Multiple(async () =>
        {
            Assert.That(await rig.Grain.WithdrawIfEndedAsync(), Is.False);
            Assert.That(rig.State.WriteCount, Is.EqualTo(0));
        });
    }

    [Test]
    public void Arguments_are_validated()
    {
        var rig = Rig.Create();

        Assert.Multiple(() =>
        {
            Assert.That(() => rig.Grain.ArmAsync("", Lease), Throws.ArgumentException);
            Assert.That(() => rig.Grain.ArmAsync(Source, TimeSpan.Zero), Throws.InstanceOf<ArgumentOutOfRangeException>());
            Assert.That(() => rig.Grain.PublishAsync(TimeSpan.Zero), Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    private sealed record Rig(
        WalCopyLogHoldGrain Grain,
        FakePersistentState<WalCopyLogHoldState> State,
        ManualTimeProvider Clock,
        InMemoryConsumerRegistry SourceRegistry,
        InMemoryConsumerRegistry CopyRegistry,
        IWalOffsetConsumer ShipperGrain)
    {
        public static Rig Create(bool shipperReadsSource = true)
        {
            var sourceRegistry = new InMemoryConsumerRegistry();
            sourceRegistry.Consumers.Add(Shipper);
            sourceRegistry.Consumers.Add(RetiredShipper);
            var copyRegistry = new InMemoryConsumerRegistry();

            var shipper = Substitute.For<IWalOffsetConsumer>();
            shipper.GetDurableReadPositionsAsync(Source)
                .Returns(Task.FromResult<long[]?>(shipperReadsSource ? [3, 4] : null));
            var retired = Substitute.For<IWalOffsetConsumer>();
            retired.GetDurableReadPositionsAsync(Arg.Any<string>()).Returns(Task.FromResult<long[]?>(null));

            var factory = Substitute.For<IGrainFactory>();
            factory.GetGrain<IWalOffsetConsumerRegistryGrain>(Source, Arg.Any<string?>()).Returns(sourceRegistry);
            factory.GetGrain<IWalOffsetConsumerRegistryGrain>(Copy, Arg.Any<string?>()).Returns(copyRegistry);
            factory.GetGrain<IWalOffsetConsumer>(Shipper).Returns(shipper);
            factory.GetGrain<IWalOffsetConsumer>(RetiredShipper).Returns(retired);

            var context = Substitute.For<IGrainContext>();
            context.GrainId.Returns(HoldId);
            var state = new FakePersistentState<WalCopyLogHoldState>();
            var clock = new ManualTimeProvider();
            var grain = new WalCopyLogHoldGrain(context, factory, state) { Clock = clock };
            return new Rig(grain, state, clock, sourceRegistry, copyRegistry, shipper);
        }
    }

    /// <summary>An in-memory offset-consumer set.</summary>
    private sealed class InMemoryConsumerRegistry : IWalOffsetConsumerRegistryGrain
    {
        public List<GrainId> Consumers { get; } = [];

        public Task RegisterAsync(GrainId consumer)
        {
            if (!Consumers.Contains(consumer))
            {
                Consumers.Add(consumer);
            }

            return Task.CompletedTask;
        }

        public Task UnregisterAsync(GrainId consumer)
        {
            Consumers.Remove(consumer);
            return Task.CompletedTask;
        }

        public Task<IReadOnlyList<GrainId>> GetConsumersAsync() =>
            Task.FromResult<IReadOnlyList<GrainId>>(Consumers.ToArray());
    }
}
