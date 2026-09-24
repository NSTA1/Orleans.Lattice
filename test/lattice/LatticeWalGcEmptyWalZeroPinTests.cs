using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Unit tests for the empty-WAL rule of issue #3453 in
/// <see cref="LatticeWalGc"/>: a <c>(Zero, -1)</c> durable materialiser pin on a
/// WAL partition that is proven empty (no entry was ever durably appended) is
/// not reported as blocking and so is never driven by the scheduler - but it
/// still holds that partition's trim, so the first entry appended afterwards is
/// retained. The rule governs reporting and scheduling only, never trim, and it
/// fails closed: a partition whose head cannot be read keeps the pin blocking.
/// <para>
/// Every exemption assertion is paired with a control that differs in exactly
/// one input (a non-empty WAL, an unreadable head, a pin that did not abstain),
/// so a test cannot pass by the rule being applied unconditionally or not at
/// all.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcEmptyWalZeroPinTests
{
    private const string Tree = "tree";
    private const string NeverWrittenLeafConsumer = "_lattice_materialiser_tree_leaf-never-written";

    private static HybridLogicalClock Hlc(long ticks) => new() { WallClockTicks = ticks };

    private static WalEntry Entry(long offset, HybridLogicalClock ts) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = ts,
            OriginClusterId = "site-a",
        },
    };

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        // Opt out of the issue #3300 durability hold, which engages for any tree
        // that never published a durable offset floor. This fixture asserts the
        // blocked verdict, a different axis.
        var options = new LatticeOptions { WalPartitions = 1, WalDurabilityHoldCeilingBytes = 0 };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>
    /// Builds a GC whose pin store reports <paramref name="consumer"/> holding a
    /// Zero HLC pin with offset <paramref name="offset"/> (<c>-1</c> is the
    /// abstention sentinel a never-written leaf publishes). When
    /// <paramref name="offset"/> is <see langword="null"/> the offset plane is
    /// unreachable, so no consumer is proven abstained.
    /// </summary>
    private static LatticeWalGc Gc(IWalStorageProvider provider, IWalCursorRegistry registry, string consumer, long? offset)
        => Gc(
            provider,
            registry,
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal) { [consumer] = HybridLogicalClock.Zero },
            offset is { } reported
                ? new Dictionary<string, long>(StringComparer.Ordinal) { [consumer] = reported }
                : null);

    /// <summary>
    /// Builds a GC over an explicit pin population. A <see langword="null"/>
    /// <paramref name="offsets"/> makes the offset plane unreachable.
    /// </summary>
    private static LatticeWalGc Gc(
        IWalStorageProvider provider,
        IWalCursorRegistry registry,
        Dictionary<string, HybridLogicalClock> pins,
        Dictionary<string, long>? offsets)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);

        var pinGrain = Substitute.For<IWalMaterialiserPinGrain>();
        pinGrain.GetPinsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, HybridLogicalClock>>(pins));
        if (offsets is not null)
        {
            pinGrain.GetPinOffsetsAsync().Returns(Task.FromResult<IReadOnlyDictionary<string, long>>(offsets));
        }
        else
        {
            pinGrain.GetPinOffsetsAsync().ThrowsAsync(new InvalidOperationException("offset plane unavailable"));
        }

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(pinGrain);
        sc.AddSingleton(factory);

        return new LatticeWalGc(sc.BuildServiceProvider(), registry, Monitor());
    }

    private static async Task<IWalCursorRegistry> RegistryWithShipperAsync()
    {
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(1_000));
        return registry;
    }

    [Test]
    public async Task RunOnceAsync_abstained_zero_pin_on_an_empty_wal_is_not_reported_and_a_later_entry_is_retained()
    {
        // Acceptance 4. The partition was never written, so the (Zero, -1) pin
        // protects nothing today and there is nothing a drive could advance
        // over: reporting it would only feed the scheduler a NoAdvance loop.
        var provider = new InMemoryWalStorageProvider();
        Assert.That(await provider.GetHighestOffsetAsync(Tree, 0, CancellationToken.None), Is.LessThan(0),
            "Precondition: the partition must be provably empty, or this is not the exempt case.");

        var registry = await RegistryWithShipperAsync();
        var gc = Gc(provider, registry, NeverWrittenLeafConsumer, offset: -1);

        var empty = await gc.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(empty.CursorFloorState, Is.Not.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "A Zero pin on a provably empty partition must not be reported as blocking the tree.");
            Assert.That(empty.BlockingConsumerId, Is.Null,
                "The scheduler drives the leaves the report names; naming this one would drive it for ever.");
            Assert.That(empty.BlockingConsumerIds, Is.Null);
        });

        // The rule never feeds trim: the first entry appended to the partition
        // afterwards must survive the next pass even though the shipper's
        // cursor is far past it, because the never-written leaf's pin still
        // blocks this partition's cursor trim.
        await provider.AppendBatchAsync(Tree, 0, new[] { Entry(0, Hlc(10)) }, CancellationToken.None);

        var afterWrite = await gc.RunOnceAsync(Tree);

        var lowestAfterWrite = await provider.GetLowestOffsetAsync(Tree, 0, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(afterWrite.EntriesTrimmed, Is.EqualTo(0),
                "The exemption is reporting-only; the pin must still hold the partition's trim.");
            Assert.That(lowestAfterWrite, Is.EqualTo(0),
                "The first entry appended after the exempt pass must still be readable.");
            Assert.That(afterWrite.BlockingConsumerId, Is.EqualTo(NeverWrittenLeafConsumer),
                "Once the partition holds an entry the pin protects something again, so it is reported.");
        });
    }

    [Test]
    public async Task RunOnceAsync_abstained_zero_pin_on_a_non_empty_wal_is_still_reported()
    {
        // The control for the test above, differing only in the WAL holding an
        // entry. Without it, an implementation that exempted every abstained
        // Zero pin would satisfy acceptance 4.
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(Tree, 0, new[] { Entry(0, Hlc(10)) }, CancellationToken.None);

        var report = await Gc(provider, await RegistryWithShipperAsync(), NeverWrittenLeafConsumer, offset: -1)
            .RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
            Assert.That(report.BlockingConsumerId, Is.EqualTo(NeverWrittenLeafConsumer));
            Assert.That(report.EntriesTrimmed, Is.EqualTo(0));
        });
    }

    [Test]
    public async Task RunOnceAsync_abstained_zero_pin_stays_blocking_when_the_wal_head_cannot_be_read()
    {
        // Acceptance 5: fail closed. A probe that throws proves nothing, so the
        // pin must be reported exactly as it was before the rule existed.
        var provider = Substitute.For<IWalStorageProvider>();
        provider.GetHighestOffsetAsync(Tree, Arg.Any<int>(), Arg.Any<CancellationToken>())
            .ThrowsAsync(new InvalidOperationException("storage unavailable"));

        var report = await Gc(provider, await RegistryWithShipperAsync(), NeverWrittenLeafConsumer, offset: -1)
            .RunOnceAsync(Tree);

        await provider.Received().GetHighestOffsetAsync(Tree, 0, Arg.Any<CancellationToken>());

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "An unreadable head must never be read as an empty partition.");
            Assert.That(report.BlockingConsumerId, Is.EqualTo(NeverWrittenLeafConsumer));
        });
    }

    [Test]
    public async Task RunOnceAsync_zero_pin_not_proven_abstained_is_reported_even_on_an_empty_wal()
    {
        // The rule is scoped to consumers the offset plane PROVED abstained
        // ("-1"). With the offset plane unreachable nothing is proven, so the
        // Zero pin must block exactly as before the rule, even on an empty
        // partition. Without this control an implementation that exempted any
        // Zero pin on an empty WAL would satisfy acceptance 4.
        var provider = new InMemoryWalStorageProvider();

        var report = await Gc(provider, await RegistryWithShipperAsync(), NeverWrittenLeafConsumer, offset: null)
            .RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin));
            Assert.That(report.BlockingConsumerId, Is.EqualTo(NeverWrittenLeafConsumer));
        });
    }

    [Test]
    public async Task RunOnceAsync_only_the_abstained_zero_pin_is_exempt_when_a_sibling_zero_pin_did_not_report()
    {
        // With the offset plane LIVE, one consumer publishing "-1" must not
        // exempt a sibling Zero pin that reported no offset at all on the same
        // empty partition. The silent sibling is caught by the population-gap
        // early return ahead of the exemption site, which is why the site's own
        // "proven abstained" clause is defence in depth rather than the only
        // guard; this test pins the composed behaviour either way.
        const string silentConsumer = "_lattice_materialiser_tree_leaf-silent";
        var provider = new InMemoryWalStorageProvider();

        var report = await Gc(
                provider,
                await RegistryWithShipperAsync(),
                new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
                {
                    [NeverWrittenLeafConsumer] = HybridLogicalClock.Zero,
                    [silentConsumer] = HybridLogicalClock.Zero,
                },
                new Dictionary<string, long>(StringComparer.Ordinal) { [NeverWrittenLeafConsumer] = -1 })
            .RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.CursorFloorState, Is.EqualTo(WalGcCursorFloorState.BlockedByUnusablePin),
                "The silent sibling proves nothing, so its Zero pin still blocks.");
            Assert.That(report.BlockingConsumerId, Is.EqualTo(silentConsumer));
            Assert.That(report.BlockingConsumerIds, Is.EqualTo(new[] { silentConsumer }),
                "Only the proven-abstained pin on the empty partition is left unreported.");
        });
    }
}
