using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Deterministic reproduction of the trim mechanism behind issue #1496
/// ("cross-cluster fall-off is undetected when WalRetention is enabled").
/// <para>
/// The WAL GC predicate unions the consumer-cursor floor with the
/// <see cref="LatticeOptions.WalRetention"/> TTL ceiling
/// (<c>LatticeWalGc.IsEligible</c>: an entry is trim-eligible when it is at or
/// below the min cursor <b>or</b> at or below the TTL ceiling). That union is
/// intentional for bounded disk: a lagging consumer that pins the log past the
/// ceiling is deliberately allowed to "fall off the log". The safety net is
/// that the consumer detects the gap on its next read and re-bootstraps.
/// </para>
/// <para>
/// That safety net fires for a <b>local</b> materialiser - its next read
/// surfaces the trimmed prefix to the fall-off-log detector. It does <b>not</b>
/// fire for the <b>cross-cluster shipper</b>: the shipper advances past the
/// trimmed prefix silently and the receiver-side detector only compares against
/// the receiver's own local WAL, so the receiver never learns of the entries it
/// never received. The result is silent, permanent cross-cluster divergence.
/// </para>
/// <para>
/// These tests pin the root-cause trim behaviour deterministically at the GC
/// seam (an injected clock, no cluster timing): with <c>WalRetention</c> set,
/// the GC trims entries that a lagging shipper's cursor still pins - the exact
/// condition that makes the shipper fall off the log. The startup validator
/// added in PR #1499 refuses this configuration on a replicated tree unless the
/// anti-entropy digest probe (the out-of-band detector that catches what the
/// fall-off detector misses) is enabled, or the operator explicitly opts out.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeWalGcRetentionFallOffTests
{
    private const string Tree = "tree";
    private const string ShipperConsumer = "cross-cluster-shipper";

    // A fixed wall-clock anchor. Entries are stamped relative to this and the
    // GC's TTL ceiling is evaluated against the injected clock below, so the
    // whole scenario is deterministic and independent of real time.
    private static readonly DateTimeOffset Anchor =
        new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static HybridLogicalClock Hlc(DateTimeOffset at, int counter = 0) =>
        new() { WallClockTicks = at.UtcTicks, Counter = counter };

    private static WalEntry Entry(long offset, DateTimeOffset at) => new()
    {
        Offset = offset,
        Mutation = new LatticeMutation
        {
            TreeId = Tree,
            Kind = MutationKind.Set,
            Key = $"k{offset}",
            Value = new byte[] { 1 },
            Timestamp = Hlc(at),
            OriginClusterId = "site-a",
        },
    };

    /// <summary>
    /// Seeds three entries: offset 0 acked by the lagging shipper, offsets 1
    /// and 2 authored while the shipper was partitioned (un-acked). All three
    /// are older than the retention window relative to <see cref="_now"/>.
    /// </summary>
    private static async Task<InMemoryWalStorageProvider> SeededProviderAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[]
            {
                Entry(0, Anchor),
                Entry(1, Anchor.AddSeconds(1)),
                Entry(2, Anchor.AddSeconds(2)),
            },
            CancellationToken.None);
        return provider;
    }

    private static IOptionsMonitor<LatticeOptions> Monitor(TimeSpan? walRetention)
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1, WalRetention = walRetention };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    private static IServiceProvider Services(IWalStorageProvider provider)
    {
        var sc = new ServiceCollection();
        sc.AddSingleton(provider);
        return sc.BuildServiceProvider();
    }

    private static async Task<List<long>> SurvivingOffsetsAsync(IWalStorageProvider provider)
    {
        var survivors = new List<long>();
        await foreach (var entry in provider.ReadAsync(Tree, 0, fromOffsetExclusive: -1, maxEntries: 100, CancellationToken.None))
        {
            survivors.Add(entry.Offset);
        }
        return survivors;
    }

    private sealed class FixedTimeProvider(DateTimeOffset now) : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => now;
    }

    [Test]
    public async Task WalRetention_trims_entries_a_lagging_shipper_cursor_still_pins()
    {
        // The lagging cross-cluster shipper has acked only offset 0; offsets 1
        // and 2 remain un-acked (authored while its outbound edge was
        // partitioned). Its cursor pins the log at offset 0's HLC.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, ShipperConsumer, Hlc(Anchor));

        // Retention window of 30s, evaluated 100s after the anchor: the TTL
        // ceiling (now - 30s = anchor + 70s) sits above every entry's
        // wall-clock stamp, so all three are TTL-eligible.
        var now = Anchor.AddSeconds(100);
        var sut = new LatticeWalGc(
            Services(provider),
            registry,
            Monitor(walRetention: TimeSpan.FromSeconds(30)),
            timeProvider: new FixedTimeProvider(now));

        var report = await sut.RunOnceAsync(Tree);

        // The bug: the TTL ceiling trims offsets 1 and 2 even though the
        // shipper's cursor at offset 0 still pins them. Those un-acked entries
        // are gone from the sender's WAL, so on partition heal the shipper
        // resumes from a cursor that now points below the oldest surviving
        // entry - it falls off the log.
        Assert.That(report.EntriesTrimmed, Is.EqualTo(3),
            "WalRetention must trim past the lagging shipper cursor (the fall-off trigger of #1496).");

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.Empty,
            "The shipper's un-acked entries are trimmed by the TTL ceiling despite its cursor pin.");
    }

    [Test]
    public async Task Without_WalRetention_the_shipper_cursor_protects_unacked_entries()
    {
        // The contrast: with no retention window, the only trim floor is the
        // consumer-cursor minimum. The lagging shipper's cursor at offset 0
        // protects the un-acked offsets 1 and 2, so they survive and can be
        // re-shipped once the partition heals. No fall-off occurs.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, ShipperConsumer, Hlc(Anchor));

        var now = Anchor.AddSeconds(100);
        var sut = new LatticeWalGc(
            Services(provider),
            registry,
            Monitor(walRetention: null),
            timeProvider: new FixedTimeProvider(now));

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.EntriesTrimmed, Is.EqualTo(1),
            "Only the acked prefix at or below the shipper cursor is trim-eligible without a TTL ceiling.");

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 1L, 2L }),
            "The lagging shipper's un-acked entries survive so they can be re-shipped on heal.");
    }
}
