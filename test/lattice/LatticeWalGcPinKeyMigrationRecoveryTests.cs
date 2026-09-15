using System.Globalization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests;

/// <summary>
/// Recovery tests for the WAL GC's durable-pin fan-in across the materialiser pin
/// grain key change (issue #1701). The shard suffix moved from <c>#s</c> to the
/// storage-safe <c>~s</c>, which changes the grain identity a pin is persisted
/// under, so every sharded pin written by an earlier build lives at a key the new
/// composer would never produce.
/// </summary>
/// <remarks>
/// <para>
/// This is what makes a plain cutover dangerous rather than merely untidy: a
/// materialiser pin holds the WAL trim floor, so a pin the GC cannot see retains
/// nothing, and the GC would trim past a durable leaf checkpoint that has not yet
/// been re-reported - discarding committed entries the leaf still needs. The
/// consequence is data loss, not an upgrade wart.
/// </para>
/// <para>
/// The fan-in therefore reads every legacy shard key alongside every new one.
/// Per consumer it prefers the pin at the key the CURRENT build would write to -
/// the only key a write can land on - and folds the remaining, stranded pins to
/// the lowest only when that key holds nothing. So a pre-upgrade pin keeps
/// holding the floor until its consumer re-pins under the safe key, and stops
/// holding it once the consumer has. These tests drive that through
/// the real <see cref="LatticeWalGc"/> with a KEY-AWARE grain factory: the sibling
/// <c>LatticeWalGcDurablePinFloorTests</c> returns one substitute for any key and
/// therefore cannot tell the two shapes apart, which is precisely the distinction
/// under test here.
/// </para>
/// <para>
/// The fan-in originally folded every key shape to the lowest pin
/// unconditionally. That was chosen believing it was self-limiting - the
/// originating pull request (#1704) recorded the intent as "a pin written by an
/// earlier build keeps holding the trim floor until its consumer re-pins" - but
/// a re-pin is written through <see cref="WalMaterialiserPinRouting.ShardKey"/>
/// and composes a DIFFERENT grain key, so the stranded row was never superseded
/// and floored the tree's trim indefinitely (issue #2433). Preferring the
/// authoritative key implements the terminating condition that was already
/// specified; it is not a new policy.
/// </para>
/// </remarks>
[TestFixture]
public sealed class LatticeWalGcPinKeyMigrationRecoveryTests
{
    private const string Tree = "tree";
    private const string LeafConsumer = "_lattice_materialiser_tree_leaf-1";

    /// <summary>Sharding only engages above one shard, so the key shapes only differ here.</summary>
    private const int PinShards = 4;

    /// <summary>
    /// The shard separator the PREVIOUS build wrote, pinned to its literal
    /// historical value rather than read from
    /// <see cref="WalMaterialiserPinRouting.LegacyShardSeparator"/>.
    /// </summary>
    /// <remarks>
    /// Deriving it from the constant under test would make these tests
    /// self-referential: changing the constant would move the product's behaviour
    /// and the test's expectation together, and a regression that stopped reading
    /// genuinely-stranded pins would still pass. This value describes state already
    /// persisted on disk by shipped builds, so it is frozen by definition.
    /// </remarks>
    private const string HistoricalLegacySeparator = "#s";

    private static HybridLogicalClock Hlc(long ticks, int counter = 0) =>
        new() { WallClockTicks = ticks, Counter = counter };

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

    private static async Task<InMemoryWalStorageProvider> SeededProviderAsync()
    {
        var provider = new InMemoryWalStorageProvider();
        await provider.AppendBatchAsync(
            Tree,
            0,
            new[] { Entry(0, Hlc(10)), Entry(1, Hlc(20)), Entry(2, Hlc(30)) },
            CancellationToken.None);
        return provider;
    }

    private static IOptionsMonitor<LatticeOptions> Monitor()
    {
        var monitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        var options = new LatticeOptions { WalPartitions = 1, WalMaterialiserPinShards = PinShards };
        monitor.CurrentValue.Returns(options);
        monitor.Get(Arg.Any<string>()).Returns(options);
        return monitor;
    }

    /// <summary>The key the current composer writes this consumer's pin to.</summary>
    private static string CurrentKey() =>
        WalMaterialiserPinRouting.ShardKey(Tree, LeafConsumer, PinShards);

    /// <summary>
    /// The key the previous build would have written the SAME consumer's pin to:
    /// the same shard, under the legacy separator. Derived from the current key so
    /// the two are guaranteed to describe one consumer rather than two unrelated
    /// shards.
    /// </summary>
    private static string LegacyKeyForSameShard()
    {
        var current = CurrentKey();
        var idx = current.LastIndexOf(WalMaterialiserPinRouting.ShardSeparator, StringComparison.Ordinal);
        Assert.That(idx, Is.GreaterThanOrEqualTo(0),
            $"Expected a sharded key at {PinShards} shards but got '{current}'.");
        var shard = current[(idx + WalMaterialiserPinRouting.ShardSeparator.Length)..];
        return Tree + HistoricalLegacySeparator + shard;
    }

    /// <summary>
    /// A current-separator shard key that is NOT this consumer's own. A pin lands
    /// here when the shard count is raised: the modulus moves the consumer to a
    /// different ordinal and its old pin is stranded under a key that is still
    /// current-SHAPED but is no longer the key any write addresses. Derived from
    /// the current key rather than composed from the routing function, on the same
    /// reasoning as <see cref="LegacyKeyForSameShard"/>.
    /// </summary>
    private static string OtherCurrentShardKey()
    {
        var current = CurrentKey();
        var idx = current.LastIndexOf(WalMaterialiserPinRouting.ShardSeparator, StringComparison.Ordinal);
        Assert.That(idx, Is.GreaterThanOrEqualTo(0),
            $"Expected a sharded key at {PinShards} shards but got '{current}'.");
        var shard = int.Parse(
            current[(idx + WalMaterialiserPinRouting.ShardSeparator.Length)..],
            CultureInfo.InvariantCulture);
        return Tree + WalMaterialiserPinRouting.ShardSeparator + ((shard + 1) % PinShards);
    }

    /// <summary>
    /// Builds a grain factory whose pin grains are addressed BY KEY, so a pin can be
    /// planted at one specific key and be invisible at every other - the only way to
    /// prove the legacy key is genuinely read, rather than coincidentally satisfied
    /// by a catch-all substitute.
    /// </summary>
    private static IServiceProvider ServicesWithPinsByKey(
        IReadOnlyDictionary<string, IReadOnlyDictionary<string, HybridLogicalClock>> pinsByKey,
        IWalStorageProvider provider)
    {
        IReadOnlyDictionary<string, HybridLogicalClock> empty =
            new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal);

        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<IWalMaterialiserPinGrain>(Arg.Any<string>()).Returns(callInfo =>
        {
            // Positional: GetGrain<T>(string primaryKey, string? grainClassNamePrefix = null)
            // takes two string parameters, so a by-type lookup is ambiguous and throws.
            var key = callInfo.ArgAt<string>(0);
            var grain = Substitute.For<IWalMaterialiserPinGrain>();
            var pins = pinsByKey.TryGetValue(key, out var found) ? found : empty;
            grain.GetPinsAsync().Returns(Task.FromResult(pins));
            return grain;
        });

        var sc = new ServiceCollection();
        sc.AddSingleton(provider);
        sc.AddSingleton(factory);
        return sc.BuildServiceProvider();
    }

    private static Dictionary<string, IReadOnlyDictionary<string, HybridLogicalClock>> PinsAt(
        params (string Key, HybridLogicalClock Pin)[] entries)
    {
        var map = new Dictionary<string, IReadOnlyDictionary<string, HybridLogicalClock>>(StringComparer.Ordinal);
        foreach (var (key, pin) in entries)
        {
            map[key] = new Dictionary<string, HybridLogicalClock>(StringComparer.Ordinal)
            {
                [LeafConsumer] = pin,
            };
        }
        return map;
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

    [Test]
    public void The_two_key_shapes_actually_differ()
    {
        // Guards the fixture itself: if the composer ever stopped sharding, every
        // test below would pass vacuously against a single shared key.
        Assert.Multiple(() =>
        {
            Assert.That(CurrentKey(), Does.Contain(WalMaterialiserPinRouting.ShardSeparator));
            Assert.That(LegacyKeyForSameShard(), Does.Contain(HistoricalLegacySeparator));
            Assert.That(CurrentKey(), Is.Not.EqualTo(LegacyKeyForSameShard()));
        });
    }

    [Test]
    public void The_legacy_separator_constant_still_names_the_historical_shape()
    {
        // The legacy separator describes grain keys already persisted by shipped
        // builds, so it is frozen. Changing it would strand exactly the pins the
        // dual read exists to rescue - silently, because nothing else observes it.
        Assert.That(
            WalMaterialiserPinRouting.LegacyShardSeparator,
            Is.EqualTo(HistoricalLegacySeparator),
            "The legacy shard separator is a persisted storage format and must not change.");
    }

    [Test]
    public async Task A_pin_stranded_at_the_legacy_key_still_holds_the_trim_floor()
    {
        // Exactly the upgrade state: the pin was persisted by the previous build
        // under "tree#s{shard}" and nothing has re-pinned under the safe key yet.
        // Its consumer is absent from the in-memory registry (a dormant leaf after
        // a restart), so only the durable pin can hold the floor.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var sut = new LatticeWalGc(
            ServicesWithPinsByKey(PinsAt((LegacyKeyForSameShard(), Hlc(10))), provider),
            registry,
            Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)),
                "A pin stranded at the legacy key must still floor the trim, or the upgrade silently "
                + "discards WAL that a durable leaf checkpoint still depends on.");
            Assert.That(report.EntriesTrimmed, Is.EqualTo(1));
        });

        var survivors = await SurvivingOffsetsAsync(provider);
        Assert.That(survivors, Is.EqualTo(new[] { 1L, 2L }),
            "The committed tail above the stranded pin must survive.");
    }

    [Test]
    public async Task A_re_pin_at_the_authoritative_key_supersedes_the_stranded_legacy_pin()
    {
        // DELIBERATE INVERSION of the behaviour this fixture originally asserted
        // (issue #2433). It previously required Hlc(10) here - the lowest pin
        // across both shapes - under the heading "most conservative floor".
        //
        // Why that expectation was wrong rather than merely conservative: the
        // originating pull request (#1704) recorded the rule as "a pin written by
        // an earlier build keeps holding the trim floor UNTIL ITS CONSUMER
        // RE-PINS". An unconditional min has no such terminating condition. A
        // re-pin is written through ShardKey, which composes a different grain
        // key, so the legacy row is never overwritten, never removed, and holds
        // the floor forever - the WAL cannot trim past it for the life of the
        // tree. The old expectation encoded that permanence as if it were the
        // intent.
        //
        // Preferring the authoritative key is safe on exactly the assumption the
        // steady state already rests on: that a pin at the key the current build
        // writes to is a truthful statement of that consumer's durable
        // checkpoint. It introduces no assumption the no-legacy-state case below
        // does not already make, and in particular assumes nothing about the
        // frontier being monotone in real time - a rolled-back consumer re-pins
        // LOWER, and prefer-authoritative keeps that lower value.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var sut = new LatticeWalGc(
            ServicesWithPinsByKey(
                PinsAt((LegacyKeyForSameShard(), Hlc(10)), (CurrentKey(), Hlc(30))),
                provider),
            registry,
            Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.MinCursor, Is.EqualTo(Hlc(30)),
            "Once the consumer has re-pinned under the authoritative key, the stranded legacy "
            + "pin must stop holding the floor - otherwise it holds it forever, because no write "
            + "or removal path addresses the legacy key.");
    }

    [Test]
    public async Task A_pin_stranded_at_another_current_shaped_shard_key_stops_holding_the_floor()
    {
        // The case a separator-only rule would miss, and the reason the fix keys
        // off the routing function rather than off the separator: raising the
        // shard count moves a consumer to a different ordinal, stranding its old
        // pin at a key that is still CURRENT-shaped. Nothing about its spelling
        // marks it as stale; only the routing function knows it is no longer the
        // key a write would address.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var sut = new LatticeWalGc(
            ServicesWithPinsByKey(
                PinsAt((OtherCurrentShardKey(), Hlc(10)), (CurrentKey(), Hlc(30))),
                provider),
            registry,
            Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.MinCursor, Is.EqualTo(Hlc(30)),
            "A pin stranded by a shard-count change is as stale as one stranded by the "
            + "separator change, and must be superseded by the authoritative pin the same way.");
    }

    [Test]
    public async Task A_pin_stranded_at_another_current_shaped_shard_key_still_holds_the_floor_alone()
    {
        // The absent case for the test above, and the property that makes the
        // rule safe: with NO pin at the authoritative key nothing is discarded.
        // Every pin is stranded, the fold is the same lowest-wins it always was,
        // and a consumer with no current-key evidence keeps flooring the WAL.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var sut = new LatticeWalGc(
            ServicesWithPinsByKey(PinsAt((OtherCurrentShardKey(), Hlc(10))), provider),
            registry,
            Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)),
            "With no authoritative pin every pin is stranded and the lowest must still hold "
            + "the floor, or a shard-count change would discard a live consumer's only pin.");
    }

    [Test]
    public async Task Two_stranded_pins_and_no_authoritative_pin_still_fold_to_the_lowest()
    {
        // The pre-fix fold, preserved verbatim for the case it was right about.
        // Both shapes present, neither authoritative: the rule must fall through
        // to lowest-wins rather than picking arbitrarily by enumeration order.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var sut = new LatticeWalGc(
            ServicesWithPinsByKey(
                PinsAt((OtherCurrentShardKey(), Hlc(20)), (LegacyKeyForSameShard(), Hlc(10))),
                provider),
            registry,
            Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)),
            "With nothing at the authoritative key the fan-in must still take the lowest "
            + "stranded pin, independently of the order the keys are enumerated in.");
    }

    [Test]
    public async Task A_pin_at_the_safe_key_is_read_with_no_legacy_state_present()
    {
        // The steady state after migration: nothing at any legacy key. The dual read
        // must not depend on legacy state existing.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var sut = new LatticeWalGc(
            ServicesWithPinsByKey(PinsAt((CurrentKey(), Hlc(10))), provider),
            registry,
            Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)));
    }

    [Test]
    public async Task A_pre_sharding_pin_at_the_bare_tree_key_still_holds_the_floor()
    {
        // The older migration this one is layered on: a pin written before sharding
        // existed lives at the bare tree name. Widening the separator must not have
        // dropped it.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var sut = new LatticeWalGc(
            ServicesWithPinsByKey(PinsAt((Tree, Hlc(10))), provider),
            registry,
            Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)),
            "The pre-sharding bare-tree key must remain part of the fan-in.");
    }

    [Test]
    public async Task No_durable_pin_anywhere_leaves_steady_state_trimming_unchanged()
    {
        // The negative control: with no pin at any key shape the GC must trim to the
        // registry cursor exactly as it always did, so the dual read cannot be
        // mistaken for an unconditional floor.
        var provider = await SeededProviderAsync();
        var registry = new InMemoryWalCursorRegistry();
        await registry.ReportCursorAsync(Tree, "shipper", Hlc(30));

        var sut = new LatticeWalGc(
            ServicesWithPinsByKey(PinsAt(), provider),
            registry,
            Monitor());

        var report = await sut.RunOnceAsync(Tree);

        Assert.Multiple(() =>
        {
            Assert.That(report.MinCursor, Is.EqualTo(Hlc(30)));
            Assert.That(report.EntriesTrimmed, Is.EqualTo(3));
        });
    }
}
