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
/// The fan-in therefore reads every legacy shard key alongside every new one and
/// takes the lowest pin per consumer, so a pre-upgrade pin keeps holding the floor
/// until its consumer re-pins under the safe key. These tests drive that through
/// the real <see cref="LatticeWalGc"/> with a KEY-AWARE grain factory: the sibling
/// <c>LatticeWalGcDurablePinFloorTests</c> returns one substitute for any key and
/// therefore cannot tell the two shapes apart, which is precisely the distinction
/// under test here.
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
    public async Task A_legacy_and_a_current_pin_resolve_to_the_most_conservative_floor()
    {
        // Mid-migration: the consumer has re-pinned under the safe key at a higher
        // frontier while its legacy pin is still present. The lower (older) pin must
        // win - retaining more WAL is always safe, trimming past a live checkpoint
        // never is.
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

        Assert.That(report.MinCursor, Is.EqualTo(Hlc(10)),
            "Per consumer the lowest pin across both key shapes must win.");
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
