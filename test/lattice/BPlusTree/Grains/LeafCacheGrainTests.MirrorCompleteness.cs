using System.Text;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Tests.Fakes;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Issue #2412. <see cref="LeafCacheGrain.GetManyAsync"/> omits a key that
/// misses the cache's mirror without asking the primary leaf, so it is only
/// correct while the mirror is a complete copy of the leaf's keyset after every
/// refresh. These tests pin the path that broke that: a refresh that faulted
/// after it had started mutating the mirror.
/// <para>
/// The budget read in <c>RefreshAsync</c> is a registry round trip, and it used
/// to be awaited after the resync <c>Clear()</c> and after the same-silo
/// revision cookie had been stamped. A transient registry fault there left an
/// empty (or half-advanced) mirror that the cookie described as provably fresh,
/// so every later same-silo read skipped the refresh and silently dropped keys
/// the leaf holds, until the leaf happened to write again. Nothing threw on the
/// later reads, nothing was logged, and the omission looked exactly like a key
/// that does not exist.
/// </para>
/// </summary>
public partial class LeafCacheGrainTests
{
    private const long MirrorEpochA = 7_001;
    private const long MirrorEpochB = 7_002;

    /// <summary>
    /// Builds a same-silo cache (a real leaf publishes the revision cookie)
    /// whose registry budget read throws while <see cref="RegistryFault.Armed"/>
    /// is set. Every cross-grain call from the cache lands on the returned
    /// substitute.
    /// </summary>
    private static (LeafCacheGrain cache, BPlusLeafGrain cookieSource, IBPlusLeafGrain primary, RegistryFault fault)
        CreateCacheWithFaultableRegistry(string testName)
    {
        var unique = $"{testName}-{Guid.NewGuid():N}";
        var leafId = GrainId.Create("leaf", unique);
        var cookieSource = BPlusLeafGrainTests.CreateLeafGrainForCrossFixtureUse(replicaId: unique);

        var primary = Substitute.For<IBPlusLeafGrain>();
        primary.GetTreeIdAsync().Returns("test-tree");
        primary.GetPendingKeysAsync().Returns(new List<string>());

        var context = Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("cache", leafId.ToString()));

        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<IBPlusLeafGrain>(Arg.Any<GrainId>()).Returns(primary);

        var optionsMonitor = Substitute.For<IOptionsMonitor<LatticeOptions>>();
        optionsMonitor.Get(Arg.Any<string>()).Returns(new LatticeOptions { CacheTtl = TimeSpan.Zero });

        var fault = new RegistryFault();
        var registry = Substitute.For<ILatticeRegistry>();
        grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        registry.GetEntryAsync(Arg.Any<string>()).Returns(_ =>
        {
            fault.Reads++;
            return fault.Armed
                ? Task.FromException<TreeRegistryEntry?>(new TimeoutException("registry read timed out (injected)"))
                : Task.FromResult<TreeRegistryEntry?>(null);
        });

        var resolver = new LatticeOptionsResolver(grainFactory, optionsMonitor);
        var cache = new LeafCacheGrain(context, grainFactory, optionsMonitor, resolver, TestOriginClusterIdResolver.Default());
        return (cache, cookieSource, primary, fault);
    }

    /// <summary>Switches the injected registry fault on and off and counts reads.</summary>
    private sealed class RegistryFault
    {
        public bool Armed { get; set; }

        public int Reads { get; set; }
    }

    private static StateDelta MirrorDelta(long epoch, long sequence, params string[] keys)
    {
        var clock = HybridLogicalClock.Zero;
        var version = new VersionVector();
        var entries = new Dictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            clock = HybridLogicalClock.Tick(clock);
            version.Tick("primary");
            entries[key] = LwwValue<byte[]>.Create(Encoding.UTF8.GetBytes("v-" + key), clock);
        }

        return new StateDelta
        {
            Entries = entries,
            Version = version,
            DeliveryCursor = new LeafDeliveryCursor { Epoch = epoch, Sequence = sequence },
        };
    }

    [Test]
    public async Task GetManyAsync_after_a_resync_whose_budget_read_faulted_still_returns_every_key_the_leaf_holds()
    {
        var (cache, cookieSource, primary, fault) = CreateCacheWithFaultableRegistry(
            nameof(GetManyAsync_after_a_resync_whose_budget_read_faulted_still_returns_every_key_the_leaf_holds));
        var keys = new List<string> { "k1", "k2", "k3" };

        // Arm the same-silo revision cookie, then take the first (resync) refresh.
        await cookieSource.SetAsync("cookie", [1]);
        primary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MirrorDelta(MirrorEpochA, 3, "k1", "k2", "k3"));
        var before = await cache.GetManyAsync(keys);
        Assert.That(before.Keys, Is.EquivalentTo(keys), "precondition: the initial mirror is complete");

        // The primary reactivates (fresh epoch, so a full-snapshot resync) and
        // its revision cookie advances. The budget read for this refresh faults.
        await cookieSource.SetAsync("cookie", [2]);
        primary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MirrorDelta(MirrorEpochB, 3, "k1", "k2", "k3"));
        fault.Armed = true;
        Assert.That(async () => await cache.GetManyAsync(keys), Throws.InstanceOf<TimeoutException>(),
            "the injected registry fault must surface to the caller, not be swallowed");
        Assert.That(fault.Reads, Is.EqualTo(2), "vacuity control: the faulting refresh did reach the budget read");

        // The registry recovers. The primary has not written since, so the
        // cookie is unchanged: only an honest (unstamped) cookie makes this read
        // refresh again rather than trusting a mirror the fault left behind.
        fault.Armed = false;
        var after = await cache.GetManyAsync(keys);

        Assert.That(after.Keys, Is.EquivalentTo(keys),
            "a key the primary leaf holds was silently omitted: the faulted refresh left the mirror "
            + "incomplete while the revision cookie claimed it was fresh");
        await primary.Received(3).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }

    [Test]
    public async Task GetManyAsync_after_an_incremental_refresh_whose_budget_read_faulted_returns_the_newly_shipped_key()
    {
        var (cache, cookieSource, primary, fault) = CreateCacheWithFaultableRegistry(
            nameof(GetManyAsync_after_an_incremental_refresh_whose_budget_read_faulted_returns_the_newly_shipped_key));
        var keys = new List<string> { "k1", "k2" };

        await cookieSource.SetAsync("cookie", [1]);
        primary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MirrorDelta(MirrorEpochA, 1, "k1"));
        var before = await cache.GetManyAsync(keys);
        Assert.That(before.Keys, Is.EquivalentTo(new[] { "k1" }), "precondition: only k1 exists yet");

        // k2 is written on the primary: same epoch, so an incremental delta.
        await cookieSource.SetAsync("cookie", [2]);
        primary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MirrorDelta(MirrorEpochA, 2, "k2"));
        fault.Armed = true;
        Assert.That(async () => await cache.GetManyAsync(keys), Throws.InstanceOf<TimeoutException>());

        fault.Armed = false;
        var after = await cache.GetManyAsync(keys);

        Assert.That(after.Keys, Is.EquivalentTo(keys),
            "k2 exists on the primary leaf but was omitted: the faulted refresh stamped the cookie "
            + "without merging the delta that carried it");
    }

    [TestCase("GetAsync")]
    [TestCase("ExistsAsync")]
    public async Task Point_read_after_a_resync_whose_budget_read_faulted_still_finds_a_key_the_leaf_holds(string verb)
    {
        var (cache, cookieSource, primary, fault) = CreateCacheWithFaultableRegistry(
            nameof(Point_read_after_a_resync_whose_budget_read_faulted_still_finds_a_key_the_leaf_holds) + verb);

        await cookieSource.SetAsync("cookie", [1]);
        primary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MirrorDelta(MirrorEpochA, 1, "k1"));
        Assert.That(await ReadAsync(cache, verb, "k1"), Is.True, "precondition: the initial mirror holds k1");

        await cookieSource.SetAsync("cookie", [2]);
        primary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MirrorDelta(MirrorEpochB, 1, "k1"));
        fault.Armed = true;
        Assert.That(async () => await ReadAsync(cache, verb, "k1"), Throws.InstanceOf<TimeoutException>());

        fault.Armed = false;
        Assert.That(await ReadAsync(cache, verb, "k1"), Is.True,
            $"{verb} reported k1 absent although the primary leaf holds it");
    }

    [Test]
    public async Task GetManyAsync_on_an_empty_delta_does_not_consult_the_registry_for_a_budget()
    {
        var (cache, cookieSource, primary, fault) = CreateCacheWithFaultableRegistry(
            nameof(GetManyAsync_on_an_empty_delta_does_not_consult_the_registry_for_a_budget));

        // Moving the budget read ahead of the mutations must not add a registry
        // round trip to a refresh that ships nothing: a permanently faulting
        // registry is harmless while every delta is empty.
        await cookieSource.SetAsync("cookie", [1]);
        primary.GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>())
            .Returns(MirrorDelta(MirrorEpochA, 0));
        fault.Armed = true;

        var result = await cache.GetManyAsync(["k1"]);

        Assert.That(result, Is.Empty);
        Assert.That(fault.Reads, Is.Zero, "an empty delta applies no budget, so it must not read one");
        await primary.Received(1).GetDeltaSinceCursorAsync(Arg.Any<LeafDeliveryCursor>());
    }

    private static async Task<bool> ReadAsync(LeafCacheGrain cache, string verb, string key) => verb switch
    {
        "GetAsync" => await cache.GetAsync(key) is not null,
        "ExistsAsync" => await cache.ExistsAsync(key),
        _ => throw new ArgumentOutOfRangeException(nameof(verb), verb, null),
    };
}
