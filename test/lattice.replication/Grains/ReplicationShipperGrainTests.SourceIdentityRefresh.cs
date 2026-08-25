using NSubstitute;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the shipper grain's source physical-identity refresh
/// backstop (configured by
/// <see cref="LatticeReplicationOptions.ShipSourceIdentityRefreshInterval"/>).
/// The shipper rebinds its logical source tree to the current physical WAL
/// to pick up a registry alias swap; that registry read is its only
/// per-tick registry access, so on an idle link it is cached and refreshed
/// only once per the interval. These tests assert the read cadence
/// collapses to the interval, that the disabled (zero) interval preserves
/// the pre-cache per-tick behaviour, and that an alias swap is still
/// detected once the interval elapses.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>
    /// Wires a counting <see cref="ILatticeRegistry"/> stub into a fresh
    /// substitute grain factory so the number of source-identity resolves a
    /// pump performs is observable. The stub resolves the logical tree to
    /// <paramref name="physical"/> (defaulting to the logical id, i.e. no
    /// alias in effect).
    /// </summary>
    private static (IGrainFactory Factory, ILatticeRegistry Registry) RegistryCountingFactory(
        string physical = Tree)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Tree).Returns(physical);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return (factory, registry);
    }

    private static LatticeReplicationOptions IdleOptions(TimeSpan refreshInterval) => new()
    {
        ClusterId = LocalCluster,
        // Disable the liveness probe and defer cursor writes so an idle
        // pump tick's only registry-touching side effect is the gated
        // source-identity resolve under test.
        LivenessProbeInterval = System.Threading.Timeout.InfiniteTimeSpan,
        ShipSourceIdentityRefreshInterval = refreshInterval,
    };

    [Test]
    public async Task SourceIdentityRefresh_resolves_once_per_interval_on_idle_shipper()
    {
        var (factory, registry) = RegistryCountingFactory();
        var (grain, _, _, _, _, _, _) = Create(IdleOptions(TimeSpan.FromSeconds(5)), grainFactory: factory);
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);

        // First tick always resolves (binds the activation).
        await grain.PumpForTestingAsync(CancellationToken.None);
        await registry.Received(1).ResolveAsync(Tree);

        // Ticks inside the interval reuse the cached identity - no read.
        clock.Advance(TimeSpan.FromSeconds(2));
        await grain.PumpForTestingAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(2)); // t = 4s, still < 5s
        await grain.PumpForTestingAsync(CancellationToken.None);
        await registry.Received(1).ResolveAsync(Tree);

        // Once the interval elapses the shipper re-resolves exactly once.
        clock.Advance(TimeSpan.FromSeconds(2)); // t = 6s >= 5s
        await grain.PumpForTestingAsync(CancellationToken.None);
        await registry.Received(2).ResolveAsync(Tree);
    }

    [Test]
    public async Task SourceIdentityRefresh_zero_interval_resolves_every_tick()
    {
        // TimeSpan.Zero disables the cache: the resolve runs on every pump
        // tick exactly as it did before the backstop existed.
        var (factory, registry) = RegistryCountingFactory();
        var (grain, _, _, _, _, _, _) = Create(IdleOptions(TimeSpan.Zero), grainFactory: factory);
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);

        await grain.PumpForTestingAsync(CancellationToken.None);
        await grain.PumpForTestingAsync(CancellationToken.None);
        await grain.PumpForTestingAsync(CancellationToken.None);

        await registry.Received(3).ResolveAsync(Tree);
    }

    [Test]
    public async Task SourceIdentityRefresh_first_tick_resolves_despite_long_interval()
    {
        // A large interval must not suppress the very first resolve: the
        // activation has to bind to a physical identity before it can ship.
        var (factory, registry) = RegistryCountingFactory();
        var (grain, _, _, _, _, _, _) = Create(IdleOptions(TimeSpan.FromHours(1)), grainFactory: factory);
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);

        await grain.PumpForTestingAsync(CancellationToken.None);

        await registry.Received(1).ResolveAsync(Tree);
    }

    [Test]
    public async Task SourceIdentityRefresh_alias_swap_detected_after_interval_not_before()
    {
        // Bind to the current identity, then swap the alias under the live
        // shipper. Detection is deferred to the backstop interval: a tick
        // inside the interval keeps the cached identity (no cursor reset),
        // and the tick at/after the interval re-resolves and heals.
        var (factory, registry) = RegistryCountingFactory();
        var (grain, state, _, _, _, _, _) =
            Create(IdleOptions(TimeSpan.FromSeconds(5)), grainFactory: factory);
        var clock = new MutableTimeProvider(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);

        // First tick binds to the pre-swap identity. First bind records the
        // identity in memory without a state write (deferred-persist).
        await grain.PumpForTestingAsync(CancellationToken.None);
        Assert.That(state.WriteCount, Is.EqualTo(0),
            "First bind must not force a state write.");

        // Alias swap: the logical tree now resolves to a new physical WAL.
        registry.ResolveAsync(Tree).Returns(Tree + "-v2");

        // Inside the interval the swap is invisible - cached identity holds,
        // so no rebind / cursor-reset write occurs.
        clock.Advance(TimeSpan.FromSeconds(2));
        await grain.PumpForTestingAsync(CancellationToken.None);
        Assert.That(state.WriteCount, Is.EqualTo(0),
            "A cached tick inside the refresh interval must not detect the swap.");

        // At/after the interval the shipper re-resolves, detects the swap,
        // resets the retired log's cursors and persists the rebind.
        clock.Advance(TimeSpan.FromSeconds(4)); // t = 6s >= 5s
        await grain.PumpForTestingAsync(CancellationToken.None);
        Assert.That(state.WriteCount, Is.EqualTo(1),
            "The tick at/after the refresh interval must detect the swap and persist the cursor reset.");
    }
}
