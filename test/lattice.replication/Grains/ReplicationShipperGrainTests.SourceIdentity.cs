using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Unit coverage for the shipper's event-driven source-identity rebind and its
/// backstop re-resolve gate (issue #1665). The steady-state pump no longer reads
/// the tree registry on every tick to detect an alias swap; instead an
/// event-driven <see cref="IReplicationShipperGrain.NotifySourceIdentityChangedAsync"/>
/// rebinds immediately, and a coarse backstop
/// (<see cref="LatticeReplicationOptions.ShipSourceIdentityBackstopInterval"/>)
/// heals a missed notification. These tests assert the registry-read gate and
/// the immediate-rebind semantics against a controllable clock.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    /// <summary>
    /// Wires a substitute <see cref="ILatticeRegistry"/> into a grain factory so
    /// the shipper's <c>ResolveSourcePhysicalAsync</c> resolves the source tree
    /// to <paramref name="physical"/>, and returns the registry so tests can
    /// assert the exact number of resolve reads.
    /// </summary>
    private static (IGrainFactory Factory, ILatticeRegistry Registry) FactoryWithRegistry(string physical)
    {
        var registry = Substitute.For<ILatticeRegistry>();
        registry.ResolveAsync(Arg.Any<string>()).Returns(physical);
        var factory = Substitute.For<IGrainFactory>();
        factory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId).Returns(registry);
        return (factory, registry);
    }

    private sealed class AdvanceableClock(DateTimeOffset start) : TimeProvider
    {
        private DateTimeOffset _utcNow = start;

        public override DateTimeOffset GetUtcNow() => _utcNow;

        public void Advance(TimeSpan delta) => _utcNow = _utcNow.Add(delta);
    }

    [Test]
    public async Task SourceIdentity_first_pump_resolves_registry_once_and_binds()
    {
        var (factory, registry) = FactoryWithRegistry(Tree);
        var (grain, state, _, _, _, _, _) = Create(grainFactory: factory);
        grain.SetCursorFlushClockForTesting(new AdvanceableClock(DateTimeOffset.UnixEpoch));

        await grain.PumpForTestingAsync(CancellationToken.None);

        await registry.Received(1).ResolveAsync(Tree);
        Assert.That(state.State.BoundPhysicalTreeId, Is.EqualTo(Tree),
            "The first pump must resolve the source identity and bind to the physical tree.");
    }

    [Test]
    public async Task SourceIdentity_idle_pumps_within_backstop_do_not_re_resolve()
    {
        var (factory, registry) = FactoryWithRegistry(Tree);
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipMaxInFlight = 1,
            LivenessProbeInterval = System.Threading.Timeout.InfiniteTimeSpan,
            ShipSourceIdentityBackstopInterval = TimeSpan.FromSeconds(30),
        };
        var (grain, _, _, _, _, _, _) = Create(opts, grainFactory: factory);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);

        await grain.PumpForTestingAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(10));
        await grain.PumpForTestingAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(10));
        await grain.PumpForTestingAsync(CancellationToken.None);

        await registry.Received(1).ResolveAsync(Tree);
    }

    [Test]
    public async Task SourceIdentity_backstop_elapsed_triggers_re_resolve()
    {
        var (factory, registry) = FactoryWithRegistry(Tree);
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipMaxInFlight = 1,
            LivenessProbeInterval = System.Threading.Timeout.InfiniteTimeSpan,
            ShipSourceIdentityBackstopInterval = TimeSpan.FromSeconds(30),
        };
        var (grain, _, _, _, _, _, _) = Create(opts, grainFactory: factory);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);

        await grain.PumpForTestingAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(31));
        await grain.PumpForTestingAsync(CancellationToken.None);

        await registry.Received(2).ResolveAsync(Tree);
    }

    [Test]
    public async Task NotifySourceIdentityChanged_rebinds_and_resets_cursors_without_registry_read()
    {
        var (factory, registry) = FactoryWithRegistry("unused");
        var seed = new ReplicationShipperState
        {
            BoundPhysicalTreeId = "phys-old",
            Cursor = new HybridLogicalClock { WallClockTicks = 123, Counter = 4 },
        };
        seed.PartitionCursors[0] = 99L;
        var (grain, state, _, _, _, _, _) = Create(seedState: seed, grainFactory: factory);
        grain.SetCursorFlushClockForTesting(new AdvanceableClock(DateTimeOffset.UnixEpoch));

        await grain.NotifySourceIdentityChangedAsync("phys-new", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.BoundPhysicalTreeId, Is.EqualTo("phys-new"),
                "A notified identity change must rebind to the new physical tree.");
            Assert.That(state.State.PartitionCursors, Is.Empty,
                "Rebinding must discard the retired log's per-partition cursors.");
            Assert.That(state.State.Cursor, Is.EqualTo(HybridLogicalClock.Zero),
                "Rebinding must reset the scalar cursor so the shipper re-ships from the new log start.");
            Assert.That(state.WriteCount, Is.EqualTo(1),
                "A cursor-resetting rebind must be flushed durably.");
        });
        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
    }

    [Test]
    public async Task NotifySourceIdentityChanged_is_noop_when_identity_unchanged()
    {
        var (factory, _) = FactoryWithRegistry("unused");
        var seed = new ReplicationShipperState
        {
            BoundPhysicalTreeId = "phys-1",
            Cursor = new HybridLogicalClock { WallClockTicks = 50, Counter = 1 },
        };
        seed.PartitionCursors[0] = 42L;
        var (grain, state, _, _, _, _, _) = Create(seedState: seed, grainFactory: factory);
        grain.SetCursorFlushClockForTesting(new AdvanceableClock(DateTimeOffset.UnixEpoch));

        await grain.NotifySourceIdentityChangedAsync("phys-1", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(state.State.PartitionCursors[0], Is.EqualTo(42L),
                "A no-op notification for the current identity must not reset cursors.");
            Assert.That(state.State.Cursor, Is.EqualTo(new HybridLogicalClock { WallClockTicks = 50, Counter = 1 }),
                "A no-op notification must not touch the scalar cursor.");
            Assert.That(state.WriteCount, Is.EqualTo(0),
                "A no-op notification must not force a durable write.");
        });
    }

    [Test]
    public async Task NotifySourceIdentityChanged_suppresses_the_next_backstop_resolve()
    {
        var (factory, registry) = FactoryWithRegistry(Tree);
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipSourceIdentityBackstopInterval = TimeSpan.FromSeconds(30),
        };
        var seed = new ReplicationShipperState { BoundPhysicalTreeId = Tree };
        var (grain, _, _, _, _, _, _) = Create(opts, seedState: seed, grainFactory: factory);
        var clock = new AdvanceableClock(DateTimeOffset.UnixEpoch);
        grain.SetCursorFlushClockForTesting(clock);

        // The push establishes the binding and stamps the backstop clock, so a
        // pump inside the backstop window must not fall back to a registry read.
        await grain.NotifySourceIdentityChangedAsync(Tree, CancellationToken.None);
        clock.Advance(TimeSpan.FromSeconds(10));
        await grain.PumpForTestingAsync(CancellationToken.None);

        await registry.DidNotReceive().ResolveAsync(Arg.Any<string>());
    }

    [TestCase("")]
    [TestCase(null)]
    public void NotifySourceIdentityChanged_rejects_null_or_empty_physical_id(string? physical)
    {
        var (factory, _) = FactoryWithRegistry("unused");
        var (grain, _, _, _, _, _, _) = Create(grainFactory: factory);

        Assert.That(
            () => grain.NotifySourceIdentityChangedAsync(physical!, CancellationToken.None),
            Throws.InstanceOf<ArgumentException>());
    }
}
