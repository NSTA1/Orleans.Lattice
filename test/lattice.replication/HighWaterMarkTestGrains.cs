using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.Replication.Grains;
using Orleans.Lattice.Replication.Tests.Fakes;
using Orleans.Runtime;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Builds high-water-mark and origin-frontier grains for unit tests (issue
/// #4586). The applier records applied identities and advances the high-water
/// mark in one <see cref="IReplicationHighWaterMarkGrain.AdvanceAppliedAsync"/>
/// call; <see cref="Substitute"/> routes that call through
/// <see cref="IReplicationHighWaterMarkGrain.TryAdvanceAsync"/>, so a test that
/// stubs or asserts the advance keeps doing so unchanged.
/// </summary>
internal static class HighWaterMarkTestGrains
{
    /// <summary>A substitute whose identity-recording advance delegates to its own <c>TryAdvanceAsync</c>.</summary>
    public static IReplicationHighWaterMarkGrain Substitute()
    {
        var hwm = NSubstitute.Substitute.For<IReplicationHighWaterMarkGrain>();
        hwm.AdvanceAppliedAsync(
                Arg.Any<string>(),
                Arg.Any<HybridLogicalClock>(),
                Arg.Any<IReadOnlyList<HybridLogicalClock>>(),
                Arg.Any<bool>(),
                Arg.Any<CancellationToken>())
            .Returns(call => (bool)call[3]
                ? hwm.TryAdvanceAsync((string)call[0], (HybridLogicalClock)call[1], (CancellationToken)call[4])
                : Task.FromResult(false));
        return hwm;
    }

    /// <summary>
    /// A real high-water-mark grain for <paramref name="treeId"/> over
    /// <paramref name="state"/>. Origin frontiers resolve through
    /// <paramref name="grainFactory"/>, or through a factory of real in-memory
    /// frontier grains when none is supplied.
    /// </summary>
    public static ReplicationHighWaterMarkGrain Real(
        FakePersistentState<ReplicationHighWaterMarkState>? state = null,
        IGrainFactory? grainFactory = null,
        string treeId = "tree",
        LatticeReplicationOptions? options = null)
    {
        var context = NSubstitute.Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("replication-hwm", treeId));
        var monitor = NSubstitute.Substitute.For<IOptionsMonitor<LatticeReplicationOptions>>();
        monitor.Get(Arg.Any<string>()).Returns(options ?? new LatticeReplicationOptions());
        monitor.CurrentValue.Returns(options ?? new LatticeReplicationOptions());
        return new ReplicationHighWaterMarkGrain(
            context,
            grainFactory ?? FrontierFactory(),
            monitor,
            state ?? new FakePersistentState<ReplicationHighWaterMarkState>());
    }

    /// <summary>
    /// A tree-frontier substitute in degraded mode (issue #4586 part 2b): no
    /// epoch, so a bootstrap pin installs nothing.
    /// </summary>
    public static IReplicationTreeFrontierGrain DegradedTreeFrontier()
    {
        var frontier = NSubstitute.Substitute.For<IReplicationTreeFrontierGrain>();
        frontier.GetAsync(Arg.Any<CancellationToken>()).Returns(new ReplicationTreeFrontierSnapshot());
        frontier.ObserveAsync(Arg.Any<string>(), Arg.Any<ReplicationSourceFrontier?>(), Arg.Any<CancellationToken>())
            .Returns(Guid.Empty);
        return frontier;
    }

    /// <summary>A real origin-frontier grain for <paramref name="origin"/>.</summary>
    public static ReplicationOriginFrontierGrain Frontier(
        string origin,
        IGrainFactory? grainFactory = null,
        FakePersistentState<ReplicationOriginFrontierState>? state = null)
    {
        var context = NSubstitute.Substitute.For<IGrainContext>();
        context.GrainId.Returns(GrainId.Create("replication-origin-frontier", origin));
        return new ReplicationOriginFrontierGrain(
            context,
            grainFactory ?? NSubstitute.Substitute.For<IGrainFactory>(),
            state ?? new FakePersistentState<ReplicationOriginFrontierState>());
    }

    /// <summary>
    /// A grain factory that serves one real in-memory origin-frontier grain per
    /// origin, created on first use and exposed through <paramref name="frontiers"/>.
    /// </summary>
    public static IGrainFactory FrontierFactory(Dictionary<string, ReplicationOriginFrontierGrain>? frontiers = null)
    {
        frontiers ??= new Dictionary<string, ReplicationOriginFrontierGrain>(StringComparer.Ordinal);
        var factory = NSubstitute.Substitute.For<IGrainFactory>();
        factory.GetGrain<IReplicationOriginFrontierGrain>(Arg.Any<string>(), Arg.Any<string?>())
            .Returns(call =>
            {
                var origin = (string)call[0];
                if (!frontiers.TryGetValue(origin, out var grain))
                {
                    grain = Frontier(origin, factory);
                    frontiers[origin] = grain;
                }

                return grain;
            });
        return factory;
    }
}
