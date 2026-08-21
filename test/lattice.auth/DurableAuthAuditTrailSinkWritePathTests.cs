using NSubstitute;
using Orleans.Lattice.Auth;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth.Tests;

/// <summary>
/// Unit tests for the enabled write path of <see cref="DurableAuthAuditTrailSink"/>
/// over a substituted grain factory and <see cref="ILattice"/> tree (no cluster):
/// with the durable trail enabled it writes the decision event to the reserved
/// audit tree, using the time-to-live overload when a TTL is configured and the
/// plain overload otherwise.
/// </summary>
[TestFixture]
public sealed class DurableAuthAuditTrailSinkWritePathTests
{
    private static LatticeAuthDecisionEvent Event() =>
        new("alice", LatticeOperation.Read, "app", LatticeEffect.Deny, policyEpoch: 1, DateTimeOffset.UtcNow, key: "k");

    private static (DurableAuthAuditTrailSink Sink, ILattice Lattice) CreateSink(LatticeAuthOptions options)
    {
        var lattice = Substitute.For<ILattice>();
        var grainFactory = Substitute.For<IGrainFactory>();
        grainFactory.GetGrain<ILattice>(AuthConstants.AuditTree).Returns(lattice);
        var monitor = new CovOptionsMonitor<LatticeAuthOptions>(options);
        return (new DurableAuthAuditTrailSink(grainFactory, monitor), lattice);
    }

    [Test]
    public async Task WriteAsync_with_no_ttl_configured_uses_the_plain_set_overload()
    {
        var (sink, lattice) = CreateSink(new LatticeAuthOptions
        {
            EnableDurableAuditTrail = true,
            AuditTrailTimeToLive = null,
        });

        await sink.WriteAsync(Event());

        await lattice.Received(1).SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
        await lattice.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task WriteAsync_with_a_ttl_configured_uses_the_ttl_set_overload()
    {
        var ttl = TimeSpan.FromHours(2);
        var (sink, lattice) = CreateSink(new LatticeAuthOptions
        {
            EnableDurableAuditTrail = true,
            AuditTrailTimeToLive = ttl,
        });

        await sink.WriteAsync(Event());

        await lattice.Received(1).SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), ttl, Arg.Any<CancellationToken>());
        await lattice.DidNotReceive().SetAsync(Arg.Any<string>(), Arg.Any<byte[]>(), Arg.Any<CancellationToken>());
    }
}
