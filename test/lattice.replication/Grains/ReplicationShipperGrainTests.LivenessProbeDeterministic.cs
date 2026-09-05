using NSubstitute;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Deterministic coverage for the idle-link liveness probe's failure legs. With
/// <c>LivenessProbeInterval</c> set to zero the probe fires on the second pump
/// (the first pump only anchors the last-contact timestamp), so a header
/// construction throw (swallowed, no backoff), a rejected probe ack (backoff),
/// and a successful ack carrying <c>PauseForMs</c> (which gates the next pump)
/// are all reachable without any sleeping. Probes ride an empty feed.
/// </summary>
public partial class ReplicationShipperGrainTests
{
    private static LatticeReplicationOptions LivenessProbeOptions() => new()
    {
        ClusterId = LocalCluster,
        ShipCursorWriteInterval = 1,
        ReplogPartitions = 1,
        WireVersionNegotiationEnabled = false,
        LivenessProbeInterval = TimeSpan.Zero,
    };

    [Test]
    public async Task TryEmitLivenessProbeAsync_header_construction_throw_is_swallowed_without_backoff()
    {
        var resolver = Substitute.For<ILatticeMergeModeResolver>();
        resolver.Resolve(Tree).Returns<LatticeMergeMode?>(
            _ => throw new InvalidOperationException("probe-header-boom"));
        var (grain, state, _, transport, _, _, _) =
            Create(LivenessProbeOptions(), modeResolver: resolver);

        await grain.PumpForTestingAsync(CancellationToken.None); // anchors last-contact
        await grain.PumpForTestingAsync(CancellationToken.None); // fires probe -> header throws

        resolver.Received().Resolve(Tree);
        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.Zero,
                "a probe whose header construction threw must not reach the transport");
            Assert.That(state.State.ConsecutiveFailures, Is.Zero,
                "a swallowed probe header failure must not apply backoff");
        });
    }

    [Test]
    public async Task TryEmitLivenessProbeAsync_rejected_ack_applies_backoff()
    {
        var (grain, state, _, transport, _, _, _) = Create(LivenessProbeOptions());
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck { Accepted = false, HighestAppliedHlc = HybridLogicalClock.Zero });

        await grain.PumpForTestingAsync(CancellationToken.None); // anchors last-contact
        await grain.PumpForTestingAsync(CancellationToken.None); // fires probe -> ack rejected

        Assert.Multiple(() =>
        {
            Assert.That(SendAsyncCallCount(transport), Is.EqualTo(1),
                "the probe must have been sent exactly once");
            Assert.That(state.State.ConsecutiveFailures, Is.GreaterThan(0),
                "a rejected probe ack must apply backoff");
        });
    }

    [Test]
    public async Task TryEmitLivenessProbeAsync_successful_ack_pause_for_ms_gates_next_pump()
    {
        var (grain, _, _, transport, _, _, _) = Create(LivenessProbeOptions());
        transport.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>())
            .Returns(new ReplicationAck
            {
                Accepted = true,
                HighestAppliedHlc = HybridLogicalClock.Zero,
                PauseForMs = 600_000,
            });

        await grain.PumpForTestingAsync(CancellationToken.None); // anchors last-contact
        await grain.PumpForTestingAsync(CancellationToken.None); // fires probe -> ack pauses
        await grain.PumpForTestingAsync(CancellationToken.None); // gated by the pause

        Assert.That(SendAsyncCallCount(transport), Is.EqualTo(1),
            "the peer-requested pause must gate the third pump so no second probe is sent");
    }
}
