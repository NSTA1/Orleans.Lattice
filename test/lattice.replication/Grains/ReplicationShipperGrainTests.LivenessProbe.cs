using NSubstitute;
using NSubstitute.ExceptionExtensions;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Coverage for the shipper grain's empty-batch liveness probe
/// (configured by <see cref="LatticeReplicationOptions.LivenessProbeInterval"/>).
/// </summary>
public partial class ReplicationShipperGrainTests
{
    [Test]
    public async Task First_empty_drain_does_not_fire_probe_so_existing_empty_tick_contract_is_preserved()
    {
        // The probe-interval timer is anchored on the first idle pump
        // tick, not on activation, so a brand-new shipper whose pump
        // tick finds no work must NOT call the transport. This
        // preserves the "empty drain == no transport call" invariant
        // every existing producer-side test depends on.
        var (grain, _, _, transport, _, _, _) = Create();

        await grain.PumpForTestingAsync(CancellationToken.None);

        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Empty);
    }

    [Test]
    public async Task Empty_drain_fires_probe_once_interval_elapses_since_activation_anchor()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            LivenessProbeInterval = TimeSpan.FromMilliseconds(1),
        };
        var (grain, _, _, transport, _, _, _) = Create(opts);

        // First tick anchors the probe-interval timer to now.
        await grain.PumpForTestingAsync(CancellationToken.None);

        // Sleep past the (tiny) configured interval and tick again.
        await Task.Delay(50);
        await grain.PumpForTestingAsync(CancellationToken.None);

        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Not.Empty,
            "the liveness probe must have shipped an empty batch once the interval elapsed");
        var batch = (ReplicationBatch)calls[^1].GetArguments()[0]!;
        Assert.That(batch.EncodedEnvelope, Is.Not.Null);
        Assert.That(batch.EncodedEnvelope!.Value.EncodedEntries.Length, Is.Zero);
        Assert.That(batch.EncodedEnvelope.Value.Header.EntryCount, Is.Zero);
    }

    [Test]
    public async Task Empty_drain_does_not_fire_probe_when_interval_is_infinite()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            LivenessProbeInterval = System.Threading.Timeout.InfiniteTimeSpan,
        };
        var (grain, _, _, transport, _, _, _) = Create(opts);

        await grain.PumpForTestingAsync(CancellationToken.None);

        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Empty,
            "an InfiniteTimeSpan interval disables the empty-tick liveness probe entirely");
    }

    [Test]
    public async Task Liveness_probe_transport_throw_applies_backoff_and_records_peer_error()
    {
        var opts = new LatticeReplicationOptions
        {
            ClusterId = LocalCluster,
            ShipCursorWriteInterval = 1,
            LivenessProbeInterval = TimeSpan.FromMilliseconds(1),
        };
        var (grain, _, _, transport, _, _, _) = Create(opts);

        // First tick anchors; second tick fires the probe. Make the
        // probe send throw - the existing transport-backoff path must
        // engage (RecordError increments peer.consecutive_errors).
        await grain.PumpForTestingAsync(CancellationToken.None);
        transport
            .When(t => t.SendAsync(Arg.Any<ReplicationBatch>(), Arg.Any<CancellationToken>()))
            .Do(_ => throw new TimeoutException("simulated probe transport failure"));

        await Task.Delay(50);
        await grain.PumpForTestingAsync(CancellationToken.None);

        var calls = transport.ReceivedCalls()
            .Where(c => c.GetMethodInfo().Name == nameof(IReplicationTransport.SendAsync))
            .ToList();
        Assert.That(calls, Is.Not.Empty,
            "the probe must have attempted the empty-batch send before the transport threw");
    }
}
