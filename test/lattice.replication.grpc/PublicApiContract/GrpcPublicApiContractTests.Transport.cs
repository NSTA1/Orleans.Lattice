using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;

namespace Orleans.Lattice.Replication.Grpc.Tests.PublicApiContract;

/// <summary>
/// Transport-surface contract tests:
/// <see cref="GrpcPushTransport"/> is the resolved
/// <see cref="IReplicationTransport"/>, batches round-trip end-to-end
/// with the encoded envelope shape preserved, and the same peer
/// <see cref="global::Grpc.Net.Client.GrpcChannel"/> is reused across
/// successive batches.
/// </summary>
public partial class GrpcPublicApiContractTests
{
    [Test]
    public async Task IReplicationTransport_resolves_to_GrpcPushTransport_after_registration()
    {
        await using var sender = _fixture.BuildSenderServices();
        var transport = sender.GetRequiredService<IReplicationTransport>();

        Assert.That(transport, Is.InstanceOf<GrpcPushTransport>());
    }

    [Test]
    public async Task SendAsync_round_trips_a_populated_envelope_and_returns_max_hwm()
    {
        await using var sender = _fixture.BuildSenderServices();
        var transport = sender.GetRequiredService<IReplicationTransport>();

        var hlcA = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 };
        var hlcB = new HybridLogicalClock { WallClockTicks = 200, Counter = 0 };
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "contract-tree",
            OriginClusterId = GrpcPublicApiContractFixture.SenderClusterId,
            Entries = new[]
            {
                new WalRecord
                {
                    TreeId = "contract-tree",
                    Op = MutationKind.Set,
                    Key = "k1",
                    Value = new byte[] { 1 },
                    Timestamp = hlcA,
                    OriginClusterId = GrpcPublicApiContractFixture.SenderClusterId,
                    Mode = LatticeMergeMode.LwwRegister,
                },
                new WalRecord
                {
                    TreeId = "contract-tree",
                    Op = MutationKind.Set,
                    Key = "k2",
                    Value = new byte[] { 2 },
                    Timestamp = hlcB,
                    OriginClusterId = GrpcPublicApiContractFixture.SenderClusterId,
                    Mode = LatticeMergeMode.LwwRegister,
                },
            },
        };

        var payload = GrpcPublicApiContractFixture.EncodeEnvelope(sender, envelope);
        var batch = new ReplicationBatch
        {
            TargetClusterId = GrpcPublicApiContractFixture.ReceiverClusterId,
            TreeName = "contract-tree",
            OriginClusterId = GrpcPublicApiContractFixture.SenderClusterId,
            Payload = payload,
        };

        var ack = await transport.SendAsync(batch, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(hlcB));
        });

        await _fixture.ReceiverApplier.Received(1).ApplyBatchAsync(
            Arg.Is<IReadOnlyList<WalRecord>>(list =>
                list.Count == 2
                && list[0].Key == "k1"
                && list[1].Key == "k2"),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task SendAsync_empty_payload_is_treated_as_heartbeat()
    {
        await using var sender = _fixture.BuildSenderServices();
        var transport = sender.GetRequiredService<IReplicationTransport>();

        var ack = await transport.SendAsync(
            GrpcPublicApiContractFixture.BuildBatch(Array.Empty<WalRecord>()),
            CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(ack.Accepted, Is.True);
            Assert.That(ack.HighestAppliedHlc, Is.EqualTo(HybridLogicalClock.Zero));
        });
    }

    [Test]
    public async Task SendAsync_reuses_the_same_peer_channel_across_batches()
    {
        var channelConstructions = 0;
        await using var sender = _fixture.BuildSenderServices(opts =>
        {
            var hostConfigured = opts.ConfigureChannel!;
            opts.ConfigureChannel = (peer, channelOptions) =>
            {
                Interlocked.Increment(ref channelConstructions);
                hostConfigured(peer, channelOptions);
            };
        });
        var transport = sender.GetRequiredService<IReplicationTransport>();

        var batch = GrpcPublicApiContractFixture.BuildBatch(Array.Empty<WalRecord>());

        await transport.SendAsync(batch, CancellationToken.None);
        await transport.SendAsync(batch, CancellationToken.None);
        await transport.SendAsync(batch, CancellationToken.None);

        Assert.That(channelConstructions, Is.EqualTo(1),
            "GrpcPushTransport must reuse a single GrpcChannel per peer cluster across SendAsync calls.");
    }
}
