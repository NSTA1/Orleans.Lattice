using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Tests.PublicApiContract;

/// <summary>
/// Pins the <see cref="IReplicationBatchEncoder"/> public contract:
/// the canonical Orleans-binary encoder produces a versioned envelope
/// that round-trips through <c>Encode</c> -> <c>Decode</c> with every
/// routing slot intact, stamps
/// <see cref="ReplicationBatchEnvelope.CurrentVersion"/> when callers
/// supply a default-zero version, and rejects payloads carrying a
/// future-version stamp with <see cref="NotSupportedException"/>.
/// </summary>
public partial class PublicReplicationApiContractTests
{
    [Test]
    public void IReplicationBatchEncoder_round_trips_envelope_with_routing_and_entries()
    {
        var encoder = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<IReplicationBatchEncoder>();

        var entry = new WalRecord
        {
            TreeId = "encoder-round-trip",
            Op = MutationKind.Set,
            Key = "k",
            Value = Bytes("v"),
            Timestamp = new HybridLogicalClock { WallClockTicks = DateTime.UtcNow.Ticks, Counter = 1 },
            OriginClusterId = PublicReplicationApiClusterFixture.SiteAClusterId,
        };

        var input = new ReplicationBatchEnvelope
        {
            TreeName = "encoder-round-trip",
            OriginClusterId = PublicReplicationApiClusterFixture.SiteAClusterId,
            Entries = new[] { entry },
        };

        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(input, writer);

        var decoded = encoder.Decode(writer.WrittenMemory);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.WireVersion, Is.EqualTo(ReplicationBatchEnvelope.CurrentVersion));
            Assert.That(decoded.TreeName, Is.EqualTo(input.TreeName));
            Assert.That(decoded.OriginClusterId, Is.EqualTo(input.OriginClusterId));
            Assert.That(decoded.Entries, Has.Count.EqualTo(1));
            Assert.That(decoded.Entries[0].Key, Is.EqualTo("k"));
            Assert.That(decoded.Entries[0].Op, Is.EqualTo(MutationKind.Set));
            Assert.That(decoded.Entries[0].OriginClusterId, Is.EqualTo(PublicReplicationApiClusterFixture.SiteAClusterId));
        });
    }

    [Test]
    public void IReplicationBatchEncoder_decode_rejects_future_wire_version()
    {
        var encoder = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<IReplicationBatchEncoder>();

        var futureEnvelope = new ReplicationBatchEnvelope
        {
            WireVersion = encoder.CurrentWireVersion + 1,
            TreeName = "future",
            OriginClusterId = PublicReplicationApiClusterFixture.SiteAClusterId,
            Entries = Array.Empty<WalRecord>(),
        };
        var writer = new ArrayBufferWriter<byte>();
        encoder.Encode(futureEnvelope, writer);

        Assert.That(
            () => encoder.Decode(writer.WrittenMemory),
            Throws.InstanceOf<NotSupportedException>());
    }

    [Test]
    public void IReplicationBatchEncoder_exposes_content_type_and_current_wire_version()
    {
        var encoder = PublicReplicationApiClusterFixture
            .ServicesFor(PublicReplicationApiClusterFixture.SiteAClusterId)
            .GetRequiredService<IReplicationBatchEncoder>();

        Assert.Multiple(() =>
        {
            Assert.That(encoder.ContentType, Is.Not.Null.And.Not.Empty);
            Assert.That(encoder.CurrentWireVersion, Is.GreaterThanOrEqualTo(1));
            Assert.That(ReplicationBatchEnvelope.CurrentVersion, Is.EqualTo(1));
            Assert.That(ReplicationBatchEnvelope.CurrentMinorVersion, Is.EqualTo(1));
        });
    }
}
