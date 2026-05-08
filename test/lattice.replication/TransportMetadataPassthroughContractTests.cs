using Orleans.Lattice.BPlusTree.Grains;
using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests;

/// <summary>
/// Pins the R-086 transport contract: an
/// <see cref="IReplicationTransport"/> implementation must preserve the
/// causal-plus metadata slots
/// (<see cref="WalRecord.VectorClock"/> and
/// <see cref="WalRecord.DependencySummary"/>) verbatim across a
/// round-trip. No reordering, no mutation, no synthesis. The transport
/// stays dumb: any normalisation, summary derivation, or merge belongs
/// in the producer / receiver, never in the wire layer.
/// </summary>
[TestFixture]
public class TransportMetadataPassthroughContractTests
{
    private ServiceProvider _services = null!;
    private IReplicationBatchEncoder _encoder = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = _services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        _encoder = new OrleansBinaryReplicationBatchEncoder(serializer);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static VersionVector MakeVector(params (string origin, long wallClock, int counter)[] entries)
    {
        var vc = new VersionVector();
        foreach (var (origin, wallClock, counter) in entries)
        {
            vc.Entries[origin] = new HybridLogicalClock { WallClockTicks = wallClock, Counter = counter };
        }
        return vc;
    }

    private static WalRecord MakeEntry(
        string key,
        long wallClock,
        VersionVector? vectorClock,
        VersionVector? dependencySummary)
        => new()
        {
            TreeId = "tree",
            Op = MutationKind.Set,
            Key = key,
            Value = new byte[] { 1, 2, 3 },
            Timestamp = new HybridLogicalClock { WallClockTicks = wallClock, Counter = 0 },
            OriginClusterId = "site-a",
            Mode = LatticeMergeMode.LwwRegister,
            VectorClock = vectorClock,
            DependencySummary = dependencySummary,
        };

    private ReadOnlyMemory<byte> Encode(ReplicationBatchEnvelope envelope)
    {
        var writer = new ArrayBufferWriter<byte>();
        _encoder.Encode(envelope, writer);
        return writer.WrittenMemory;
    }

    private ReplicationBatchEnvelope Decode(ReadOnlyMemory<byte> payload)
        => _encoder.Decode(payload);

    private async Task<ReadOnlyMemory<byte>> RoundTripAsync(
        LoopbackTransport transport,
        ReplicationBatchEnvelope envelope)
    {
        var payload = Encode(envelope);
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = envelope.TreeName,
            OriginClusterId = envelope.OriginClusterId,
            Payload = payload,
        };

        await transport.SendAsync(batch, CancellationToken.None);

        var recorded = transport.Sent.Single();
        return recorded.Payload;
    }

    [Test]
    public async Task LoopbackTransport_preserves_vector_clock_and_dependency_summary()
    {
        var transport = new LoopbackTransport();
        var vectorClock = MakeVector(("site-a", 100, 0), ("site-b", 200, 1));
        var dependencySummary = MakeVector(("site-a", 100, 0), ("site-b", 200, 1));

        var envelope = new ReplicationBatchEnvelope
        {
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[] { MakeEntry("k", 100, vectorClock, dependencySummary) },
        };

        var bytes = await RoundTripAsync(transport, envelope);
        var decoded = Decode(bytes).Entries.Single();

        Assert.Multiple(() =>
        {
            Assert.That(decoded.VectorClock, Is.Not.Null);
            Assert.That(decoded.VectorClock!.Entries, Is.EqualTo(vectorClock.Entries));
            Assert.That(decoded.DependencySummary, Is.Not.Null);
            Assert.That(decoded.DependencySummary!.Entries, Is.EqualTo(dependencySummary.Entries));
        });
    }

    [Test]
    public async Task LoopbackTransport_preserves_null_metadata_for_legacy_entries()
    {
        var transport = new LoopbackTransport();
        var envelope = new ReplicationBatchEnvelope
        {
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[] { MakeEntry("k", 100, vectorClock: null, dependencySummary: null) },
        };

        var bytes = await RoundTripAsync(transport, envelope);
        var decoded = Decode(bytes).Entries.Single();

        Assert.Multiple(() =>
        {
            Assert.That(decoded.VectorClock, Is.Null);
            Assert.That(decoded.DependencySummary, Is.Null);
        });
    }

    [Test]
    public async Task LoopbackTransport_preserves_independent_vector_clock_and_dependency_summary()
    {
        // The two slots are reserved as distinct fields specifically so a
        // future Bloom-filter-shaped DependencySummary can ship without
        // re-numbering the wire format. Pin the contract that the
        // transport never aliases or merges them.
        var transport = new LoopbackTransport();
        var vectorClock = MakeVector(("site-a", 100, 0), ("site-b", 200, 0));
        var dependencySummary = MakeVector(("site-a", 50, 0));

        var envelope = new ReplicationBatchEnvelope
        {
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[] { MakeEntry("k", 100, vectorClock, dependencySummary) },
        };

        var bytes = await RoundTripAsync(transport, envelope);
        var decoded = Decode(bytes).Entries.Single();

        Assert.Multiple(() =>
        {
            Assert.That(decoded.VectorClock!.Entries, Has.Count.EqualTo(2));
            Assert.That(decoded.DependencySummary!.Entries, Has.Count.EqualTo(1));
            Assert.That(decoded.VectorClock.GetClock("site-b").WallClockTicks, Is.EqualTo(200L));
            Assert.That(decoded.DependencySummary.Entries.ContainsKey("site-b"), Is.False);
        });
    }

    [Test]
    public async Task LoopbackTransport_preserves_per_entry_vector_clocks_in_a_multi_entry_batch()
    {
        var transport = new LoopbackTransport();
        var vc1 = MakeVector(("site-a", 100, 0));
        var vc2 = MakeVector(("site-a", 200, 0), ("site-b", 50, 0));
        var vc3 = MakeVector(("site-b", 75, 0));

        var envelope = new ReplicationBatchEnvelope
        {
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[]
            {
                MakeEntry("k1", 100, vc1, vc1),
                MakeEntry("k2", 200, vc2, vc2),
                MakeEntry("k3", 300, vc3, vc3),
            },
        };

        var bytes = await RoundTripAsync(transport, envelope);
        var decoded = Decode(bytes).Entries;

        Assert.That(decoded, Has.Count.EqualTo(3));
        Assert.Multiple(() =>
        {
            Assert.That(decoded[0].VectorClock!.Entries, Is.EqualTo(vc1.Entries));
            Assert.That(decoded[1].VectorClock!.Entries, Is.EqualTo(vc2.Entries));
            Assert.That(decoded[2].VectorClock!.Entries, Is.EqualTo(vc3.Entries));
            Assert.That(decoded[0].DependencySummary!.Entries, Is.EqualTo(vc1.Entries));
            Assert.That(decoded[1].DependencySummary!.Entries, Is.EqualTo(vc2.Entries));
            Assert.That(decoded[2].DependencySummary!.Entries, Is.EqualTo(vc3.Entries));
        });
    }

    [Test]
    public async Task LoopbackTransport_preserves_entry_order_in_a_multi_entry_batch()
    {
        var transport = new LoopbackTransport();
        var vc = MakeVector(("site-a", 100, 0));
        var envelope = new ReplicationBatchEnvelope
        {
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[]
            {
                MakeEntry("k1", 100, vc, vc),
                MakeEntry("k2", 200, vc, vc),
                MakeEntry("k3", 300, vc, vc),
                MakeEntry("k4", 400, vc, vc),
            },
        };

        var bytes = await RoundTripAsync(transport, envelope);
        var decoded = Decode(bytes).Entries;

        Assert.That(decoded.Select(e => e.Key), Is.EqualTo(new[] { "k1", "k2", "k3", "k4" }));
        Assert.That(
            decoded.Select(e => e.Timestamp.WallClockTicks),
            Is.EqualTo(new[] { 100L, 200L, 300L, 400L }));
    }

    [Test]
    public async Task LoopbackTransport_preserves_metadata_across_repeated_sends()
    {
        var transport = new LoopbackTransport();
        var vectorClock = MakeVector(("site-a", 100, 0), ("site-b", 200, 0));
        var dependencySummary = MakeVector(("site-a", 100, 0));

        var envelope = new ReplicationBatchEnvelope
        {
            TreeName = "tree",
            OriginClusterId = "site-a",
            Entries = new[] { MakeEntry("k", 100, vectorClock, dependencySummary) },
        };

        var payload = Encode(envelope);
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = envelope.TreeName,
            OriginClusterId = envelope.OriginClusterId,
            Payload = payload,
        };

        await transport.SendAsync(batch, CancellationToken.None);
        await transport.SendAsync(batch, CancellationToken.None);

        var recorded = transport.Sent.ToArray();
        Assert.That(recorded, Has.Length.EqualTo(2));
        foreach (var rec in recorded)
        {
            var decoded = Decode(rec.Payload).Entries.Single();
            Assert.Multiple(() =>
            {
                Assert.That(decoded.VectorClock!.Entries, Is.EqualTo(vectorClock.Entries));
                Assert.That(decoded.DependencySummary!.Entries, Is.EqualTo(dependencySummary.Entries));
            });
        }
    }
}