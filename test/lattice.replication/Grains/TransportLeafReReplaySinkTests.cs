using System.Buffers;
using Microsoft.Extensions.DependencyInjection;
using NUnit.Framework;
using Orleans.Lattice;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grains;
using Orleans.Serialization;

namespace Orleans.Lattice.Replication.Tests.Grains;

/// <summary>
/// Tests for <see cref="TransportLeafReReplaySink"/>, the production leaf
/// re-replay egress that frames selected entries into a
/// <see cref="ReplicationBatchEnvelope"/> and re-ships them through the
/// ordinary <see cref="IReplicationTransport"/>.
/// </summary>
[TestFixture]
public sealed class TransportLeafReReplaySinkTests
{
    private const string Tree = "orders";
    private const string Peer = "cluster-b";
    private const string Origin = "cluster-a";

    private ServiceProvider _services = null!;
    private OrleansBinaryReplicationBatchEncoder _encoder = null!;

    [OneTimeSetUp]
    public void OneTimeSetUp()
    {
        _services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = _services.GetRequiredService<Serializer<ReplicationBatchEnvelope>>();
        _encoder = new OrleansBinaryReplicationBatchEncoder(serializer);
    }

    [OneTimeTearDown]
    public void OneTimeTearDown() => _services.Dispose();

    private static WalRecord EntryWithTreeId(string? treeId, string key = "k") => new()
    {
        TreeId = treeId ?? string.Empty,
        Op = MutationKind.Set,
        Key = key,
        Value = new byte[] { 1, 2, 3 },
        Timestamp = new HybridLogicalClock { WallClockTicks = 100, Counter = 0 },
        OriginClusterId = Origin,
    };

    private sealed class CapturingTransport : IReplicationTransport
    {
        public ReplicationBatch? Sent { get; private set; }

        public Task<ReplicationAck> SendAsync(ReplicationBatch batch, CancellationToken cancellationToken)
        {
            Sent = batch;
            return Task.FromResult(new ReplicationAck { Accepted = true });
        }
    }

    [Test]
    public async Task ReplayAsync_restamps_empty_TreeId_from_batch_tree_name()
    {
        // WAL-sourced re-replay entries arrive with an empty TreeId because
        // the durable WAL codec strips that batch-constant slot and the read
        // path does not restore it. The sink must re-stamp every entry from
        // the batch tree name so the receiver never sees an empty TreeId,
        // which it can neither apply nor quarantine.
        var transport = new CapturingTransport();
        var sink = new TransportLeafReReplaySink(transport, _encoder, Origin);

        var entries = new[]
        {
            EntryWithTreeId(treeId: string.Empty, key: "a"),
            EntryWithTreeId(treeId: string.Empty, key: "b"),
        };

        var shipped = await sink.ReplayAsync(Peer, Tree, entries, CancellationToken.None);

        Assert.That(shipped, Is.EqualTo(2));
        Assert.That(transport.Sent, Is.Not.Null);
        var sent = transport.Sent!.Value;

        // The in-memory envelope every entry carries the batch tree name.
        Assert.That(sent.Envelope, Is.Not.Null);
        var envelope = sent.Envelope!.Value;
        Assert.That(
            envelope.Entries.Select(e => e.TreeId),
            Is.All.EqualTo(Tree));

        // And the same holds after a real wire round-trip through the encoder,
        // proving the receiver decodes non-empty TreeIds.
        var decoded = _encoder.Decode(sent.Payload);
        Assert.That(decoded.Entries.Select(e => e.TreeId), Is.All.EqualTo(Tree));
        Assert.That(decoded.Entries, Has.Count.EqualTo(2));
    }

    [Test]
    public async Task ReplayAsync_preserves_matching_TreeId()
    {
        // Bootstrap-fallback entries already carry the correct TreeId; the
        // re-stamp is a no-op for them.
        var transport = new CapturingTransport();
        var sink = new TransportLeafReReplaySink(transport, _encoder, Origin);

        var entries = new[] { EntryWithTreeId(treeId: Tree, key: "a") };

        await sink.ReplayAsync(Peer, Tree, entries, CancellationToken.None);

        var decoded = _encoder.Decode(transport.Sent!.Value.Payload);
        Assert.That(decoded.Entries.Single().TreeId, Is.EqualTo(Tree));
    }

    [Test]
    public async Task ReplayAsync_empty_entries_ships_nothing()
    {
        var transport = new CapturingTransport();
        var sink = new TransportLeafReReplaySink(transport, _encoder, Origin);

        var shipped = await sink.ReplayAsync(
            Peer, Tree, Array.Empty<WalRecord>(), CancellationToken.None);

        Assert.That(shipped, Is.Zero);
        Assert.That(transport.Sent, Is.Null);
    }
}
