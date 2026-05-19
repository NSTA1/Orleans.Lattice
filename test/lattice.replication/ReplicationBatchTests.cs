using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Primitives;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationBatchTests
{
    [Test]
    public void Default_value_has_null_strings_and_empty_payload()
    {
        var batch = default(ReplicationBatch);

        Assert.Multiple(() =>
        {
            Assert.That(batch.TargetClusterId, Is.Null);
            Assert.That(batch.TreeName, Is.Null);
            Assert.That(batch.OriginClusterId, Is.Null);
            Assert.That(batch.Payload.IsEmpty, Is.True);
            Assert.That(batch.Envelope, Is.Null);
        });
    }

    [Test]
    public void Init_assigns_every_property()
    {
        var bytes = new byte[] { 1, 2, 3 };
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer-1",
            TreeName = "orders",
            OriginClusterId = "site-a",
            Payload = bytes,
        };

        Assert.Multiple(() =>
        {
            Assert.That(batch.TargetClusterId, Is.EqualTo("peer-1"));
            Assert.That(batch.TreeName, Is.EqualTo("orders"));
            Assert.That(batch.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(batch.Payload.ToArray(), Is.EqualTo(bytes));
            Assert.That(batch.Envelope, Is.Null,
                "Envelope is optional and defaults to null when the caller did not supply one.");
        });
    }

    [Test]
    public void Init_carries_typed_envelope_when_supplied()
    {
        // Pre-built envelopes let transports that re-marshal onto
        // their own wire (e.g. the gRPC streaming push transport)
        // skip the per-send decode-then-re-encode round-trip the
        // opaque-bytes-only seam would force. Pin the round-trip:
        // every field the caller wrote must be readable back without
        // mutation.
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "orders",
            OriginClusterId = "site-a",
            Entries = new List<WalRecord>
            {
                new() { TreeId = "orders", Op = MutationKind.Set, Key = "k", Value = new byte[] { 9 } },
            },
        };

        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer-1",
            TreeName = "orders",
            OriginClusterId = "site-a",
            Payload = new byte[] { 1, 2, 3 },
            Envelope = envelope,
        };

        Assert.Multiple(() =>
        {
            Assert.That(batch.Envelope, Is.Not.Null);
            Assert.That(batch.Envelope!.Value.WireVersion, Is.EqualTo(1));
            Assert.That(batch.Envelope!.Value.TreeName, Is.EqualTo("orders"));
            Assert.That(batch.Envelope!.Value.OriginClusterId, Is.EqualTo("site-a"));
            Assert.That(batch.Envelope!.Value.Entries, Has.Count.EqualTo(1));
            Assert.That(batch.Envelope!.Value.Entries[0].Key, Is.EqualTo("k"));
        });
    }

    [Test]
    public void With_expression_produces_modified_copy()
    {
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer-1",
            TreeName = "orders",
            OriginClusterId = "site-a",
            Payload = ReadOnlyMemory<byte>.Empty,
        };

        var modified = batch with { TargetClusterId = "peer-2" };

        Assert.Multiple(() =>
        {
            Assert.That(modified.TargetClusterId, Is.EqualTo("peer-2"));
            Assert.That(modified.TreeName, Is.EqualTo("orders"));
            Assert.That(batch.TargetClusterId, Is.EqualTo("peer-1"));
        });
    }

    [Test]
    public void With_expression_can_attach_an_envelope_to_an_existing_batch()
    {
        // The shipper builds the batch once and then attaches the
        // typed envelope just before SendAsync; this is the call
        // shape the gRPC fast path consumes. Pin that a `with`-
        // expression toggle on the Envelope slot preserves every
        // other property exactly.
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer-1",
            TreeName = "orders",
            OriginClusterId = "site-a",
            Payload = new byte[] { 1 },
        };
        var envelope = new ReplicationBatchEnvelope
        {
            WireVersion = 1,
            TreeName = "orders",
            OriginClusterId = "site-a",
            Entries = Array.Empty<WalRecord>(),
        };

        var attached = batch with { Envelope = envelope };

        Assert.Multiple(() =>
        {
            Assert.That(attached.Envelope, Is.Not.Null);
            Assert.That(batch.Envelope, Is.Null, "the original instance is unchanged");
            Assert.That(attached.TargetClusterId, Is.EqualTo(batch.TargetClusterId));
            Assert.That(attached.TreeName, Is.EqualTo(batch.TreeName));
            Assert.That(attached.OriginClusterId, Is.EqualTo(batch.OriginClusterId));
            Assert.That(attached.Payload.ToArray(), Is.EqualTo(batch.Payload.ToArray()));
        });
    }
}
