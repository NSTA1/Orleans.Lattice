using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationBatchEncodedEnvelopeSlotTests
{
    [Test]
    public void EncodedEnvelope_defaults_to_null()
    {
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "t",
            OriginClusterId = "site-a",
        };
        Assert.That(batch.EncodedEnvelope, Is.Null);
    }

    [Test]
    public void EncodedEnvelope_can_be_populated_independently_of_Envelope_and_Payload()
    {
        var encoded = new ReplicationBatchEncodedEnvelope
        {
            Header = new EncodedBatchHeader { EntryCount = 1 },
            EncodedEntries = new ArraySegment<byte>[] { new(new byte[] { 1 }) },
        };
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "t",
            OriginClusterId = "site-a",
            EncodedEnvelope = encoded,
        };
        Assert.That(batch.EncodedEnvelope, Is.Not.Null);
        Assert.That(batch.EncodedEnvelope!.Value.Header.EntryCount, Is.EqualTo(1));
        Assert.That(batch.Envelope, Is.Null);
        Assert.That(batch.Payload.IsEmpty, Is.True);
    }

    [Test]
    public void EncodedEnvelope_coexists_with_Envelope_and_Payload_for_transports_that_pick()
    {
        var batch = new ReplicationBatch
        {
            TargetClusterId = "peer",
            TreeName = "t",
            OriginClusterId = "site-a",
            Payload = new byte[] { 1, 2, 3 },
            Envelope = new ReplicationBatchEnvelope { WireVersion = 1, TreeName = "t", OriginClusterId = "site-a" },
            EncodedEnvelope = new ReplicationBatchEncodedEnvelope { Header = new EncodedBatchHeader { EntryCount = 0 } },
        };
        Assert.That(batch.Payload.Length, Is.EqualTo(3));
        Assert.That(batch.Envelope, Is.Not.Null);
        Assert.That(batch.EncodedEnvelope, Is.Not.Null);
    }
}
