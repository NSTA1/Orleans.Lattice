using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Replication.Tests;

[TestFixture]
public class ReplicationBatchEncodedEnvelopeTests
{
    [Test]
    public void Default_struct_has_default_header_and_empty_entries()
    {
        var env = default(ReplicationBatchEncodedEnvelope);
        Assert.That(env.Header, Is.EqualTo(default(EncodedBatchHeader)));
        Assert.That(env.EncodedEntries.IsEmpty, Is.True);
    }

    [Test]
    public void Holds_header_and_entry_segments_verbatim()
    {
        var header = new EncodedBatchHeader
        {
            Magic = EncodedBatchHeader.MagicValue,
            WireVersion = EncodedBatchHeader.CurrentWireVersion,
            OriginClusterIdHash = EncodedBatchHeader.HashClusterId("site-x"),
            EntryCount = 2,
            BatchSequence = 7L,
        };
        var seg0 = new ArraySegment<byte>(new byte[] { 1, 2, 3 });
        var seg1 = new ArraySegment<byte>(new byte[] { 9, 8, 7, 6 });
        var entries = new ArraySegment<byte>[] { seg0, seg1 };

        var env = new ReplicationBatchEncodedEnvelope
        {
            Header = header,
            EncodedEntries = entries,
        };

        Assert.That(env.Header, Is.EqualTo(header));
        Assert.That(env.EncodedEntries.Length, Is.EqualTo(2));
        Assert.That(env.EncodedEntries.Span[0], Is.EqualTo(seg0));
        Assert.That(env.EncodedEntries.Span[1], Is.EqualTo(seg1));
    }

    [Test]
    public void Equality_considers_header_value()
    {
        var entries = new ArraySegment<byte>[] { new(new byte[] { 1 }) };
        var a = new ReplicationBatchEncodedEnvelope
        {
            Header = new EncodedBatchHeader { EntryCount = 1 },
            EncodedEntries = entries,
        };
        var b = new ReplicationBatchEncodedEnvelope
        {
            Header = new EncodedBatchHeader { EntryCount = 1 },
            EncodedEntries = entries,
        };
        var c = a with { Header = a.Header with { EntryCount = 2 } };

        Assert.That(a, Is.EqualTo(b));
        Assert.That(a, Is.Not.EqualTo(c));
    }
}
