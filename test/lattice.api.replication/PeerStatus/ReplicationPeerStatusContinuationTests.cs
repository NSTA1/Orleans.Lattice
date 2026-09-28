using System.Buffers.Text;
using System.Text;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <see cref="ReplicationPeerStatusContinuation"/>: lossless round
/// trips and fail-closed rejection of every malformed token shape.
/// </summary>
[TestFixture]
public sealed class ReplicationPeerStatusContinuationTests
{
    private static string Token(string payload) =>
        "1." + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(payload));

    [TestCase("orders", false, "east", ReplicationContactDirection.Outbound)]
    [TestCase("a/crm/contacts", true, "west", ReplicationContactDirection.Inbound)]
    [TestCase("t/other/a:b:c", false, "peer:with:colons", ReplicationContactDirection.Inbound)]
    [TestCase("\u00e9t\u00e9", false, "12", ReplicationContactDirection.Outbound)]
    public void Encode_then_Decode_round_trips(string tree, bool stripped, string peer, ReplicationContactDirection direction)
    {
        var cursor = new ReplicationPeerStatusCursor(tree, stripped, peer, direction);

        var decoded = ReplicationPeerStatusContinuation.Decode(ReplicationPeerStatusContinuation.Encode(cursor));

        Assert.That(decoded, Is.EqualTo(cursor));
    }

    [Test]
    public void Encode_produces_a_url_safe_opaque_token()
    {
        var token = ReplicationPeerStatusContinuation.Encode(
            new ReplicationPeerStatusCursor("a/crm/contacts", true, "east", ReplicationContactDirection.Outbound));

        Assert.Multiple(() =>
        {
            Assert.That(token, Does.StartWith("1."));
            Assert.That(token, Does.Not.Contain("/").And.Not.Contain("+").And.Not.Contain("="));
            Assert.That(token, Does.Not.Contain("contacts"));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public void Decode_of_no_token_is_the_first_page(string? token)
    {
        Assert.That(ReplicationPeerStatusContinuation.Decode(token), Is.Null);
    }

    [TestCase("garbage")]
    [TestCase("2.AAAA")]
    [TestCase("1.***")]
    public void Decode_rejects_a_token_that_is_not_a_versioned_base64url_payload(string token)
    {
        Assert.That(() => ReplicationPeerStatusContinuation.Decode(token), Throws.ArgumentException);
    }

    [TestCase("")]
    [TestCase("6:orders")]
    [TestCase("6:orders4:east0")]
    [TestCase("6:orders4:east001")]
    [TestCase("6:orders4:east20")]
    [TestCase("6:orders4:east02")]
    [TestCase("0:4:east00")]
    [TestCase("6:orders0:00")]
    [TestCase("99:orders4:east00")]
    [TestCase("-1:x4:east00")]
    [TestCase("x:orders4:east00")]
    [TestCase(":orders4:east00")]
    public void Decode_rejects_a_malformed_payload(string payload)
    {
        Assert.That(() => ReplicationPeerStatusContinuation.Decode(Token(payload)), Throws.ArgumentException);
    }

    [Test]
    public void Decode_rejects_an_oversized_token_before_decoding_it()
    {
        var token = "1." + new string('A', ReplicationPeerStatusContinuation.MaxTokenLength);

        Assert.That(() => ReplicationPeerStatusContinuation.Decode(token), Throws.ArgumentException);
    }

    [Test]
    public void Decode_rejection_names_the_query_parameter()
    {
        var ex = Assert.Throws<ArgumentException>(() => ReplicationPeerStatusContinuation.Decode("garbage"));

        Assert.That(ex!.ParamName, Is.EqualTo("query"));
    }
}
