using System.Buffers.Text;
using System.Text;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Api.Replication.Tests.PeerStatus;

/// <summary>
/// Unit tests for <see cref="ReplicationPeerStatusContinuation"/>: lossless round
/// trips and fail-closed rejection of every malformed token shape, including a
/// token of the retired version 1 format.
/// </summary>
[TestFixture]
public sealed class ReplicationPeerStatusContinuationTests
{
    private static string Token(string payload) =>
        "2." + Base64Url.EncodeToString(Encoding.UTF8.GetBytes(payload));

    [TestCase("orders", "east", ReplicationContactDirection.Outbound)]
    [TestCase("t/acme/a/crm/contacts", "west", ReplicationContactDirection.Inbound)]
    [TestCase("t/other/a:b:c", "peer:with:colons", ReplicationContactDirection.Inbound)]
    [TestCase("\u00e9t\u00e9", "12", ReplicationContactDirection.Outbound)]
    public void Encode_then_Decode_round_trips(string tree, string peer, ReplicationContactDirection direction)
    {
        var cursor = new ReplicationPeerStatusCursor(tree, peer, direction);

        var decoded = ReplicationPeerStatusContinuation.Decode(ReplicationPeerStatusContinuation.Encode(cursor));

        Assert.That(decoded, Is.EqualTo(cursor));
    }

    [Test]
    public void Encode_produces_a_url_safe_opaque_token()
    {
        var token = ReplicationPeerStatusContinuation.Encode(
            new ReplicationPeerStatusCursor("t/acme/a/crm/contacts", "east", ReplicationContactDirection.Outbound));

        Assert.Multiple(() =>
        {
            Assert.That(token, Does.StartWith("2."));
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
    [TestCase("3.AAAA")]
    [TestCase("2.***")]
    public void Decode_rejects_a_token_that_is_not_a_versioned_base64url_payload(string token)
    {
        Assert.That(() => ReplicationPeerStatusContinuation.Decode(token), Throws.ArgumentException);
    }

    [Test]
    public void Decode_rejects_a_version_1_token()
    {
        // Version 1 carried a trailing tenant-rendering flag; its tokens are refused, never misread.
        var legacy = "1." + Base64Url.EncodeToString(Encoding.UTF8.GetBytes("6:orders4:east01"));

        Assert.That(() => ReplicationPeerStatusContinuation.Decode(legacy), Throws.ArgumentException);
    }

    [TestCase("")]
    [TestCase("6:orders")]
    [TestCase("6:orders4:east")]
    [TestCase("6:orders4:east00")]
    [TestCase("6:orders4:east2")]
    [TestCase("0:4:east0")]
    [TestCase("6:orders0:0")]
    [TestCase("99:orders4:east0")]
    [TestCase("-1:x4:east0")]
    [TestCase("x:orders4:east0")]
    [TestCase(":orders4:east0")]
    public void Decode_rejects_a_malformed_payload(string payload)
    {
        Assert.That(() => ReplicationPeerStatusContinuation.Decode(Token(payload)), Throws.ArgumentException);
    }

    [Test]
    public void Decode_accepts_a_well_formed_version_2_payload()
    {
        Assert.That(
            ReplicationPeerStatusContinuation.Decode(Token("6:orders4:east1")),
            Is.EqualTo(new ReplicationPeerStatusCursor("orders", "east", ReplicationContactDirection.Inbound)));
    }

    [Test]
    public void Decode_rejects_an_oversized_token_before_decoding_it()
    {
        var token = "2." + new string('A', ReplicationPeerStatusContinuation.MaxTokenLength);

        Assert.That(() => ReplicationPeerStatusContinuation.Decode(token), Throws.ArgumentException);
    }

    [Test]
    public void Decode_rejection_names_the_query_parameter()
    {
        var ex = Assert.Throws<ArgumentException>(() => ReplicationPeerStatusContinuation.Decode("garbage"));

        Assert.That(ex!.ParamName, Is.EqualTo("query"));
    }
}
