using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Covers <see cref="OrphanedLeafPassCursor"/>, the opaque resume token that
/// makes a bounded orphaned-leaf pass drivable to completion (issue 3302).
/// <para>
/// The token is the whole mechanism by which an operator finishes a repair the
/// response deadline would otherwise truncate, so the two things it must never
/// do are lose a key and silently restart. A key that did not survive the round
/// trip byte-for-byte would resume the walk in the wrong place and skip every
/// leaf in between - a silent partial repair reported as a complete one - and a
/// malformed token coerced to "start" would re-walk work already done while the
/// operator believed the pass was continuing.
/// </para>
/// </summary>
[TestFixture]
public sealed class OrphanedLeafPassCursorTests
{
    /// <summary>
    /// A resume key is an arbitrary caller-supplied string, so it can contain
    /// the token's own delimiter, non-ASCII text, or nothing at all. Every one
    /// of these must come back exactly as it went in - which is what the base64
    /// key segment buys and a naive inlined key would not.
    /// </summary>
    [TestCase(0, "")]
    [TestCase(0, "a")]
    [TestCase(7, "orders/2026-09-20")]
    [TestCase(31, "olp1:9:garbage")]
    [TestCase(63, "key:with:many:colons")]
    [TestCase(1, "unicode-\u00e9\u00fc-\u4e2d\u6587")]
    [TestCase(int.MaxValue, "\u0000\u0001 embedded control")]
    public void Encode_then_Decode_round_trips_the_position_exactly(int shardIndex, string key)
    {
        var decoded = OrphanedLeafPassCursor.Decode(OrphanedLeafPassCursor.Encode(shardIndex, key));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ShardIndex, Is.EqualTo(shardIndex));
            Assert.That(
                decoded.ResumeFromInclusive,
                Is.EqualTo(key),
                "a key that does not survive the round trip resumes the walk in the wrong place");
        });
    }

    /// <summary>
    /// A null shard cursor means "start this shard at its chain head", which is
    /// how a between-shard yield is expressed. It must not collapse into the
    /// empty string, because the shard-root walk treats those differently.
    /// </summary>
    [Test]
    public void A_null_shard_cursor_round_trips_as_null_not_as_empty()
    {
        var decoded = OrphanedLeafPassCursor.Decode(OrphanedLeafPassCursor.Encode(12, null));

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ShardIndex, Is.EqualTo(12));
            Assert.That(decoded.ResumeFromInclusive, Is.Null);
        });
    }

    /// <summary>
    /// The empty string is a legitimate resume key and must not be confused
    /// with "no key". This is the one pair the encoding could plausibly conflate,
    /// so it is asserted against the null case above rather than alone.
    /// </summary>
    [Test]
    public void An_empty_resume_key_is_distinguishable_from_no_resume_key()
    {
        Assert.That(
            OrphanedLeafPassCursor.Encode(4, string.Empty),
            Is.Not.EqualTo(OrphanedLeafPassCursor.Encode(4, null)),
            "conflating an empty key with no key would restart a shard mid-pass");
    }

    /// <summary>
    /// A first call has no token, and both spellings of "no token" must mean
    /// the start of the pass rather than an error.
    /// </summary>
    [TestCase(null)]
    [TestCase("")]
    public void A_missing_token_starts_the_pass(string? token)
    {
        var decoded = OrphanedLeafPassCursor.Decode(token);

        Assert.Multiple(() =>
        {
            Assert.That(decoded.ShardIndex, Is.Zero);
            Assert.That(decoded.ResumeFromInclusive, Is.Null);
            Assert.That(decoded, Is.EqualTo(OrphanedLeafPassCursor.Start));
        });
    }

    /// <summary>
    /// Anything this surface did not produce is rejected, never coerced to the
    /// start of the pass. Coercion is the dangerous failure: the operator hands
    /// back a token they believe resumes the pass, the walk silently starts
    /// over, and its findings are read as the remainder of a tree that was in
    /// fact only re-examined from the beginning.
    /// </summary>
    [TestCase("nonsense")]
    [TestCase("olp0:0:")]
    [TestCase("OLP1:0:")]
    [TestCase("olp1:")]
    [TestCase("olp1:0")]
    [TestCase("olp1::")]
    [TestCase("olp1:-1:")]
    [TestCase("olp1:+1:")]
    [TestCase("olp1: 1:")]
    [TestCase("olp1:notanumber:")]
    [TestCase("olp1:99999999999999999999:")]
    [TestCase("olp1:0:not!base64")]
    [TestCase("olp1:0:knot!base64")]
    [TestCase("olp1:0:kYWJ")]
    public void A_token_this_version_did_not_produce_is_rejected(string token)
    {
        Assert.That(
            () => OrphanedLeafPassCursor.Decode(token),
            Throws.ArgumentException,
            "a token that cannot be read must fail loudly, never silently restart the pass");
    }
}
