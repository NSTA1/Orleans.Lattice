using System.IO.Hashing;
using System.Buffers;
using System.Buffers.Binary;
using System.Text;

using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// The projection digest is a cross-silo drift fingerprint: two silos that
/// applied the same WAL prefix must produce byte-identical digests. Every
/// string field a contribution folds in goes through
/// <c>BPlusLeafGrain.FeedString</c>, so any change to how that method
/// transcodes has to emit exactly the bytes the previous shape emitted, or
/// every persisted <c>ProjectionHash</c> in the estate silently stops
/// comparing against a peer running the other build.
/// <para>
/// <c>FeedString</c> now transcodes once when the worst-case UTF-8 encoding
/// fits its stack budget, using the written length as the length prefix
/// instead of a separate <c>GetByteCount</c> pass, and keeps the two-pass
/// shape above that budget. These fixtures assert the emitted bytes against a
/// verbatim reimplementation of the two-pass form, over the corpus that
/// distinguishes the two arms: the empty string, ASCII, multi-byte and
/// surrogate-pair text, and lengths either side of both the stack budget and
/// the rent threshold.
/// </para>
/// </summary>
[TestFixture]
public sealed class BPlusLeafGrainDigestFeedStringTests
{
    private const int ScratchBytes = 256;

    private static IEnumerable<string> Corpus()
    {
        yield return string.Empty;
        yield return "a";
        yield return "tenant/orders/000123";
        yield return "\u00e9v\u00e9nement";
        yield return "\u65e5\u672c\u8a9e";
        yield return "\U0001F600";
        yield return "mixed/\u00e9/\u65e5/\U0001F600/end";

        // Either side of the point where the worst case (3n + 3) stops
        // fitting the stack budget, and either side of the rent threshold.
        foreach (var length in new[] { 83, 84, 85, 86, 255, 256, 257, 1024 })
        {
            yield return new string('k', length);
            yield return new string('\u00e9', length);
            yield return new string('\u65e5', length);
            yield return string.Concat(Enumerable.Repeat("\U0001F600", length));
        }
    }

    /// <summary>
    /// Verbatim copy of the two-pass body <c>FeedString</c> replaced. It is
    /// the wire format, so it is reproduced here rather than referenced.
    /// </summary>
    private static void TwoPassFeedString(XxHash128 hasher, string value, Span<byte> scratch)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32LittleEndian(scratch[..4], byteCount);
        hasher.Append(scratch[..4]);
        if (byteCount == 0) return;

        if (byteCount <= ScratchBytes)
        {
            Span<byte> buf = stackalloc byte[ScratchBytes];
            var written = Encoding.UTF8.GetBytes(value, buf);
            hasher.Append(buf[..written]);
        }
        else
        {
            var rented = ArrayPool<byte>.Shared.Rent(byteCount);
            try
            {
                var written = Encoding.UTF8.GetBytes(value, rented);
                hasher.Append(rented.AsSpan(0, written));
            }
            finally
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    [Test]
    public void FeedString_emits_the_same_bytes_as_the_two_pass_form()
    {
        Span<byte> scratch = stackalloc byte[16];
        foreach (var value in Corpus())
        {
            var expectedHasher = new XxHash128();
            TwoPassFeedString(expectedHasher, value, scratch);
            var expected = expectedHasher.GetCurrentHash();

            var actualHasher = new XxHash128();
            BPlusLeafGrain.FeedString(actualHasher, value, scratch);
            var actual = actualHasher.GetCurrentHash();

            Assert.That(
                actual,
                Is.EqualTo(expected),
                $"digest diverged for a {value.Length}-char value "
                + $"({Encoding.UTF8.GetByteCount(value)} UTF-8 bytes)");
        }
    }

    [Test]
    public void FeedString_prefixes_the_utf8_byte_count_not_the_char_count()
    {
        // The prefix is now taken from the transcode's written count rather
        // than from a separate GetByteCount pass. For any non-ASCII value the
        // two numbers differ from the char count, so a prefix accidentally
        // sourced from value.Length would be caught here.
        Span<byte> scratch = stackalloc byte[16];
        Span<byte> prefix = stackalloc byte[4];
        foreach (var value in new[] { "\u00e9", "\u65e5", "\U0001F600", "a\u00e9b" })
        {
            var hasher = new XxHash128();
            BPlusLeafGrain.FeedString(hasher, value, scratch);

            var expectedHasher = new XxHash128();
            var byteCount = Encoding.UTF8.GetByteCount(value);
            BinaryPrimitives.WriteInt32LittleEndian(prefix, byteCount);
            expectedHasher.Append(prefix);
            expectedHasher.Append(Encoding.UTF8.GetBytes(value));

            Assert.That(hasher.GetCurrentHash(), Is.EqualTo(expectedHasher.GetCurrentHash()), value);
        }
    }

    [Test]
    public void FeedString_distinguishes_values_that_share_a_prefix()
    {
        // Length prefixing is what stops "ab" + "c" hashing the same as
        // "a" + "bc". The single-pass rewrite must not have dropped it.
        Span<byte> scratch = stackalloc byte[16];

        var left = new XxHash128();
        BPlusLeafGrain.FeedString(left, "ab", scratch);
        BPlusLeafGrain.FeedString(left, "c", scratch);

        var right = new XxHash128();
        BPlusLeafGrain.FeedString(right, "a", scratch);
        BPlusLeafGrain.FeedString(right, "bc", scratch);

        Assert.That(left.GetCurrentHash(), Is.Not.EqualTo(right.GetCurrentHash()));
    }

    [Test]
    public void FeedString_emits_a_zero_prefix_and_nothing_else_for_the_empty_string()
    {
        Span<byte> scratch = stackalloc byte[16];
        var actual = new XxHash128();
        BPlusLeafGrain.FeedString(actual, string.Empty, scratch);

        var expected = new XxHash128();
        Span<byte> zero = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(zero, 0);
        expected.Append(zero);

        Assert.That(actual.GetCurrentHash(), Is.EqualTo(expected.GetCurrentHash()));
    }
}
