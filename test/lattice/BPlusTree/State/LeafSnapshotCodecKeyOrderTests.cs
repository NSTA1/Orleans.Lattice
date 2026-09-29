using System.Buffers;
using System.Text;

using Orleans.Lattice.BPlusTree.State;

namespace Orleans.Lattice.Tests.BPlusTree.State;

/// <summary>
/// Differential coverage for <c>LeafSnapshotCodec.CompareKeysUtf8</c>, the
/// comparator every leaf seek and every ascending-order check runs on.
/// <para>
/// The comparator answers most comparisons from the first byte at which the
/// two keys differ, and only falls through to a rune walk when that byte is
/// non-ASCII. That shortcut is only safe if it is indistinguishable from the
/// walk, so these tests pin it against a reference implementation of the walk
/// rather than against hand-picked expectations: a randomised corpus of ASCII,
/// multibyte, prefix-related and deliberately malformed keys must agree in
/// sign on every pair, and the comparator must remain a total order.
/// </para>
/// <para>
/// The reference below is a copy of the walk, not a call to it. A test that
/// called the production comparator to check the production comparator would
/// agree with itself no matter what either did.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafSnapshotCodecKeyOrderTests
{
    private const int Seed = 20260929;

    /// <summary>
    /// The pre-trim comparator: walk both keys rune by rune, ranking each code
    /// point into UTF-16 ordinal order, and fall back to a raw byte compare
    /// the moment either side fails to decode.
    /// </summary>
    private static int ReferenceCompare(ReadOnlySpan<byte> left, ReadOnlySpan<byte> right)
    {
        var leftRemaining = left;
        var rightRemaining = right;
        while (!leftRemaining.IsEmpty && !rightRemaining.IsEmpty)
        {
            if (Rune.DecodeFromUtf8(leftRemaining, out var leftRune, out var leftConsumed) != OperationStatus.Done
                || Rune.DecodeFromUtf8(rightRemaining, out var rightRune, out var rightConsumed) != OperationStatus.Done)
            {
                return leftRemaining.SequenceCompareTo(rightRemaining);
            }

            var cmp = ReferenceRank(leftRune.Value).CompareTo(ReferenceRank(rightRune.Value));
            if (cmp != 0)
            {
                return cmp;
            }

            leftRemaining = leftRemaining[leftConsumed..];
            rightRemaining = rightRemaining[rightConsumed..];
        }

        return leftRemaining.IsEmpty ? (rightRemaining.IsEmpty ? 0 : -1) : 1;
    }

    private static long ReferenceRank(int codePoint)
        => codePoint >= 0x10000
            ? (0xD800L << 16) + (codePoint - 0x10000)
            : (long)codePoint << 16;

    private static void AssertAgrees(byte[] left, byte[] right, string what)
    {
        var expected = Math.Sign(ReferenceCompare(left, right));
        var actual = Math.Sign(LeafSnapshotCodec.CompareKeysUtf8(left, right));
        Assert.That(
            actual,
            Is.EqualTo(expected),
            $"{what}: [{Describe(left)}] vs [{Describe(right)}]");
    }

    private static string Describe(byte[] key) => Convert.ToHexString(key);

    [Test]
    public void Compare_agrees_with_the_rune_walk_on_ascii_keys_sharing_a_prefix()
    {
        // The realistic shape: a common prefix, then a divergence on an ASCII
        // byte. This is the case the trim answers without walking.
        var random = new Random(Seed);
        for (var i = 0; i < 2000; i++)
        {
            var left = Encoding.UTF8.GetBytes($"tenant/shard/key-{random.Next(0, 64):D4}");
            var right = Encoding.UTF8.GetBytes($"tenant/shard/key-{random.Next(0, 64):D4}");
            AssertAgrees(left, right, "ascii shared prefix");
        }
    }

    [Test]
    public void Compare_agrees_with_the_rune_walk_on_random_unicode_keys()
    {
        var random = new Random(Seed + 1);
        for (var i = 0; i < 2000; i++)
        {
            var left = Encoding.UTF8.GetBytes(RandomUnicode(random));
            var right = Encoding.UTF8.GetBytes(RandomUnicode(random));
            AssertAgrees(left, right, "random unicode");
        }
    }

    [Test]
    public void Compare_agrees_with_the_rune_walk_on_malformed_utf8()
    {
        // Raw random bytes are mostly not well-formed UTF-8. The comparator
        // must stay a total order over them rather than throwing, and must
        // still land on the same answer the walk's byte-compare fallback does.
        var random = new Random(Seed + 2);
        for (var i = 0; i < 4000; i++)
        {
            var left = new byte[random.Next(0, 12)];
            var right = new byte[random.Next(0, 12)];
            random.NextBytes(left);
            random.NextBytes(right);
            AssertAgrees(left, right, "random bytes");
        }
    }

    [Test]
    public void Compare_agrees_with_the_rune_walk_when_one_key_is_a_prefix_of_the_other()
    {
        var random = new Random(Seed + 3);
        for (var i = 0; i < 1000; i++)
        {
            var full = Encoding.UTF8.GetBytes(RandomUnicode(random));
            if (full.Length == 0)
            {
                continue;
            }

            // Truncate anywhere, including mid-sequence, so the shared prefix
            // can end inside a multi-byte code point.
            var prefix = full[..random.Next(0, full.Length)];
            AssertAgrees(prefix, full, "prefix vs full");
            AssertAgrees(full, prefix, "full vs prefix");
        }
    }

    [Test]
    public void Compare_orders_supplementary_code_points_below_the_private_use_area()
    {
        // The whole reason the walk exists: ordinal string comparison orders
        // UTF-16 code units, so a surrogate pair sorts below U+E000..U+FFFF
        // even though its UTF-8 bytes sort above them. A raw byte compare
        // would get this backwards.
        var supplementary = Encoding.UTF8.GetBytes("key-\U0001F600");
        var privateUse = Encoding.UTF8.GetBytes("key-\uF8FF");

        Assert.Multiple(() =>
        {
            Assert.That(LeafSnapshotCodec.CompareKeysUtf8(supplementary, privateUse), Is.Negative);
            Assert.That(LeafSnapshotCodec.CompareKeysUtf8(privateUse, supplementary), Is.Positive);
            Assert.That(
                Math.Sign(LeafSnapshotCodec.CompareKeysUtf8(supplementary, privateUse)),
                Is.EqualTo(Math.Sign(string.CompareOrdinal("key-\U0001F600", "key-\uF8FF"))));
        });
    }

    [Test]
    public void Compare_matches_ordinal_string_order_for_well_formed_keys()
    {
        var random = new Random(Seed + 4);
        for (var i = 0; i < 2000; i++)
        {
            var leftText = RandomUnicode(random);
            var rightText = RandomUnicode(random);
            var actual = Math.Sign(LeafSnapshotCodec.CompareKeysUtf8(
                Encoding.UTF8.GetBytes(leftText), Encoding.UTF8.GetBytes(rightText)));

            Assert.That(
                actual,
                Is.EqualTo(Math.Sign(string.CompareOrdinal(leftText, rightText))),
                $"'{leftText}' vs '{rightText}'");
        }
    }

    [Test]
    public void Compare_is_antisymmetric_and_reflexive()
    {
        var random = new Random(Seed + 5);
        for (var i = 0; i < 2000; i++)
        {
            var left = new byte[random.Next(0, 16)];
            var right = new byte[random.Next(0, 16)];
            random.NextBytes(left);
            random.NextBytes(right);

            var forward = Math.Sign(LeafSnapshotCodec.CompareKeysUtf8(left, right));
            var backward = Math.Sign(LeafSnapshotCodec.CompareKeysUtf8(right, left));

            Assert.Multiple(() =>
            {
                Assert.That(backward, Is.EqualTo(-forward), $"[{Describe(left)}] vs [{Describe(right)}]");
                Assert.That(LeafSnapshotCodec.CompareKeysUtf8(left, left), Is.Zero);
                Assert.That(LeafSnapshotCodec.CompareKeysUtf8(right, right), Is.Zero);
            });
        }
    }

    [Test]
    public void Compare_treats_the_empty_key_as_the_lowest()
    {
        var empty = Array.Empty<byte>();
        Assert.Multiple(() =>
        {
            Assert.That(LeafSnapshotCodec.CompareKeysUtf8(empty, empty), Is.Zero);
            Assert.That(LeafSnapshotCodec.CompareKeysUtf8(empty, "a"u8.ToArray()), Is.Negative);
            Assert.That(LeafSnapshotCodec.CompareKeysUtf8("a"u8.ToArray(), empty), Is.Positive);
            Assert.That(LeafSnapshotCodec.CompareKeysUtf8(empty, new byte[] { 0x80 }), Is.Negative);
        });
    }

    /// <summary>
    /// A short string drawn from a deliberately awkward alphabet: ASCII either
    /// side of the digits, a 2-byte, a 3-byte, a private-use and a
    /// supplementary code point, so divergences land on every sequence width.
    /// </summary>
    private static string RandomUnicode(Random random)
    {
        const string Alphabet = "ab01/-\u00e9\u0442\u6f22\u5b57\uF8FF";
        var builder = new StringBuilder();
        var length = random.Next(0, 10);
        for (var i = 0; i < length; i++)
        {
            if (random.Next(6) == 0)
            {
                builder.Append("\U0001F600");
                continue;
            }

            builder.Append(Alphabet[random.Next(Alphabet.Length)]);
        }

        return builder.ToString();
    }
}
