using System.Text;

using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Tests.BPlusTree.Grains;

/// <summary>
/// Pins the frame-seek paths (<c>TryLocate</c> and <c>LowerBound</c>) across
/// the boundary at which a key's UTF-8 form stops fitting the stack budget and
/// the seek rents instead.
/// <para>
/// Both sites compare the <c>StackKeyBytes</c> budget against
/// <c>GetMaxByteCount</c>, which is <c>3 * charCount + 3</c>: an 84-character
/// key stays on the stack at 255 bytes and an 85-character one rents at 258,
/// even though both encode to well under 256 bytes of ASCII. The seam is
/// therefore in character count and not in encoded length, which is easy to
/// get wrong. These fixtures walk keys either side of it - and multi-byte keys
/// whose written length and worst case diverge sharply - so a mis-sized or
/// mis-sliced buffer surfaces as a wrong answer rather than as a silent
/// out-of-bounds write.
/// </para>
/// </summary>
[TestFixture]
public sealed class LeafEntryCacheScratchBoundaryTests
{
    /// <summary>Character counts either side of the stack/rent seam at 84/85 chars.</summary>
    private static readonly int[] KeyLengths = [1, 8, 40, 83, 84, 85, 86, 120, 255, 256, 400];

    private static LeafEntryCache NewCache()
        => new(new SortedDictionary<string, LwwValue<byte[]>>(StringComparer.Ordinal));

    private static LwwValue<byte[]> Value(int i) => new()
    {
        Value = [(byte)(i & 0xFF), (byte)((i >> 8) & 0xFF)],
        Timestamp = new HybridLogicalClock { WallClockTicks = 500L + i, Counter = i },
    };

    /// <summary>
    /// Builds a sorted corpus whose keys span the stack/rent seam in both
    /// character count and encoded byte count. <paramref name="filler"/> lets a
    /// caller choose an encoding width: one ASCII byte, two bytes, or three.
    /// </summary>
    private static (LeafSnapshotRow[] Rows, string[] Keys) Corpus(char filler)
    {
        var keys = new List<string>();
        for (var i = 0; i < KeyLengths.Length; i++)
        {
            var length = KeyLengths[i];
            // Four keys per length, so the seek has to discriminate between
            // neighbours that share a long common prefix.
            for (var n = 0; n < 4; n++)
            {
                var prefix = $"{i:D2}-{n:D2}-";
                keys.Add(prefix + new string(filler, Math.Max(1, length - prefix.Length)));
            }
        }

        var ordered = keys.Distinct(StringComparer.Ordinal).ToArray();
        Array.Sort(ordered, StringComparer.Ordinal);

        var rows = new LeafSnapshotRow[ordered.Length];
        for (var i = 0; i < ordered.Length; i++)
        {
            rows[i] = new LeafSnapshotRow(ordered[i], Value(i));
        }

        return (rows, ordered);
    }

    private static LeafEntryCache Attached(LeafSnapshotRow[] rows)
    {
        var cache = NewCache();
        Assert.That(cache.TryAttachSnapshot(LeafSnapshotCodec.Encode(rows), 0L), Is.True);
        return cache;
    }

    /// <summary>
    /// Every key in the frame is found, with its own value, whichever side of
    /// the stack/rent seam its worst-case encoding falls on. A stack slice cut
    /// short would truncate the probe and miss.
    /// </summary>
    [TestCase('a', TestName = "TryLocate_finds_every_key_across_the_scratch_seam(ascii)")]
    [TestCase('\u00e9', TestName = "TryLocate_finds_every_key_across_the_scratch_seam(two_byte)")]
    [TestCase('\u65e5', TestName = "TryLocate_finds_every_key_across_the_scratch_seam(three_byte)")]
    public void TryLocate_finds_every_key_across_the_scratch_seam(char filler)
    {
        var (rows, keys) = Corpus(filler);
        var cache = Attached(rows);

        Assert.Multiple(() =>
        {
            for (var i = 0; i < keys.Length; i++)
            {
                Assert.That(cache.TryGetRow(keys[i], out var row), Is.True, $"missed '{keys[i]}'");
                Assert.That(row.Value, Is.EqualTo(Value(i).Value).AsCollection, $"wrong row for '{keys[i]}'");
            }
        });
    }

    /// <summary>
    /// A key the frame does not carry misses, including probes that share a
    /// long prefix with a present key and probes longer than the stack budget.
    /// A slice sized to the wrong length could otherwise compare equal on a
    /// truncated prefix and report a false hit.
    /// </summary>
    [Test]
    public void TryLocate_misses_absent_keys_across_the_scratch_seam()
    {
        var (rows, keys) = Corpus('a');
        var cache = Attached(rows);

        string[] absent =
        [
            string.Empty,
            "\u0001",
            "zzzzzzzz",
            keys[0] + "x",
            keys[^1] + "x",
            keys[keys.Length / 2][..^1],
            new string('a', 300),
            new string('\u65e5', 300),
        ];

        Assert.Multiple(() =>
        {
            foreach (var probe in absent)
            {
                Assert.That(cache.TryGetRow(probe, out _), Is.False, $"unexpected hit for a {probe.Length}-char probe");
            }
        });
    }

    /// <summary>
    /// A bounded range over the frame yields exactly the half-open window the
    /// bounds describe, with bounds either side of the seam. This drives
    /// <c>LowerBound</c> rather than <c>TryLocate</c>, which sizes its scratch
    /// the same way.
    /// </summary>
    [Test]
    public void LowerBound_resolves_bounds_across_the_scratch_seam()
    {
        var (rows, keys) = Corpus('a');

        (int Start, int End)[] windows =
        [
            (0, keys.Length),
            (0, 1),
            (1, 5),
            (keys.Length / 3, 2 * keys.Length / 3),
            (keys.Length - 2, keys.Length),
            (5, 5),
        ];

        Assert.Multiple(() =>
        {
            foreach (var (start, end) in windows)
            {
                var cache = Attached(rows);
                var startKey = keys[start];
                var endKey = end < keys.Length ? keys[end] : null;

                var observed = new List<string>();
                foreach (var row in cache.EnumerateRange(startKey, endKey))
                {
                    observed.Add(row.Key);
                }

                var expected = keys
                    .Where(k => string.CompareOrdinal(k, startKey) >= 0
                        && (endKey is null || string.CompareOrdinal(k, endKey) < 0))
                    .ToArray();

                Assert.That(observed, Is.EqualTo(expected).AsCollection, $"window [{start}, {end})");
            }
        });
    }

    /// <summary>
    /// The seam is where it is claimed to be: <c>GetMaxByteCount</c> crosses the
    /// 256-byte budget between 84 and 85 characters, which is what makes the
    /// corpus above straddle both branches. A change to the budget that silently
    /// moved the branch would leave these fixtures exercising only one arm.
    /// </summary>    [Test]
    public void The_scratch_seam_sits_between_eighty_four_and_eighty_five_characters()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Encoding.UTF8.GetMaxByteCount(84), Is.LessThanOrEqualTo(256));
            Assert.That(Encoding.UTF8.GetMaxByteCount(85), Is.GreaterThan(256));
        });
    }
}
