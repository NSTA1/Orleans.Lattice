namespace Orleans.Lattice.GrainIndex.Tests;

/// <summary>
/// The load-bearing property of the key encoder: for every order-preserving
/// type, ordinal comparison of two encoded value components must agree with the
/// natural order of the values themselves. Each fixture below shuffles a value
/// set that deliberately straddles the traps - sign boundaries, negative zero,
/// NaN, minimum and maximum, empty and prefix strings - sorts it by the
/// encoding, and asserts the result matches the natural sort. That is stronger
/// than a handful of hand-picked pairs, because a sign-bit or escaping mistake
/// shows up as a mis-ordered pair anywhere in the set.
/// </summary>
[TestFixture]
public class GrainIndexKeyEncoderOrderingTests
{
    /// <summary>
    /// Sorts <paramref name="values"/> by their encoded value component and
    /// asserts the result matches sorting them with <paramref name="comparer"/>.
    /// The input is shuffled with a fixed seed first, so the assertion cannot
    /// pass by accident of input order and cannot vary between runs.
    /// </summary>
    private static void AssertEncodingPreservesOrder<T>(IReadOnlyList<T> values, IComparer<T> comparer)
    {
        var shuffled = Shuffle(values);

        var byEncoding = shuffled.ToArray();
        Array.Sort(
            byEncoding.Select(GrainIndexKeyEncoder.EncodeValue).ToArray(),
            byEncoding,
            StringComparer.Ordinal);

        var byNature = shuffled.ToArray();
        Array.Sort(byNature, comparer);

        Assert.That(byNature.Length, Is.GreaterThan(1), "an ordering test needs at least two values");

        for (var i = 0; i < byNature.Length; i++)
        {
            Assert.That(
                comparer.Compare(byEncoding[i], byNature[i]),
                Is.Zero,
                $"position {i}: encoded order put {Describe(byEncoding[i])} where natural order has {Describe(byNature[i])}");
        }
    }

    private static string Describe<T>(T value) => value?.ToString() ?? "<null>";

    private static T[] Shuffle<T>(IReadOnlyList<T> values)
    {
        var array = values.ToArray();
        var random = new Random(20260831);
        for (var i = array.Length - 1; i > 0; i--)
        {
            int j = random.Next(i + 1);
            (array[i], array[j]) = (array[j], array[i]);
        }

        return array;
    }

    [Test]
    public void Encoding_preserves_order_for_int_across_the_sign_boundary()
    {
        int[] values =
        [
            int.MinValue, int.MinValue + 1, -1_000_000, -256, -2, -1, 0, 1, 2, 255, 256,
            1_000_000, int.MaxValue - 1, int.MaxValue,
        ];

        AssertEncodingPreservesOrder(values, Comparer<int>.Default);
    }

    [Test]
    public void Encoding_preserves_order_for_long_across_the_sign_boundary()
    {
        long[] values =
        [
            long.MinValue, long.MinValue + 1, int.MinValue, -1, 0, 1, int.MaxValue,
            long.MaxValue - 1, long.MaxValue,
        ];

        AssertEncodingPreservesOrder(values, Comparer<long>.Default);
    }

    [Test]
    public void Encoding_preserves_order_for_short_and_sbyte()
    {
        AssertEncodingPreservesOrder<short>(
            [short.MinValue, -1, 0, 1, short.MaxValue],
            Comparer<short>.Default);
        AssertEncodingPreservesOrder<sbyte>(
            [sbyte.MinValue, -1, 0, 1, sbyte.MaxValue],
            Comparer<sbyte>.Default);
    }

    [Test]
    public void Encoding_preserves_order_for_the_unsigned_integral_types()
    {
        AssertEncodingPreservesOrder<byte>([0, 1, 127, 128, 255], Comparer<byte>.Default);
        AssertEncodingPreservesOrder<ushort>([0, 1, 32_767, 32_768, ushort.MaxValue], Comparer<ushort>.Default);
        AssertEncodingPreservesOrder<uint>([0, 1, int.MaxValue, 2_147_483_648, uint.MaxValue], Comparer<uint>.Default);
        AssertEncodingPreservesOrder<ulong>(
            [0, 1, long.MaxValue, 9_223_372_036_854_775_808, ulong.MaxValue],
            Comparer<ulong>.Default);
    }

    [Test]
    public void Encoding_preserves_order_for_char()
    {
        AssertEncodingPreservesOrder(
            ['\u0000', '\u0001', ' ', '0', '9', 'A', 'Z', 'a', 'z', '\u00ff', '\uffff'],
            Comparer<char>.Default);
    }

    [Test]
    public void Encoding_preserves_order_for_double_including_negative_zero_and_nan()
    {
        double[] values =
        [
            double.NaN, double.NegativeInfinity, double.MinValue, -1e300, -1.5, -1.0,
            -double.Epsilon, -0.0, 0.0, double.Epsilon, 1.0, 1.5, 1e300, double.MaxValue,
            double.PositiveInfinity,
        ];

        AssertEncodingPreservesOrder(values, Comparer<double>.Default);
    }

    [Test]
    public void Encoding_preserves_order_for_float_including_negative_zero_and_nan()
    {
        float[] values =
        [
            float.NaN, float.NegativeInfinity, float.MinValue, -1.5f, -float.Epsilon, -0.0f,
            0.0f, float.Epsilon, 1.5f, float.MaxValue, float.PositiveInfinity,
        ];

        AssertEncodingPreservesOrder(values, Comparer<float>.Default);
    }

    [Test]
    public void Encoding_collapses_every_nan_payload_to_one_value_component()
    {
        double quiet = double.NaN;
        double signalling = BitConverter.Int64BitsToDouble(unchecked((long)0xFFF8_0000_0000_0001UL));

        Assert.That(double.IsNaN(signalling), Is.True);
        Assert.That(
            GrainIndexKeyEncoder.EncodeValue(signalling),
            Is.EqualTo(GrainIndexKeyEncoder.EncodeValue(quiet)),
            "two NaN values compare equal, so they must share one key slot");
    }

    [Test]
    public void Encoding_preserves_order_for_datetime()
    {
        DateTime[] values =
        [
            DateTime.MinValue,
            new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc),
            new(2000, 2, 29, 12, 0, 0, DateTimeKind.Utc),
            new(2026, 8, 31, 9, 45, 57, DateTimeKind.Utc),
            DateTime.MaxValue,
        ];

        AssertEncodingPreservesOrder(values, Comparer<DateTime>.Default);
    }

    [Test]
    public void Encoding_preserves_order_for_datetimeoffset_across_offsets()
    {
        // The same instant written three ways, plus neighbours either side, so a
        // naive "encode the local ticks" mistake mis-orders the middle three.
        DateTimeOffset[] values =
        [
            DateTimeOffset.MinValue,
            new(2026, 8, 31, 0, 0, 0, TimeSpan.Zero),
            new(2026, 8, 31, 9, 0, 0, TimeSpan.FromHours(9)),
            new(2026, 8, 30, 19, 0, 0, TimeSpan.FromHours(-5)),
            new(2026, 8, 31, 10, 0, 0, TimeSpan.Zero),
            DateTimeOffset.MaxValue,
        ];

        AssertEncodingPreservesOrder(values, Comparer<DateTimeOffset>.Default);
    }

    [Test]
    public void Encoding_preserves_ordinal_order_for_string_including_the_reserved_code_units()
    {
        string[] values =
        [
            string.Empty, "\u0000", "\u0000\u0000", "\u0001", "\u0002", "A", "A\u0000", "A\u0001",
            "AB", "AZ", "Ab", "a", "ab", "z", "\u00e9", "\uffff",
        ];

        AssertEncodingPreservesOrder(values, StringComparer.Ordinal);
    }

    [Test]
    public void Encoding_preserves_order_for_bool()
    {
        AssertEncodingPreservesOrder([false, true], Comparer<bool>.Default);
    }

    [Test]
    public void Encoding_puts_null_below_every_present_value_for_a_nullable()
    {
        int?[] values = [null, int.MinValue, -1, 0, 1, int.MaxValue];
        AssertEncodingPreservesOrder(values, Comparer<int?>.Default);
    }

    [Test]
    public void Encoding_puts_null_below_the_empty_string()
    {
        AssertEncodingPreservesOrder<string?>(
            [null, string.Empty, "\u0000", "a"],
            Comparer<string?>.Create(static (left, right) =>
                left is null ? (right is null ? 0 : -1) : right is null ? 1 : string.CompareOrdinal(left, right)));
    }

    [Test]
    public void Encoding_puts_null_below_every_present_value_for_a_nullable_datetimeoffset()
    {
        DateTimeOffset?[] values =
        [
            null,
            DateTimeOffset.MinValue,
            new DateTimeOffset(2026, 8, 31, 0, 0, 0, TimeSpan.Zero),
            DateTimeOffset.MaxValue,
        ];

        AssertEncodingPreservesOrder(values, Comparer<DateTimeOffset?>.Default);
    }

    [Test]
    public void Encoding_preserves_order_for_every_nullable_ordered_type()
    {
        AssertEncodingPreservesOrder<long?>([null, long.MinValue, 0, long.MaxValue], Comparer<long?>.Default);
        AssertEncodingPreservesOrder<short?>([null, short.MinValue, 0, short.MaxValue], Comparer<short?>.Default);
        AssertEncodingPreservesOrder<sbyte?>([null, sbyte.MinValue, 0, sbyte.MaxValue], Comparer<sbyte?>.Default);
        AssertEncodingPreservesOrder<byte?>([null, 0, 255], Comparer<byte?>.Default);
        AssertEncodingPreservesOrder<ushort?>([null, 0, ushort.MaxValue], Comparer<ushort?>.Default);
        AssertEncodingPreservesOrder<uint?>([null, 0, uint.MaxValue], Comparer<uint?>.Default);
        AssertEncodingPreservesOrder<ulong?>([null, 0, ulong.MaxValue], Comparer<ulong?>.Default);
        AssertEncodingPreservesOrder<char?>([null, '\u0000', 'a', '\uffff'], Comparer<char?>.Default);
        AssertEncodingPreservesOrder<bool?>([null, false, true], Comparer<bool?>.Default);
        AssertEncodingPreservesOrder<double?>([null, double.NaN, -1.0, 0.0, 1.0], Comparer<double?>.Default);
        AssertEncodingPreservesOrder<float?>([null, float.NaN, -1.0f, 0.0f, 1.0f], Comparer<float?>.Default);
        AssertEncodingPreservesOrder<DateTime?>(
            [null, DateTime.MinValue, DateTime.MaxValue],
            Comparer<DateTime?>.Default);
    }

    [Test]
    public void Encoded_keys_for_one_property_sort_by_value_then_grain_key()
    {
        string[] keys =
        [
            GrainIndexKeyEncoder.EncodeKey("Age", 30, "carol"),
            GrainIndexKeyEncoder.EncodeKey("Age", -1, "dave"),
            GrainIndexKeyEncoder.EncodeKey("Age", 17, "bob"),
            GrainIndexKeyEncoder.EncodeKey("Age", 17, "alice"),
        ];

        Array.Sort(keys, StringComparer.Ordinal);

        Assert.That(
            keys,
            Is.EqualTo(new[]
            {
                GrainIndexKeyEncoder.EncodeKey("Age", -1, "dave"),
                GrainIndexKeyEncoder.EncodeKey("Age", 17, "alice"),
                GrainIndexKeyEncoder.EncodeKey("Age", 17, "bob"),
                GrainIndexKeyEncoder.EncodeKey("Age", 30, "carol"),
            }));
    }
}
