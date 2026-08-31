using System.Runtime.CompilerServices;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// The on-tree key encoding for grain-index entries: the single place that
/// decides what an index entry's key looks like, and the only place a query
/// planner should go to build a key range for a property predicate.
/// </summary>
/// <remarks>
/// <para>
/// <b>Layout.</b> One lattice tree backs a whole index - never one tree per
/// property - so the property name is the leading key-range prefix and each
/// property occupies its own contiguous range inside the shared tree. A key is
/// three <see cref="Separator"/>-delimited components:
/// </para>
/// <code>
/// {propertyName} SEP {valueComponent} SEP {encodedGrainKey}
/// </code>
/// <para>
/// <see cref="Separator"/> is <c>U+0000</c>, the lowest code unit, and the tree
/// orders keys with <see cref="StringComparer.Ordinal"/>. Because no
/// <c>valueComponent</c> ever contains the separator (the string encoding
/// escapes it away and every other encoding is hexadecimal or a single digit),
/// the layout is injective: the first separator after the property prefix ends
/// the value and the second begins the grain key, so an entry names exactly one
/// grain and one property value. The grain key is last precisely so it may
/// contain anything at all.
/// </para>
/// <para>
/// <b>Ordering.</b> For a property type with a total order the value component
/// is <i>order preserving</i>: ordinal comparison of two encoded keys for the
/// same property yields the same answer as comparing the two property values
/// with <see cref="Comparer{T}.Default"/> (ordinal comparison for
/// <see cref="string"/>, matching the tree's own key comparer). That is what
/// makes <c>Age &gt;= 18</c> a single contiguous range scan rather than a full
/// scan. The v1 order-preserving set is exactly the integral types, the
/// floating-point types, <see cref="DateTime"/>, <see cref="DateTimeOffset"/>,
/// <see cref="string"/>, and <see cref="bool"/>;
/// <see cref="IsOrderPreserving(Type)"/> reports it.
/// </para>
/// <para>
/// <b>Fallback.</b> A property whose type has no total order in that set gets a
/// constant, empty value component, so its entries collapse to one key per
/// grain and the query side answers predicates over it by scanning the
/// property's range and evaluating the stored JSON payload (a payload-predicate
/// scan). A pleasant side effect is that such an entry's key never moves, so a
/// value change updates it in place with no tombstone.
/// </para>
/// <para>
/// <b>Null.</b> An ordered value component starts with a one-character presence
/// flag - <c>U+0001</c> for a null value, <c>U+0002</c> for a present one - so
/// null sorts below every present value (as <see cref="Comparer{T}.Default"/>
/// orders it) and an empty string is never confused with null.
/// </para>
/// </remarks>
public static class GrainIndexKeyEncoder
{
    /// <summary>
    /// The component separator, <c>U+0000</c>. It is the lowest code unit, so a
    /// property's entries form a contiguous ordinal range that no other
    /// property's name can fall inside, and so a shorter value sorts before a
    /// longer value that extends it.
    /// </summary>
    public const char Separator = '\u0000';

    /// <summary>
    /// The exclusive-upper-bound character, <c>U+0001</c>: the successor of
    /// <see cref="Separator"/>. Appending it to a key prefix yields the first
    /// key ordinally past every key carrying that prefix.
    /// </summary>
    public const char RangeUpperBound = '\u0001';

    /// <summary>
    /// The value-component flag marking a <c>null</c> property value. It sorts
    /// below <see cref="PresentFlag"/>, so nulls occupy the bottom of a
    /// property's range.
    /// </summary>
    public const char NullFlag = '\u0001';

    /// <summary>
    /// The value-component flag marking a present (non-null) property value,
    /// immediately followed by the order-preserving payload.
    /// </summary>
    public const char PresentFlag = '\u0002';

    private const ulong SignBias64 = 0x8000_0000_0000_0000UL;
    private const uint SignBias32 = 0x8000_0000U;

    /// <summary>
    /// Reports whether <paramref name="propertyType"/> is encoded with an
    /// order-preserving value component, and therefore whether a range
    /// predicate over the property can be served as a contiguous key range
    /// rather than a payload-predicate scan.
    /// </summary>
    /// <param name="propertyType">
    /// The property's declared CLR type. A <see cref="Nullable{T}"/> is
    /// classified by its underlying type.
    /// </param>
    /// <returns>
    /// <c>true</c> for the integral types, <see cref="float"/>,
    /// <see cref="double"/>, <see cref="DateTime"/>,
    /// <see cref="DateTimeOffset"/>, <see cref="string"/>, and
    /// <see cref="bool"/>; otherwise <c>false</c>.
    /// </returns>
    /// <exception cref="ArgumentNullException"><paramref name="propertyType"/> is <c>null</c>.</exception>
    public static bool IsOrderPreserving(Type propertyType)
    {
        ArgumentNullException.ThrowIfNull(propertyType);
        var type = Nullable.GetUnderlyingType(propertyType) ?? propertyType;
        return type == typeof(bool)
            || type == typeof(sbyte)
            || type == typeof(byte)
            || type == typeof(short)
            || type == typeof(ushort)
            || type == typeof(int)
            || type == typeof(uint)
            || type == typeof(long)
            || type == typeof(ulong)
            || type == typeof(char)
            || type == typeof(float)
            || type == typeof(double)
            || type == typeof(DateTime)
            || type == typeof(DateTimeOffset)
            || type == typeof(string);
    }

    /// <summary>
    /// Encodes <paramref name="value"/> into the value component of an index
    /// key: the presence flag plus the order-preserving payload for a type in
    /// the ordered set, or the empty string for any other type.
    /// </summary>
    /// <typeparam name="TValue">
    /// The property's declared CLR type. The encoding is chosen from
    /// <c>typeof(TValue)</c>, not from the runtime type of
    /// <paramref name="value"/>, so it matches the encoding used when the entry
    /// was written.
    /// </typeparam>
    /// <param name="value">The property value to encode.</param>
    /// <returns>
    /// The encoded value component, which never contains
    /// <see cref="Separator"/>.
    /// </returns>
    public static string EncodeValue<TValue>(TValue value)
    {
        Span<char> stack = stackalloc char[StackBufferLength];
        var builder = new KeyBuilder(stack);
        try
        {
            AppendValue(ref builder, value);
            return builder.ToString();
        }
        finally
        {
            builder.Dispose();
        }
    }

    /// <summary>
    /// Composes a full index-entry key from an already-encoded value component.
    /// </summary>
    /// <remarks>
    /// This is the query-planner's route in: it takes a component produced by
    /// <see cref="EncodeValue{TValue}(TValue)"/> rather than a raw value, and
    /// is named distinctly from <see cref="EncodeKey{TValue}(string, TValue, string)"/>
    /// so a <see cref="string"/>-valued property cannot silently bind to the
    /// wrong one.
    /// </remarks>
    /// <param name="propertyName">The projected property's name. Must not be <c>null</c>, empty, or contain <see cref="Separator"/> or <see cref="RangeUpperBound"/>.</param>
    /// <param name="encodedValue">The value component, as returned by <see cref="EncodeValue{TValue}(TValue)"/>. Must not be <c>null</c> or contain <see cref="Separator"/>.</param>
    /// <param name="grainKey">The encoded grain key, as produced by the index's <see cref="IGrainKeyCodec"/>. Must not be <c>null</c>.</param>
    /// <returns>The entry key.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="propertyName"/> is empty or carries a reserved character, or <paramref name="encodedValue"/> carries the separator.</exception>
    public static string ComposeKey(string propertyName, string encodedValue, string grainKey)
    {
        ValidatePropertyName(propertyName);
        ValidateEncodedValue(encodedValue);
        ArgumentNullException.ThrowIfNull(grainKey);

        return string.Concat(propertyName, SeparatorString, encodedValue, SeparatorString, grainKey);
    }

    /// <summary>
    /// Encodes a property value and composes the full index-entry key in one
    /// step.
    /// </summary>
    /// <typeparam name="TValue">The property's declared CLR type.</typeparam>
    /// <param name="propertyName">The projected property's name. Must not be <c>null</c>, empty, or carry a reserved character.</param>
    /// <param name="value">The property value.</param>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <returns>The entry key.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="propertyName"/> or <paramref name="grainKey"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="propertyName"/> is empty or carries a reserved character.</exception>
    public static string EncodeKey<TValue>(string propertyName, TValue value, string grainKey)
    {
        ValidatePropertyName(propertyName);
        ArgumentNullException.ThrowIfNull(grainKey);

        Span<char> stack = stackalloc char[StackBufferLength];
        var builder = new KeyBuilder(stack);
        try
        {
            builder.Append(propertyName);
            builder.Append(Separator);
            AppendValue(ref builder, value);
            builder.Append(Separator);
            builder.Append(grainKey);
            return builder.ToString();
        }
        finally
        {
            builder.Dispose();
        }
    }

    /// <summary>
    /// The inclusive lower bound of the key range holding every entry for
    /// <paramref name="propertyName"/>.
    /// </summary>
    /// <param name="propertyName">The projected property's name.</param>
    /// <returns>The inclusive range start.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="propertyName"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="propertyName"/> is empty or carries a reserved character.</exception>
    public static string PropertyRangeStartInclusive(string propertyName)
    {
        ValidatePropertyName(propertyName);
        return propertyName + Separator;
    }

    /// <summary>
    /// The exclusive upper bound of the key range holding every entry for
    /// <paramref name="propertyName"/>. No other property's entries can fall
    /// inside <c>[start, end)</c>, because a property name cannot contain a
    /// character as low as <see cref="RangeUpperBound"/>.
    /// </summary>
    /// <param name="propertyName">The projected property's name.</param>
    /// <returns>The exclusive range end.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="propertyName"/> is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="propertyName"/> is empty or carries a reserved character.</exception>
    public static string PropertyRangeEndExclusive(string propertyName)
    {
        ValidatePropertyName(propertyName);
        return propertyName + RangeUpperBound;
    }

    /// <summary>
    /// The inclusive lower bound of the key range holding every entry for
    /// <paramref name="propertyName"/> whose value is <b>at least</b> the value
    /// encoded as <paramref name="encodedValue"/>. It is also the start of the
    /// exact-value range, so an equality lookup and a
    /// <c>&gt;=</c> scan share the same lower bound.
    /// </summary>
    /// <param name="propertyName">The projected property's name.</param>
    /// <param name="encodedValue">The encoded value component.</param>
    /// <returns>The inclusive range start.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="propertyName"/> is empty or carries a reserved character, or <paramref name="encodedValue"/> carries the separator.</exception>
    public static string ValueRangeStartInclusive(string propertyName, string encodedValue)
    {
        ValidatePropertyName(propertyName);
        ValidateEncodedValue(encodedValue);
        return string.Concat(propertyName, SeparatorString, encodedValue, SeparatorString);
    }

    /// <summary>
    /// The exclusive upper bound of the key range holding every entry for
    /// <paramref name="propertyName"/> whose value is exactly the value encoded
    /// as <paramref name="encodedValue"/>. It is also the inclusive lower bound
    /// of a strictly-greater-than scan.
    /// </summary>
    /// <param name="propertyName">The projected property's name.</param>
    /// <param name="encodedValue">The encoded value component.</param>
    /// <returns>The exclusive range end.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    /// <exception cref="ArgumentException"><paramref name="propertyName"/> is empty or carries a reserved character, or <paramref name="encodedValue"/> carries the separator.</exception>
    public static string ValueRangeEndExclusive(string propertyName, string encodedValue)
    {
        ValidatePropertyName(propertyName);
        ValidateEncodedValue(encodedValue);
        return string.Concat(propertyName, SeparatorString, encodedValue, RangeUpperBoundString);
    }

    /// <summary>
    /// Splits an index-entry key back into its three components. The value
    /// component is returned in its encoded form: the key carries an
    /// order-preserving projection of the value, not the value itself, so the
    /// authoritative typed value is the entry's JSON payload.
    /// </summary>
    /// <param name="key">The entry key to parse.</param>
    /// <param name="propertyName">On success, the projected property's name.</param>
    /// <param name="encodedValue">On success, the encoded value component.</param>
    /// <param name="grainKey">On success, the encoded grain key.</param>
    /// <returns><c>true</c> when <paramref name="key"/> has the entry-key shape; otherwise <c>false</c>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="key"/> is <c>null</c>.</exception>
    public static bool TryParseKey(
        string key,
        out string propertyName,
        out string encodedValue,
        out string grainKey)
    {
        ArgumentNullException.ThrowIfNull(key);

        int first = key.IndexOf(Separator);
        if (first <= 0)
        {
            propertyName = string.Empty;
            encodedValue = string.Empty;
            grainKey = string.Empty;
            return false;
        }

        int second = key.IndexOf(Separator, first + 1);
        if (second < 0)
        {
            propertyName = string.Empty;
            encodedValue = string.Empty;
            grainKey = string.Empty;
            return false;
        }

        propertyName = key[..first];
        encodedValue = key[(first + 1)..second];
        grainKey = key[(second + 1)..];
        return true;
    }

    /// <summary>
    /// Appends the value component for <paramref name="value"/>. This is the
    /// projection path's entry point: the type test folds to a constant per
    /// generic instantiation, so no boxing and no per-entry type lookup occurs.
    /// </summary>
    internal static void AppendValue<TValue>(ref KeyBuilder builder, TValue value)
    {
        // Ordered value types. Each test is a comparison of two constants once
        // the generic method is instantiated, so the JIT folds the chain to the
        // single matching branch. The reinterpret-cast reads the value in place
        // rather than boxing it, which matters because this runs once per
        // property per grain mutation.
        if (typeof(TValue) == typeof(int)) { AppendPresent(ref builder, SignBias(Unsafe.As<TValue, int>(ref value))); return; }
        if (typeof(TValue) == typeof(long)) { AppendPresent(ref builder, SignBias(Unsafe.As<TValue, long>(ref value))); return; }
        if (typeof(TValue) == typeof(short)) { AppendPresent(ref builder, SignBias(Unsafe.As<TValue, short>(ref value))); return; }
        if (typeof(TValue) == typeof(sbyte)) { AppendPresent(ref builder, SignBias(Unsafe.As<TValue, sbyte>(ref value))); return; }
        if (typeof(TValue) == typeof(byte)) { AppendPresent(ref builder, Unsafe.As<TValue, byte>(ref value)); return; }
        if (typeof(TValue) == typeof(ushort)) { AppendPresent(ref builder, Unsafe.As<TValue, ushort>(ref value)); return; }
        if (typeof(TValue) == typeof(uint)) { AppendPresent(ref builder, Unsafe.As<TValue, uint>(ref value)); return; }
        if (typeof(TValue) == typeof(ulong)) { AppendPresent(ref builder, Unsafe.As<TValue, ulong>(ref value)); return; }
        if (typeof(TValue) == typeof(bool)) { AppendPresentBoolean(ref builder, Unsafe.As<TValue, bool>(ref value)); return; }
        if (typeof(TValue) == typeof(char)) { AppendPresentChar(ref builder, Unsafe.As<TValue, char>(ref value)); return; }
        if (typeof(TValue) == typeof(double)) { AppendPresent(ref builder, TotalOrder(Unsafe.As<TValue, double>(ref value))); return; }
        if (typeof(TValue) == typeof(float)) { AppendPresentSingle(ref builder, TotalOrder(Unsafe.As<TValue, float>(ref value))); return; }
        if (typeof(TValue) == typeof(DateTime)) { AppendPresent(ref builder, SignBias(Unsafe.As<TValue, DateTime>(ref value).Ticks)); return; }
        if (typeof(TValue) == typeof(DateTimeOffset)) { AppendPresent(ref builder, SignBias(Unsafe.As<TValue, DateTimeOffset>(ref value).UtcTicks)); return; }

        // Nullable forms of the ordered value types. Reading through
        // GetValueOrDefault after a HasValue test keeps the nullable on the
        // stack; Value would re-check and can throw.
        if (typeof(TValue) == typeof(int?)) { ref var v = ref Unsafe.As<TValue, int?>(ref value); if (v.HasValue) { AppendPresent(ref builder, SignBias(v.GetValueOrDefault())); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(long?)) { ref var v = ref Unsafe.As<TValue, long?>(ref value); if (v.HasValue) { AppendPresent(ref builder, SignBias(v.GetValueOrDefault())); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(short?)) { ref var v = ref Unsafe.As<TValue, short?>(ref value); if (v.HasValue) { AppendPresent(ref builder, SignBias(v.GetValueOrDefault())); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(sbyte?)) { ref var v = ref Unsafe.As<TValue, sbyte?>(ref value); if (v.HasValue) { AppendPresent(ref builder, SignBias(v.GetValueOrDefault())); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(byte?)) { ref var v = ref Unsafe.As<TValue, byte?>(ref value); if (v.HasValue) { AppendPresent(ref builder, v.GetValueOrDefault()); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(ushort?)) { ref var v = ref Unsafe.As<TValue, ushort?>(ref value); if (v.HasValue) { AppendPresent(ref builder, v.GetValueOrDefault()); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(uint?)) { ref var v = ref Unsafe.As<TValue, uint?>(ref value); if (v.HasValue) { AppendPresent(ref builder, v.GetValueOrDefault()); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(ulong?)) { ref var v = ref Unsafe.As<TValue, ulong?>(ref value); if (v.HasValue) { AppendPresent(ref builder, v.GetValueOrDefault()); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(bool?)) { ref var v = ref Unsafe.As<TValue, bool?>(ref value); if (v.HasValue) { AppendPresentBoolean(ref builder, v.GetValueOrDefault()); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(char?)) { ref var v = ref Unsafe.As<TValue, char?>(ref value); if (v.HasValue) { AppendPresentChar(ref builder, v.GetValueOrDefault()); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(double?)) { ref var v = ref Unsafe.As<TValue, double?>(ref value); if (v.HasValue) { AppendPresent(ref builder, TotalOrder(v.GetValueOrDefault())); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(float?)) { ref var v = ref Unsafe.As<TValue, float?>(ref value); if (v.HasValue) { AppendPresentSingle(ref builder, TotalOrder(v.GetValueOrDefault())); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(DateTime?)) { ref var v = ref Unsafe.As<TValue, DateTime?>(ref value); if (v.HasValue) { AppendPresent(ref builder, SignBias(v.GetValueOrDefault().Ticks)); } else { builder.Append(NullFlag); } return; }
        if (typeof(TValue) == typeof(DateTimeOffset?)) { ref var v = ref Unsafe.As<TValue, DateTimeOffset?>(ref value); if (v.HasValue) { AppendPresent(ref builder, SignBias(v.GetValueOrDefault().UtcTicks)); } else { builder.Append(NullFlag); } return; }

        if (typeof(TValue) == typeof(string))
        {
            string? text = Unsafe.As<TValue, string>(ref value);
            if (text is null)
            {
                builder.Append(NullFlag);
            }
            else
            {
                builder.Append(PresentFlag);
                AppendEscaped(ref builder, text);
            }

            return;
        }

        // Every other property type falls back to the payload-predicate scan:
        // a constant, empty value component, so the entry key is stable across
        // value changes and all of the property's entries sort by grain key.
    }

    /// <summary>
    /// Escapes <paramref name="text"/> so it cannot contain
    /// <see cref="Separator"/>, using a prefix-free, strictly monotone
    /// two-character code for the only two affected code units. Because the
    /// code is prefix free and order preserving per code unit, ordinal
    /// comparison of two escaped strings matches ordinal comparison of the
    /// originals - including the case where one is a prefix of the other.
    /// </summary>
    private static void AppendEscaped(ref KeyBuilder builder, string text)
    {
        int start = 0;
        for (var i = 0; i < text.Length; i++)
        {
            char c = text[i];
            if (c > RangeUpperBound)
                continue;

            builder.Append(text.AsSpan(start, i - start));
            builder.Append(RangeUpperBound);
            builder.Append(c == Separator ? '\u0001' : '\u0002');
            start = i + 1;
        }

        builder.Append(text.AsSpan(start));
    }

    private static void AppendPresent(ref KeyBuilder builder, ulong bits)
    {
        builder.Append(PresentFlag);
        AppendHex(ref builder, bits, 16);
    }

    private static void AppendPresentSingle(ref KeyBuilder builder, uint bits)
    {
        builder.Append(PresentFlag);
        AppendHex(ref builder, bits, 8);
    }

    private static void AppendPresentChar(ref KeyBuilder builder, char value)
    {
        builder.Append(PresentFlag);
        AppendHex(ref builder, value, 4);
    }

    private static void AppendPresentBoolean(ref KeyBuilder builder, bool value)
    {
        builder.Append(PresentFlag);
        builder.Append(value ? '1' : '0');
    }

    private static void AppendHex(ref KeyBuilder builder, ulong bits, int digits)
    {
        Span<char> destination = builder.GetSpan(digits);
        for (var i = digits - 1; i >= 0; i--)
        {
            int nibble = (int)(bits & 0xF);
            destination[i] = (char)(nibble < 10 ? '0' + nibble : 'a' + (nibble - 10));
            bits >>= 4;
        }

        builder.Advance(digits);
    }

    /// <summary>
    /// Maps a signed integer onto the unsigned range with its ordering intact,
    /// by flipping the sign bit: the most negative value becomes 0 and the most
    /// positive becomes <c>ulong.MaxValue</c>, so fixed-width hexadecimal
    /// digits sort exactly as the signed values do.
    /// </summary>
    private static ulong SignBias(long value) => unchecked((ulong)value) ^ SignBias64;

    /// <summary>
    /// Maps a <see cref="double"/> onto the unsigned range in the order
    /// <see cref="Comparer{T}.Default"/> uses. Flipping every bit of a negative
    /// value reverses the descending magnitude order of IEEE-754 negatives, and
    /// flipping the sign bit of a non-negative value lifts it above them.
    /// Every NaN payload collapses to the single lowest slot, which is where
    /// .NET's total order puts NaN and which makes two NaN-valued grains share
    /// one value component, exactly as two equal values do.
    /// </summary>
    private static ulong TotalOrder(double value)
    {
        if (double.IsNaN(value))
            return 0UL;

        long bits = BitConverter.DoubleToInt64Bits(value);
        return bits < 0 ? ~unchecked((ulong)bits) : unchecked((ulong)bits) ^ SignBias64;
    }

    /// <inheritdoc cref="TotalOrder(double)"/>
    private static uint TotalOrder(float value)
    {
        if (float.IsNaN(value))
            return 0U;

        int bits = BitConverter.SingleToInt32Bits(value);
        return bits < 0 ? ~unchecked((uint)bits) : unchecked((uint)bits) ^ SignBias32;
    }

    private static void ValidatePropertyName(string propertyName)
    {
        ArgumentException.ThrowIfNullOrEmpty(propertyName);
        if (propertyName.IndexOf(Separator) >= 0 || propertyName.IndexOf(RangeUpperBound) >= 0)
        {
            throw new ArgumentException(
                "A projected property name must not contain the reserved key characters U+0000 or U+0001.",
                nameof(propertyName));
        }
    }

    private static void ValidateEncodedValue(string encodedValue)
    {
        ArgumentNullException.ThrowIfNull(encodedValue);
        if (encodedValue.IndexOf(Separator) >= 0)
        {
            throw new ArgumentException(
                "An encoded value component must not contain the key separator (U+0000).",
                nameof(encodedValue));
        }
    }

    /// <summary>
    /// The stack budget for a single key. Comfortably covers a property name,
    /// a fixed-width numeric payload, and a typical grain key; anything longer
    /// grows into a pooled array.
    /// </summary>
    internal const int StackBufferLength = 256;

    private const string SeparatorString = "\u0000";
    private const string RangeUpperBoundString = "\u0001";
}
