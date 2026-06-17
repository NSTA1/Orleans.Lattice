using System.Buffers.Binary;

namespace Orleans.Lattice;

/// <summary>
/// Encodes and decodes the materialised value an aggregation view stores under a
/// bare group key. Readers obtain the bytes through
/// <c>ILatticeView.GetAsync(groupKey)</c> and decode them with the matching
/// helper for the view's <see cref="AggregationKind"/>:
/// <list type="bullet">
/// <item><see cref="AggregationKind.Count"/> and <see cref="AggregationKind.SetUnion"/> store an <see cref="long"/> (decode with <see cref="DecodeInt64"/>).</item>
/// <item><see cref="AggregationKind.Sum"/>, <see cref="AggregationKind.Min"/>, and <see cref="AggregationKind.Max"/> store a <see cref="double"/> (decode with <see cref="DecodeDouble"/>).</item>
/// </list>
/// A <see langword="null"/> read means the group has no live members.
/// </summary>
public static class LatticeAggregationValue
{
    /// <summary>Encodes a 64-bit integer aggregate (count or set-union cardinality) as 8 big-endian bytes.</summary>
    /// <param name="value">The integer aggregate to encode.</param>
    public static byte[] EncodeInt64(long value)
    {
        var buffer = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(buffer, value);
        return buffer;
    }

    /// <summary>Decodes an aggregate produced by <see cref="EncodeInt64"/>.</summary>
    /// <param name="bytes">The 8-byte big-endian payload. Must not be <see langword="null"/> and must be 8 bytes.</param>
    public static long DecodeInt64(byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        if (bytes.Length != sizeof(long))
        {
            throw new ArgumentException($"Expected {sizeof(long)} bytes, got {bytes.Length}.", nameof(bytes));
        }

        return BinaryPrimitives.ReadInt64BigEndian(bytes);
    }

    /// <summary>Encodes a double-precision aggregate (sum / min / max) as 8 big-endian bytes.</summary>
    /// <param name="value">The double aggregate to encode.</param>
    public static byte[] EncodeDouble(double value)
    {
        var buffer = new byte[sizeof(double)];
        BinaryPrimitives.WriteDoubleBigEndian(buffer, value);
        return buffer;
    }

    /// <summary>Decodes an aggregate produced by <see cref="EncodeDouble"/>.</summary>
    /// <param name="bytes">The 8-byte big-endian payload. Must not be <see langword="null"/> and must be 8 bytes.</param>
    public static double DecodeDouble(byte[] bytes)
    {
        ArgumentNullException.ThrowIfNull(bytes);
        if (bytes.Length != sizeof(double))
        {
            throw new ArgumentException($"Expected {sizeof(double)} bytes, got {bytes.Length}.", nameof(bytes));
        }

        return BinaryPrimitives.ReadDoubleBigEndian(bytes);
    }
}
