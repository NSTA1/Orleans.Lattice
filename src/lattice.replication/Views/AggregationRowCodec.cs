using System.IO.Hashing;
using System.Text;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Reserved-key layout and binary (de)serialisation for an aggregation view's
/// internal rows, which live in the <c>view-{name}</c> tree under a reserved NUL
/// (<c>\u0000</c>) prefix that can never collide with a materialised group key
/// (group keys are forbidden from beginning with NUL). Three row families share
/// the tree alongside the bare-keyed materialised group values:
/// <list type="bullet">
/// <item><b>Membership</b> (<c>\u0000m{sourceKey}</c>) - the group and value a source key last contributed; the "read before write" retraction pointer.</item>
/// <item><b>Accumulator</b> (<c>\u0000a{groupKey}\u0000{slot}</c>) - the running count and sum of a group shard (count / sum kinds).</item>
/// <item><b>Inverse</b> (<c>\u0000i{groupKey}\u0000{slot}</c>) - the per-source-key contributions of a group shard (min / max / set-union kinds).</item>
/// </list>
/// The payloads never travel the wire (they are opaque bytes in the view tree),
/// so they use a compact manual encoding rather than an Orleans serializer.
/// </summary>
internal static class AggregationRowCodec
{
    /// <summary>The reserved NUL prefix every internal row key begins with.</summary>
    internal const string ReservedPrefix = "\u0000";

    /// <summary>
    /// The lowest key a materialised group value can take: reads of the
    /// view-facing surface start here to skip the reserved-prefixed internal rows
    /// (all of which sort below this because NUL is the lowest character).
    /// </summary>
    internal const string FirstNonReservedKey = "\u0001";

    /// <summary>Returns the membership row key for <paramref name="sourceKey"/>.</summary>
    internal static string MembershipKey(string sourceKey) => "\u0000m" + sourceKey;

    /// <summary>Returns the accumulator row key for a group shard.</summary>
    internal static string AccumulatorKey(string groupKey, int slot) => "\u0000a" + groupKey + "\u0000" + slot.ToString();

    /// <summary>Returns the inverse-contribution row key for a group shard.</summary>
    internal static string InverseKey(string groupKey, int slot) => "\u0000i" + groupKey + "\u0000" + slot.ToString();

    /// <summary>
    /// Maps a source key to its accumulator shard in <c>[0, fanout)</c> using a
    /// process-independent hash so every cluster shards identically.
    /// </summary>
    internal static int Slot(string sourceKey, int fanout)
    {
        if (fanout <= 1)
        {
            return 0;
        }

        var hash = XxHash32.HashToUInt32(Encoding.UTF8.GetBytes(sourceKey));
        return (int)(hash % (uint)fanout);
    }

    /// <summary>Encodes a membership row.</summary>
    internal static byte[] EncodeMembership(in MembershipRow row)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8);
        writer.Write(row.GroupKey);
        writer.Write(row.Member is not null);
        writer.Write(row.Numeric);
        if (row.Member is not null)
        {
            writer.Write(row.Member);
        }

        writer.Flush();
        return stream.ToArray();
    }

    /// <summary>Decodes a membership row produced by <see cref="EncodeMembership"/>.</summary>
    internal static MembershipRow DecodeMembership(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8);
        var groupKey = reader.ReadString();
        var hasMember = reader.ReadBoolean();
        var numeric = reader.ReadDouble();
        string? member = hasMember ? reader.ReadString() : null;
        return new MembershipRow(groupKey, numeric, member);
    }

    /// <summary>Encodes an accumulator row.</summary>
    internal static byte[] EncodeAccumulator(in AccumulatorRow row)
    {
        var buffer = new byte[sizeof(long) + sizeof(double)];
        System.Buffers.Binary.BinaryPrimitives.WriteInt64BigEndian(buffer, row.Count);
        System.Buffers.Binary.BinaryPrimitives.WriteDoubleBigEndian(buffer.AsSpan(sizeof(long)), row.Sum);
        return buffer;
    }

    /// <summary>Decodes an accumulator row produced by <see cref="EncodeAccumulator"/>.</summary>
    internal static AccumulatorRow DecodeAccumulator(byte[] bytes)
    {
        var count = System.Buffers.Binary.BinaryPrimitives.ReadInt64BigEndian(bytes);
        var sum = System.Buffers.Binary.BinaryPrimitives.ReadDoubleBigEndian(bytes.AsSpan(sizeof(long)));
        return new AccumulatorRow(count, sum);
    }

    /// <summary>Encodes an inverse-contribution row (a source-key to contribution map).</summary>
    internal static byte[] EncodeInverse(IReadOnlyDictionary<string, MemberEntry> entries)
    {
        using var stream = new MemoryStream();
        using var writer = new BinaryWriter(stream, Encoding.UTF8);
        writer.Write(entries.Count);
        foreach (var (sourceKey, entry) in entries)
        {
            writer.Write(sourceKey);
            writer.Write(entry.Member is not null);
            writer.Write(entry.Numeric);
            if (entry.Member is not null)
            {
                writer.Write(entry.Member);
            }
        }

        writer.Flush();
        return stream.ToArray();
    }

    /// <summary>Decodes an inverse-contribution row produced by <see cref="EncodeInverse"/>.</summary>
    internal static Dictionary<string, MemberEntry> DecodeInverse(byte[] bytes)
    {
        using var stream = new MemoryStream(bytes);
        using var reader = new BinaryReader(stream, Encoding.UTF8);
        var count = reader.ReadInt32();
        var map = new Dictionary<string, MemberEntry>(count, StringComparer.Ordinal);
        for (var i = 0; i < count; i++)
        {
            var sourceKey = reader.ReadString();
            var hasMember = reader.ReadBoolean();
            var numeric = reader.ReadDouble();
            string? member = hasMember ? reader.ReadString() : null;
            map[sourceKey] = new MemberEntry(numeric, member);
        }

        return map;
    }

    /// <summary>The group and value a source key last contributed.</summary>
    /// <param name="GroupKey">The group the source key last belonged to.</param>
    /// <param name="Numeric">The numeric the source key last contributed (sum / min / max).</param>
    /// <param name="Member">The member the source key last contributed (set-union), or <see langword="null"/>.</param>
    internal readonly record struct MembershipRow(string GroupKey, double Numeric, string? Member);

    /// <summary>A group shard's running count and sum.</summary>
    /// <param name="Count">The number of live source keys in the shard.</param>
    /// <param name="Sum">The running sum of the shard's numeric contributions.</param>
    internal readonly record struct AccumulatorRow(long Count, double Sum);

    /// <summary>A single source key's contribution inside an inverse row.</summary>
    /// <param name="Numeric">The numeric contributed (min / max).</param>
    /// <param name="Member">The member contributed (set-union), or <see langword="null"/>.</param>
    internal readonly record struct MemberEntry(double Numeric, string? Member);
}
