using System.Collections.Immutable;
using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Replication;

/// <summary>
/// The durable read positions a shipper vouches its peer has acknowledged
/// (issue #4684): per write-ahead-log partition of <see cref="PhysicalTreeId"/>,
/// the lowest offset not yet acknowledged, capped at held saga terminals. Shipped
/// beside the applied low watermark, and only while the shipper vouches one, so
/// it never covers a record the shipper passed without delivering. A receiver
/// compares it with the sibling boundaries an import of another tree captured.
/// Travels as its own call header so a receiver that predates it keeps reading
/// the watermark.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.ReplicationAckedPositions)]
internal sealed record ReplicationAckedPositions
{
    /// <summary>The longest text form <see cref="TryParse"/> accepts.</summary>
    public const int MaxTextLength = 4096;

    /// <summary>The most partitions <see cref="TryParse"/> accepts.</summary>
    public const int MaxPartitions = 256;

    private const string FormatVersion = "1";

    /// <summary>The physical write-ahead log the positions are offsets in.</summary>
    [Id(0)] public required string PhysicalTreeId { get; init; }

    /// <summary>Per partition, the lowest offset the peer has not acknowledged.</summary>
    [Id(1)] public required ImmutableArray<long> Positions { get; init; }

    /// <summary>
    /// Whether these positions are on <paramref name="physicalTreeId"/> and at or
    /// past every one of <paramref name="tails"/>.
    /// </summary>
    public bool CoversTails(string physicalTreeId, ImmutableArray<long> tails)
    {
        if (!string.Equals(PhysicalTreeId, physicalTreeId, StringComparison.Ordinal) || Positions.Length < tails.Length)
        {
            return false;
        }

        for (var p = 0; p < tails.Length; p++)
        {
            if (Positions[p] < tails[p])
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Renders the canonical text form <see cref="TryParse"/> reads.</summary>
    public string ToText() => string.Join(
        '|',
        FormatVersion,
        Convert.ToBase64String(Encoding.UTF8.GetBytes(PhysicalTreeId)),
        string.Join(',', Positions.Select(p => p.ToString(CultureInfo.InvariantCulture))));

    /// <summary>
    /// Parses the text form strictly. The text is wire input from a peer, so an
    /// unknown version, an empty or undecodable log id, a negative or malformed
    /// position, too many partitions, or over-long text is refused, and the
    /// receiver treats the push as vouching no positions.
    /// </summary>
    public static bool TryParse(string? text, out ReplicationAckedPositions? positions)
    {
        positions = null;
        if (string.IsNullOrEmpty(text) || text.Length > MaxTextLength)
        {
            return false;
        }

        var parts = text.Split('|');
        if (parts.Length != 3 || !string.Equals(parts[0], FormatVersion, StringComparison.Ordinal) || parts[1].Length == 0 || parts[2].Length == 0)
        {
            return false;
        }

        string physical;
        try
        {
            physical = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true)
                .GetString(Convert.FromBase64String(parts[1]));
        }
        catch (FormatException)
        {
            return false;
        }
        catch (ArgumentException)
        {
            return false;
        }

        if (physical.Length == 0)
        {
            return false;
        }

        var fields = parts[2].Split(',');
        if (fields.Length > MaxPartitions)
        {
            return false;
        }

        var builder = ImmutableArray.CreateBuilder<long>(fields.Length);
        foreach (var field in fields)
        {
            if (!long.TryParse(field, NumberStyles.None, CultureInfo.InvariantCulture, out var position))
            {
                return false;
            }

            builder.Add(position);
        }

        positions = new ReplicationAckedPositions { PhysicalTreeId = physical, Positions = builder.MoveToImmutable() };
        return true;
    }
}
