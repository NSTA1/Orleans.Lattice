using System.Collections.Immutable;
using System.Globalization;
using System.Text;

namespace Orleans.Lattice.Replication;

/// <summary>
/// A chunk of the cross-tree purge frontier an origin advertises (issue #4733):
/// per origin tree, the decision sequence at or below which the origin stores
/// no cross-tree decision of the tree and never will again. Travels beside a
/// push as its own call header, read only on an origin-authenticated push from
/// a configured peer, and parsed strictly and bounded. A receiver takes the
/// maximum per tree, so chunks may arrive in any order and repeat.
/// </summary>
[GenerateSerializer]
[Immutable]
[Alias(ReplicationTypeAliases.CrossTreePurgeFrontier)]
internal sealed record CrossTreePurgeFrontier
{
    /// <summary>The longest text form <see cref="TryParse"/> accepts.</summary>
    public const int MaxTextLength = 6144;

    /// <summary>The most trees one chunk carries.</summary>
    public const int MaxEntries = 64;

    private const string FormatVersion = "1";

    /// <summary>Per tree, the advertised frontier.</summary>
    [Id(0)] public required ImmutableDictionary<string, long> Frontiers { get; init; }

    /// <summary>Renders the canonical text form <see cref="TryParse"/> reads.</summary>
    public string ToText() => FormatVersion + "|" + string.Join(
        ',',
        Frontiers.OrderBy(static f => f.Key, StringComparer.Ordinal).Select(static f =>
            Convert.ToBase64String(Encoding.UTF8.GetBytes(f.Key)) + ":" + f.Value.ToString(CultureInfo.InvariantCulture)));

    /// <summary>
    /// Parses the text form strictly. The text is wire input from a peer, so an
    /// unknown version, an empty or undecodable tree id, a duplicate tree, a
    /// negative or malformed frontier, too many entries, or over-long text is
    /// refused, and the push advertises nothing.
    /// </summary>
    public static bool TryParse(string? text, out CrossTreePurgeFrontier? frontier)
    {
        frontier = null;
        if (string.IsNullOrEmpty(text) || text.Length > MaxTextLength)
        {
            return false;
        }

        var bar = text.IndexOf('|', StringComparison.Ordinal);
        if (bar < 0 || !string.Equals(text[..bar], FormatVersion, StringComparison.Ordinal) || bar == text.Length - 1)
        {
            return false;
        }

        var fields = text[(bar + 1)..].Split(',');
        if (fields.Length > MaxEntries)
        {
            return false;
        }

        var strict = new UTF8Encoding(encoderShouldEmitUTF8Identifier: false, throwOnInvalidBytes: true);
        var builder = ImmutableDictionary.CreateBuilder<string, long>(StringComparer.Ordinal);
        foreach (var field in fields)
        {
            var colon = field.IndexOf(':', StringComparison.Ordinal);
            if (colon <= 0
                || !long.TryParse(field.AsSpan(colon + 1), NumberStyles.None, CultureInfo.InvariantCulture, out var value))
            {
                return false;
            }

            string tree;
            try
            {
                tree = strict.GetString(Convert.FromBase64String(field[..colon]));
            }
            catch (Exception ex) when (ex is FormatException or ArgumentException)
            {
                return false;
            }

            if (tree.Length == 0 || !builder.TryAdd(tree, value))
            {
                return false;
            }
        }

        frontier = new CrossTreePurgeFrontier { Frontiers = builder.ToImmutable() };
        return true;
    }
}
