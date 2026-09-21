using System.Globalization;
using System.Text;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The position a bounded orphaned-leaf pass resumes from - which physical
/// shard of the tree, and where in that shard's sibling chain - encoded as the
/// opaque token <see cref="OrphanedLeafRepairReport.ResumeFrom"/> carries and
/// <see cref="ILattice.RepairOrphanedLeavesAsync"/> accepts back (issue 3302).
/// <para>
/// It is deliberately opaque to callers. An operator drives the pass by handing
/// the previous report's token straight back, never by constructing one, so the
/// encoding stays free to change and a caller cannot invent a position the walk
/// never reached.
/// </para>
/// <para>
/// <b>Why the key is base64 rather than inlined, and why a present key carries
/// a marker.</b> A resume key is an arbitrary caller-supplied string, so it can
/// contain any delimiter this encoding might pick, including the one separating
/// it from the shard index. Encoding the key's UTF-8 bytes leaves the token with
/// a fixed, unambiguous shape, so a decode either yields the exact position the
/// shard named or is rejected - never a silently truncated key, which would
/// resume the walk in the wrong place and skip the leaves in between. The
/// <see cref="KeyMarker"/> then separates a <see langword="null"/> cursor ("start
/// this shard at its chain head") from an empty-string cursor, which base64
/// alone encodes identically; the two mean different things to the shard walk,
/// and an empty string is a legal key in a store whose keys are arbitrary
/// strings.
/// </para>
/// </summary>
internal readonly record struct OrphanedLeafPassCursor
{
    /// <summary>
    /// The token prefix. Versioned so that a future change of shape is
    /// rejected loudly by an older silo rather than mis-parsed by it.
    /// </summary>
    private const string Prefix = "olp1:";

    /// <summary>
    /// Introduces a present resume key, so that an absent key segment is
    /// unambiguously <see langword="null"/> and never an empty key.
    /// </summary>
    private const char KeyMarker = 'k';

    /// <summary>The start of the pass - the first shard, from the chain head.</summary>
    internal static OrphanedLeafPassCursor Start { get; } = new()
    {
        ShardIndex = 0,
        ResumeFromInclusive = null,
    };

    /// <summary>
    /// The lowest physical shard index the next call still has work in. Shards
    /// below it were examined to the end of their chains by an earlier call.
    /// </summary>
    internal int ShardIndex { get; init; }

    /// <summary>
    /// Where to resume inside <see cref="ShardIndex"/>'s sibling chain, or
    /// <see langword="null"/> to start that shard at its chain head. This is
    /// the shard-level cursor
    /// <see cref="OrphanedLeafRepairPage.ResumeFromInclusive"/> handed back,
    /// passed through unaltered.
    /// </summary>
    internal string? ResumeFromInclusive { get; init; }

    /// <summary>
    /// Encodes a resume position as the opaque token a report carries.
    /// </summary>
    internal static string Encode(int shardIndex, string? resumeFromInclusive)
    {
        var key = resumeFromInclusive is null
            ? string.Empty
            : KeyMarker + Convert.ToBase64String(Encoding.UTF8.GetBytes(resumeFromInclusive));

        return $"{Prefix}{shardIndex.ToString(CultureInfo.InvariantCulture)}:{key}";
    }

    /// <summary>
    /// Decodes a token produced by <see cref="Encode"/>. A <see langword="null"/>
    /// or empty token means "start at the beginning", which is how a first call
    /// is expressed.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// The token is not one this version produced. It is rejected rather than
    /// coerced to <see cref="Start"/>: silently restarting a pass an operator
    /// believed was resuming would re-walk work already done and, worse, report
    /// the re-walk's findings as though they were the remainder.
    /// </exception>
    internal static OrphanedLeafPassCursor Decode(string? token)
    {
        if (string.IsNullOrEmpty(token)) return Start;

        if (!token.StartsWith(Prefix, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"'{token}' is not an orphaned-leaf pass resume token. Pass the " +
                "ResumeFrom value from the previous report unaltered, or null to start a new pass.",
                nameof(token));
        }

        var body = token.AsSpan(Prefix.Length);
        var separator = body.IndexOf(':');
        if (separator < 0)
        {
            throw new ArgumentException(
                $"'{token}' is a malformed orphaned-leaf pass resume token: no shard separator.",
                nameof(token));
        }

        if (!int.TryParse(
                body[..separator],
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var shardIndex))
        {
            throw new ArgumentException(
                $"'{token}' is a malformed orphaned-leaf pass resume token: unreadable shard index.",
                nameof(token));
        }

        var keySegment = body[(separator + 1)..];
        if (keySegment.IsEmpty)
        {
            return new OrphanedLeafPassCursor { ShardIndex = shardIndex, ResumeFromInclusive = null };
        }

        if (keySegment[0] != KeyMarker)
        {
            throw new ArgumentException(
                $"'{token}' is a malformed orphaned-leaf pass resume token: unmarked resume key.",
                nameof(token));
        }

        keySegment = keySegment[1..];
        if (keySegment.IsEmpty)
        {
            return new OrphanedLeafPassCursor { ShardIndex = shardIndex, ResumeFromInclusive = string.Empty };
        }

        var keyBytes = new byte[keySegment.Length];
        if (!Convert.TryFromBase64Chars(keySegment, keyBytes, out var written))
        {
            throw new ArgumentException(
                $"'{token}' is a malformed orphaned-leaf pass resume token: unreadable resume key.",
                nameof(token));
        }

        return new OrphanedLeafPassCursor
        {
            ShardIndex = shardIndex,
            ResumeFromInclusive = Encoding.UTF8.GetString(keyBytes, 0, written),
        };
    }
}
