using System.Text;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The structural/keyword fallback ranker for repository-context search. When no
/// semantic index or embedding provider is configured, or the embedder is
/// unreachable, search degrades to this deterministic token-overlap scorer over
/// the projected records the structural walk already captured, so a query always
/// returns the best structural matches rather than throwing.
/// <para>
/// A record's score is the number of distinct query tokens that appear (as a
/// case-insensitive substring) in its searchable text - its key, path,
/// fully-qualified name, topic, tags, and scalar field values, which for a
/// per-file content-projection record include the file's bounded body text - with
/// a small bonus for a whole-token match. The scorer holds no state and touches no
/// store, so its recall behaviour is unit-testable in isolation.
/// </para>
/// </summary>
internal static class RepoContextKeywordSearch
{
    /// <summary>
    /// Tokenizes <paramref name="query"/> into distinct lower-case terms, splitting
    /// on non-alphanumeric characters. Returns an empty list for a null or blank
    /// query.
    /// </summary>
    /// <param name="query">The free-text query to tokenize.</param>
    /// <returns>The distinct lower-case query tokens.</returns>
    internal static IReadOnlyList<string> Tokenize(string? query)
    {
        if (string.IsNullOrWhiteSpace(query))
        {
            return Array.Empty<string>();
        }

        var tokens = new List<string>();
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var builder = new StringBuilder();
        foreach (var ch in query)
        {
            if (char.IsLetterOrDigit(ch))
            {
                builder.Append(char.ToLowerInvariant(ch));
            }
            else if (builder.Length > 0)
            {
                Flush(builder, tokens, seen);
            }
        }

        Flush(builder, tokens, seen);
        return tokens;
    }

    /// <summary>
    /// Scores a single projected entry against the query tokens: the count of
    /// distinct tokens found as a case-insensitive substring of the entry's
    /// searchable text, plus a unit bonus per whole-token (word-boundary) match.
    /// Returns <c>0</c> when nothing matches.
    /// </summary>
    /// <param name="entry">The projected entry to score. Must not be <see langword="null"/>.</param>
    /// <param name="tokens">The query tokens produced by <see cref="Tokenize(string?)"/>. Must not be <see langword="null"/>.</param>
    /// <returns>The match score, higher meaning a better structural match.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="entry"/> or <paramref name="tokens"/> is null.</exception>
    internal static double Score(RepoContextEntryView entry, IReadOnlyList<string> tokens)
    {
        ArgumentNullException.ThrowIfNull(entry);
        ArgumentNullException.ThrowIfNull(tokens);
        if (tokens.Count == 0)
        {
            return 0;
        }

        var haystack = BuildHaystack(entry);
        double score = 0;
        foreach (var token in tokens)
        {
            var index = haystack.IndexOf(token, StringComparison.Ordinal);
            if (index < 0)
            {
                continue;
            }

            score += 1;
            if (IsWholeToken(haystack, index, token.Length))
            {
                score += 1;
            }
        }

        return score;
    }

    /// <summary>
    /// Ranks <paramref name="entries"/> against <paramref name="tokens"/> and
    /// returns up to <paramref name="k"/> scored hits in descending score order,
    /// dropping entries that match nothing. Ties keep the ordinal key order the
    /// entries were supplied in, so the result is deterministic.
    /// </summary>
    /// <param name="entries">The projected entries to rank. Must not be <see langword="null"/>.</param>
    /// <param name="tokens">The query tokens. Must not be <see langword="null"/>.</param>
    /// <param name="k">The maximum number of hits to return. Must be positive.</param>
    /// <returns>The best structural matches, at most <paramref name="k"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="entries"/> or <paramref name="tokens"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="k"/> is not positive.</exception>
    internal static IReadOnlyList<RepoContextSearchHit> Rank(
        IReadOnlyList<RepoContextEntryView> entries, IReadOnlyList<string> tokens, int k)
    {
        ArgumentNullException.ThrowIfNull(entries);
        ArgumentNullException.ThrowIfNull(tokens);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(k);

        var scored = new List<RepoContextSearchHit>();
        foreach (var entry in entries)
        {
            var score = Score(entry, tokens);
            if (score > 0)
            {
                scored.Add(new RepoContextSearchHit { Score = score, Entry = entry, VectorId = null });
            }
        }

        scored.Sort(static (x, y) =>
        {
            var byScore = y.Score.CompareTo(x.Score);
            return byScore != 0 ? byScore : string.CompareOrdinal(x.Entry.Key, y.Entry.Key);
        });

        return scored.Count > k ? scored.GetRange(0, k) : scored;
    }

    private static string BuildHaystack(RepoContextEntryView entry)
    {
        var builder = new StringBuilder();
        Append(builder, entry.Key);
        Append(builder, entry.Path);
        Append(builder, entry.FullyQualifiedName);
        Append(builder, entry.Topic);
        Append(builder, entry.Id);
        foreach (var tag in entry.Tags)
        {
            Append(builder, tag);
        }

        foreach (var value in entry.Fields.Values)
        {
            Append(builder, value);
        }

        return builder.ToString().ToLowerInvariant();
    }

    private static void Append(StringBuilder builder, string? value)
    {
        if (!string.IsNullOrEmpty(value))
        {
            builder.Append(value).Append('\n');
        }
    }

    private static bool IsWholeToken(string haystack, int index, int length)
    {
        var before = index == 0 || !char.IsLetterOrDigit(haystack[index - 1]);
        var afterIndex = index + length;
        var after = afterIndex >= haystack.Length || !char.IsLetterOrDigit(haystack[afterIndex]);
        return before && after;
    }

    private static void Flush(StringBuilder builder, List<string> tokens, HashSet<string> seen)
    {
        if (builder.Length == 0)
        {
            return;
        }

        var token = builder.ToString();
        builder.Clear();
        if (seen.Add(token))
        {
            tokens.Add(token);
        }
    }
}
