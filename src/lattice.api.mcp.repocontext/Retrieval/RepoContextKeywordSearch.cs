namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The structural/keyword fallback ranker for repository-context search. When no
/// semantic index or embedding provider is configured, or the embedder is
/// unreachable, search degrades to this deterministic ranker over the projected
/// records the structural walk already captured, so a query always returns the
/// best structural matches rather than throwing.
/// <para>
/// Records are ranked with Okapi BM25 computed over the bounded candidate set the
/// caller supplies: a term's contribution rises with its frequency in a record
/// (saturating, so a single flooded field cannot dominate), is normalised by the
/// record's length against the candidate-set average, and is weighted by inverse
/// document frequency so a distinctive term outranks a ubiquitous one - the
/// deficiency of a flat token-overlap count. A record's searchable text is folded
/// from its key, path, fully-qualified name, topic, tags, and scalar field values
/// (which for a per-file content-projection record include the file's bounded body
/// text), with high-signal name-like fields weighted above body text. Text is
/// tokenised identifier-aware (splitting <c>camelCase</c> and letter/digit
/// boundaries and lower-casing), so a query term matches a sub-token of a compound
/// identifier. The ranker holds no state and touches no store, so its behaviour is
/// unit-testable in isolation.
/// </para>
/// </summary>
internal static class RepoContextKeywordSearch
{
    /// <summary>The BM25 term-frequency saturation parameter.</summary>
    private const double K1 = 1.2d;

    /// <summary>The BM25 length-normalisation parameter.</summary>
    private const double B = 0.75d;

    // Field weights fold high-signal, name-like fields above body text so a name
    // match outranks an incidental body mention. Pure-noise fields (content
    // digests, sizes, line numbers, timestamps) are omitted entirely by returning
    // a zero weight, keeping them out of both term frequency and length.
    private static readonly Dictionary<string, double> FieldWeights = new(StringComparer.OrdinalIgnoreCase)
    {
        ["title"] = 3d,
        ["signature"] = 2d,
        ["filePath"] = 2d,
        ["displayName"] = 2d,
        ["references"] = 1d,
        ["body"] = 1d,
        ["text"] = 1d,
        ["language"] = 1d,
        ["version"] = 1d,
        ["defaultBranch"] = 1d,
        ["kind"] = 1d,
    };

    /// <summary>
    /// Tokenizes <paramref name="query"/> into distinct lower-case terms, splitting
    /// on non-alphanumeric characters and on identifier boundaries (a
    /// <c>camelCase</c> hump or a letter/digit transition). Returns an empty list
    /// for a null or blank query.
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
        var buffer = new char[32];
        var length = 0;
        var previous = '\0';
        for (var i = 0; i <= query.Length; i++)
        {
            var atEnd = i == query.Length;
            var current = atEnd ? '\0' : query[i];
            if (!atEnd && char.IsLetterOrDigit(current))
            {
                if (length > 0 && IsBoundary(previous, current))
                {
                    FlushQueryToken(buffer, length, tokens, seen);
                    length = 0;
                }

                if (length == buffer.Length)
                {
                    Array.Resize(ref buffer, buffer.Length * 2);
                }

                buffer[length++] = char.ToLowerInvariant(current);
                previous = current;
            }
            else if (length > 0)
            {
                FlushQueryToken(buffer, length, tokens, seen);
                length = 0;
                previous = '\0';
            }
        }

        return tokens;
    }

    /// <summary>
    /// Ranks <paramref name="entries"/> against <paramref name="tokens"/> with BM25
    /// and returns up to <paramref name="k"/> scored hits in descending score order,
    /// dropping entries that match no query term. Document frequency, average length,
    /// and per-term inverse document frequency are computed over the supplied
    /// candidate set, so ranking reflects the whole bounded scan rather than any one
    /// record in isolation. Ties keep ordinal key order, so the result is
    /// deterministic.
    /// </summary>
    /// <param name="entries">The projected entries to rank. Must not be <see langword="null"/>.</param>
    /// <param name="tokens">The query tokens produced by <see cref="Tokenize(string?)"/>. Must not be <see langword="null"/>.</param>
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

        var documentCount = entries.Count;
        if (documentCount == 0 || tokens.Count == 0)
        {
            return Array.Empty<RepoContextSearchHit>();
        }

        // Distinct query terms with a stable index; the term index doubles as the
        // corpus alphabet for term-frequency and document-frequency accounting.
        var termIndex = new Dictionary<string, int>(StringComparer.Ordinal);
        foreach (var token in tokens)
        {
            if (token.Length != 0 && !termIndex.ContainsKey(token))
            {
                termIndex[token] = termIndex.Count;
            }
        }

        var termCount = termIndex.Count;
        if (termCount == 0)
        {
            return Array.Empty<RepoContextSearchHit>();
        }

        var termLookup = termIndex.GetAlternateLookup<ReadOnlySpan<char>>();
        var documentFrequency = new int[termCount];
        var documentTermFrequencies = new double[documentCount][];
        var documentLengths = new double[documentCount];
        var totalLength = 0d;
        var buffer = new char[64];

        for (var d = 0; d < documentCount; d++)
        {
            double[]? termFrequency = null;
            var documentLength = 0d;
            AccumulateEntry(entries[d], termLookup, termCount, ref termFrequency, ref documentLength, ref buffer);

            documentTermFrequencies[d] = termFrequency!;
            documentLengths[d] = documentLength;
            totalLength += documentLength;

            if (termFrequency is not null)
            {
                for (var t = 0; t < termCount; t++)
                {
                    if (termFrequency[t] > 0d)
                    {
                        documentFrequency[t]++;
                    }
                }
            }
        }

        var averageLength = totalLength / documentCount;
        if (averageLength <= 0d)
        {
            return Array.Empty<RepoContextSearchHit>();
        }

        var inverseDocumentFrequency = new double[termCount];
        for (var t = 0; t < termCount; t++)
        {
            inverseDocumentFrequency[t] =
                Math.Log(1d + ((documentCount - documentFrequency[t] + 0.5d) / (documentFrequency[t] + 0.5d)));
        }

        var scored = new List<RepoContextSearchHit>();
        for (var d = 0; d < documentCount; d++)
        {
            var termFrequency = documentTermFrequencies[d];
            if (termFrequency is null)
            {
                continue;
            }

            var normalization = K1 * (1d - B + (B * documentLengths[d] / averageLength));
            var score = 0d;
            for (var t = 0; t < termCount; t++)
            {
                var frequency = termFrequency[t];
                if (frequency <= 0d)
                {
                    continue;
                }

                score += inverseDocumentFrequency[t] * (frequency * (K1 + 1d)) / (frequency + normalization);
            }

            if (score > 0d)
            {
                scored.Add(new RepoContextSearchHit { Score = score, Entry = entries[d], VectorId = null });
            }
        }

        scored.Sort(static (x, y) =>
        {
            var byScore = y.Score.CompareTo(x.Score);
            return byScore != 0 ? byScore : string.CompareOrdinal(x.Entry.Key, y.Entry.Key);
        });

        // Reasons are attached only to the hits actually returned (at most k), not
        // to every scored candidate, so the explanation pass never touches the
        // wider bounded scan. The distinct query terms are folded into a single
        // span-lookup set reused across those hits.
        var limit = scored.Count > k ? k : scored.Count;
        if (limit == 0)
        {
            return Array.Empty<RepoContextSearchHit>();
        }

        var queryTerms = new HashSet<string>(termCount, StringComparer.Ordinal);
        foreach (var term in termIndex.Keys)
        {
            queryTerms.Add(term);
        }

        var queryLookup = queryTerms.GetAlternateLookup<ReadOnlySpan<char>>();
        var results = new List<RepoContextSearchHit>(limit);
        for (var i = 0; i < limit; i++)
        {
            var hit = scored[i];
            results.Add(hit with { Reasons = BuildKeywordReasons(hit.Entry, queryLookup) });
        }

        return results;
    }

    /// <summary>
    /// Builds the deterministic, ordinal-ordered reason set for one keyword hit by
    /// replaying which projected field each distinct query term matched - the
    /// information BM25 scoring computes and then discards. Reasons are emitted in a
    /// fixed high-signal-first order (path, symbol, tags, topic, content, key) and
    /// capped at <see cref="RepoContextSearchReasons.MaxReasons"/>, dropping the
    /// lowest-signal reasons first. Every reason is derived from the stored record's
    /// own fields, never from the raw query text.
    /// </summary>
    /// <param name="entry">The matched entry. Must not be <see langword="null"/>.</param>
    /// <param name="queryTerms">The distinct lower-case query terms as a span lookup.</param>
    /// <returns>The ordered, capped reasons; an empty list when no field is attributable.</returns>
    internal static IReadOnlyList<string> BuildKeywordReasons(
        RepoContextEntryView entry, HashSet<string>.AlternateLookup<ReadOnlySpan<char>> queryTerms)
    {
        ArgumentNullException.ThrowIfNull(entry);

        var reasons = new List<string>(RepoContextSearchReasons.MaxReasons);

        if (ContainsQueryTerm(entry.Path, queryTerms))
        {
            reasons.Add(RepoContextSearchReasons.PathNameMatch);
        }

        if (reasons.Count < RepoContextSearchReasons.MaxReasons
            && ContainsQueryTerm(entry.FullyQualifiedName, queryTerms))
        {
            reasons.Add(RepoContextSearchReasons.SymbolPrefix + entry.FullyQualifiedName);
        }

        foreach (var tag in entry.Tags)
        {
            if (reasons.Count >= RepoContextSearchReasons.MaxReasons)
            {
                break;
            }

            if (ContainsQueryTerm(tag, queryTerms))
            {
                reasons.Add(RepoContextSearchReasons.TagPrefix + tag);
            }
        }

        if (reasons.Count < RepoContextSearchReasons.MaxReasons
            && ContainsQueryTerm(entry.Topic, queryTerms))
        {
            reasons.Add(RepoContextSearchReasons.TopicMatch);
        }

        if (reasons.Count < RepoContextSearchReasons.MaxReasons
            && ContainsContentMatch(entry, queryTerms))
        {
            reasons.Add(RepoContextSearchReasons.ContentMatch);
        }

        if (reasons.Count < RepoContextSearchReasons.MaxReasons
            && ContainsQueryTerm(entry.Key, queryTerms))
        {
            reasons.Add(RepoContextSearchReasons.KeyMatch);
        }

        return reasons.Count == 0 ? Array.Empty<string>() : reasons;
    }

    // True when any scored content field (a field with a positive weight, the same
    // set BM25 folds into the score) contains one of the query terms.
    private static bool ContainsContentMatch(
        RepoContextEntryView entry, HashSet<string>.AlternateLookup<ReadOnlySpan<char>> queryTerms)
    {
        foreach (var (name, value) in entry.Fields)
        {
            if (FieldWeights.TryGetValue(name, out var weight) && weight > 0d
                && ContainsQueryTerm(value, queryTerms))
            {
                return true;
            }
        }

        return false;
    }

    // Identifier-aware scan of a single field: tokenizes exactly as the scorer does
    // (camelCase/letter-digit splitting, lower-casing) and returns on the first
    // token that is a query term. Tokens are matched against the span lookup with
    // no per-token string allocation; the stack buffer covers any realistic token,
    // and an over-long token simply cannot match (never a false positive).
    private static bool ContainsQueryTerm(
        string? text, HashSet<string>.AlternateLookup<ReadOnlySpan<char>> queryTerms)
    {
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        Span<char> buffer = stackalloc char[64];
        var length = 0;
        var previous = '\0';
        for (var i = 0; i <= text.Length; i++)
        {
            var atEnd = i == text.Length;
            var current = atEnd ? '\0' : text[i];
            if (!atEnd && char.IsLetterOrDigit(current))
            {
                if (length > 0 && IsBoundary(previous, current))
                {
                    if (length <= buffer.Length && queryTerms.Contains(buffer[..length]))
                    {
                        return true;
                    }

                    length = 0;
                }

                if (length < buffer.Length)
                {
                    buffer[length] = char.ToLowerInvariant(current);
                }

                length++;
                previous = current;
            }
            else if (length > 0)
            {
                if (length <= buffer.Length && queryTerms.Contains(buffer[..length]))
                {
                    return true;
                }

                length = 0;
                previous = '\0';
            }
        }

        return false;
    }

    private static void AccumulateEntry(
        RepoContextEntryView entry,
        Dictionary<string, int>.AlternateLookup<ReadOnlySpan<char>> termLookup,
        int termCount,
        ref double[]? termFrequency,
        ref double documentLength,
        ref char[] buffer)
    {
        Accumulate(entry.Path, 3d, termLookup, termCount, ref termFrequency, ref documentLength, ref buffer);
        Accumulate(entry.FullyQualifiedName, 3d, termLookup, termCount, ref termFrequency, ref documentLength, ref buffer);
        Accumulate(entry.Topic, 2d, termLookup, termCount, ref termFrequency, ref documentLength, ref buffer);
        Accumulate(entry.Id, 1d, termLookup, termCount, ref termFrequency, ref documentLength, ref buffer);
        Accumulate(entry.Key, 1d, termLookup, termCount, ref termFrequency, ref documentLength, ref buffer);

        foreach (var tag in entry.Tags)
        {
            Accumulate(tag, 2d, termLookup, termCount, ref termFrequency, ref documentLength, ref buffer);
        }

        foreach (var (name, value) in entry.Fields)
        {
            var weight = FieldWeights.TryGetValue(name, out var configured) ? configured : 0d;
            if (weight > 0d)
            {
                Accumulate(value, weight, termLookup, termCount, ref termFrequency, ref documentLength, ref buffer);
            }
        }
    }

    private static void Accumulate(
        string? text,
        double weight,
        Dictionary<string, int>.AlternateLookup<ReadOnlySpan<char>> termLookup,
        int termCount,
        ref double[]? termFrequency,
        ref double documentLength,
        ref char[] buffer)
    {
        if (string.IsNullOrEmpty(text))
        {
            return;
        }

        var length = 0;
        var previous = '\0';
        for (var i = 0; i <= text.Length; i++)
        {
            var atEnd = i == text.Length;
            var current = atEnd ? '\0' : text[i];
            if (!atEnd && char.IsLetterOrDigit(current))
            {
                if (length > 0 && IsBoundary(previous, current))
                {
                    Emit(buffer, length, weight, termLookup, termCount, ref termFrequency, ref documentLength);
                    length = 0;
                }

                if (length == buffer.Length)
                {
                    Array.Resize(ref buffer, buffer.Length * 2);
                }

                buffer[length++] = char.ToLowerInvariant(current);
                previous = current;
            }
            else if (length > 0)
            {
                Emit(buffer, length, weight, termLookup, termCount, ref termFrequency, ref documentLength);
                length = 0;
                previous = '\0';
            }
        }
    }

    private static void Emit(
        char[] buffer,
        int length,
        double weight,
        Dictionary<string, int>.AlternateLookup<ReadOnlySpan<char>> termLookup,
        int termCount,
        ref double[]? termFrequency,
        ref double documentLength)
    {
        documentLength += weight;
        if (termLookup.TryGetValue(buffer.AsSpan(0, length), out var index))
        {
            termFrequency ??= new double[termCount];
            termFrequency[index] += weight;
        }
    }

    private static void FlushQueryToken(char[] buffer, int length, List<string> tokens, HashSet<string> seen)
    {
        var token = new string(buffer, 0, length);
        if (seen.Add(token))
        {
            tokens.Add(token);
        }
    }

    // A sub-token boundary inside a run of letters and digits: a camelCase hump
    // (lower-to-upper) or a letter/digit transition. Both arguments are known to be
    // letters or digits, so the digit test alone distinguishes a letter/digit edge.
    private static bool IsBoundary(char previous, char current)
    {
        if (char.IsLower(previous) && char.IsUpper(current))
        {
            return true;
        }

        return char.IsDigit(previous) != char.IsDigit(current);
    }
}
