using System.Runtime.CompilerServices;

namespace Orleans.Lattice.GrainIndex.Query;

/// <summary>
/// Runs a <see cref="GrainIndexQueryPlan"/> against an index tree.
/// <para>
/// The executor does no planning and inspects no expression: it walks the ranges
/// the planner produced, hands each clause's residual predicate to the tree's own
/// server-side push-down, and stitches the per-clause grain keys together. That
/// split is what keeps the per-entry cost to one key scan and one substring.
/// </para>
/// </summary>
internal sealed class GrainIndexQueryExecutor
{
    private static readonly byte[] NoPayload = [];

    private readonly ILattice _tree;

    internal GrainIndexQueryExecutor(ILattice tree) => _tree = tree;

    /// <summary>
    /// Streams the plan's matches, each grain once.
    /// </summary>
    /// <param name="plan">The planned query.</param>
    /// <param name="pageSize">Entries per round trip.</param>
    /// <param name="execution">How to walk the tree.</param>
    /// <param name="payloads">
    /// Whether the caller needs entry payloads. When <c>false</c> the scan uses
    /// the key-only surface, so no payload crosses the wire at all.
    /// </param>
    /// <param name="cancellationToken">Stops the scan.</param>
    internal async IAsyncEnumerable<GrainIndexMatch> ExecuteAsync(
        GrainIndexQueryPlan plan,
        int pageSize,
        GrainIndexQueryExecution execution,
        bool payloads,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (plan.IsProvablyEmpty)
            yield break;

        cancellationToken.ThrowIfCancellationRequested();

        if (plan.IsSingleScan)
        {
            // The common shape: one property, one clause. A grain contributes
            // exactly one entry per property and there is no second branch to
            // union with, so this streams with no de-duplication set and no
            // candidate buffer at all.
            var only = plan.Disjuncts[0].Clauses[0];
            await foreach (var match in ScanAsync(only, pageSize, execution, payloads, cancellationToken).ConfigureAwait(false))
            {
                yield return match;
            }

            yield break;
        }

        var disjuncts = plan.Disjuncts;

        // Only a union can produce the same grain twice, so the de-duplication
        // set is allocated only when there is more than one branch.
        var seen = disjuncts.Length > 1 ? new HashSet<string>(StringComparer.Ordinal) : null;

        for (var i = 0; i < disjuncts.Length; i++)
        {
            var clauses = disjuncts[i].Clauses;
            if (clauses.Length == 1)
            {
                await foreach (var match in ScanAsync(clauses[0], pageSize, execution, payloads, cancellationToken).ConfigureAwait(false))
                {
                    if (seen is null || seen.Add(match.GrainKey))
                    {
                        yield return match;
                    }
                }

                continue;
            }

            var candidates = await IntersectAsync(clauses, pageSize, execution, payloads, cancellationToken)
                .ConfigureAwait(false);

            foreach (var candidate in candidates)
            {
                if (seen is null || seen.Add(candidate.Key))
                {
                    yield return candidate.Value;
                }
            }
        }
    }

    private async Task<Dictionary<string, GrainIndexMatch>> IntersectAsync(
        GrainIndexScanClause[] clauses,
        int pageSize,
        GrainIndexQueryExecution execution,
        bool payloads,
        CancellationToken cancellationToken)
    {
        // Clauses arrive most selective first, so the narrowest scan is the one
        // that gets buffered and every later clause only shrinks the set. The
        // later clauses are key-only regardless of what the caller asked for:
        // their payloads are never reported, only their grain keys are.
        var candidates = new Dictionary<string, GrainIndexMatch>(StringComparer.Ordinal);
        await foreach (var match in ScanAsync(clauses[0], pageSize, execution, payloads, cancellationToken).ConfigureAwait(false))
        {
            candidates[match.GrainKey] = match;
        }

        for (var i = 1; i < clauses.Length && candidates.Count > 0; i++)
        {
            var survivors = new Dictionary<string, GrainIndexMatch>(StringComparer.Ordinal);
            await foreach (var match in ScanAsync(clauses[i], pageSize, execution, payloads: false, cancellationToken).ConfigureAwait(false))
            {
                if (candidates.TryGetValue(match.GrainKey, out var driving))
                {
                    survivors[match.GrainKey] = driving;
                }
            }

            candidates = survivors;
        }

        return candidates;
    }

    private async IAsyncEnumerable<GrainIndexMatch> ScanAsync(
        GrainIndexScanClause clause,
        int pageSize,
        GrainIndexQueryExecution execution,
        bool payloads,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var ranges = clause.Ranges;
        string property = clause.Property.Name;

        for (var i = 0; i < ranges.Length; i++)
        {
            var range = ranges[i];

            if (execution == GrainIndexQueryExecution.Stream)
            {
                await foreach (var match in StreamAsync(clause, range, property, payloads, cancellationToken).ConfigureAwait(false))
                {
                    yield return match;
                }

                continue;
            }

            string cursorId = await OpenCursorAsync(clause, range, execution, payloads, cancellationToken)
                .ConfigureAwait(false);
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    if (payloads)
                    {
                        var page = await _tree.NextEntriesAsync(cursorId, pageSize, cancellationToken).ConfigureAwait(false);
                        var entries = page.Entries;
                        for (var e = 0; e < entries.Count; e++)
                        {
                            var entry = entries[e];
                            if (TryReadGrainKey(entry.Key, out string grainKey))
                            {
                                yield return new GrainIndexMatch(grainKey, property, entry.Value);
                            }
                        }

                        if (!page.HasMore)
                            break;
                    }
                    else
                    {
                        var page = await _tree.NextKeysAsync(cursorId, pageSize, cancellationToken).ConfigureAwait(false);
                        var keys = page.Keys;
                        for (var k = 0; k < keys.Count; k++)
                        {
                            if (TryReadGrainKey(keys[k], out string grainKey))
                            {
                                yield return new GrainIndexMatch(grainKey, property, NoPayload);
                            }
                        }

                        if (!page.HasMore)
                            break;
                    }
                }
            }
            finally
            {
                // The cursor holds server-side state (and, in snapshot mode, a
                // pin against tombstone pruning), so it is closed even when the
                // consumer abandons the enumeration part-way.
                await _tree.CloseCursorAsync(cursorId, CancellationToken.None).ConfigureAwait(false);
            }
        }
    }

    private async IAsyncEnumerable<GrainIndexMatch> StreamAsync(
        GrainIndexScanClause clause,
        GrainIndexKeyRange range,
        string property,
        bool payloads,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (payloads)
        {
            var entries = clause.Residual is { } predicate
                ? _tree.EntriesWherePredicateAsync(predicate, range.StartInclusive, range.EndExclusive, false, null, cancellationToken)
                : _tree.EntriesAsync(range.StartInclusive, range.EndExclusive, false, null, cancellationToken);

            await foreach (var entry in entries.WithCancellation(cancellationToken).ConfigureAwait(false))
            {
                if (TryReadGrainKey(entry.Key, out string grainKey))
                {
                    yield return new GrainIndexMatch(grainKey, property, entry.Value);
                }
            }

            yield break;
        }

        var keys = clause.Residual is { } keyPredicate
            ? _tree.KeysWherePredicateAsync(keyPredicate, range.StartInclusive, range.EndExclusive, false, null, cancellationToken)
            : _tree.KeysAsync(range.StartInclusive, range.EndExclusive, false, null, cancellationToken);

        await foreach (string key in keys.WithCancellation(cancellationToken).ConfigureAwait(false))
        {
            if (TryReadGrainKey(key, out string grainKey))
            {
                yield return new GrainIndexMatch(grainKey, property, NoPayload);
            }
        }
    }

    private Task<string> OpenCursorAsync(
        GrainIndexScanClause clause,
        GrainIndexKeyRange range,
        GrainIndexQueryExecution execution,
        bool payloads,
        CancellationToken cancellationToken)
    {
        var residual = clause.Residual;
        string start = range.StartInclusive;
        string end = range.EndExclusive;

        if (execution == GrainIndexQueryExecution.SnapshotCursor)
        {
            if (payloads)
            {
                return residual is { } entryPredicate
                    ? _tree.OpenSnapshotEntryCursorWherePredicateAsync(entryPredicate, start, end, false, cancellationToken)
                    : _tree.OpenSnapshotEntryCursorAsync(start, end, false, cancellationToken);
            }

            return residual is { } keyPredicate
                ? _tree.OpenSnapshotKeyCursorWherePredicateAsync(keyPredicate, start, end, false, cancellationToken)
                : _tree.OpenSnapshotKeyCursorAsync(start, end, false, cancellationToken);
        }

        if (payloads)
        {
            return residual is { } durableEntryPredicate
                ? _tree.OpenEntryCursorWherePredicateAsync(durableEntryPredicate, start, end, false, false, cancellationToken)
                : _tree.OpenEntryCursorAsync(start, end, false, false, cancellationToken);
        }

        return residual is { } durableKeyPredicate
            ? _tree.OpenKeyCursorWherePredicateAsync(durableKeyPredicate, start, end, false, false, cancellationToken)
            : _tree.OpenKeyCursorAsync(start, end, false, false, cancellationToken);
    }

    /// <summary>
    /// Slices the grain key out of an entry key. The grain key is everything
    /// after the second separator, so one substring is enough - there is no need
    /// to split out the property name (the clause already knows it) or the
    /// encoded value (the query does not use it).
    /// </summary>
    private static bool TryReadGrainKey(string key, out string grainKey)
    {
        int first = key.IndexOf(GrainIndexKeyEncoder.Separator);
        if (first < 0)
        {
            grainKey = string.Empty;
            return false;
        }

        int second = key.IndexOf(GrainIndexKeyEncoder.Separator, first + 1);
        if (second < 0)
        {
            grainKey = string.Empty;
            return false;
        }

        grainKey = key[(second + 1)..];
        return true;
    }
}
