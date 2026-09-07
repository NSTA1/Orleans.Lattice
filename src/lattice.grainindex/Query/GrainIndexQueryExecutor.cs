using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

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

            foreach (var candidate in candidates.Survivors)
            {
                if (seen is null || seen.Add(candidate.Key))
                {
                    yield return candidate.Value.Match;
                }
            }
        }
    }

    private async Task<CandidateSet> IntersectAsync(
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
        var candidates = new CandidateSet();
        await foreach (var match in ScanAsync(clauses[0], pageSize, execution, payloads, cancellationToken).ConfigureAwait(false))
        {
            candidates.Seed(match);
        }

        for (var pass = 1; pass < clauses.Length && candidates.Count > 0; pass++)
        {
            // A later clause only has to answer "is this grain still a
            // candidate?", so it is scanned as raw entry keys and probed through
            // a span over the tree's own string. That keeps the per-entry cost of
            // an intersect pass to one dictionary probe and no allocation at all:
            // the grain-key substring and the match that used to carry it were
            // both built only to be thrown away on the overwhelming majority of
            // entries, which do not survive.
            var survivors = 0;
            await foreach (string entryKey in ScanEntryKeysAsync(clauses[pass], pageSize, execution, cancellationToken).ConfigureAwait(false))
            {
                if (candidates.Advance(entryKey, pass))
                {
                    survivors++;
                }
            }

            candidates.Prune(pass, survivors);
        }

        return candidates;
    }

    /// <summary>
    /// Scans one clause as raw entry keys, with no grain-key projection at all.
    /// <para>
    /// This is the intersect path's scan. It deliberately does not share
    /// <see cref="ScanAsync"/>'s enumerator: routing the key-only surface through
    /// a wrapping iterator would add a state-machine hop to every entry of the
    /// single-scan fast path, which is the shape most queries take, to save a few
    /// lines on the path that is only reached by a multi-clause AND.
    /// </para>
    /// </summary>
    private async IAsyncEnumerable<string> ScanEntryKeysAsync(
        GrainIndexScanClause clause,
        int pageSize,
        GrainIndexQueryExecution execution,
        [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var ranges = clause.Ranges;

        for (var i = 0; i < ranges.Length; i++)
        {
            var range = ranges[i];

            if (execution == GrainIndexQueryExecution.Stream)
            {
                var streamed = clause.Residual is { } streamPredicate
                    ? _tree.KeysWherePredicateAsync(streamPredicate, range.StartInclusive, range.EndExclusive, false, null, cancellationToken)
                    : _tree.KeysAsync(range.StartInclusive, range.EndExclusive, false, null, cancellationToken);

                await foreach (string key in streamed.WithCancellation(cancellationToken).ConfigureAwait(false))
                {
                    yield return key;
                }

                continue;
            }

            string cursorId = await OpenCursorAsync(clause, range, execution, payloads: false, cancellationToken)
                .ConfigureAwait(false);
            try
            {
                while (true)
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    var page = await _tree.NextKeysAsync(cursorId, pageSize, cancellationToken).ConfigureAwait(false);
                    var keys = page.Keys;
                    for (var k = 0; k < keys.Count; k++)
                    {
                        yield return keys[k];
                    }

                    if (!page.HasMore)
                        break;
                }
            }
            finally
            {
                await _tree.CloseCursorAsync(cursorId, CancellationToken.None).ConfigureAwait(false);
            }
        }
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
        if (!TryReadGrainKey(key, out ReadOnlySpan<char> span))
        {
            grainKey = string.Empty;
            return false;
        }

        grainKey = new string(span);
        return true;
    }

    /// <summary>
    /// Locates the grain key inside an entry key without materialising it, so a
    /// caller that only needs to look the grain up can probe with the span.
    /// </summary>
    private static bool TryReadGrainKey(string key, out ReadOnlySpan<char> grainKey)
    {
        int first = key.IndexOf(GrainIndexKeyEncoder.Separator);
        if (first < 0)
        {
            grainKey = default;
            return false;
        }

        int second = key.IndexOf(GrainIndexKeyEncoder.Separator, first + 1);
        if (second < 0)
        {
            grainKey = default;
            return false;
        }

        grainKey = key.AsSpan(second + 1);
        return true;
    }

    /// <summary>
    /// The driving clause's matches, carrying the pass number each one last
    /// survived so an intersect pass can prune in place instead of rebuilding the
    /// set. Probing goes through the dictionary's span alternate lookup, which is
    /// why the ordinal comparer is passed explicitly - the default comparer does
    /// not support one.
    /// </summary>
    private sealed class CandidateSet
    {
        private readonly Dictionary<string, Candidate> _map = new(StringComparer.Ordinal);
        private readonly Dictionary<string, Candidate>.AlternateLookup<ReadOnlySpan<char>> _lookup;

        internal CandidateSet() => _lookup = _map.GetAlternateLookup<ReadOnlySpan<char>>();

        /// <summary>How many grains are still candidates.</summary>
        internal int Count => _map.Count;

        /// <summary>The surviving candidates, keyed by grain key.</summary>
        internal Dictionary<string, Candidate> Survivors => _map;

        /// <summary>Records a driving-clause match as a candidate.</summary>
        internal void Seed(GrainIndexMatch match) => _map[match.GrainKey] = new Candidate(match);

        /// <summary>
        /// Marks the candidate named by <paramref name="entryKey"/> as having
        /// survived <paramref name="pass"/>, reporting whether it did.
        /// </summary>
        internal bool Advance(string entryKey, int pass)
        {
            if (!TryReadGrainKey(entryKey, out ReadOnlySpan<char> grainKey))
                return false;

            // One probe, and no string: the ref is the slot itself, so stamping
            // the pass does not cost a second hash and lookup the way reading the
            // value and writing it back would.
            ref var candidate = ref CollectionsMarshal.GetValueRefOrNullRef(_lookup, grainKey);
            if (Unsafe.IsNullRef(ref candidate) || candidate.LastPass != pass - 1)
                return false;

            candidate.LastPass = pass;
            return true;
        }

        /// <summary>
        /// Drops the candidates that did not survive <paramref name="pass"/>,
        /// given how many did.
        /// </summary>
        internal void Prune(int pass, int survivors)
        {
            if (survivors == _map.Count)
                return;

            if (survivors == 0)
            {
                _map.Clear();
                return;
            }

            // Removing during enumeration is supported on Dictionary<,>, so the
            // set is pruned in place - a multi-clause AND no longer allocates a
            // fresh survivor dictionary, and its rehash, on every pass.
            foreach (var pair in _map)
            {
                if (pair.Value.LastPass != pass)
                {
                    _map.Remove(pair.Key);
                }
            }
        }
    }

    /// <summary>One buffered candidate and the last intersect pass it survived.</summary>
    private struct Candidate(GrainIndexMatch match)
    {
        /// <summary>The driving clause's match, which is what gets reported.</summary>
        internal GrainIndexMatch Match = match;

        /// <summary>The index of the last clause this candidate matched.</summary>
        internal int LastPass = 0;
    }
}
