using System.Globalization;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Auth;

/// <summary>
/// The shared read path of the <see cref="AuthConstants.AdminAuditTree"/> tree, behind the
/// operator and tenant audit readers. Callers authorize before calling it; it reads under
/// the system origin, like every other access to the reserved tree.
/// </summary>
/// <remarks>
/// A page is the newest <c>pageSize</c> matches strictly older than the cursor, in
/// (<see cref="LatticeAdminAuditRecord.TimestampUtc"/>, <see cref="LatticeAdminAuditRecord.OperationId"/>)
/// descending order. The scan holds at most <c>pageSize + 1</c> final records in memory, so
/// a page costs one pass over the narrowed key range rather than a load of the whole trail.
/// Pending intents are few (in-flight calls only) and are read in full, so a call with both
/// a pending intent and its final record is returned once, as the final record. On a
/// single-partition read the final record of an admitted intent can sort beyond the narrowed
/// range (it is stamped when the call completes), so a second scan of the partition tail
/// resolves those intents before any is reported as pending.
/// </remarks>
internal static class AdminAuditReader
{
    /// <summary>Ordering of a page: newest first, then operation id descending (ordinal).</summary>
    private static readonly Comparer<LatticeAdminAuditRecord> NewestFirst = Comparer<LatticeAdminAuditRecord>.Create(static (a, b) =>
    {
        var byTime = b.TimestampUtc.UtcTicks.CompareTo(a.TimestampUtc.UtcTicks);
        return byTime != 0 ? byTime : string.CompareOrdinal(b.OperationId, a.OperationId);
    });

    /// <summary>Reads one page of the trail.</summary>
    /// <param name="grainFactory">The grain factory.</param>
    /// <param name="scope">The partition to read, or <see langword="null"/> for every partition.</param>
    /// <param name="includePending">Whether in-flight intents are returned as pending records.</param>
    /// <param name="from">Inclusive lower bound on the record timestamp, or <see langword="null"/>.</param>
    /// <param name="to">Exclusive upper bound on the record timestamp, or <see langword="null"/>.</param>
    /// <param name="matches">The caller's remaining filters (actor, action, outcome).</param>
    /// <param name="pageToken">The exclusive cursor of the previous page, or <see langword="null"/>.</param>
    /// <param name="pageSize">The page size, at least one.</param>
    /// <param name="cancellationToken">Cancels the scan.</param>
    /// <returns>The page, newest first, and the cursor of the next page or <see langword="null"/>.</returns>
    /// <exception cref="ArgumentException"><paramref name="pageToken"/> is malformed.</exception>
    internal static async Task<(IReadOnlyList<LatticeAdminAuditRecord> Records, string? NextPageToken)> ReadPageAsync(
        IGrainFactory grainFactory,
        string? scope,
        bool includePending,
        DateTimeOffset? from,
        DateTimeOffset? to,
        Func<LatticeAdminAuditRecord, bool> matches,
        string? pageToken,
        int pageSize,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(matches);
        ArgumentOutOfRangeException.ThrowIfLessThan(pageSize, 1);

        var cursor = ParseToken(pageToken);

        // Nothing has ever been audited on this cluster: do not create the tree by reading it.
        if (!await grainFactory.GetLatticeRegistry().ExistsAsync(AuthConstants.AdminAuditTree).ConfigureAwait(false))
        {
            return (Array.Empty<LatticeAdminAuditRecord>(), null);
        }

        bool Admits(LatticeAdminAuditRecord record)
        {
            if (scope is not null && !string.Equals(record.TargetScope, scope, StringComparison.Ordinal))
            {
                return false;
            }

            var ticks = record.TimestampUtc.UtcTicks;
            if ((from is { } f && ticks < f.UtcTicks) || (to is { } t && ticks >= t.UtcTicks))
            {
                return false;
            }

            if (cursor is { } c
                && (ticks > c.Ticks || (ticks == c.Ticks && string.CompareOrdinal(record.OperationId, c.OperationId) >= 0)))
            {
                return false;
            }

            return matches(record);
        }

        var tree = AdminAuditStore.Tree(grainFactory);
        var pending = new Dictionary<string, LatticeAdminAuditRecord>(StringComparer.Ordinal);
        var page = new SortedSet<LatticeAdminAuditRecord>(NewestFirst);
        var limit = pageSize + 1;

        using (LatticeAccessGateContext.EnterSystemOrigin())
        {
            if (includePending)
            {
                await foreach (var entry in tree
                    .ScanEntriesAsync(AdminAuditKeys.PendingPrefix, AdminAuditKeys.PendingEnd, cancellationToken: cancellationToken)
                    .ConfigureAwait(false))
                {
                    if (AdminAuditStore.Deserialize(entry.Value) is { } intent)
                    {
                        pending[intent.OperationId] = intent with { Outcome = LatticeAdminAuditOutcome.Pending };
                    }
                }
            }

            var (start, end) = FinalRange(scope, from, to, cursor);
            await foreach (var entry in tree
                .ScanEntriesAsync(start, end, cancellationToken: cancellationToken)
                .ConfigureAwait(false))
            {
                if (entry.Key.StartsWith(AdminAuditKeys.PendingPrefix, StringComparison.Ordinal)
                    || AdminAuditStore.Deserialize(entry.Value) is not { } record
                    || record.Outcome == LatticeAdminAuditOutcome.Pending)
                {
                    continue;
                }

                pending.Remove(record.OperationId);
                if (Admits(record))
                {
                    Add(page, record, limit);
                }
            }

            if (scope is not null && end is not null && pending.Count > 0)
            {
                await DropResolvedIntentsAsync(tree, scope, end, pending, Admits, cancellationToken).ConfigureAwait(false);
            }
        }

        foreach (var intent in pending.Values)
        {
            if (Admits(intent))
            {
                Add(page, intent, limit);
            }
        }

        if (page.Count <= pageSize)
        {
            return (page.ToArray(), null);
        }

        var records = new LatticeAdminAuditRecord[pageSize];
        var i = 0;
        foreach (var record in page)
        {
            if (i == pageSize)
            {
                break;
            }

            records[i++] = record;
        }

        return (records, FormatToken(records[^1]));
    }

    /// <summary>Formats the cursor that resumes after <paramref name="last"/>.</summary>
    /// <param name="last">The last record of a page.</param>
    internal static string FormatToken(LatticeAdminAuditRecord last) =>
        string.Create(CultureInfo.InvariantCulture, $"{last.TimestampUtc.UtcTicks:D19}{AdminAuditKeys.Separator}{last.OperationId}");

    private static void Add(SortedSet<LatticeAdminAuditRecord> page, LatticeAdminAuditRecord record, int limit)
    {
        page.Add(record);
        if (page.Count > limit)
        {
            page.Remove(page.Max!);
        }
    }

    private static (long Ticks, string OperationId)? ParseToken(string? pageToken)
    {
        if (pageToken is null)
        {
            return null;
        }

        var separator = pageToken.IndexOf(AdminAuditKeys.Separator);
        if (separator != 19
            || separator == pageToken.Length - 1
            || !long.TryParse(pageToken.AsSpan(0, 19), NumberStyles.None, CultureInfo.InvariantCulture, out var ticks))
        {
            throw new ArgumentException("The audit page token is malformed.", nameof(pageToken));
        }

        return (ticks, pageToken[(separator + 1)..]);
    }

    /// <summary>
    /// The key range of the final records to scan. A single partition is narrowed to the
    /// time window and the cursor; every partition is scanned whole (pending keys skipped),
    /// because the keys of different partitions interleave in time.
    /// </summary>
    private static (string? Start, string? End) FinalRange(
        string? scope,
        DateTimeOffset? from,
        DateTimeOffset? to,
        (long Ticks, string OperationId)? cursor)
    {
        if (scope is null)
        {
            return (null, null);
        }

        var prefix = AdminAuditKeys.ScopeStart(scope);
        var start = from is { } f
            ? string.Create(CultureInfo.InvariantCulture, $"{prefix}{f.UtcTicks:D19}")
            : prefix;

        var end = AdminAuditKeys.ScopeEnd(scope);
        if (to is { } t)
        {
            end = Min(end, string.Create(CultureInfo.InvariantCulture, $"{prefix}{t.UtcTicks:D19}"));
        }

        if (cursor is { } c)
        {
            end = Min(end, string.Create(CultureInfo.InvariantCulture, $"{prefix}{c.Ticks:D19}{AdminAuditKeys.Separator}{c.OperationId}"));
        }

        return (start, end);
    }

    /// <summary>
    /// Drops the intents whose final record lies beyond the narrowed range of a single-partition
    /// read. A final record is stamped when the call completes, so it can sort after the
    /// <c>To</c> bound or the cursor while its intent sorts inside them; without this pass such
    /// a call would surface as pending although it has finished. Only the partition tail from
    /// the oldest admitted intent onward is scanned, and the scan stops once every admitted
    /// intent is resolved.
    /// </summary>
    private static async Task DropResolvedIntentsAsync(
        ILattice tree,
        string scope,
        string narrowedEnd,
        Dictionary<string, LatticeAdminAuditRecord> pending,
        Func<LatticeAdminAuditRecord, bool> admits,
        CancellationToken cancellationToken)
    {
        var unresolved = 0;
        long oldest = long.MaxValue;
        foreach (var intent in pending.Values)
        {
            if (admits(intent))
            {
                unresolved++;
                oldest = Math.Min(oldest, intent.TimestampUtc.UtcTicks);
            }
        }

        if (unresolved == 0)
        {
            return;
        }

        var start = Max(narrowedEnd, string.Create(CultureInfo.InvariantCulture, $"{AdminAuditKeys.ScopeStart(scope)}{oldest:D19}"));
        var scopeEnd = AdminAuditKeys.ScopeEnd(scope);
        if (string.CompareOrdinal(start, scopeEnd) >= 0)
        {
            return;
        }

        await foreach (var entry in tree
            .ScanEntriesAsync(start, scopeEnd, cancellationToken: cancellationToken)
            .ConfigureAwait(false))
        {
            if (AdminAuditStore.Deserialize(entry.Value) is not { } record
                || record.Outcome == LatticeAdminAuditOutcome.Pending
                || !pending.TryGetValue(record.OperationId, out var intent))
            {
                continue;
            }

            pending.Remove(record.OperationId);
            if (admits(intent) && --unresolved == 0)
            {
                return;
            }
        }
    }

    private static string Min(string a, string b) => string.CompareOrdinal(a, b) <= 0 ? a : b;

    private static string Max(string a, string b) => string.CompareOrdinal(a, b) >= 0 ? a : b;
}
