using System.Globalization;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Operations;

/// <summary>
/// The default <see cref="ILatticeOperationIndexGrain"/>. Keeps a tenant's
/// operations newest-first and bounded: finished entries are dropped once their
/// retention has elapsed, and past <see cref="LatticeOperationOptions.MaxIndexedOperations"/>
/// the oldest finished entries go first.
/// </summary>
internal sealed class LatticeOperationIndexGrain(
    IGrainContext context,
    [PersistentState("lattice-operation-index", LatticeOptions.StorageProviderName)]
    IPersistentState<LatticeOperationIndexState> state,
    IGrainFactory grainFactory,
    IOptions<LatticeOperationOptions> options) : IGrainBase, ILatticeOperationIndexGrain
{
    private const char TokenSeparator = ':';

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    /// <summary>The clock used for retention. Unit tests substitute a controllable one.</summary>
    internal TimeProvider Clock { get; set; } = TimeProvider.System;

    private List<LatticeOperationIndexEntry> Entries => state.State.Entries;

    /// <inheritdoc />
    public async Task AddAsync(string operationId, string kind, DateTimeOffset startedAtUtc)
    {
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        ArgumentException.ThrowIfNullOrEmpty(kind);

        if (Find(operationId) >= 0)
        {
            return;
        }

        var entry = new LatticeOperationIndexEntry
        {
            OperationId = operationId,
            Kind = kind,
            StartedAtUtc = startedAtUtc,
        };

        var position = 0;
        while (position < Entries.Count && !IsAfter(entry, Entries[position]))
        {
            position++;
        }

        Entries.Insert(position, entry);
        Prune();
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task MarkFinishedAsync(string operationId, DateTimeOffset finishedAtUtc)
    {
        var index = Find(operationId);
        if (index < 0)
        {
            return;
        }

        Entries[index].FinishedAtUtc = finishedAtUtc;
        Prune();
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task RemoveAsync(string operationId)
    {
        var index = Find(operationId);
        if (index < 0)
        {
            return;
        }

        Entries.RemoveAt(index);
        await state.WriteStateAsync();
    }

    /// <inheritdoc />
    public async Task<LatticeOperationIndexPage> ListAsync(string? kindPrefix, string? pageToken, int pageSize)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(pageSize);
        var after = ParseToken(pageToken);

        if (Prune())
        {
            await state.WriteStateAsync();
        }

        var ids = new List<string>(Math.Min(pageSize, Entries.Count));
        LatticeOperationIndexEntry? last = null;
        var more = false;
        foreach (var entry in Entries)
        {
            if (after is { } cursor && !IsAfterCursor(entry, cursor))
            {
                continue;
            }

            if (kindPrefix is not null && !entry.Kind.StartsWith(kindPrefix, StringComparison.Ordinal))
            {
                continue;
            }

            if (ids.Count == pageSize)
            {
                more = true;
                break;
            }

            ids.Add(entry.OperationId);
            last = entry;
        }

        var next = more && last is not null
            ? string.Create(CultureInfo.InvariantCulture, $"{last.StartedAtUtc.UtcTicks}{TokenSeparator}{last.OperationId}")
            : null;
        return new LatticeOperationIndexPage(ids, next);
    }

    /// <summary>
    /// Drops finished entries past their retention, then the oldest entries past the
    /// size cap (finished first). Pruned operations are asked to read themselves,
    /// which clears their own expired record; the call is not awaited so the index
    /// never waits on an operation grain that may itself be calling the index.
    /// </summary>
    /// <returns><see langword="true"/> when anything was dropped.</returns>
    private bool Prune()
    {
        var now = Clock.GetUtcNow();
        var retention = options.Value.Retention;
        var tenantId = TenantId();
        var dropped = false;

        for (var i = Entries.Count - 1; i >= 0; i--)
        {
            if (Entries[i].FinishedAtUtc is { } finished && finished + retention <= now)
            {
                Forget(tenantId, Entries[i].OperationId);
                Entries.RemoveAt(i);
                dropped = true;
            }
        }

        var cap = Math.Max(1, options.Value.MaxIndexedOperations);
        while (Entries.Count > cap)
        {
            var victim = Entries.FindLastIndex(static e => e.FinishedAtUtc is not null);
            if (victim < 0)
            {
                victim = Entries.Count - 1;
            }

            Entries.RemoveAt(victim);
            dropped = true;
        }

        return dropped;
    }

    private void Forget(string tenantId, string operationId) =>
        grainFactory.GetGrain<ILatticeOperationGrain>(LatticeOperationKey.For(tenantId, operationId))
            .GetAsync()
            .Ignore();

    private string TenantId() => LatticeOperationKey.ParseIndex(context.GrainId.Key.ToString()!);

    private int Find(string operationId) =>
        Entries.FindIndex(e => string.Equals(e.OperationId, operationId, StringComparison.Ordinal));

    /// <summary>Newest-first order: a later start sorts first, ties by ordinal id.</summary>
    private static bool IsAfter(LatticeOperationIndexEntry candidate, LatticeOperationIndexEntry existing) =>
        candidate.StartedAtUtc > existing.StartedAtUtc
        || (candidate.StartedAtUtc == existing.StartedAtUtc
            && string.CompareOrdinal(candidate.OperationId, existing.OperationId) < 0);

    private static bool IsAfterCursor(LatticeOperationIndexEntry entry, (long Ticks, string Id) cursor) =>
        entry.StartedAtUtc.UtcTicks < cursor.Ticks
        || (entry.StartedAtUtc.UtcTicks == cursor.Ticks && string.CompareOrdinal(entry.OperationId, cursor.Id) > 0);

    private static (long Ticks, string Id)? ParseToken(string? pageToken)
    {
        if (pageToken is null)
        {
            return null;
        }

        var separator = pageToken.IndexOf(TokenSeparator);
        if (separator <= 0
            || !long.TryParse(pageToken.AsSpan(0, separator), NumberStyles.None, CultureInfo.InvariantCulture, out var ticks)
            || !LatticeOperationKey.IsValid(pageToken[(separator + 1)..]))
        {
            throw new ArgumentException("The page token is not a valid operation-list continuation token.", nameof(pageToken));
        }

        return (ticks, pageToken[(separator + 1)..]);
    }
}
