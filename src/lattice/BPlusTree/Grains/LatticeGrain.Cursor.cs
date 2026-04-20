namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Stateful cursor forwarding. Each <c>ILattice</c> cursor method
/// simply routes to a per-<c>{treeId}/{cursorId}</c>
/// <see cref="ILatticeCursorGrain"/> activation where the real work and
/// state persistence happens.
/// </summary>
internal sealed partial class LatticeGrain
{
    /// <inheritdoc />
    public async Task<string> OpenKeyCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var cursorId = Guid.NewGuid().ToString("N");
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        await cursor.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Keys,
            StartInclusive = startInclusive,
            EndExclusive = endExclusive,
            Reverse = reverse,
        });
        return cursorId;
    }

    /// <inheritdoc />
    public async Task<string> OpenEntryCursorAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        bool reverse = false,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        var cursorId = Guid.NewGuid().ToString("N");
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        await cursor.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.Entries,
            StartInclusive = startInclusive,
            EndExclusive = endExclusive,
            Reverse = reverse,
        });
        return cursorId;
    }

    /// <inheritdoc />
    public async Task<string> OpenDeleteRangeCursorAsync(string startInclusive, string endExclusive, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        cancellationToken.ThrowIfCancellationRequested();
        var cursorId = Guid.NewGuid().ToString("N");
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        await cursor.OpenAsync(TreeId, new LatticeCursorSpec
        {
            Kind = LatticeCursorKind.DeleteRange,
            StartInclusive = startInclusive,
            EndExclusive = endExclusive,
            Reverse = false,
        });
        return cursorId;
    }

    /// <inheritdoc />
    public Task<LatticeCursorKeysPage> NextKeysAsync(string cursorId, int pageSize, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cursorId);
        cancellationToken.ThrowIfCancellationRequested();
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        return cursor.NextKeysAsync(pageSize);
    }

    /// <inheritdoc />
    public Task<LatticeCursorEntriesPage> NextEntriesAsync(string cursorId, int pageSize, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cursorId);
        cancellationToken.ThrowIfCancellationRequested();
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        return cursor.NextEntriesAsync(pageSize);
    }

    /// <inheritdoc />
    public Task<LatticeCursorDeleteProgress> DeleteRangeStepAsync(string cursorId, int maxToDelete, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cursorId);
        cancellationToken.ThrowIfCancellationRequested();
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        return cursor.DeleteRangeStepAsync(maxToDelete);
    }

    /// <inheritdoc />
    public Task CloseCursorAsync(string cursorId, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cursorId);
        cancellationToken.ThrowIfCancellationRequested();
        var cursor = grainFactory.GetGrain<ILatticeCursorGrain>(BuildCursorKey(cursorId));
        return cursor.CloseAsync();
    }

    /// <summary>
    /// Builds the <c>{treeId}/{cursorId}</c> composite key used to address a
    /// cursor grain activation.
    /// </summary>
    private string BuildCursorKey(string cursorId) => $"{TreeId}/{cursorId}";
}
