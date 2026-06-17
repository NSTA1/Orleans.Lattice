using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Default <see cref="ILatticeView"/>. Query methods delegate to the underlying
/// <c>view-{name}</c> <see cref="ILattice"/>; lag and rebuild delegate to the
/// per-view <see cref="IViewMaintainerGrain"/>.
/// </summary>
internal sealed class LatticeView(string viewName, ILattice viewTree, IViewMaintainerGrain maintainer, bool isAggregation = false) : ILatticeView
{
    // Aggregation views keep internal accumulator / inverse / membership rows
    // under the reserved NUL prefix (see AggregationRowCodec). The view-facing
    // surface starts every unbounded scan above that range so readers see only
    // the materialised group values, never the internal rows.
    private string? ReservedFloor => isAggregation ? AggregationRowCodec.FirstNonReservedKey : null;

    /// <inheritdoc />
    public string ViewName { get; } = viewName;

    /// <inheritdoc />
    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return viewTree.GetAsync(key, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        if (!isAggregation)
        {
            return await viewTree.CountAsync(cancellationToken);
        }

        // Count only the materialised group values, excluding the reserved
        // internal rows, so a group's accumulator shards never inflate the count.
        var count = 0;
        await foreach (var _ in viewTree.KeysAsync(ReservedFloor, cancellationToken: cancellationToken))
        {
            count++;
        }

        return count;
    }

    /// <inheritdoc />
    public IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default) =>
        viewTree.KeysAsync(startInclusive ?? ReservedFloor, endExclusive, cancellationToken: cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default) =>
        viewTree.EntriesAsync(startInclusive ?? ReservedFloor, endExclusive, cancellationToken: cancellationToken);

    /// <inheritdoc />
    public Task<long> GetLagAsync(CancellationToken cancellationToken = default) =>
        maintainer.GetLagAsync(cancellationToken);

    /// <inheritdoc />
    public Task RebuildAsync(CancellationToken cancellationToken = default) =>
        maintainer.RebuildAsync(cancellationToken);

    /// <inheritdoc />
    public Task WaitForSourceHlcAsync(HybridLogicalClock target, TimeSpan timeout, CancellationToken cancellationToken = default) =>
        maintainer.WaitForSourceHlcAsync(target, timeout, cancellationToken);

    /// <inheritdoc />
    public async Task WaitForSourceHeadAsync(TimeSpan timeout, CancellationToken cancellationToken = default)
    {
        var head = await maintainer.CaptureSourceHeadHlcAsync(cancellationToken);
        await maintainer.WaitForSourceHlcAsync(head, timeout, cancellationToken);
    }
}
