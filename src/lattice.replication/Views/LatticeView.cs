using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Replication.Views;

/// <summary>
/// Default <see cref="ILatticeView"/>. Query methods delegate to the underlying
/// <c>view-{name}</c> <see cref="ILattice"/>; lag and rebuild delegate to the
/// per-view <see cref="IViewMaintainerGrain"/>.
/// </summary>
internal sealed class LatticeView(string viewName, ILattice viewTree, IViewMaintainerGrain maintainer) : ILatticeView
{
    /// <inheritdoc />
    public string ViewName { get; } = viewName;

    /// <inheritdoc />
    public Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        return viewTree.GetAsync(key, cancellationToken);
    }

    /// <inheritdoc />
    public Task<int> CountAsync(CancellationToken cancellationToken = default) =>
        viewTree.CountAsync(cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<string> KeysAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default) =>
        viewTree.KeysAsync(startInclusive, endExclusive, cancellationToken: cancellationToken);

    /// <inheritdoc />
    public IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(string? startInclusive = null, string? endExclusive = null, CancellationToken cancellationToken = default) =>
        viewTree.EntriesAsync(startInclusive, endExclusive, cancellationToken: cancellationToken);

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
