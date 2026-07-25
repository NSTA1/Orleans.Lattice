using System.Runtime.CompilerServices;
using Orleans.Lattice.Primitives;

namespace Orleans.Lattice.Views;

/// <summary>
/// Default <see cref="ILatticeView"/>. Query methods resolve the maintainer's
/// <b>active generation</b> view tree (see <c>ViewMaintainerGrain.ShadowSwap</c>)
/// rather than a hard-coded <c>view-{name}</c> id, so reads automatically follow a
/// shadow-swap rebuild from the old fully-built generation to the new one. The
/// active tree id is cached for <see cref="LatticeViewOptions.ReadHandleCacheTtl"/>
/// to avoid a maintainer grain hop per read; during the brief post-swap window the
/// cache may still point at the prior generation, so a reader can serve
/// fully-built but slightly stale data, but never a half-built or empty tree. Lag,
/// rebuild, reconcile, and digest delegate to the per-view
/// <see cref="IViewMaintainerGrain"/>.
/// </summary>
internal sealed class LatticeView(
    string viewName,
    IGrainFactory grainFactory,
    IViewMaintainerGrain maintainer,
    TimeSpan readHandleCacheTtl,
    bool isAggregation = false) : ILatticeView
{
    private readonly object _gate = new();
    private ILattice? _cachedTree;
    private DateTime _cacheExpiresUtc = DateTime.MinValue;

    // Aggregation views keep internal accumulator / inverse / membership rows
    // under the reserved NUL prefix (see AggregationRowCodec). The view-facing
    // surface starts every unbounded scan above that range so readers see only
    // the materialised group values, never the internal rows.
    private string? ReservedFloor => isAggregation ? AggregationRowCodec.FirstNonReservedKey : null;

    /// <inheritdoc />
    public string ViewName { get; } = viewName;

    /// <summary>
    /// Resolves the active-generation view tree, refreshing the cached id from the
    /// maintainer once the per-handle TTL has elapsed. The TTL bounds how long a
    /// reader can keep serving the prior generation after a swap.
    /// </summary>
    private async ValueTask<ILattice> ResolveTreeAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (_cachedTree is not null && DateTime.UtcNow < _cacheExpiresUtc)
            {
                return _cachedTree;
            }
        }

        var treeId = await maintainer.GetActiveTreeIdAsync(cancellationToken);
        var tree = grainFactory.GetGrain<ILattice>(treeId);

        lock (_gate)
        {
            _cachedTree = tree;
            _cacheExpiresUtc = DateTime.UtcNow + readHandleCacheTtl;
        }

        return tree;
    }

    /// <summary>Drops the cached active-tree id so the next read re-resolves it immediately.</summary>
    private void InvalidateCache()
    {
        lock (_gate)
        {
            _cachedTree = null;
            _cacheExpiresUtc = DateTime.MinValue;
        }
    }

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(key);
        using var readScope = ViewReadContext.BeginScope();
        var tree = await ResolveTreeAsync(cancellationToken);
        return await tree.GetAsync(key, cancellationToken);
    }

    /// <inheritdoc />
    public async Task<int> CountAsync(CancellationToken cancellationToken = default)
    {
        using var readScope = ViewReadContext.BeginScope();
        var tree = await ResolveTreeAsync(cancellationToken);
        if (!isAggregation)
        {
            return await tree.CountAsync(cancellationToken);
        }

        // Count only the materialised group values, excluding the reserved
        // internal accumulator / inverse / membership rows under the NUL
        // prefix, so a group's accumulator shards never inflate the count.
        // The server-side ranged count over [ReservedFloor, null) reuses the
        // whole-tree count machinery (fully-covered leaves contribute their
        // full count; only the boundary leaf at ReservedFloor is
        // partial-counted) and ships only an integer, so no group-value keys
        // are materialised across the wire.
        return await tree.CountAsync(ReservedFloor, null, cancellationToken);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<string> KeysAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var readScope = ViewReadContext.BeginScope();
        var tree = await ResolveTreeAsync(cancellationToken);
        await foreach (var key in tree.KeysAsync(startInclusive ?? ReservedFloor, endExclusive, cancellationToken: cancellationToken))
        {
            yield return key;
        }
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<KeyValuePair<string, byte[]>> EntriesAsync(
        string? startInclusive = null,
        string? endExclusive = null,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        using var readScope = ViewReadContext.BeginScope();
        var tree = await ResolveTreeAsync(cancellationToken);
        await foreach (var entry in tree.EntriesAsync(startInclusive ?? ReservedFloor, endExclusive, cancellationToken: cancellationToken))
        {
            yield return entry;
        }
    }

    /// <inheritdoc />
    public Task<long> GetLagAsync(CancellationToken cancellationToken = default) =>
        maintainer.GetLagAsync(cancellationToken);

    /// <inheritdoc />
    public async Task RebuildAsync(CancellationToken cancellationToken = default)
    {
        await maintainer.RebuildAsync(cancellationToken);

        // An explicit rebuild swaps the active generation; drop the cached id so a
        // read immediately following this call observes the rebuilt generation
        // deterministically rather than waiting out the TTL.
        InvalidateCache();
    }

    /// <inheritdoc />
    public async Task<bool> ReconcileAsync(CancellationToken cancellationToken = default)
    {
        var repaired = await maintainer.ReconcileAsync(cancellationToken);

        // A repairing reconcile swaps in a new generation; refresh deterministically.
        InvalidateCache();
        return repaired;
    }

    /// <inheritdoc />
    public Task<ViewDigest> ComputeDigestAsync(CancellationToken cancellationToken = default) =>
        maintainer.ComputeViewDigestAsync(cancellationToken);

    /// <inheritdoc />
    public Task WaitForSourceHlcAsync(HybridLogicalClock target, TimeSpan timeout, CancellationToken cancellationToken = default) =>
        maintainer.WaitForSourceHlcAsync(target, timeout, cancellationToken);

    /// <inheritdoc />
    public Task WaitForSourceHeadAsync(TimeSpan timeout, CancellationToken cancellationToken = default) =>
        // Single maintainer round-trip: the grain captures the source head and
        // waits for the view to apply up to it in-process, instead of this
        // handle issuing a separate CaptureSourceHeadHlc then WaitForSourceHlc.
        maintainer.WaitForSourceHeadAsync(timeout, cancellationToken);
}
