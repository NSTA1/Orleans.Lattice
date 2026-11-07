using Orleans.Runtime;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Ambient id of the leaf whose WAL replay is issuing commit-log slice reads
/// (issue #5877). The partition's <see cref="LeafReplayCoordinatorGrain"/>
/// reads it to record how far each reader has got, so the replay page cache
/// keeps the pages a reader will read next. Advisory only: a read with no
/// reader is served exactly as one with a reader.
/// </summary>
/// <remarks>
/// Flows through Orleans <see cref="RequestContext"/> keyed
/// <see cref="LatticeEventConstants.ReplayReaderRequestContextKey"/>.
/// </remarks>
internal static class LeafReplayReaderContext
{
    /// <summary>The current reader, or <see langword="null"/> outside a replay scope.</summary>
    public static string? Current
        => RequestContext.Get(LatticeEventConstants.ReplayReaderRequestContextKey) as string;

    /// <summary>
    /// Marks the ambient context with <paramref name="reader"/> for the
    /// lifetime of the returned scope, restoring the prior value on disposal.
    /// </summary>
    public static IDisposable BeginScope(string reader)
    {
        ArgumentNullException.ThrowIfNull(reader);
        var previous = Current;
        RequestContext.Set(LatticeEventConstants.ReplayReaderRequestContextKey, reader);
        return new Scope(previous);
    }

    private sealed class Scope(string? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;
            if (previous is null)
                RequestContext.Remove(LatticeEventConstants.ReplayReaderRequestContextKey);
            else
                RequestContext.Set(LatticeEventConstants.ReplayReaderRequestContextKey, previous);
        }
    }
}
