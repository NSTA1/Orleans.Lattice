using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Ambient signal flagging that the current logical call is being driven
/// by the commit-log adapter rather than by a foreground caller. Library
/// internal: the WAL-first commit path on
/// <see cref="BPlusTree.Grains.BPlusLeafGrain"/> opens a scope around the
/// post-commit observer publish so a downstream replication-aware
/// observer can short-circuit and avoid re-appending its own input back
/// into the WAL (closing what would otherwise be a producer-consumer
/// loop).
/// </summary>
/// <remarks>
/// <para>
/// The signal flows on an Orleans <see cref="RequestContext"/> entry
/// keyed <c>"ol.cls"</c>. The flag is intentionally <em>not</em>
/// surfaced as a slot on <see cref="LatticeMutation"/> - observers that
/// need to honour the loop-prevention contract read
/// <see cref="Current"/> directly inside their
/// <see cref="IMutationObserver.OnMutationAsync"/> body, which runs
/// synchronously inside the originating commit''s request context.
/// </para>
/// <para>
/// Internal by design: the type is the contract between the core
/// library''s commit pipeline and the replication package''s built-in
/// observer, not a public extensibility surface. Third-party observers
/// that wish to participate consume <c>InternalsVisibleTo</c> on the
/// core assembly (the replication package does so) or fall back to the
/// public <see cref="LatticeMutation.OriginClusterId"/> +
/// <see cref="LatticeMutation.Category"/> heuristics.
/// </para>
/// </remarks>
internal static class LatticeCommitLogContext
{
    /// <summary>
    /// Gets a value indicating whether the current logical call is being
    /// driven by the commit-log adapter. Reads the ambient
    /// <see cref="RequestContext"/>; returns <see langword="false"/> when
    /// the key is unset (the common case for any direct foreground
    /// caller).
    /// </summary>
    public static bool Current =>
        RequestContext.Get(LatticeEventConstants.CommitLogSourceRequestContextKey) is bool flag && flag;

    /// <summary>
    /// Sets <see cref="Current"/> to <see langword="true"/> for the
    /// lifetime of the returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent.
    /// </summary>
    public static IDisposable BeginScope()
    {
        var previous = Current;
        RequestContext.Set(LatticeEventConstants.CommitLogSourceRequestContextKey, true);
        return new Scope(previous);
    }

    private sealed class Scope(bool previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            if (previous)
            {
                RequestContext.Set(LatticeEventConstants.CommitLogSourceRequestContextKey, true);
            }
            else
            {
                RequestContext.Remove(LatticeEventConstants.CommitLogSourceRequestContextKey);
            }
        }
    }
}
