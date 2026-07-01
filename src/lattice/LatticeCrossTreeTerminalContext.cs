using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Internal ambient signal used by the cross-tree atomic-write sub-saga
/// to communicate the enclosing operation's <c>operationId</c> and full
/// participant tree-id set down into the per-shard terminal-mutation
/// publish helpers, so each emitted cross-tree terminal
/// <see cref="LatticeMutation"/> / <c>WalRecord</c> carries
/// <see cref="LatticeMutation.CrossTreeOperationId"/> and
/// <see cref="LatticeMutation.CrossTreeParticipants"/> for the
/// receiver-side cross-tree visibility barrier.
/// </summary>
/// <remarks>
/// <para>
/// The sub-saga's <c>BroadcastTerminalsAsync</c> sets this ambient at the
/// head of each per-shard terminal call. The per-shard
/// <c>ShardRootGrain.AppendTxTerminalAsync</c> reads <see cref="Current"/>
/// while assembling the terminal record and stamps both slots. Single-tree
/// saga terminals and non-saga writes leave the ambient unset; the
/// receiver treats a null <c>CrossTreeOperationId</c> as "not a cross-tree
/// terminal" and routes the record through the legacy single-tree
/// per-shard gate.
/// </para>
/// </remarks>
internal static class LatticeCrossTreeTerminalContext
{
    /// <summary>
    /// Gets or sets the ambient cross-tree terminal metadata on the
    /// current <see cref="RequestContext"/>. Setting <c>null</c> (or a
    /// value with a null/empty <c>OperationId</c>) removes the entry,
    /// matching the "not a cross-tree terminal" default.
    /// </summary>
    public static CrossTreeTerminalInfo? Current
    {
        get
        {
            var raw = RequestContext.Get(LatticeEventConstants.CrossTreeTerminalRequestContextKey);
            return raw is CrossTreeTerminalInfo info ? info : null;
        }
        set
        {
            if (value is null
                || string.IsNullOrEmpty(value.Value.OperationId)
                || value.Value.Participants.Count == 0)
            {
                RequestContext.Remove(LatticeEventConstants.CrossTreeTerminalRequestContextKey);
            }
            else
            {
                RequestContext.Set(
                    LatticeEventConstants.CrossTreeTerminalRequestContextKey,
                    value.Value);
            }
        }
    }

    /// <summary>
    /// Stamps <paramref name="operationId"/> + <paramref name="participants"/>
    /// as the ambient cross-tree terminal metadata for the lifetime of the
    /// returned scope, restoring the prior value on
    /// <see cref="IDisposable.Dispose"/>. Safe to nest; disposal is
    /// idempotent. A null/empty <paramref name="operationId"/> or empty
    /// participant set explicitly clears the ambient.
    /// </summary>
    public static IDisposable With(string? operationId, IReadOnlyList<string>? participants)
    {
        var previous = Current;
        Current = string.IsNullOrEmpty(operationId) || participants is null || participants.Count == 0
            ? null
            : new CrossTreeTerminalInfo(operationId, participants);
        return new Scope(previous);
    }

    private sealed class Scope(CrossTreeTerminalInfo? previous) : IDisposable
    {
        private bool _disposed;

        public void Dispose()
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
            Current = previous;
        }
    }
}

/// <summary>
/// Ambient cross-tree terminal metadata: the coordinator
/// <c>operationId</c> and the canonical participant tree-id set carried
/// from a cross-tree sub-saga's terminal broadcast into the per-shard
/// terminal publish helpers.
/// </summary>
/// <remarks>
/// Carried as a value on the per-call <see cref="RequestContext"/>, so it
/// must be Orleans-serializable: Orleans deep-copies the request context on
/// every (even same-silo) grain call, and an unregistered type faults the
/// copier. <c>Immutable</c> is safe because the struct is never
/// mutated after construction.
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.CrossTreeTerminalInfo)]
internal readonly record struct CrossTreeTerminalInfo(
    [property: Id(0)] string OperationId,
    [property: Id(1)] IReadOnlyList<string> Participants);
