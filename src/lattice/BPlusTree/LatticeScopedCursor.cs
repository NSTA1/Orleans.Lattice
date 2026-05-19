using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice;

/// <summary>
/// A thin <see cref="IAsyncDisposable"/> wrapper around a server-side
/// cursor ID returned by one of the <c>ILattice.Open*CursorAsync</c>
/// methods. Disposing the scope calls
/// <see cref="ILattice.CloseCursorAsync(string, CancellationToken)"/>
/// exactly once.
/// <para>
/// Scoped cursors are an ergonomic affordance for callers whose
/// cursor lifetime is bounded by a single stack frame; they do
/// <b>not</b> change the durability contract of the underlying cursor
/// grain. Callers that need to persist a cursor ID across process
/// restarts, hand it to a different process, or resume after a crash
/// should keep using the raw <c>Open*CursorAsync</c> /
/// <c>CloseCursorAsync</c> shape - see
/// <c>docs/lattice/durable-cursors.md</c> for the full lifecycle
/// story.
/// </para>
/// <para>
/// Idempotent: a second <see cref="DisposeAsync"/> is a no-op. The
/// underlying <see cref="ILattice.CloseCursorAsync(string, CancellationToken)"/>
/// is itself idempotent, so a redundant disposal at the end of a
/// using-block whose body already closed the cursor explicitly is
/// safe.
/// </para>
/// </summary>
public sealed class LatticeScopedCursor : IAsyncDisposable
{
    private readonly ILattice _lattice;
    private int _disposed;

    /// <summary>
    /// Initializes a new scoped cursor over <paramref name="cursorId"/>.
    /// Disposing the instance calls
    /// <see cref="ILattice.CloseCursorAsync(string, CancellationToken)"/>
    /// on <paramref name="lattice"/> exactly once.
    /// </summary>
    /// <param name="lattice">The tree the cursor was opened against. Not null.</param>
    /// <param name="cursorId">The server-assigned cursor ID. Not null.</param>
    public LatticeScopedCursor(ILattice lattice, string cursorId)
    {
        ArgumentNullException.ThrowIfNull(lattice);
        ArgumentNullException.ThrowIfNull(cursorId);
        _lattice = lattice;
        Id = cursorId;
    }

    /// <summary>
    /// The server-assigned opaque cursor ID. Pass this to
    /// <see cref="ILattice.NextKeysAsync"/>,
    /// <see cref="ILattice.NextEntriesAsync"/>, or
    /// <see cref="ILattice.DeleteRangeStepAsync"/> to advance the cursor.
    /// </summary>
    public string Id { get; }

    /// <summary>
    /// Implicit conversion to the raw cursor ID, so a
    /// <see cref="LatticeScopedCursor"/> can be passed directly to any
    /// <c>Next*Async</c> / <c>DeleteRangeStepAsync</c> overload that
    /// accepts a <see cref="string"/> cursor ID.
    /// </summary>
    /// <param name="scope">The scoped cursor.</param>
    public static implicit operator string(LatticeScopedCursor scope)
    {
        ArgumentNullException.ThrowIfNull(scope);
        return scope.Id;
    }

    /// <summary>
    /// Closes the underlying server-side cursor by calling
    /// <see cref="ILattice.CloseCursorAsync(string, CancellationToken)"/>.
    /// Idempotent: a second call is a no-op.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
        await _lattice.CloseCursorAsync(Id).ConfigureAwait(false);
    }
}

