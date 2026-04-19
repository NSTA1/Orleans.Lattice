using System.ComponentModel;

namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// Stateful cursor / iterator coordinator (F-033). One grain activation per
/// open cursor, keyed by <c>{treeId}/{cursorId}</c>. Persists the scan spec
/// and last-yielded key after every page so a paused cursor survives silo
/// failovers and topology changes (shard splits). Resumption is transparent:
/// a new activation reads its persisted state and continues from the last
/// yielded key.
/// <para>
/// Ordering across resumptions is preserved because the continuation uses the
/// same <see cref="ILattice.KeysAsync"/> / <see cref="ILattice.EntriesAsync"/>
/// path bounded by the persisted last-yielded key — and those paths already
/// deliver strict global ordering under topology change (F-032).
/// </para>
/// <para>
/// Guarded: external callers must go through <see cref="ILattice"/>
/// (<c>OpenKeyCursorAsync</c>, <c>NextKeysAsync</c>, <c>CloseCursorAsync</c>,
/// etc.), which addresses the cursor grain on their behalf.
/// </para>
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
[Alias(TypeAliases.ILatticeCursorGrain)]
public interface ILatticeCursorGrain : IGrainWithStringKey
{
    /// <summary>
    /// Initializes the cursor with the given scan specification. Idempotent
    /// on the same <paramref name="spec"/> — a second open is a no-op and the
    /// cursor continues from its persisted progress. Throws
    /// <see cref="InvalidOperationException"/> if the cursor was opened with
    /// a different spec.
    /// </summary>
    /// <param name="treeId">The logical tree ID the cursor scans.</param>
    /// <param name="spec">The scan specification (range, direction, kind).</param>
    Task OpenAsync(string treeId, LatticeCursorSpec spec);

    /// <summary>
    /// Returns the next page of up to <paramref name="pageSize"/> keys.
    /// Valid only when the cursor was opened with
    /// <see cref="LatticeCursorKind.Keys"/>; throws
    /// <see cref="InvalidOperationException"/> otherwise.
    /// </summary>
    Task<LatticeCursorKeysPage> NextKeysAsync(int pageSize);

    /// <summary>
    /// Returns the next page of up to <paramref name="pageSize"/> entries.
    /// Valid only when the cursor was opened with
    /// <see cref="LatticeCursorKind.Entries"/>; throws
    /// <see cref="InvalidOperationException"/> otherwise.
    /// </summary>
    Task<LatticeCursorEntriesPage> NextEntriesAsync(int pageSize);

    /// <summary>
    /// Deletes up to <paramref name="maxToDelete"/> keys from the cursor's
    /// range and returns the resulting progress. Valid only when the cursor
    /// was opened with <see cref="LatticeCursorKind.DeleteRange"/>; throws
    /// <see cref="InvalidOperationException"/> otherwise.
    /// </summary>
    Task<LatticeCursorDeleteProgress> DeleteRangeStepAsync(int maxToDelete);

    /// <summary>
    /// Closes the cursor, clears its persisted state, and requests
    /// deactivation. Idempotent — calling on an already-closed cursor is a
    /// no-op.
    /// </summary>
    Task CloseAsync();

    /// <summary>
    /// Returns <c>true</c> while the cursor is open and able to yield more
    /// data (or idempotently accept further step calls).
    /// </summary>
    Task<bool> IsOpenAsync();
}
