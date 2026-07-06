namespace Orleans.Lattice;

/// <summary>
/// The data-plane operation an access-gate authorizes for a single logical
/// call. Modelled as flags so a composite request (for example an atomic
/// write that both writes and deletes keys) can carry the union of the
/// capabilities it exercises, and a future policy can grant a caller a set of
/// capabilities in one mask.
/// </summary>
/// <remarks>
/// This is in-process request vocabulary consumed by an
/// <see cref="ILatticeAccessGate"/>. It is never persisted or sent on the wire
/// by the core library, so it carries no Orleans serialization attributes.
/// </remarks>
[Flags]
public enum LatticeOperation
{
    /// <summary>No operation. The default; used to represent an unset mask.</summary>
    None = 0,

    /// <summary>Read a single key's value.</summary>
    Read = 1,

    /// <summary>Write (insert or update) a single key's value.</summary>
    Write = 2,

    /// <summary>Delete a single key.</summary>
    Delete = 4,

    /// <summary>Read a contiguous key range.</summary>
    RangeRead = 8,

    /// <summary>Delete a contiguous key range.</summary>
    RangeDelete = 16,

    /// <summary>Apply a CRDT delta / merge to a key.</summary>
    CrdtApply = 32,

    /// <summary>
    /// Initiate a multi-key / cross-tree atomic write. This is the capability
    /// to <em>start</em> an atomic batch or cross-tree transaction; it does not
    /// by itself authorize the individual key mutations the batch performs.
    /// Each leg of the batch is <b>additionally</b> authorized by its own
    /// <see cref="Write"/> / <see cref="Delete"/> capability when enforcement is
    /// wired in, so an atomic write requires both the <see cref="AtomicWrite"/>
    /// capability to initiate it and the per-leg capability for every key it
    /// touches.
    /// </summary>
    AtomicWrite = 64,

    /// <summary>Bulk-load / snapshot-restore a tree's contents in one call.</summary>
    BulkLoad = 128,

    /// <summary>
    /// Administrative / lifecycle operation on a tree (for example create,
    /// drop, or reconfigure) that is not an ordinary data read or write.
    /// </summary>
    Admin = 256,

    /// <summary>
    /// Capture (back up) the entire authorized scope of a tree, prefix, or key.
    /// A high-privilege read capability that is deliberately <b>distinct</b> from
    /// <see cref="Read"/> / <see cref="RangeRead"/>: holding it authorizes reading
    /// the whole requested scope for capture, and by design it bypasses the
    /// per-key read key-filter that an ordinary read honours, so a partial read
    /// grant never silently narrows a backup. Granting it does not grant any
    /// other capability.
    /// </summary>
    Backup = 512,

    /// <summary>
    /// Author / bulk-load a captured backup into a target tree, prefix, or key.
    /// This capability <b>subsumes</b> the target-scope write / bulk-load
    /// authority: holding it authorizes populating the scope from a backup, so no
    /// separate <see cref="Write"/> or <see cref="BulkLoad"/> grant is required to
    /// restore into that scope.
    /// </summary>
    Restore = 1024,
}
