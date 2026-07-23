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

    /// <summary>
    /// Administer a tree's <b>schema</b>: the schema-management control plane, as
    /// distinct from an ordinary data-plane mutation. Holding it authorizes the
    /// schema-management verbs - setting or changing the enforcement policy,
    /// advancing the schema version, triggering a background shadow-build
    /// remediation / migration, toggling strict-mode ingest, and replaying
    /// dead-letter entries - over the requested scope. It is a high-privilege
    /// capability that is deliberately <b>distinct</b> from <see cref="Admin"/>:
    /// holding <see cref="Admin"/> does not confer it, and holding it does not
    /// confer <see cref="Admin"/> or any data-plane capability. Inspecting schema
    /// state stays on <see cref="Read"/> - this capability gates schema
    /// <em>changes</em> only, never the read side. Granting it grants no other
    /// capability.
    /// </summary>
    SchemaAdmin = 2048,

    /// <summary>
    /// Read the cluster's operational <b>telemetry</b>: a <b>cluster-wide,
    /// scopeless</b> capability that is deliberately <b>distinct</b> from every
    /// other operation. Unlike the data-plane operations it does not attach to a
    /// tree, prefix, or key - it authorizes reading cluster-level telemetry as a
    /// whole - so it is never part of the data-plane <c>All</c> aggregate.
    /// Holding it grants <b>nothing else</b>: no data read, no administration, no
    /// schema or backup authority. Conversely <b>no</b> other operation confers
    /// it - not even <see cref="Admin"/> - so a full data-plane or administrative
    /// grant never silently exposes telemetry, and a telemetry grant never
    /// silently exposes data. It must be granted explicitly and on its own.
    /// </summary>
    Telemetry = 4096,

    /// <summary>
    /// Configure a tree's cross-cluster <b>replication</b> at runtime: the
    /// replication control plane, as distinct from an ordinary data-plane
    /// mutation. Holding it authorizes the replication-management verbs -
    /// enabling replication for a tree (fixing its wire merge mode), disabling
    /// it, and inspecting the runtime replicated-tree set - over the requested
    /// scope. It is a high-privilege capability that is deliberately
    /// <b>distinct</b> from <see cref="Admin"/>: holding <see cref="Admin"/>
    /// does not confer it, and holding it does not confer <see cref="Admin"/>,
    /// <see cref="Backup"/>, <see cref="SchemaAdmin"/>, or any data-plane
    /// capability. Enabling replication egresses a tree's data to another
    /// cluster, so it must be granted explicitly and on its own; no other
    /// operation - not even <see cref="Admin"/> - confers it, and it is never
    /// part of the data-plane <c>All</c> aggregate. Granting it grants
    /// <b>nothing else</b>.
    /// </summary>
    Replication = 8192,
}
