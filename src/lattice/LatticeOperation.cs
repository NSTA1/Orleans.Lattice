namespace Orleans.Lattice;

/// <summary>
/// The operation an access-gate authorizes for a single logical call: a
/// data-plane read or write, or one of the control-plane and cluster-wide
/// capabilities below. Modelled as flags so a composite request (for example
/// an atomic write that both writes and deletes keys) can carry the union of
/// the capabilities it exercises, and a policy can grant a caller a set of
/// capabilities in one mask.
/// </summary>
/// <remarks>
/// This is the request vocabulary consumed by an
/// <see cref="ILatticeAccessGate"/>. It carries no Orleans serialization
/// attributes of its own, but it is not in-process only: the core library's
/// <see cref="LatticeAuthorizationDeniedException"/> and
/// <see cref="LatticeWriteRejectedException"/> serialize the operation they
/// carry when they travel back to a caller, and add-on packages persist
/// operation masks with the Orleans serializer (for example an installed
/// app's consented capability ceiling). Its numeric values are therefore wire
/// and storage format and must never be renumbered.
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
    /// Administrative operation on a tree that is not an ordinary data read or
    /// write (for example snapshot, merge, compaction, a leaf-projection
    /// rebuild, or reconfiguring per-tree settings). Destructive or structural
    /// lifecycle verbs - dropping, recovering or purging a tree, reshard,
    /// resize and its undo, orphaned-leaf repair, and WAL placement moves -
    /// require <see cref="TreeLifecycle"/>
    /// instead, which this capability does not confer.
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

    /// <summary>
    /// Perform an <b>irreversible or structural whole-tree lifecycle</b>
    /// operation: dropping, recovering, or purging a tree, changing its shard
    /// count or topology (reshard), changing its B+ node capacity (resize) or
    /// undoing that change, unsplicing orphaned leaves, or moving its
    /// write-ahead-log placement and reclaiming the moved-away source. These
    /// are the highest-blast-radius verbs the
    /// tree-administration control plane exposes, so this capability is
    /// deliberately <b>distinct</b> from <see cref="Admin"/>: holding
    /// <see cref="Admin"/> does not confer it, and holding it does not confer
    /// <see cref="Admin"/>, <see cref="Backup"/>, <see cref="Restore"/>,
    /// <see cref="SchemaAdmin"/>, or any data-plane capability. Routine
    /// administration (create / alias / reconfigure) stays on
    /// <see cref="Admin"/> (an existence probe needs only a whole-tree read); a
    /// destructive or structural rebuild requires this bit,
    /// granted explicitly and on its own so a cluster-wide administration grant
    /// never silently authorizes destroying or rebuilding a tree. It is never part
    /// of the data-plane <c>All</c> aggregate. Granting it grants <b>nothing
    /// else</b>.
    /// </summary>
    TreeLifecycle = 16384,

    /// <summary>
    /// Install, upgrade, enable, disable, or uninstall an <b>app</b> on the
    /// cluster, including re-consenting an installed version's capability
    /// ceiling and reconciling its grants: a
    /// <b>cluster-wide, scopeless</b> capability, granted over
    /// <c>LatticeScope.ClusterWide()</c> exactly as <see cref="Telemetry"/>
    /// is. It does not attach to a tree, prefix, or key - it authorizes changing
    /// the cluster's installed apps and their lifecycle as a whole. The app
    /// lifecycle control surface also requires it to list or describe apps and
    /// read their consent, because an install record carries the app's consented
    /// ceiling and role bindings. A scopeless capability is
    /// evaluated against the cluster-wide scope, so a collision between that scope
    /// and a real tree id is harmless: scopeless capability bits never overlap the
    /// data-plane operation bits, so a data-plane grant over such a tree can never
    /// confer this capability and this capability can never confer data access.
    /// It is deliberately <b>distinct</b> from <see cref="Admin"/>: holding
    /// <see cref="Admin"/> does not confer it, and holding it does not confer
    /// <see cref="Admin"/>, <see cref="TreeLifecycle"/>, or any data-plane
    /// capability. It is never part of the data-plane <c>All</c> aggregate, so it
    /// must be granted explicitly and on its own. Granting it grants <b>nothing
    /// else</b>.
    /// </summary>
    AppInstall = 32768,
}
