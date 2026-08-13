using Orleans.Lattice;
using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The read-only result of a tree-administration capability probe: which
/// whole-tree administration operations the current caller may perform over a
/// single tree, evaluated through the same fail-closed access gates the real
/// operations use but with <b>no side effects</b>. Every flag is default-deny: a
/// flag is <see langword="true"/> only when the gate would authorize the
/// corresponding operation for the probed tree, and <see langword="false"/> for
/// any denial.
/// </summary>
/// <remarks>
/// <para>
/// This is the foundation capability payload for the tree-administration facade
/// (<see cref="ILatticeTreeAdmin"/>). At this scaffolding stage the facade owns no
/// operations of its own; it presents one coherent surface by <b>composition</b> -
/// it wraps the existing schema control facade (<see cref="ILatticeSchemaControl"/>)
/// by delegation rather than re-implementing it - so this capability report embeds
/// the composed <see cref="Schema"/> capabilities alongside a whole-tree
/// administration grant flag. As the dependent sub-issues land their operations
/// (bulk-load, delete, resize, reshard, and the rest), each adds its own probe flag
/// here.
/// </para>
/// <para>
/// The probe is a UX affordance for a management surface so it can disable controls
/// the caller cannot use; it is <b>not</b> a security boundary. The control facade
/// still authorizes every real operation fail-closed on attempt, so an
/// over-optimistic client that acts on a stale or wrong flag is still refused by the
/// server.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.LatticeTreeAdminCapabilities)]
[Immutable]
public sealed record LatticeTreeAdminCapabilities
{
    /// <summary>The tree id these capabilities were evaluated over.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller holds whole-tree administration
    /// authority (<see cref="LatticeOperation.Admin"/>) over the tree. This is the
    /// <b>routine</b> administration grant: it covers the non-destructive lifecycle
    /// verbs (create / exists / alias / reconfigure) and schema-management
    /// delegation, so a management surface can grey out those administration
    /// controls when it is <see langword="false"/>. It does <b>not</b> cover the
    /// irreversible / structural operations (drop, resize, reshard, WAL move),
    /// which report separately through <see cref="CanManageTreeLifecycle"/>.
    /// </summary>
    [Id(1)] public bool CanAdministerTree { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller holds the distinct whole-tree
    /// <see cref="LatticeOperation.TreeLifecycle"/> capability over the tree - the
    /// authority for the <b>irreversible or structural</b> lifecycle operations
    /// (drop / purge, reshard, resize, WAL placement move). This is deliberately
    /// separate from <see cref="CanAdministerTree"/>: holding routine administration
    /// authority does not confer it, so a management surface can grey out the
    /// destructive / structural controls independently of the ordinary
    /// administration controls when it is <see langword="false"/>.
    /// </summary>
    [Id(4)] public bool CanManageTreeLifecycle { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller may read the tree's administrative
    /// diagnostics (<see cref="LatticeOperation.Read"/> over the whole tree): shard
    /// hotness, shard diagnostics, the shard-map topology, per-shard projection
    /// digests, and rolled-up tree statistics. A management surface can grey out the
    /// read-only diagnostics panels when it is <see langword="false"/>.
    /// </summary>
    [Id(3)] public bool CanViewDiagnostics { get; init; }

    /// <summary>
    /// The composed schema-management capabilities for the tree, delegated to the
    /// wrapped <see cref="ILatticeSchemaControl"/> facade. Never <see langword="null"/>:
    /// a caller with no schema grant sees an all-deny schema capability set for the
    /// tree.
    /// </summary>
    [Id(2)] public required LatticeSchemaCapabilities Schema { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller holds the whole-tree
    /// <see cref="LatticeOperation.BulkLoad"/> capability over the tree - the
    /// authority to stream a bottom-up bulk-load (tree creation) into it. This is a
    /// distinct grant from routine administration (<see cref="CanAdministerTree"/>)
    /// and from the destructive lifecycle grant (<see cref="CanManageTreeLifecycle"/>),
    /// so a management surface can grey out the bulk-load control independently.
    /// </summary>
    [Id(5)] public bool CanBulkLoad { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller holds the whole-tree
    /// <see cref="LatticeOperation.Restore"/> capability over the tree <b>and</b> a
    /// backup/restore engine is registered on the cluster - the authority to restore
    /// a captured backup into the tree (and to revert that restore) through
    /// <see cref="ILatticeTreeAdmin.RestoreTreeAsync"/>. This is a distinct grant from
    /// routine administration (<see cref="CanAdministerTree"/>), the destructive
    /// lifecycle grant (<see cref="CanManageTreeLifecycle"/>), and the bulk-load grant
    /// (<see cref="CanBulkLoad"/>), so a management surface can grey out the restore
    /// control independently. It is <see langword="false"/> when no restore engine is
    /// available, so the surface never offers a restore the cluster cannot serve.
    /// </summary>
    [Id(6)] public bool CanRestore { get; init; }
}
