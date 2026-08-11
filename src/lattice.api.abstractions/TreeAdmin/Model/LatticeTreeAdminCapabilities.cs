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
    /// authority (<see cref="LatticeOperation.Admin"/>) over the tree. The
    /// whole-tree lifecycle operations the dependent sub-issues add (create, drop,
    /// resize, reshard) gate on this grant, so a management surface can grey out the
    /// administration controls when it is <see langword="false"/>.
    /// </summary>
    [Id(1)] public bool CanAdministerTree { get; init; }

    /// <summary>
    /// The composed schema-management capabilities for the tree, delegated to the
    /// wrapped <see cref="ILatticeSchemaControl"/> facade. Never <see langword="null"/>:
    /// a caller with no schema grant sees an all-deny schema capability set for the
    /// tree.
    /// </summary>
    [Id(2)] public required LatticeSchemaCapabilities Schema { get; init; }
}
