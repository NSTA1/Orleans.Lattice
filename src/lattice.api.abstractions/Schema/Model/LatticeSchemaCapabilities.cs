using Orleans.Lattice;

namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// The read-only result of a schema-management capability probe: which
/// schema-management operations the current caller may perform over a single tree,
/// evaluated through the same fail-closed schema access gate the real operations use
/// but with <b>no side effects</b> - no policy is read or written, no data is
/// scanned. Every flag is default-deny: a flag is <see langword="true"/> only when
/// the gate would authorize the corresponding operation for the probed tree, and
/// <see langword="false"/> for any denial.
/// </summary>
/// <remarks>
/// <para>
/// The probe is a UX affordance for a management surface (for example the state
/// explorer's schema area) so it can disable controls the caller cannot use; it is
/// <b>not</b> a security boundary. The control facade still authorizes every real
/// operation fail-closed on attempt, so an over-optimistic client that acts on a
/// stale or wrong flag is still refused by the server.
/// </para>
/// <para>
/// The underlying gate distinguishes two capabilities for a tree: ordinary
/// <see cref="LatticeOperation.Read"/> authority (the inspect verbs and the
/// compliance audit) and the <see cref="LatticeOperation.SchemaAdmin"/>
/// schema-management authority (policy / version / remediation mutations). The
/// read-flag group (<see cref="CanViewPolicy"/>, <see cref="CanViewDeadLetters"/>,
/// <see cref="CanViewVersionConfig"/>, <see cref="CanViewRemediationStatus"/>,
/// <see cref="CanScanCompliance"/>) reflects the read grant; the manage-flag group
/// (<see cref="CanManagePolicy"/>, <see cref="CanManageVersion"/>,
/// <see cref="CanRemediate"/>) reflects the schema-admin grant.
/// </para>
/// </remarks>
[GenerateSerializer]
[Alias(ApiSchemaTypeAliases.LatticeSchemaCapabilities)]
[Immutable]
public sealed record LatticeSchemaCapabilities
{
    /// <summary>The tree id these capabilities were evaluated over.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary><see langword="true"/> when the caller may read the tree's enforcement policy.</summary>
    [Id(1)] public bool CanViewPolicy { get; init; }

    /// <summary><see langword="true"/> when the caller may list / count the tree's dead-letter entries.</summary>
    [Id(2)] public bool CanViewDeadLetters { get; init; }

    /// <summary><see langword="true"/> when the caller may read the tree's version config.</summary>
    [Id(3)] public bool CanViewVersionConfig { get; init; }

    /// <summary><see langword="true"/> when the caller may read the tree's remediation status.</summary>
    [Id(4)] public bool CanViewRemediationStatus { get; init; }

    /// <summary><see langword="true"/> when the caller may run a read-only compliance audit of the tree.</summary>
    [Id(5)] public bool CanScanCompliance { get; init; }

    /// <summary><see langword="true"/> when the caller may set / clear the tree's enforcement policy.</summary>
    [Id(6)] public bool CanManagePolicy { get; init; }

    /// <summary>
    /// <see langword="true"/> when the caller may change the tree's version config
    /// (set / advance / migrate / clear).
    /// </summary>
    [Id(7)] public bool CanManageVersion { get; init; }

    /// <summary><see langword="true"/> when the caller may start a background remediation of the tree.</summary>
    [Id(8)] public bool CanRemediate { get; init; }
}
