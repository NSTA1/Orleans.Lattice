using Orleans.Lattice;

namespace Orleans.Lattice.Api.Schema;

/// <summary>
/// The read-only result of a schema-management capability probe: caller permissions,
/// independently installed host features, and advisory operation readiness for one
/// tree. The probe does not mutate stored state or scan document values. When
/// versioning is installed and the caller has read permission, it reads the tree's
/// version configuration to determine migration readiness.
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
/// These existing flags describe permission only, not host support or readiness.
/// The <c>Supports*</c> flags report installed support independently of grants;
/// <see cref="CanStartMigration"/> and <see cref="CanStartRemediation"/> combine
/// support, authorization and known prerequisites. Starts still validate their
/// supplied arguments and current state before acceptance.
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

    /// <summary>Whether enforcement is installed, independently of caller grants.</summary>
    [Id(9)] public bool SupportsEnforcement { get; init; }
    /// <summary>Whether envelope versioning is installed.</summary>
    [Id(10)] public bool SupportsVersioning { get; init; }
    /// <summary>Whether the remediation engine is installed.</summary>
    [Id(11)] public bool SupportsRemediation { get; init; }
    /// <summary>Whether compliance scans are installed.</summary>
    [Id(12)] public bool SupportsCompliance { get; init; }
    /// <summary>Whether dead-letter inspection is installed.</summary>
    [Id(13)] public bool SupportsDeadLetters { get; init; }
    /// <summary>Whether durable runtime catalogs are installed.</summary>
    [Id(14)] public bool SupportsRuntimeRegistration { get; init; }
    /// <summary>Whether this caller can currently start a migration of this configured tree.</summary>
    [Id(15)] public bool CanStartMigration { get; init; }
    /// <summary>Whether this caller can start remediation with a supplied valid transform and policy.</summary>
    [Id(16)] public bool CanStartRemediation { get; init; }
    /// <summary>An actionable explanation when migration cannot currently start.</summary>
    [Id(17)] public string? MigrationUnavailableReason { get; init; }
    /// <summary>An actionable explanation when remediation cannot currently start.</summary>
    [Id(18)] public string? RemediationUnavailableReason { get; init; }
}
