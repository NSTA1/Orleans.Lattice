using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// What the caller may do with one tree's schema, from the facade's side-effect
/// free capability probe. Every flag defaults to "no": a probe that failed, was
/// refused or answered nothing grants nothing. The flags are advisory - the
/// cluster still authorizes every real operation - so they only decide which
/// controls are offered.
/// </summary>
/// <param name="ViewPolicy">May read the enforcement policy.</param>
/// <param name="ManagePolicy">May set or clear the enforcement policy.</param>
/// <param name="ViewVersion">May read the envelope-version config.</param>
/// <param name="ManageVersion">May set, clear or advance the version config, and migrate.</param>
/// <param name="ViewRemediation">May read the remediation status.</param>
/// <param name="Remediate">May start a remediation.</param>
/// <param name="ScanCompliance">May run the read-only compliance scan.</param>
/// <param name="ViewDeadLetters">May count and list the dead letters.</param>
internal sealed record SchemaGrants(
    bool ViewPolicy,
    bool ManagePolicy,
    bool ViewVersion,
    bool ManageVersion,
    bool ViewRemediation,
    bool Remediate,
    bool ScanCompliance,
    bool ViewDeadLetters)
{
    /// <summary>The grants of a caller who may do nothing.</summary>
    public static SchemaGrants None { get; } = new(false, false, false, false, false, false, false, false);

    /// <summary>Whether the server granted any schema authority before installed-support filtering.</summary>
    public bool HasAuthority { get; init; }

    /// <summary>Whether policy enforcement is installed, not whether the caller is authorized.</summary>
    public bool SupportsEnforcement { get; init; }

    /// <summary>Whether versioning is installed.</summary>
    public bool SupportsVersioning { get; init; }

    /// <summary>Whether remediation is installed.</summary>
    public bool SupportsRemediation { get; init; }

    /// <summary>Whether compliance scans are installed.</summary>
    public bool SupportsCompliance { get; init; }

    /// <summary>Whether dead-letter inspection is installed.</summary>
    public bool SupportsDeadLetters { get; init; }

    /// <summary>Whether a durable runtime version catalog can be administered.</summary>
    public bool SupportsRuntimeRegistration { get; init; }

    /// <summary>Whether migration prerequisites and authority currently admit a start.</summary>
    public bool CanStartMigration { get; init; }

    /// <summary>The reason migration is not currently ready.</summary>
    public string? MigrationUnavailableReason { get; init; }

    /// <summary>Whether remediation prerequisites and authority currently admit a start.</summary>
    public bool CanStartRemediation { get; init; }

    /// <summary>The reason remediation is not currently ready.</summary>
    public string? RemediationUnavailableReason { get; init; }

    /// <summary>Whether any capability is granted.</summary>
    public bool HasAny =>
        ViewPolicy || ManagePolicy || ViewVersion || ManageVersion || ViewRemediation || Remediate || ScanCompliance || ViewDeadLetters;

    /// <summary>Whether any change is granted, so the caller is more than a reader.</summary>
    public bool CanChangeAnything => ManagePolicy || ManageVersion || Remediate;

    /// <summary>Projects the facade's probe result; <see langword="null"/> grants nothing.</summary>
    /// <param name="capabilities">The probe result.</param>
    /// <returns>The grants.</returns>
    public static SchemaGrants From(LatticeSchemaCapabilities? capabilities) => capabilities is null
        ? None
        : new(
            capabilities.CanViewPolicy && capabilities.SupportsEnforcement,
            capabilities.CanManagePolicy && capabilities.SupportsEnforcement,
            capabilities.CanViewVersionConfig && capabilities.SupportsVersioning,
            capabilities.CanManageVersion && capabilities.SupportsVersioning,
            capabilities.CanViewRemediationStatus && capabilities.SupportsRemediation,
            capabilities.CanRemediate && capabilities.SupportsRemediation,
            capabilities.CanScanCompliance && capabilities.SupportsCompliance,
            capabilities.CanViewDeadLetters && capabilities.SupportsDeadLetters)
        {
            HasAuthority = capabilities.CanViewPolicy || capabilities.CanManagePolicy
                || capabilities.CanViewVersionConfig || capabilities.CanManageVersion
                || capabilities.CanViewRemediationStatus || capabilities.CanRemediate
                || capabilities.CanScanCompliance || capabilities.CanViewDeadLetters,
            SupportsEnforcement = capabilities.SupportsEnforcement,
            SupportsVersioning = capabilities.SupportsVersioning,
            SupportsRemediation = capabilities.SupportsRemediation,
            SupportsCompliance = capabilities.SupportsCompliance,
            SupportsDeadLetters = capabilities.SupportsDeadLetters,
            SupportsRuntimeRegistration = capabilities.SupportsRuntimeRegistration,
            CanStartMigration = capabilities.CanStartMigration,
            MigrationUnavailableReason = capabilities.MigrationUnavailableReason,
            CanStartRemediation = capabilities.CanStartRemediation,
            RemediationUnavailableReason = capabilities.RemediationUnavailableReason,
        };
}
