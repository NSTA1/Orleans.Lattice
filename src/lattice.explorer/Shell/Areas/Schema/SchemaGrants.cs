using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

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
            capabilities.CanViewPolicy,
            capabilities.CanManagePolicy,
            capabilities.CanViewVersionConfig,
            capabilities.CanManageVersion,
            capabilities.CanViewRemediationStatus,
            capabilities.CanRemediate,
            capabilities.CanScanCompliance,
            capabilities.CanViewDeadLetters);
}
