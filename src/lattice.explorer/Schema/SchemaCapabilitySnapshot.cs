using Orleans.Lattice.Api.Schema;

namespace Orleans.Lattice.Explorer.Schema;

/// <summary>
/// The advisory set of schema-management operations the connected user may perform
/// over a single tree, as reported by the backend capability probe. Every flag is
/// default-deny: a value of <see langword="false"/> means "not known to be
/// permitted", and the flags are a UX affordance only - the server remains the
/// fail-closed enforcement point, so a real action must still handle a denial even
/// when the matching flag was <see langword="true"/>.
/// </summary>
/// <remarks>
/// This is the explorer-owned mirror of the schema control-API
/// <see cref="LatticeSchemaCapabilities"/> result, kept in the Schema feature
/// project so the per-tree probe can drive the UI's per-action grey-out without the
/// pure navigation layer taking a schema-API dependency.
/// </remarks>
public sealed record SchemaCapabilitySnapshot
{
    /// <summary>A snapshot with every capability denied. The safe default.</summary>
    public static SchemaCapabilitySnapshot None { get; } = new();

    /// <summary>Whether the caller may read the tree's enforcement policy.</summary>
    public bool CanViewPolicy { get; init; }

    /// <summary>Whether the caller may list / count the tree's dead-letter entries.</summary>
    public bool CanViewDeadLetters { get; init; }

    /// <summary>Whether the caller may read the tree's version config.</summary>
    public bool CanViewVersionConfig { get; init; }

    /// <summary>Whether the caller may read the tree's remediation status.</summary>
    public bool CanViewRemediationStatus { get; init; }

    /// <summary>Whether the caller may run a read-only compliance audit of the tree.</summary>
    public bool CanScanCompliance { get; init; }

    /// <summary>Whether the caller may set / clear the tree's enforcement policy.</summary>
    public bool CanManagePolicy { get; init; }

    /// <summary>Whether the caller may change the tree's version config (set / advance / migrate / clear).</summary>
    public bool CanManageVersion { get; init; }

    /// <summary>Whether the caller may start a background remediation of the tree.</summary>
    public bool CanRemediate { get; init; }

    /// <summary><see langword="true"/> when the caller has any schema capability over the tree.</summary>
    public bool HasAny =>
        CanViewPolicy || CanViewDeadLetters || CanViewVersionConfig || CanViewRemediationStatus
        || CanScanCompliance || CanManagePolicy || CanManageVersion || CanRemediate;

    /// <summary>Maps a control-API <see cref="LatticeSchemaCapabilities"/> onto the explorer-owned snapshot.</summary>
    /// <param name="capabilities">The probe result. Must not be <see langword="null"/>.</param>
    public static SchemaCapabilitySnapshot From(LatticeSchemaCapabilities capabilities)
    {
        ArgumentNullException.ThrowIfNull(capabilities);
        return new SchemaCapabilitySnapshot
        {
            CanViewPolicy = capabilities.CanViewPolicy,
            CanViewDeadLetters = capabilities.CanViewDeadLetters,
            CanViewVersionConfig = capabilities.CanViewVersionConfig,
            CanViewRemediationStatus = capabilities.CanViewRemediationStatus,
            CanScanCompliance = capabilities.CanScanCompliance,
            CanManagePolicy = capabilities.CanManagePolicy,
            CanManageVersion = capabilities.CanManageVersion,
            CanRemediate = capabilities.CanRemediate,
        };
    }
}
