using System.ComponentModel;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// The thin adapter methods the tree-administration tool module exposes as MCP
/// schema-control tools. Every method is a stateless, static shim over the
/// transport-agnostic <see cref="ILatticeSchemaControl"/> facade: it resolves the
/// facade from the tool invocation's request service provider (bound by the MCP
/// SDK from <c>RequestContext.Services</c>), marshals the tool-call arguments into
/// the facade's model types, and returns the facade result verbatim. No
/// authorization, read, write, or schema logic lives here - the facade owns it,
/// and its fail-closed schema access gate (schema-admin authority for a mutation,
/// read authority for an inspect) refuses an unauthorized caller even if one
/// somehow reaches an invocation.
/// </summary>
/// <remarks>
/// The methods are grouped into <b>inspection</b> reads (advertised read-only) and
/// <b>management</b> writes (advertised destructive). They are held as static
/// method groups so the tool module materialises each tool's delegate exactly once
/// when it builds its tool list, never per <c>tools/call</c>. The facade DTOs are
/// reused verbatim as the tool argument and result shapes, so this surface adds no
/// new serializable wire type.
/// </remarks>
internal static class TreeAdminSchemaToolHandlers
{
    // ----- Inspection (read-only) -----

    /// <summary>Reads the enforcement policy for a tree, or <c>null</c> when none exists.</summary>
    public static Task<LatticeSchemaPolicy?> GetPolicyAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.GetPolicyAsync(treeId, cancellationToken);
    }

    /// <summary>Lists the strict-mode dead-letter entries retained for a tree.</summary>
    public static async Task<SchemaDeadLetterListResult> ListDeadLettersAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var entries = new List<LatticeSchemaDeadLetterEntry>();
        await foreach (var entry in schema.ListDeadLettersAsync(treeId, cancellationToken).ConfigureAwait(false))
        {
            entries.Add(entry);
        }

        return new SchemaDeadLetterListResult { TreeId = treeId, Entries = entries };
    }

    /// <summary>Counts the strict-mode dead-letter entries retained for a tree.</summary>
    public static Task<int> CountDeadLettersAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.CountDeadLettersAsync(treeId, cancellationToken);
    }

    /// <summary>Reads the version config for a tree, or <c>null</c> when the tree is unversioned.</summary>
    public static Task<LatticeSchemaVersionConfig?> GetVersionConfigAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.GetVersionConfigAsync(treeId, cancellationToken);
    }

    /// <summary>Reads the current or last-known remediation status for a tree.</summary>
    public static Task<LatticeSchemaRemediationReport> GetRemediationStatusAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.GetRemediationStatusAsync(treeId, cancellationToken);
    }

    /// <summary>Scans every current value of a tree against its compiled policy and returns a compliance report.</summary>
    public static Task<LatticeSchemaComplianceReport> ScanComplianceAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.ScanComplianceAsync(treeId, cancellationToken);
    }

    /// <summary>Probes which schema-management operations the caller may perform over a tree, with no side effects.</summary>
    public static Task<LatticeSchemaCapabilities> ProbeCapabilitiesAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.ProbeCapabilitiesAsync(treeId, cancellationToken);
    }

    // ----- Management (destructive) -----

    /// <summary>Sets or replaces the enforcement policy for a tree, returning the applied policy.</summary>
    public static async Task<LatticeSchemaPolicy> SetPolicyAsync(
        ILatticeSchemaControl schema,
        string treeId,
        [Description("The enforcement policy to apply: an ordered rule set (empty accepts every value) plus the per-tree strictIngest flag. A value is valid only when it satisfies every rule.")]
        LatticeSchemaPolicy policy,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        await schema.SetPolicyAsync(treeId, policy, cancellationToken).ConfigureAwait(false);
        return policy;
    }

    /// <summary>Clears the enforcement policy for a tree. Returns <c>true</c> when a policy was removed.</summary>
    public static Task<bool> ClearPolicyAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.ClearPolicyAsync(treeId, cancellationToken);
    }

    /// <summary>Opts a tree in to (or replaces its) envelope versioning, returning the installed config.</summary>
    public static async Task<LatticeSchemaVersionConfig> SetVersionConfigAsync(
        ILatticeSchemaControl schema,
        string treeId,
        [Description("The schema-family id stamped into every value's envelope for this tree.")]
        uint schemaId,
        [Description("The target schema version new writes are stamped at. Must be at least 1; the target is monotonic and only ever advances.")]
        uint targetVersion,
        [Description("When true, strict-mode ingest dead-letters a replicated / restored item whose version cannot be upcast to the target instead of applying it. Defaults to false (trusted ingest).")]
        bool strictIngest = false,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        var config = new LatticeSchemaVersionConfig(schemaId, targetVersion, strictIngest);
        await schema.SetVersionConfigAsync(treeId, config, cancellationToken).ConfigureAwait(false);
        return config;
    }

    /// <summary>Opts a tree back out of envelope versioning. Returns <c>true</c> when a config was removed.</summary>
    public static Task<bool> ClearVersionConfigAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.ClearVersionConfigAsync(treeId, cancellationToken);
    }

    /// <summary>Advances a tree's target schema version, returning the updated config.</summary>
    public static Task<LatticeSchemaVersionConfig> AdvanceTargetVersionAsync(
        ILatticeSchemaControl schema,
        string treeId,
        [Description("The new target version. Must be strictly greater than the tree's current target.")]
        uint newTargetVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.AdvanceTargetVersionAsync(treeId, newTargetVersion, cancellationToken);
    }

    /// <summary>Advances a tree's target version and runs a background eager migration, returning the terminal report.</summary>
    public static Task<LatticeSchemaRemediationReport> AdvanceAndMigrateAsync(
        ILatticeSchemaControl schema,
        string treeId,
        [Description("The new target version. Must be strictly greater than the tree's current target.")]
        uint newTargetVersion,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.AdvanceAndMigrateAsync(treeId, newTargetVersion, cancellationToken);
    }

    /// <summary>Re-stamps every existing value of a tree to its current target version, returning the terminal report.</summary>
    public static Task<LatticeSchemaRemediationReport> MigrateToTargetVersionAsync(
        ILatticeSchemaControl schema,
        string treeId,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.MigrateToTargetVersionAsync(treeId, cancellationToken);
    }

    /// <summary>Starts (or idempotently resumes) a background remediation of a tree, returning the terminal report.</summary>
    public static Task<LatticeSchemaRemediationReport> RemediateAsync(
        ILatticeSchemaControl schema,
        string treeId,
        [Description("The per-value remediation transform IR that rewrites each stored value (for example a Passthrough pipeline of SetMember / DropMember / RenameMember operations).")]
        LatticeValueTransform transform,
        [Description("The enforcement policy the transformed values must satisfy for the remediation to cut over. The remediation aborts on the first value the transform cannot make compliant.")]
        LatticeSchemaPolicy targetPolicy,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(schema);
        return schema.RemediateAsync(treeId, transform, targetPolicy, cancellationToken);
    }
}
