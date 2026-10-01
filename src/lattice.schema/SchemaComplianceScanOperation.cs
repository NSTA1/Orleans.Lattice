namespace Orleans.Lattice.Schema;

/// <summary>
/// The tracked long-running operation that runs a schema compliance scan
/// (<see cref="ILatticeSchemaComplianceAdmin.ScanComplianceAsync"/>) in the
/// background: its kind, its phases and its unit. The result keys are on
/// <see cref="SchemaComplianceScanResults"/>.
/// </summary>
/// <remarks>
/// The scan counts the tree's live entries in <see cref="CountingPhase"/>, then
/// validates every value in <see cref="ScanningPhase"/>, reporting the entries
/// scanned against that count. The count is taken before the scan and the tree
/// stays writable, so when the scan passes it the total becomes unknown rather
/// than reading below the entries already scanned.
/// </remarks>
public static class SchemaComplianceScanOperation
{
    /// <summary>The operation kind. Result reference: the scanned tree id.</summary>
    public const string Kind = "schema.compliance-scan";

    /// <summary>The first phase: counting the tree's live entries.</summary>
    public const string CountingPhase = "Counting";

    /// <summary>The second phase: validating every value against the tree's policy.</summary>
    public const string ScanningPhase = "Scanning";

    /// <summary>The unit <see cref="ScanningPhase"/> counts.</summary>
    public const string EntriesUnit = "entries";
}
