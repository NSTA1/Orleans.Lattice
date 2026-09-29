using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>A finished compliance scan and when it finished.</summary>
/// <param name="Report">The scan report.</param>
/// <param name="ScannedAt">When the scan finished.</param>
internal sealed record SchemaComplianceResult(LatticeSchemaComplianceReport Report, DateTimeOffset ScannedAt)
{
    /// <summary>Whether every scanned value complied with a policy.</summary>
    public bool IsCompliant => Report.HasPolicy && Report.NonCompliantCount == 0;
}
