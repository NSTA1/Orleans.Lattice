using Orleans.Lattice.Api.State;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Schema;

/// <summary>Shared fixtures for the Schema area's tests.</summary>
internal static class SchemaTestData
{
    /// <summary>A logical tree in the catalogue.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <returns>The entry.</returns>
    public static TreeCatalogEntry Entry(string treeId) => new()
    {
        TreeId = treeId,
        ShardCount = 4,
        Config = new TreeConfigSummary { ShardCount = 4, VirtualShardCount = 64, WalPartitions = 2 },
    };

    /// <summary>A policy of three rules - UTF-8, a size limit and a member pattern - with strict ingest off.</summary>
    /// <returns>The policy.</returns>
    public static LatticeSchemaPolicy Policy() => new(
    [
        LatticeSchemaRule.Utf8(),
        LatticeSchemaRule.MaxLength(4096, "fits a page"),
        LatticeSchemaRule.Regex("^[A-Z]{3}$", "currency"),
    ]);

    /// <summary>A compliance report.</summary>
    /// <param name="treeId">The tree.</param>
    /// <param name="compliant">Compliant values.</param>
    /// <param name="nonCompliant">Non-compliant values, all failing one reason.</param>
    /// <returns>The report.</returns>
    public static LatticeSchemaComplianceReport Report(string treeId, int compliant, int nonCompliant) => new()
    {
        TreeId = treeId,
        HasPolicy = true,
        CompliantCount = compliant,
        NonCompliantCount = nonCompliant,
        ScannedCount = compliant + nonCompliant,
        RuleBreakdown = nonCompliant == 0
            ? []
            : [new LatticeSchemaComplianceRuleCount { Reason = "currency does not match", Count = nonCompliant }],
    };

    /// <summary>A dead letter.</summary>
    /// <param name="key">The diverted key.</param>
    /// <param name="preview">The value preview, as UTF-8.</param>
    /// <returns>The entry.</returns>
    public static LatticeSchemaDeadLetterEntry DeadLetter(string key, string preview = "{\"currency\":\"euro\"}") => new(
        key,
        System.Text.Encoding.UTF8.GetBytes(preview),
        preview.Length,
        "currency does not match",
        LatticeSchemaDeadLetterSource.Replication,
        new DateTimeOffset(2026, 1, 1, 12, 0, 0, TimeSpan.Zero));
}
