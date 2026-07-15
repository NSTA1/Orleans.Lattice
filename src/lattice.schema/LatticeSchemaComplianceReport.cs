using System.Collections.ObjectModel;

namespace Orleans.Lattice.Schema;

/// <summary>
/// A read-only, serializable snapshot of a tree's schema-compliance audit: how many
/// of its current values satisfy the tree's compiled enforcement policy, how many do
/// not, and - grouped by failure reason - which rules the non-compliant values
/// break. It is the observable output of <see cref="ILatticeSchemaComplianceAdmin"/>.
/// <para>
/// The audit is a pure read: it never rewrites, dead-letters, or otherwise mutates
/// data. It is the diagnostic sibling of the mutating remediation path - where
/// <see cref="LatticeSchemaRemediationReport"/> describes a build-and-cutover, this
/// report describes the current state of the tree against its policy so an operator
/// can decide whether a remediation is warranted.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaComplianceReport)]
[Immutable]
public readonly record struct LatticeSchemaComplianceReport
{
    /// <summary>The audited tree id.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// Whether the tree currently has an enforcement policy. When <c>false</c> the
    /// tree is ungoverned, nothing is validated, and the counts are all zero: an
    /// ungoverned tree has no notion of (non-)compliance.
    /// </summary>
    [Id(1)] public required bool HasPolicy { get; init; }

    /// <summary>The number of scanned values that satisfy every rule of the current policy.</summary>
    [Id(2)] public required int CompliantCount { get; init; }

    /// <summary>The number of scanned values that fail at least one rule of the current policy.</summary>
    [Id(3)] public required int NonCompliantCount { get; init; }

    /// <summary>
    /// The total number of values scanned (<see cref="CompliantCount"/> +
    /// <see cref="NonCompliantCount"/>). Reported best-effort, mirroring
    /// <see cref="LatticeSchemaRemediationReport.ScannedCount"/>.
    /// </summary>
    [Id(4)] public required int ScannedCount { get; init; }

    /// <summary>
    /// The non-compliant population grouped by failure reason. Empty when the tree
    /// is ungoverned or fully compliant.
    /// </summary>
    [Id(5)] public required IReadOnlyList<LatticeSchemaComplianceRuleCount> RuleBreakdown { get; init; }

    /// <summary>The report for an ungoverned tree: no policy, nothing scanned.</summary>
    /// <param name="treeId">The audited tree id.</param>
    /// <returns>An ungoverned, all-zero report.</returns>
    public static LatticeSchemaComplianceReport Ungoverned(string treeId) =>
        new()
        {
            TreeId = treeId,
            HasPolicy = false,
            CompliantCount = 0,
            NonCompliantCount = 0,
            ScannedCount = 0,
            RuleBreakdown = ReadOnlyCollection<LatticeSchemaComplianceRuleCount>.Empty,
        };
}
