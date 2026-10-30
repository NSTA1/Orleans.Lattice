using System.Globalization;

namespace Orleans.Lattice.Tests.Docs;

/// <summary>
/// Checks the area census in <c>docs/lattice/formal-coverage.md</c> against the
/// modules under <c>spec/</c>.
/// <para>
/// WHY THIS EXISTS (#5168). The page's <c>## Areas</c> table states, per area,
/// how many modules, mutations, behaviour rows and detector tests it has. Each
/// spec README's counts table is gated against its manifest, but this summary
/// was not, so it drifted: the shard-ownership row kept reading four modules
/// after <c>ShardOwnershipFold</c> (#5140) made it five. Nothing noticed,
/// because a figure written by hand and checked by nobody is true only on the
/// day it was typed.
/// </para>
/// <para>
/// WHAT IT CHECKS. Every row of the areas table maps to the module set declared
/// in <see cref="AreaModules"/>, and every figure in the row must equal the
/// figure derived from those modules (see <see cref="FormalCoverageModuleCensus"/>).
/// The table's rows must be exactly the declared areas. Every discovered module
/// must be accounted for exactly once across the areas table and the page's
/// other census section, the focused materialised-view companions table, whose
/// mutation column is checked the same way. A module that appears in no census,
/// or in two, is a finding.
/// </para>
/// <para>
/// WHERE THE MAPPING LIVES. The area-to-module map is declared here, once, rather
/// than inferred from the page's links or the spec directory layout: an area is
/// an editorial grouping (the scheduling area spans one directory, the atomic
/// commit directory feeds two areas, and the view companions span four
/// directories), so inference would encode a guess. Adding a module therefore
/// means adding it to this map or the companions table, which is the point - the
/// census cannot silently omit it.
/// </para>
/// </summary>
internal static class FormalCoverageAreaCensus
{
    /// <summary>The page, relative to the repository root.</summary>
    public const string PagePath = "docs/lattice/formal-coverage.md";

    /// <summary>The heading of the areas table.</summary>
    public const string AreasSection = "Areas";

    /// <summary>The heading of the materialised-view companions census.</summary>
    public const string CompanionsSection = "Focused materialised-view companions";

    /// <summary>The areas table's header row, in order.</summary>
    public static IReadOnlyList<string> AreasHeaders { get; } =
        ["Area", "Modules", "Mutations", "Behaviour rows", "Detector tests", "Page"];

    /// <summary>The companions table's module column.</summary>
    public const string CompanionModuleHeader = "Module";

    /// <summary>The companions table's mutation column.</summary>
    public const string CompanionMutationsHeader = "Mutants";

    /// <summary>
    /// The module set each row of the areas table counts, keyed by the row's
    /// Area cell. The single declaration of the mapping.
    /// </summary>
    public static IReadOnlyDictionary<string, IReadOnlyList<string>> AreaModules { get; } =
        new Dictionary<string, IReadOnlyList<string>>(StringComparer.Ordinal)
        {
            ["Single-cluster atomic commit"] = ["AtomicCommit"],
            ["WAL durability lifecycle"] = ["WalDurability", "WalMove"],
            ["Shard ownership"] =
            [
                "ShardOwnership", "ShardOwnershipCrdt", "ShardOwnershipCutover", "ShardOwnershipFold",
                "ShardOwnershipRetention",
            ],
            ["Cross-cluster atomic visibility"] = ["AtomicCommitCrossCluster"],
            ["Replication convergence"] =
                ["Replication", "ReplicationCausalDelivery", "ReplicationLowWatermark", "ReplicationReBootstrap"],
            ["Backup and restore"] =
                ["BackupCapture", "BackupCutover", "BackupIncremental", "BackupProvenance", "BackupRestore"],
            ["B+ tree topology"] =
                ["BPlusCascade", "BPlusReclaimRecovery", "BPlusTopology", "SplitGateCycles", "SplitLink", "SplitLinkTurns"],
            ["Materialised-view tail and lifecycle"] = ["MaterialisedViewTail"],
            ["Tree lifecycle composition"] = ["TreeLifecycle", "TreeLifecycleAdmission"],
            ["Anti-entropy remediation"] = ["AntiEntropy"],
            ["Shared alias routing and revert"] = ["AliasRevert", "AliasRouting"],
            ["Tenant quotas"] = ["TenantFootprint", "TenantQuotaEvaluation", "TenantTreeCount"],
            ["Tenant rate budgets"] = ["TenantRateBudget"],
            ["Tenant isolation"] = ["TenantIsolation"],
            ["Scheduling and progress"] =
            [
                "MaterialiserDrain", "ReplayPermits", "ReplicationShipping", "SagaReadProgress", "TagIndexReconcile",
                "TurnOccupancy", "WalAdmission", "WalBackPressure", "WalGcStarvationDrive",
                "WalGcStarvationDriveWitness",
            ],
            ["Schema enforcement and versioning"] = ["SchemaPolicyConfig", "SchemaStageOrdering", "SchemaVersionFamily", "SchemaDurableOperation", "SchemaComplianceScan", "SchemaStrictIngest"],
        ["Schema and view composition"] = ["SchemaViewComposition"],
        };

    /// <summary>
    /// The findings for the areas table: its shape, its row set against
    /// <paramref name="areaModules"/>, and every figure against the figures
    /// derived from <paramref name="modules"/>. Empty when the table is true.
    /// </summary>
    /// <param name="markdown">The page's text.</param>
    /// <param name="modules">Every discovered module's census, keyed by name.</param>
    /// <param name="areaModules">The declared area-to-module map.</param>
    public static IReadOnlyList<string> AreaFindings(
        string markdown,
        IReadOnlyDictionary<string, FormalCoverageModuleCensus> modules,
        IReadOnlyDictionary<string, IReadOnlyList<string>> areaModules)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        ArgumentNullException.ThrowIfNull(modules);
        ArgumentNullException.ThrowIfNull(areaModules);

        var table = MarkdownTable.Read(markdown, AreasSection, PagePath);
        var findings = new List<string>();

        if (!table.Headers.SequenceEqual(AreasHeaders, StringComparer.Ordinal))
        {
            findings.Add(
                $"the '## {AreasSection}' table must have the columns [{string.Join(" | ", AreasHeaders)}], "
                + $"not [{string.Join(" | ", table.Headers)}].");
            return findings;
        }

        var seen = new HashSet<string>(StringComparer.Ordinal);
        foreach (var row in table.Rows)
        {
            var area = row.Cells[0];
            if (!seen.Add(area))
            {
                findings.Add($"line {row.LineNumber}: the area '{area}' has more than one row.");
                continue;
            }

            if (!areaModules.TryGetValue(area, out var declared))
            {
                findings.Add(
                    $"line {row.LineNumber}: the area '{area}' has no declared module set in "
                    + $"{nameof(FormalCoverageAreaCensus)}.{nameof(AreaModules)}, so none of its figures can be checked.");
                continue;
            }

            var members = declared.Where(modules.ContainsKey).Select(m => modules[m]).ToList();
            var derived = new[]
            {
                (Header: "Modules", Value: members.Count),
                (Header: "Mutations", Value: members.Sum(m => m.Mutations)),
                (Header: "Behaviour rows", Value: members.Sum(m => m.BehaviourRows)),
                (Header: "Detector tests", Value: members.SelectMany(m => m.Detectors).Distinct(StringComparer.Ordinal).Count()),
            };

            foreach (var (header, value) in derived)
            {
                var cell = row.Cells[table.ColumnOf(header)];
                if (!TryNumber(cell, out var stated))
                {
                    findings.Add($"line {row.LineNumber}: '{area}' {header} cell '{cell}' is not a whole number.");
                }
                else if (stated != value)
                {
                    findings.Add(
                        $"line {row.LineNumber}: '{area}' states {stated} {header.ToLowerInvariant()}, but its modules "
                        + $"({string.Join(", ", declared)}) derive {value}.");
                }
            }
        }

        foreach (var missing in areaModules.Keys.Where(a => !seen.Contains(a)).Order(StringComparer.Ordinal))
        {
            findings.Add(
                $"the declared area '{missing}' has no row in the '## {AreasSection}' table, so its modules are "
                + "counted by no census.");
        }

        return findings;
    }

    /// <summary>
    /// The findings for the companions census: every row's mutation figure
    /// against its module's manifest.
    /// </summary>
    /// <param name="markdown">The page's text.</param>
    /// <param name="modules">Every discovered module's census, keyed by name.</param>
    public static IReadOnlyList<string> CompanionFindings(
        string markdown,
        IReadOnlyDictionary<string, FormalCoverageModuleCensus> modules)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        ArgumentNullException.ThrowIfNull(modules);

        var table = MarkdownTable.Read(markdown, CompanionsSection, PagePath);
        var name = table.ColumnOf(CompanionModuleHeader);
        var mutations = table.ColumnOf(CompanionMutationsHeader);
        var findings = new List<string>();

        foreach (var row in table.Rows)
        {
            var module = row.Cells[name].Trim('`');
            if (!modules.TryGetValue(module, out var census))
            {
                continue;
            }

            var cell = row.Cells[mutations];
            if (!TryNumber(cell, out var stated) || stated != census.Mutations)
            {
                findings.Add(
                    $"line {row.LineNumber}: companion '{module}' states '{cell}' {CompanionMutationsHeader.ToLowerInvariant()}, "
                    + $"but its manifest declares {census.Mutations}.");
            }
        }

        return findings;
    }

    /// <summary>
    /// The findings for module accounting: each discovered module must be
    /// counted exactly once across the declared areas and the companions table,
    /// and every name either census uses must be a discovered module.
    /// </summary>
    /// <param name="markdown">The page's text.</param>
    /// <param name="discovered">The names of every discovered module.</param>
    /// <param name="areaModules">The declared area-to-module map.</param>
    public static IReadOnlyList<string> AccountingFindings(
        string markdown,
        IReadOnlyCollection<string> discovered,
        IReadOnlyDictionary<string, IReadOnlyList<string>> areaModules)
    {
        ArgumentNullException.ThrowIfNull(markdown);
        ArgumentNullException.ThrowIfNull(discovered);
        ArgumentNullException.ThrowIfNull(areaModules);

        var companions = MarkdownTable.Read(markdown, CompanionsSection, PagePath);
        var name = companions.ColumnOf(CompanionModuleHeader);

        var claims = areaModules
            .SelectMany(a => a.Value.Select(m => (Module: m, Census: $"the '{a.Key}' area")))
            .Concat(companions.Rows.Select(r => (Module: r.Cells[name].Trim('`'), Census: $"the companions table (line {r.LineNumber})")))
            .ToList();

        var findings = new List<string>();
        var known = discovered.ToHashSet(StringComparer.Ordinal);

        foreach (var unknown in claims.Where(c => !known.Contains(c.Module)))
        {
            findings.Add($"{unknown.Census} names '{unknown.Module}', which is not a module discovered under spec/.");
        }

        foreach (var module in discovered.Order(StringComparer.Ordinal))
        {
            var where = claims.Where(c => string.Equals(c.Module, module, StringComparison.Ordinal)).Select(c => c.Census).ToList();
            if (where.Count == 0)
            {
                findings.Add(
                    $"module '{module}' is accounted for by no census on {PagePath}. Add it to a row of "
                    + $"{nameof(FormalCoverageAreaCensus)}.{nameof(AreaModules)} (and correct that row's figures) or to the "
                    + "companions table.");
            }
            else if (where.Count > 1)
            {
                findings.Add($"module '{module}' is counted {where.Count} times: {string.Join("; ", where)}.");
            }
        }

        return findings;
    }

    private static bool TryNumber(string cell, out int value) =>
        int.TryParse(
            cell.Replace(",", string.Empty, StringComparison.Ordinal),
            NumberStyles.None,
            CultureInfo.InvariantCulture,
            out value);
}
