using System.Collections.ObjectModel;
using System.Globalization;

namespace Orleans.Lattice.Schema;

/// <summary>
/// Converts between a <see cref="LatticeSchemaComplianceReport"/> and the string
/// result map a tracked compliance scan
/// (<see cref="SchemaComplianceScanOperation.Kind"/>) records, so a caller polling
/// the operation can rebuild the report it would have received from the blocking
/// scan.
/// </summary>
public static class SchemaComplianceScanResults
{
    /// <summary>Result key: the scanned (effective) tree id.</summary>
    public const string TreeIdKey = "treeId";

    /// <summary>Result key: <c>true</c> when the tree has a policy, otherwise <c>false</c>.</summary>
    public const string HasPolicyKey = "hasPolicy";

    /// <summary>Result key: the compliant value count.</summary>
    public const string CompliantCountKey = "compliantCount";

    /// <summary>Result key: the non-compliant value count.</summary>
    public const string NonCompliantCountKey = "nonCompliantCount";

    /// <summary>Result key: the scanned value count.</summary>
    public const string ScannedCountKey = "scannedCount";

    /// <summary>Result key: the number of rows in the failure-reason breakdown.</summary>
    public const string RuleCountKey = "ruleCount";

    /// <summary>
    /// The prefix of the breakdown keys: row <c>i</c> is held under
    /// <c>rule.{i}.reason</c> and <c>rule.{i}.count</c>.
    /// </summary>
    public const string RulePrefix = "rule.";

    private const string ReasonSuffix = ".reason";
    private const string CountSuffix = ".count";

    /// <summary>Encodes <paramref name="report"/> as a result map.</summary>
    /// <param name="report">The report.</param>
    /// <returns>The result map.</returns>
    public static IReadOnlyDictionary<string, string> ToResultMap(LatticeSchemaComplianceReport report)
    {
        var breakdown = report.RuleBreakdown ?? ReadOnlyCollection<LatticeSchemaComplianceRuleCount>.Empty;
        var map = new Dictionary<string, string>(6 + (breakdown.Count * 2), StringComparer.Ordinal)
        {
            [TreeIdKey] = report.TreeId,
            [HasPolicyKey] = report.HasPolicy ? "true" : "false",
            [CompliantCountKey] = report.CompliantCount.ToString(CultureInfo.InvariantCulture),
            [NonCompliantCountKey] = report.NonCompliantCount.ToString(CultureInfo.InvariantCulture),
            [ScannedCountKey] = report.ScannedCount.ToString(CultureInfo.InvariantCulture),
            [RuleCountKey] = breakdown.Count.ToString(CultureInfo.InvariantCulture),
        };

        for (var i = 0; i < breakdown.Count; i++)
        {
            var index = i.ToString(CultureInfo.InvariantCulture);
            map[RulePrefix + index + ReasonSuffix] = breakdown[i].Reason;
            map[RulePrefix + index + CountSuffix] = breakdown[i].Count.ToString(CultureInfo.InvariantCulture);
        }

        return map;
    }

    /// <summary>Rebuilds the report of a succeeded compliance scan from its result map.</summary>
    /// <param name="result">The operation's result map. Must not be <c>null</c>.</param>
    /// <param name="report">The rebuilt report when this returns <see langword="true"/>.</param>
    /// <returns><see langword="true"/> when the map describes a compliance scan.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="result"/> is <c>null</c>.</exception>
    public static bool TryReadReport(IReadOnlyDictionary<string, string> result, out LatticeSchemaComplianceReport report)
    {
        ArgumentNullException.ThrowIfNull(result);
        report = default;
        if (!result.TryGetValue(TreeIdKey, out var treeId)
            || string.IsNullOrEmpty(treeId)
            || !result.TryGetValue(HasPolicyKey, out var hasPolicyText)
            || !bool.TryParse(hasPolicyText, out var hasPolicy)
            || !TryReadInt(result, CompliantCountKey, out var compliant)
            || !TryReadInt(result, NonCompliantCountKey, out var nonCompliant)
            || !TryReadInt(result, ScannedCountKey, out var scanned)
            || !TryReadInt(result, RuleCountKey, out var ruleCount))
        {
            return false;
        }

        var rows = ruleCount == 0 ? Array.Empty<LatticeSchemaComplianceRuleCount>() : new LatticeSchemaComplianceRuleCount[ruleCount];
        for (var i = 0; i < ruleCount; i++)
        {
            var index = i.ToString(CultureInfo.InvariantCulture);
            if (!result.TryGetValue(RulePrefix + index + ReasonSuffix, out var reason)
                || !TryReadInt(result, RulePrefix + index + CountSuffix, out var count))
            {
                return false;
            }

            rows[i] = new LatticeSchemaComplianceRuleCount { Reason = reason, Count = count };
        }

        report = new LatticeSchemaComplianceReport
        {
            TreeId = treeId,
            HasPolicy = hasPolicy,
            CompliantCount = compliant,
            NonCompliantCount = nonCompliant,
            ScannedCount = scanned,
            RuleBreakdown = rows.Length == 0 ? ReadOnlyCollection<LatticeSchemaComplianceRuleCount>.Empty : rows,
        };
        return true;
    }

    private static bool TryReadInt(IReadOnlyDictionary<string, string> result, string key, out int value)
    {
        value = 0;
        return result.TryGetValue(key, out var text)
            && int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out value);
    }
}
