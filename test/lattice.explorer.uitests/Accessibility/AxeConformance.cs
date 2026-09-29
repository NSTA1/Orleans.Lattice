using System.Text;
using Deque.AxeCore.Commons;
using Deque.AxeCore.Playwright;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// The shared axe-core configuration every accessibility sweep runs under: the rule
/// set, the impact threshold that fails a build, and the guards that stop a sweep
/// passing vacuously.
/// </summary>
/// <remarks>
/// <para>
/// There is deliberately no allow-list and no mechanism to add one. Every critical or
/// serious violation of the scoped rule set fails; a finding is fixed or tracked as its
/// own issue.
/// </para>
/// <para>
/// A sweep of a blank page reports nothing, so a sweep also proves its own premises: it
/// is taken of a page whose heading has rendered, after motion has settled, and every
/// requested WCAG tag must resolve to at least one rule axe evaluated.
/// </para>
/// </remarks>
internal static class AxeConformance
{
    /// <summary>WCAG 2.0, 2.1 and 2.2, levels A and AA.</summary>
    internal static readonly List<string> WcagTags = ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];

    private static readonly HashSet<string> BlockingImpacts = new(StringComparer.OrdinalIgnoreCase) { "critical", "serious" };

    // The bundled axe-core withholds the only rule behind each of these tags from a
    // tag-scoped run (target-size is disabled by default, label-content-name-mismatch is
    // experimental). Without naming them, WCAG 2.2 AA and SC 2.5.3 would be checked by no
    // rule at all and report a clean pass.
    private static readonly Dictionary<string, RuleOptions> ForceEnabledRules = new(StringComparer.Ordinal)
    {
        ["target-size"] = new RuleOptions { Enabled = true },
        ["label-content-name-mismatch"] = new RuleOptions { Enabled = true },
    };

    /// <summary>The run options every sweep uses. Shared, and never mutated.</summary>
    internal static AxeRunOptions RunOptions { get; } = new()
    {
        RunOnly = new RunOnlyOptions { Type = "tag", Values = WcagTags },
        Rules = ForceEnabledRules,
    };

    /// <summary>
    /// Sweeps <paramref name="page"/> once its motion has settled, and fails on any critical
    /// or serious violation, or if the rule set was vacuous.
    /// </summary>
    /// <param name="page">A rendered page.</param>
    /// <param name="surface">What is being swept, for the failure message.</param>
    public static async Task SweepAsync(IPage page, string surface)
    {
        await Shell.WaitForMotionToSettleAsync(page);
        var results = await page.RunAxe(RunOptions);
        AssertRuleSetIsNotVacuous(results, surface);

        var blocking = results.Violations.Where(violation => violation.Impact is not null && BlockingImpacts.Contains(violation.Impact)).ToList();
        Assert.That(blocking, Is.Empty, () => Describe(blocking, surface));
    }

    /// <summary>Fails when any tag in <see cref="WcagTags"/> resolved to no evaluated rule.</summary>
    /// <param name="results">The axe result.</param>
    /// <param name="surface">What was swept.</param>
    public static void AssertRuleSetIsNotVacuous(AxeResult results, string surface)
    {
        var evaluated = new HashSet<string>(StringComparer.Ordinal);
        foreach (var item in results.Violations.Concat(results.Passes).Concat(results.Incomplete).Concat(results.Inapplicable))
        {
            foreach (var tag in item.Tags ?? [])
            {
                evaluated.Add(tag);
            }
        }

        var missing = WcagTags.Where(tag => !evaluated.Contains(tag)).ToList();
        Assert.That(missing, Is.Empty, () =>
            $"The axe run on {surface} evaluated no rule carrying [{string.Join(", ", missing)}], so those criteria were never "
            + "checked and a clean result means nothing. Do not narrow the tags; name the withheld rule in ForceEnabledRules.");
    }

    private static string Describe(IReadOnlyList<AxeResultItem> violations, string surface)
    {
        var report = new StringBuilder("axe-core reported critical or serious WCAG 2.0/2.1/2.2 A/AA violations on ").Append(surface).Append(':');
        foreach (var violation in violations)
        {
            report.AppendLine().Append('[').Append(violation.Impact).Append("] ").Append(violation.Id).Append(": ").Append(violation.Help);
            foreach (var node in violation.Nodes)
            {
                report.AppendLine().Append("    at ").Append(node.Target?.ToString()).Append(": ").Append(node.Html);
            }
        }

        return report.ToString();
    }
}
