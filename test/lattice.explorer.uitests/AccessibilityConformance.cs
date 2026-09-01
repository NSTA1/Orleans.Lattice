using System.Text;
using Deque.AxeCore.Commons;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// The shared axe-core configuration every accessibility sweep in this project runs
/// under: the rule set, the impact threshold that fails a build, and the guards that
/// stop a sweep passing vacuously.
/// <para>
/// <b>There is deliberately no allow-list here, and no mechanism to add one.</b> The
/// sweep once carried one holding a single entry, <c>color-contrast</c>, because the
/// dark theme's <c>--lx-color-text-dim</c> measured 3.19:1 against the 4.5:1 WCAG AA
/// minimum. That was a real defect in the design tokens, so it was tracked as #1801
/// and fixed rather than suppressed. #1801 removed the entry, which emptied the set -
/// and an empty allow-list plus the filter that consumed it is an invitation to refill
/// it, so the mechanism went with it. Every critical or serious violation of the scoped
/// rule set fails, with no exception to argue about. A finding is either fixed or
/// tracked as its own issue.
/// </para>
/// </summary>
internal static class AccessibilityConformance
{
    /// <summary>
    /// The rule impacts that fail a sweep. axe classifies each violation as minor,
    /// moderate, serious, or critical; the two most severe are blocking.
    /// </summary>
    private static readonly HashSet<string> BlockingImpacts =
        new(StringComparer.OrdinalIgnoreCase) { "critical", "serious" };

    /// <summary>
    /// The conformance rule tags the sweep runs: WCAG 2.0, 2.1 and 2.2, levels A and AA.
    /// <para>
    /// The 2.1 and 2.2 tags are the point of issue #1849. The sweep previously ran
    /// <c>wcag2a</c> / <c>wcag2aa</c> alone, which put SC 1.4.11 Non-text Contrast (a
    /// 2.1 criterion, tag <c>wcag21aa</c>) and SC 2.5.8 Target Size (a 2.2 criterion,
    /// tag <c>wcag22aa</c>) out of scope <i>by construction</i> - which is exactly why
    /// borders measured at 1.21:1 passed.
    /// </para>
    /// </summary>
    internal static readonly List<string> WcagTags =
        ["wcag2a", "wcag2aa", "wcag21a", "wcag21aa", "wcag22aa"];

    /// <summary>
    /// Rules the bundled axe-core does not run under a tag-scoped sweep even though
    /// they carry one of <see cref="WcagTags"/>, and must therefore be turned on
    /// explicitly by id.
    /// <para>
    /// This is the sharpest false-pass trap in the whole widening, and it is not
    /// hypothetical: each of these tags maps to exactly one rule in axe-core 4.7, and
    /// both of those rules are withheld from a default run - <c>target-size</c> is
    /// declared <c>enabled: false</c>, and <c>label-content-name-mismatch</c> is tagged
    /// <c>experimental</c>. Adding <c>wcag22aa</c> and <c>wcag21a</c> to the tag list
    /// without naming their rules here would have run zero rules for either criterion
    /// and reported an entirely vacuous clean pass for WCAG 2.2 AA and for SC 2.5.3.
    /// An explicit per-rule <c>enabled</c> takes precedence over both the rule's own
    /// default and the tag filter, so naming a rule here is the deterministic way to
    /// guarantee it runs.
    /// </para>
    /// <para>
    /// <see cref="AssertRuleSetIsNotVacuous"/> is the standing guard that this list
    /// stays complete: it fails if any requested tag resolves to no evaluated rule, so
    /// a future axe bump that withholds another rule cannot silently narrow the gate.
    /// It is what caught both entries below.
    /// </para>
    /// </summary>
    private static readonly Dictionary<string, RuleOptions> ForceEnabledRules = new(StringComparer.Ordinal)
    {
        // SC 2.5.8 Target Size (Minimum), WCAG 2.2 AA - the only wcag22aa rule.
        ["target-size"] = new RuleOptions { Enabled = true },

        // SC 2.5.3 Label in Name, WCAG 2.1 A - the only wcag21a rule.
        ["label-content-name-mismatch"] = new RuleOptions { Enabled = true },
    };

    /// <summary>
    /// The axe run options every sweep uses: the WCAG 2.0 / 2.1 / 2.2 A and AA rule set,
    /// with the disabled-by-default rules in that set explicitly enabled.
    /// </summary>
    /// <remarks>
    /// Allocated once and shared. <c>AxeRunOptions</c> is treated as immutable by the
    /// sweep - nothing mutates it - so one instance serves every test case rather than
    /// one per sweep.
    /// </remarks>
    internal static AxeRunOptions RunOptions { get; } = new()
    {
        RunOnly = new RunOnlyOptions { Type = "tag", Values = WcagTags },
        Rules = ForceEnabledRules,
    };

    /// <summary>
    /// Returns the critical and serious violations from <paramref name="results"/>.
    /// Returns an empty list when the sweep was clean, so the caller asserts emptiness.
    /// </summary>
    /// <param name="results">The axe result to filter.</param>
    internal static List<AxeResultItem> BlockingViolations(AxeResult results)
    {
        var blocking = new List<AxeResultItem>();
        foreach (var violation in results.Violations)
        {
            if (violation.Impact is not null && BlockingImpacts.Contains(violation.Impact))
            {
                blocking.Add(violation);
            }
        }

        return blocking;
    }

    /// <summary>
    /// Fails when any tag in <see cref="WcagTags"/> resolved to no evaluated rule.
    /// <para>
    /// A tag that matches nothing is silently ignored by axe: it runs the remaining
    /// rules and reports a clean pass for a criterion it never checked. That is the
    /// worst failure mode a conformance gate can have, because it is indistinguishable
    /// from success. Every rule axe evaluated - whether it passed, failed, was
    /// incomplete, or was inapplicable to this page - reports its own tags, so the union
    /// of those tags is exactly the set of criteria the run actually covered.
    /// </para>
    /// </summary>
    /// <param name="results">The axe result to audit.</param>
    /// <param name="surface">A description of the swept surface, for the failure message.</param>
    internal static void AssertRuleSetIsNotVacuous(AxeResult results, string surface)
    {
        var evaluated = new HashSet<string>(StringComparer.Ordinal);
        var ruleCount = 0;
        ruleCount += CollectTags(results.Violations, evaluated);
        ruleCount += CollectTags(results.Passes, evaluated);
        ruleCount += CollectTags(results.Incomplete, evaluated);
        ruleCount += CollectTags(results.Inapplicable, evaluated);

        List<string>? missing = null;
        foreach (var tag in WcagTags)
        {
            if (!evaluated.Contains(tag))
            {
                (missing ??= []).Add(tag);
            }
        }

        Assert.That(missing, Is.Null, () =>
            $"The axe run on {surface} evaluated no rule carrying [{string.Join(", ", missing!)}], so "
            + "those criteria were never checked and a clean result means nothing. Either the bundled "
            + "axe-core no longer ships a rule with that tag, or the rule it ships is withheld from a "
            + "tag-scoped run (disabled by default, or tagged experimental) and is missing from "
            + "AccessibilityConformance.ForceEnabledRules. Do not narrow WcagTags to make this pass. "
            + $"The run evaluated {ruleCount} rules on axe-core {results.TestEngine?.Version ?? "unknown"}.");
    }

    /// <summary>
    /// Renders <paramref name="violations"/> as a multi-line report naming each rule,
    /// its impact, its help URL, and the elements it fired on.
    /// </summary>
    /// <param name="violations">The violations to describe.</param>
    /// <param name="surface">A description of the swept surface.</param>
    internal static string Describe(IReadOnlyList<AxeResultItem> violations, string surface)
    {
        var report = new StringBuilder();
        report.Append("axe-core reported critical/serious WCAG 2.0/2.1/2.2 A/AA violations on ")
              .Append(surface)
              .Append(':');

        foreach (var violation in violations)
        {
            report.AppendLine()
                  .Append('[').Append(violation.Impact).Append("] ")
                  .Append(violation.Id).Append(": ").Append(violation.Help)
                  .Append(" (").Append(violation.HelpUrl).Append(')');

            foreach (var node in violation.Nodes)
            {
                report.AppendLine().Append("    at: ").Append(node.Target?.ToString());
            }
        }

        return report.ToString();
    }

    private static int CollectTags(AxeResultItem[] items, HashSet<string> into)
    {
        foreach (var item in items)
        {
            if (item.Tags is null)
            {
                continue;
            }

            foreach (var tag in item.Tags)
            {
                into.Add(tag);
            }
        }

        return items.Length;
    }
}
