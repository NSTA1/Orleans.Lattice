using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.Json;
using Deque.AxeCore.Commons;
using Deque.AxeCore.Playwright;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.UiTests.Accessibility;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// A checkpoint's axe-core run, timed and bounded (#5565). Every run's duration is logged. A
/// run slower than <see cref="SlowThreshold"/> is timed again one rule at a time, so the log
/// names the rules that cost the time. A run that has not finished within
/// <see cref="Timeout"/> fails the checkpoint with a message naming the slowest rules. When the
/// slowest rule alone takes <see cref="SlowThreshold"/> or more, it is also bisected by DOM
/// (<see cref="BisectAsync"/>), naming the subtree it spends its time on.
/// </summary>
/// <remarks>
/// <para>
/// An axe pass runs on the page's main thread. A long one starves the Blazor circuit's
/// keepalives, the server drops the circuit, and without this bound the checkpoint fails only
/// on the console error that follows, with nothing pointing at axe.
/// </para>
/// <para>
/// The bound fails the checkpoint; it cannot stop axe in the page. The per-rule timing
/// therefore waits, within <see cref="DrainBound"/>, for the timed-out run to finish (axe
/// refuses to start a second run while one is in progress), then times each rule the first
/// run evaluated with its own <c>axe.run</c>. The circuit is not protected by any of this:
/// the slow rule is named so the page can be fixed.
/// </para>
/// </remarks>
internal static class AxeTiming
{
    /// <summary>How long an axe run may take before the checkpoint fails.</summary>
    public static readonly TimeSpan Timeout = TimeSpan.FromSeconds(30);

    /// <summary>How long a run may take before its rules are timed one at a time.</summary>
    public static readonly TimeSpan SlowThreshold = TimeSpan.FromSeconds(10);

    /// <summary>How long a timed-out run, and then the per-rule timing, are each waited for.</summary>
    public static readonly TimeSpan DrainBound = TimeSpan.FromMinutes(4);

    /// <summary>How many of the slowest rules a report names.</summary>
    public const int ReportedRules = 5;

    /// <summary>How many branching levels a slow rule's DOM bisection descends at most.</summary>
    public const int BisectionDepth = 6;

    /// <summary>A subtree this small is not bisected further.</summary>
    public const int BisectionMinimumElements = 8;

    /// <summary>How long a slow rule's DOM bisection may spend.</summary>
    public static readonly TimeSpan BisectionBudget = TimeSpan.FromMinutes(3);

    // Times each rule alone, in the order given, with nothing but violations collected so a
    // rule's own work dominates its time. A rule axe refuses is reported with its error.
    private const string TimeRulesScript =
        """
        async (ids) => {
          const timings = [];
          for (const id of ids) {
            const started = performance.now();
            let error = null;
            try {
              await axe.run(document, { runOnly: { type: 'rule', values: [id] }, rules: { [id]: { enabled: true } }, resultTypes: ['violations'] });
            } catch (e) {
              error = String(e && e.message ? e.message : e);
            }
            timings.push({ id, ms: performance.now() - started, error });
          }
          return timings;
        }
        """;

    // Descends from body into the subtree a single rule spends the most time on: at each
    // branching level the children holding targets are timed (most targets first) with the
    // rule run alone on that child, and the walk follows the slowest. A child that alone takes
    // half its parent's time must be the slowest, so its siblings are not timed. A lone child
    // holding every target is followed without timing. The walk stops at maxDepth branching
    // levels, at minElements elements, or when the time budget is spent.
    private const string BisectScript =
        """
        async ([id, rootMs, maxDepth, minElements, budgetMs]) => {
          const started = performance.now();
          const targetSelector = 'a[href],area[href],button,input:not([type=hidden]),select,textarea,summary,[tabindex],[contenteditable],[role=button],[role=link],[role=checkbox],[role=radio],[role=switch],[role=tab],[role=option],[role=menuitem],[role=combobox],[role=slider],[role=spinbutton],[role=treeitem]';
          const count = el => ({
            elements: el.getElementsByTagName('*').length + 1,
            targets: (el.matches(targetSelector) ? 1 : 0) + el.querySelectorAll(targetSelector).length,
          });
          const segment = el => {
            let s = el.tagName.toLowerCase();
            if (el.id) { return s + '#' + CSS.escape(el.id); }
            const classes = [...el.classList].slice(0, 2);
            if (classes.length > 0) { s += '.' + classes.map(c => CSS.escape(c)).join('.'); }
            const parent = el.parentElement;
            if (parent) {
              const same = [...parent.children].filter(c => c.tagName === el.tagName);
              if (same.length > 1) { s += ':nth-of-type(' + (same.indexOf(el) + 1) + ')'; }
            }
            return s;
          };
          const time = async el => {
            const t = performance.now();
            await axe.run({ include: [el] }, { runOnly: { type: 'rule', values: [id] }, rules: { [id]: { enabled: true } }, resultTypes: ['violations'] });
            return performance.now() - t;
          };
          let node = document.body;
          let selector = 'body';
          let lastTimed = rootMs;
          let depth = 0;
          let exhausted = false;
          const path = [{ selector, ms: rootMs, ...count(node) }];
          while (depth < maxDepth && path[path.length - 1].elements > minElements) {
            const children = [...node.children].map(el => ({ el, ...count(el) })).filter(c => c.targets > 0).sort((a, b) => b.targets - a.targets);
            if (children.length === 0) { break; }
            let best = null;
            if (children.length === 1) {
              best = { ...children[0], ms: null };
            } else {
              depth++;
              for (const child of children) {
                if (performance.now() - started > budgetMs) { exhausted = true; break; }
                child.ms = await time(child.el);
                if (best === null || child.ms > best.ms) { best = child; }
                if (best.ms >= lastTimed * 0.5) { break; }
              }
              if (best === null) { break; }
              lastTimed = best.ms;
            }
            node = best.el;
            selector += ' > ' + segment(node);
            path.push({ selector, ms: best.ms, elements: best.elements, targets: best.targets });
            if (exhausted) { break; }
          }
          return { path, exhausted };
        }
        """;

    /// <summary>Runs axe on <paramref name="page"/> under the checkpoint's bound.</summary>
    /// <param name="page">The page.</param>
    /// <param name="surface">What is swept, for the log and the failure.</param>
    /// <param name="options">The axe run options.</param>
    /// <param name="timeout">The bound; <see cref="Timeout"/> unless a self-test shortens it.</param>
    /// <param name="slowThreshold">The per-rule threshold; <see cref="SlowThreshold"/> unless a self-test shortens it.</param>
    /// <param name="ruleBisectThreshold">How slow the slowest rule must be to be bisected by DOM; <see cref="SlowThreshold"/> unless a self-test shortens it.</param>
    /// <returns>The outcome.</returns>
    public static async Task<AxeTimedRun> RunAsync(IPage page, string surface, AxeRunOptions options, TimeSpan? timeout = null, TimeSpan? slowThreshold = null, TimeSpan? ruleBisectThreshold = null)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentException.ThrowIfNullOrWhiteSpace(surface);
        ArgumentNullException.ThrowIfNull(options);
        var bound = timeout ?? Timeout;
        var threshold = slowThreshold ?? SlowThreshold;

        var clock = Stopwatch.StartNew();
        var run = page.RunAxe(options);
        var finished = await Task.WhenAny(run, Task.Delay(bound)).ConfigureAwait(false) == run;
        AxeResult? result = null;
        if (finished)
        {
            result = await run.ConfigureAwait(false);
        }
        else if (await Task.WhenAny(run, Task.Delay(DrainBound)).ConfigureAwait(false) == run && run.IsCompletedSuccessfully)
        {
            // The checkpoint already fails on the bound; the late result names the rules to time.
            result = run.Result;
        }

        var elapsed = clock.Elapsed;
        var drained = run.IsCompleted;
        Log($"axe took {Seconds(elapsed)} on {surface}{(finished ? string.Empty : $" (bound {Seconds(bound)}{(drained ? string.Empty : ", still running")})")}.");

        IReadOnlyList<AxeRuleTiming> rules = [];
        IReadOnlyList<AxeBisectionStep> bisection = [];
        if (elapsed >= threshold && drained)
        {
            rules = await TimeRulesAsync(page, RuleIdsOf(result)).ConfigureAwait(false);
            Log(DescribeRules(rules, surface));
            if (rules.Count > 0 && rules[0].Error is null && rules[0].Duration >= (ruleBisectThreshold ?? SlowThreshold))
            {
                bisection = await BisectAsync(page, rules[0]).ConfigureAwait(false);
                Log(DescribeBisection(bisection, rules[0].RuleId, surface));
            }
        }

        return new AxeTimedRun(result, elapsed, finished, rules, finished ? null : DescribeTimeout(surface, bound, elapsed, drained, rules, bisection))
        {
            Bisection = bisection,
        };
    }

    /// <summary>
    /// Follows <paramref name="rule"/> down the DOM of <paramref name="page"/> to the subtree it
    /// spends the most time on: at each branching level the rule is run alone on each child that
    /// holds targets, and the slowest child is followed (at most <see cref="BisectionDepth"/>
    /// levels, down to <see cref="BisectionMinimumElements"/> elements, within
    /// <see cref="BisectionBudget"/>).
    /// </summary>
    /// <param name="page">The page, with axe injected and no axe run in progress.</param>
    /// <param name="rule">The slow rule and its time on the whole page.</param>
    /// <param name="budget">The time budget; <see cref="BisectionBudget"/> unless a self-test changes it.</param>
    /// <returns>The path from <c>body</c> to the slowest subtree; empty when the walk itself did not return within <see cref="DrainBound"/>.</returns>
    public static async Task<IReadOnlyList<AxeBisectionStep>> BisectAsync(IPage page, AxeRuleTiming rule, TimeSpan? budget = null)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(rule);
        var walk = page.EvaluateAsync<JsonElement>(
            BisectScript,
            new object[] { rule.RuleId, rule.Duration.TotalMilliseconds, BisectionDepth, BisectionMinimumElements, (budget ?? BisectionBudget).TotalMilliseconds });
        if (await Task.WhenAny(walk, Task.Delay(DrainBound)).ConfigureAwait(false) != walk)
        {
            return [];
        }

        var steps = new List<AxeBisectionStep>();
        foreach (var step in (await walk.ConfigureAwait(false)).GetProperty("path").EnumerateArray())
        {
            var ms = step.GetProperty("ms");
            steps.Add(new AxeBisectionStep(
                step.GetProperty("selector").GetString()!,
                ms.ValueKind == JsonValueKind.Number ? TimeSpan.FromMilliseconds(ms.GetDouble()) : null,
                step.GetProperty("elements").GetInt32(),
                step.GetProperty("targets").GetInt32()));
        }

        return steps;
    }

    /// <summary>A slow rule's bisection, one level per line, slowest subtree last.</summary>
    /// <param name="steps">The bisection.</param>
    /// <param name="ruleId">The rule.</param>
    /// <param name="surface">What was swept.</param>
    public static string DescribeBisection(IReadOnlyList<AxeBisectionStep> steps, string ruleId, string surface)
    {
        if (steps.Count == 0)
        {
            return $"axe's {ruleId} on {surface} could not be bisected by DOM within {Seconds(DrainBound)}.";
        }

        var report = new StringBuilder($"axe's {ruleId} on {surface}, bisected by DOM (slowest subtree last):");
        foreach (var step in steps)
        {
            report.AppendLine().Append("    ")
                .Append(step.Duration is { } duration ? Seconds(duration) : "(only child with targets)")
                .Append(" - ").Append(step.Elements.ToString(CultureInfo.InvariantCulture)).Append(" elements, ")
                .Append(step.Targets.ToString(CultureInfo.InvariantCulture)).Append(" targets: ").Append(step.Selector);
        }

        return report.ToString();
    }

    /// <summary>
    /// Times each of <paramref name="ruleIds"/> alone on <paramref name="page"/>, which must
    /// already have axe injected and no axe run in progress.
    /// </summary>
    /// <param name="page">The page.</param>
    /// <param name="ruleIds">The rules to time; empty to time every rule the WCAG tags and forced rules name.</param>
    /// <returns>Each rule's time, slowest first; empty when the timing itself did not finish within <see cref="DrainBound"/>.</returns>
    public static async Task<IReadOnlyList<AxeRuleTiming>> TimeRulesAsync(IPage page, IReadOnlyCollection<string> ruleIds)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(ruleIds);
        var ids = ruleIds.Count > 0
            ? ruleIds.ToArray()
            : await page.EvaluateAsync<string[]>(
                "([tags, forced]) => [...new Set([...axe.getRules(tags).map(r => r.ruleId), ...forced])]",
                new object[] { AxeConformance.WcagTags.ToArray(), AxeConformance.RunOptions.Rules.Keys.ToArray() }).ConfigureAwait(false);

        var timing = page.EvaluateAsync<JsonElement>(TimeRulesScript, ids);
        if (await Task.WhenAny(timing, Task.Delay(DrainBound)).ConfigureAwait(false) != timing)
        {
            return [];
        }

        var timings = new List<AxeRuleTiming>();
        foreach (var item in (await timing.ConfigureAwait(false)).EnumerateArray())
        {
            var error = item.GetProperty("error");
            timings.Add(new AxeRuleTiming(
                item.GetProperty("id").GetString()!,
                TimeSpan.FromMilliseconds(item.GetProperty("ms").GetDouble()),
                error.ValueKind == JsonValueKind.String ? error.GetString() : null));
        }

        return [.. timings.OrderByDescending(rule => rule.Duration)];
    }

    /// <summary>The slowest rules, one per line, for a log or a failure.</summary>
    /// <param name="rules">The timings, slowest first.</param>
    /// <param name="surface">What was swept.</param>
    public static string DescribeRules(IReadOnlyList<AxeRuleTiming> rules, string surface)
    {
        if (rules.Count == 0)
        {
            return $"axe's rules on {surface} could not be timed one at a time within {Seconds(DrainBound)}.";
        }

        var report = new StringBuilder($"axe's slowest rules on {surface}, each run alone:");
        foreach (var rule in rules.Take(ReportedRules))
        {
            report.AppendLine().Append("    ").Append(rule.RuleId).Append(": ").Append(Seconds(rule.Duration));
            if (rule.Error is not null)
            {
                report.Append(" (").Append(rule.Error).Append(')');
            }
        }

        return report.ToString();
    }

    private static string DescribeTimeout(string surface, TimeSpan bound, TimeSpan elapsed, bool drained, IReadOnlyList<AxeRuleTiming> rules, IReadOnlyList<AxeBisectionStep> bisection)
    {
        var report = new StringBuilder($"axe did not finish within {Seconds(bound)} on {surface}");
        report.Append(drained ? $"; it took {Seconds(elapsed)}." : $"; it was still running after {Seconds(elapsed)}.");
        report.Append(" A pass this long starves the page's Blazor circuit; fix the page, not the bound (#5565).");
        if (drained)
        {
            report.AppendLine().Append(DescribeRules(rules, surface));
            if (bisection.Count > 0)
            {
                report.AppendLine().Append(DescribeBisection(bisection, rules[0].RuleId, surface));
            }
        }

        return report.ToString();
    }

    private static List<string> RuleIdsOf(AxeResult? result) =>
        result is null
            ? []
            : [.. result.Violations.Concat(result.Passes).Concat(result.Incomplete).Concat(result.Inapplicable)
                .Select(item => item.Id)
                .Where(id => !string.IsNullOrEmpty(id))
                .Distinct(StringComparer.Ordinal)];

    private static string Seconds(TimeSpan span) => span.TotalSeconds.ToString("0.0", CultureInfo.InvariantCulture) + " s";

    private static void Log(string line) => TestContext.Out.WriteLine(line);
}
