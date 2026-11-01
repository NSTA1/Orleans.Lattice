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
/// <see cref="Timeout"/> fails the checkpoint with a message naming the slowest rules.
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

    /// <summary>Runs axe on <paramref name="page"/> under the checkpoint's bound.</summary>
    /// <param name="page">The page.</param>
    /// <param name="surface">What is swept, for the log and the failure.</param>
    /// <param name="options">The axe run options.</param>
    /// <param name="timeout">The bound; <see cref="Timeout"/> unless a self-test shortens it.</param>
    /// <param name="slowThreshold">The per-rule threshold; <see cref="SlowThreshold"/> unless a self-test shortens it.</param>
    /// <returns>The outcome.</returns>
    public static async Task<AxeTimedRun> RunAsync(IPage page, string surface, AxeRunOptions options, TimeSpan? timeout = null, TimeSpan? slowThreshold = null)
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
        if (elapsed >= threshold && drained)
        {
            rules = await TimeRulesAsync(page, RuleIdsOf(result)).ConfigureAwait(false);
            Log(DescribeRules(rules, surface));
        }

        return new AxeTimedRun(result, elapsed, finished, rules, finished ? null : DescribeTimeout(surface, bound, elapsed, drained, rules));
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

    private static string DescribeTimeout(string surface, TimeSpan bound, TimeSpan elapsed, bool drained, IReadOnlyList<AxeRuleTiming> rules)
    {
        var report = new StringBuilder($"axe did not finish within {Seconds(bound)} on {surface}");
        report.Append(drained ? $"; it took {Seconds(elapsed)}." : $"; it was still running after {Seconds(elapsed)}.");
        report.Append(" A pass this long starves the page's Blazor circuit; fix the page, not the bound (#5565).");
        if (drained)
        {
            report.AppendLine().Append(DescribeRules(rules, surface));
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
