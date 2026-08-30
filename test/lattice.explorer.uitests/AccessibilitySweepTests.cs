using Deque.AxeCore.Commons;
using Deque.AxeCore.Playwright;
using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// Automated accessibility baseline for the Explorer home surface. Runs the axe-core
/// engine over the rendered shell and asserts zero critical/serious violations of the
/// WCAG 2.0 A and AA rule sets, and additionally asserts every <c>role="tab"</c> reports
/// a valid enumerated <c>aria-selected</c> value.
/// <para>
/// The enumerated-attribute assertion is the capability that catches #1793 (an
/// <c>aria-selected</c> bound to a bare <c>bool</c>, which Blazor renders as a valueless
/// HTML boolean attribute rather than the enumerated <c>"true"</c>/<c>"false"</c> the
/// ARIA spec requires). Note the axe sweep alone does <b>not</b> catch that exact
/// regression - axe's <c>aria-required-attr</c> is satisfied by mere presence of the
/// attribute and tolerates a valueless <c>aria-selected</c>. See <c>AxeMutationProof.md</c>
/// in this directory for the recorded mutation-test result documenting this honestly.
/// </para>
/// <para>
/// Carries <c>[Category("Integration")]</c> in addition to <c>[Category("UI")]</c>
/// because the suite transitively depends on a running <c>IHost</c> (the in-process
/// Explorer web head).
/// </para>
/// </summary>
[TestFixture]
[Category("UI")]
[Category("Integration")]
public sealed class AccessibilitySweepTests : UiTestBase
{
    private const int Width = 1400;
    private const int Height = 900;

    // The rule impacts that fail the build. axe classifies each violation as minor,
    // moderate, serious, or critical; we gate on the two most severe.
    private static readonly HashSet<string> BlockingImpacts =
        new(StringComparer.OrdinalIgnoreCase) { "critical", "serious" };

    // The WCAG 2.0 A and AA rule tags. Scoping the run to these keeps the baseline
    // focused on established conformance criteria rather than best-practice advisories.
    private static readonly List<string> WcagTags = ["wcag2a", "wcag2aa"];

    [Test]
    public async Task Home_surface_has_no_critical_or_serious_wcag_violations()
    {
        var page = await OpenHomeAsync(Width, Height);

        var results = await page.RunAxe(new AxeRunOptions
        {
            RunOnly = new RunOnlyOptions { Type = "tag", Values = WcagTags },
        });

        var blocking = results.Violations
            .Where(v => v.Impact is not null && BlockingImpacts.Contains(v.Impact))
            .ToList();

        Assert.That(blocking, Is.Empty, () => DescribeViolations(blocking));
    }

    [Test]
    public async Task Every_tab_reports_a_valid_enumerated_aria_selected_value()
    {
        // Directly guards #1793. The ARIA spec defines aria-selected as an enumerated
        // attribute whose only valid tokens are "true" and "false"; an "undefined"
        // (absent) value is also permitted. A bare boolean-attribute form (rendered by
        // Blazor when the value is a C# bool) produces aria-selected with no value, which
        // no tab may report. axe does not flag this, so we assert it explicitly.
        var page = await OpenHomeAsync(Width, Height);

        // Wait for the area tab strip to be attached before snapshotting the tab set.
        // CountAsync is a point-in-time query with no auto-wait, so anchor on a
        // web-first assertion first to avoid racing the circuit's initial render.
        var tabs = page.Locator("[role=tab]");
        await Assertions.Expect(tabs.First).ToBeAttachedAsync();

        var count = await tabs.CountAsync();
        Assert.That(count, Is.GreaterThan(0), "Expected at least one role=tab element on the home surface.");

        for (var i = 0; i < count; i++)
        {
            var tab = tabs.Nth(i);
            var hasAttribute = await tab.EvaluateAsync<bool>("el => el.hasAttribute('aria-selected')");
            Assert.That(hasAttribute, Is.True, $"role=tab element at index {i} is missing aria-selected.");

            var value = await tab.GetAttributeAsync("aria-selected");
            Assert.That(
                value,
                Is.EqualTo("true").Or.EqualTo("false"),
                $"role=tab element at index {i} has aria-selected=\"{value ?? "<null>"}\", "
                + "which is not a valid enumerated value. A valueless (boolean-attribute) "
                + "aria-selected renders as null here and is the exact #1793 regression.");
        }
    }

    private static string DescribeViolations(IReadOnlyList<AxeResultItem> violations)
    {
        var lines = violations.Select(v =>
        {
            var targets = string.Join("; ", v.Nodes.Select(n => n.Target?.ToString()));
            return $"[{v.Impact}] {v.Id}: {v.Help} ({v.HelpUrl}){Environment.NewLine}    at: {targets}";
        });

        return "axe-core reported critical/serious WCAG 2 A/AA violations on the home surface:"
            + Environment.NewLine
            + string.Join(Environment.NewLine, lines);
    }
}
