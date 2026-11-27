using System.Text.RegularExpressions;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// The tenant-pinning gate for the built-in query catalogue (issue #5582). A
/// tenant-confined telemetry grant is safe only because every series selector in
/// every server-authored template carries the facade's <c>$scope$</c> slot, so the
/// pinned <c>tenant="..."</c> matcher lands on every metric a query reads. A
/// template that mentioned the slot once but read a second, unpinned metric would
/// leak cross-tenant and platform aggregates to a tenant grantee, and the coarser
/// "contains the placeholder" check cannot see that.
/// </summary>
/// <remarks>
/// Deterministic text scans over <see cref="LatticeTelemetryQueries.Definitions"/>.
/// The gate fails loudly when it discovers no templates or no metric selectors at
/// all, so it can never pass vacuously.
/// </remarks>
[TestFixture]
public sealed class TelemetryTemplateScopeGateTests
{
    // A metric-name token: every built-in series is an orleans_lattice_* exposition name.
    private static readonly Regex MetricToken = new(
        @"(?<![A-Za-z0-9_:])(?<name>orleans_lattice_[A-Za-z0-9_:]*)",
        RegexOptions.CultureInvariant);

    private static readonly Regex SelectorBlock = new(
        @"\{(?<body>[^}]*)\}",
        RegexOptions.CultureInvariant | RegexOptions.NonBacktracking);

    private static IReadOnlyList<TelemetryQueryDefinition> Definitions => LatticeTelemetryQueries.Definitions;

    [Test]
    public void The_gate_discovers_the_built_in_templates()
    {
        Assert.That(Definitions, Is.Not.Empty,
            "The scope gate found no templates to check. It must never pass vacuously: if the catalogue "
            + "moved, point the gate at its new home rather than letting it go silent.");
    }

    [Test]
    public void Every_template_reads_at_least_one_recognisable_metric()
    {
        var offenders = Definitions
            .Where(d => !MetricToken.IsMatch(d.QueryTemplate))
            .Select(d => d.QueryId)
            .ToArray();

        Assert.That(offenders, Is.Empty,
            "The gate recognises series by their orleans_lattice_ exposition name, and found none in these "
            + "templates, so it cannot vouch that they are tenant-pinned. Extend the gate's metric pattern. "
            + "Offenders: " + string.Join(", ", offenders));
    }

    [Test]
    public void Every_metric_selector_in_every_template_carries_the_scope_slot()
    {
        var checkedSelectors = 0;
        var offenders = new List<string>();
        foreach (var definition in Definitions)
        {
            var template = definition.QueryTemplate;
            foreach (Match match in MetricToken.Matches(template))
            {
                checkedSelectors++;
                var after = template.AsSpan(match.Groups["name"].Index + match.Groups["name"].Length);
                if (!after.StartsWith("{" + TelemetryQueryTemplate.ScopeToken, StringComparison.Ordinal))
                {
                    offenders.Add($"{definition.QueryId}: {match.Groups["name"].Value}");
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(checkedSelectors, Is.GreaterThan(0), "The gate checked no metric selectors at all.");
            Assert.That(offenders, Is.Empty,
                "Every metric a template reads must open its selector with the scope slot, so the facade's "
                + "tenant matcher pins it; an unpinned selector leaks every tenant's and the platform's "
                + "series to a tenant-confined grant. Offenders: " + string.Join(", ", offenders));
        });
    }

    [Test]
    public void Every_label_selector_block_opens_with_the_scope_slot()
    {
        var checkedBlocks = 0;
        var offenders = new List<string>();
        foreach (var definition in Definitions)
        {
            foreach (Match block in SelectorBlock.Matches(definition.QueryTemplate))
            {
                checkedBlocks++;
                if (!block.Groups["body"].Value.StartsWith(TelemetryQueryTemplate.ScopeToken, StringComparison.Ordinal))
                {
                    offenders.Add($"{definition.QueryId}: {block.Value}");
                }
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(checkedBlocks, Is.GreaterThan(0), "The gate checked no selector blocks at all.");
            Assert.That(offenders, Is.Empty,
                "A selector block without the scope slot (a bare {__name__=...} or {job=...} selector) reads "
                + "unpinned series. Offenders: " + string.Join(", ", offenders));
        });
    }

    [Test]
    public void Rendering_under_a_tenant_scope_pins_every_metric_selector()
    {
        foreach (var definition in Definitions)
        {
            var template = TelemetryQueryTemplate.Parse(definition.QueryTemplate);
            var rendered = template.Render(TelemetryScopeSelector.ForTenant("acme", null), TelemetryRateWindow.Default);
            var metrics = MetricToken.Matches(definition.QueryTemplate).Count;
            var pinned = Regex.Matches(rendered, Regex.Escape("{tenant=\"acme\","), RegexOptions.CultureInvariant).Count;

            Assert.That(pinned, Is.EqualTo(metrics),
                $"'{definition.QueryId}' rendered {pinned} pinned selectors for {metrics} metrics: {rendered}");
        }
    }
}
