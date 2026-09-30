using System.Text.Json;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Asserts that every dashboard-variable matcher a bundled Grafana panel applies
/// (<c>label=~"$var"</c>) names a label the instrument behind the selector
/// actually carries, and that every query variable takes its values from a label
/// its source instrument carries.
/// </summary>
/// <remarks>
/// <para>
/// A variable-driven matcher is a presence test as well as a value test. A
/// selected value is non-empty, and the bundled variables' All value is
/// <c>.+</c>, so either way the matcher selects only series that carry the
/// label. Applied to an instrument that does not carry it, the target selects
/// nothing at all: the panel renders no data, or - behind <c>or vector(0)</c> - a
/// permanent, confident zero. Three panels shipped like that. The Backup
/// dashboard's <c>scope</c> variable was sourced from a kind-only counter and
/// applied to twelve instruments that carry no <c>scope</c>, blanking most of the
/// dashboard; the CommitPath retry panel filtered the status-only
/// <c>provider.retry.attempts</c> by <c>tree</c>; and the Replication fell-off-log
/// panel filtered a <c>tree</c>/<c>origin</c> counter by <c>peer</c>.
/// </para>
/// <para>
/// The documented label set is the Tags column of
/// <c>docs/lattice.dashboards/metrics-to-panel-map.md</c>, read through the same
/// parser <see cref="MetricDocTenantDimensionTests"/> gates, so the rule a panel
/// is checked against is the contract a reader of that document relies on.
/// Collector-supplied labels (<c>cluster</c>, <c>instance</c>, <c>job</c>) are
/// attached by the scrape, not the instrument, and are exempt. A selector whose
/// token cannot be resolved to a documented row fails the gate rather than being
/// skipped, and the scan's population is asserted, so the guard cannot go vacuous.
/// </para>
/// </remarks>
[TestFixture]
public sealed class DashboardSelectorLabelPresenceTests
{
    /// <summary>Labels the scrape attaches to every series, whatever the instrument emits.</summary>
    private static readonly IReadOnlySet<string> CollectorLabels =
        new HashSet<string>(StringComparer.Ordinal) { "cluster", "instance", "job" };

    /// <summary>
    /// A floor on the variable-driven matchers the scan must find. The real count
    /// is roughly twice this; the floor is a vacuity guard, not a census.
    /// </summary>
    private const int MinimumVariableMatchers = 200;

    private static readonly Regex SelectorRegex =
        new(@"(?<token>[a-zA-Z_:][a-zA-Z0-9_:]*)\{(?<body>[^}]*)\}", RegexOptions.Compiled);

    private static readonly Regex MatcherRegex =
        new("(?<label>[a-zA-Z_][a-zA-Z0-9_]*)\\s*(?<op>=~|!~|!=|=)\\s*\"(?<value>[^\"]*)\"", RegexOptions.Compiled);

    private static readonly Regex VariableReferenceRegex =
        new(@"\$\{?(?<name>[A-Za-z0-9_]+)", RegexOptions.Compiled);

    private static readonly Regex LabelValuesRegex =
        new(@"^\s*label_values\(\s*(?<token>[a-zA-Z_:][a-zA-Z0-9_:]*)\s*(?:\{[^}]*\})?\s*,\s*(?<label>[a-zA-Z_][a-zA-Z0-9_]*)\s*\)\s*$",
            RegexOptions.Compiled);

    /// <summary>
    /// What may follow an instrument's sanitized name in an exported series name:
    /// the exporter's unit word, then the family type suffix.
    /// </summary>
    private static readonly Regex SeriesSuffixRegex =
        new(@"^(?:_(?:milliseconds|seconds|bytes|percent|ratio))?(?:_total|_bucket|_count|_sum)?$", RegexOptions.Compiled);

    /// <summary>One variable-driven label matcher on one dashboard selector.</summary>
    internal sealed record VariableMatcher(string Dashboard, string Site, string Token, string Label, string Variable);

    /// <summary>One query variable and the selector it takes its values from.</summary>
    internal sealed record VariableSource(string Dashboard, string Variable, string Token, string Label);

    /// <summary>Everything one dashboard scan finds.</summary>
    internal sealed record ScanResult(
        IReadOnlyList<VariableMatcher> Matchers,
        IReadOnlyList<VariableSource> Sources,
        IReadOnlyList<string> Violations,
        IReadOnlyList<string> Unresolved);

    // ------------------------------------------------------------- the gate

    /// <summary>
    /// Every variable-driven matcher must name a label its instrument carries.
    /// </summary>
    [Test]
    public void Every_variable_driven_matcher_filters_on_a_label_its_instrument_carries()
    {
        var violations = ScanBundled().SelectMany(static r => r.Violations).Where(static v => v.StartsWith("matcher", StringComparison.Ordinal)).ToList();

        Assert.That(violations, Is.Empty,
            "A `label=~\"$var\"` matcher selects only series that carry the label, so applied to an instrument that "
            + "does not carry it the target selects nothing - an empty panel, or a permanent zero behind "
            + "`or vector(0)`. Drop the matcher for that instrument (and say in the panel description that the "
            + "selector does not narrow it), or group and filter by a label the instrument does carry.\n"
            + string.Join("\n", violations));
    }

    /// <summary>
    /// Every query variable must take its values from a label its source
    /// instrument carries.
    /// </summary>
    [Test]
    public void Every_query_variable_takes_its_values_from_a_label_its_instrument_carries()
    {
        var violations = ScanBundled().SelectMany(static r => r.Violations).Where(static v => v.StartsWith("variable", StringComparison.Ordinal)).ToList();

        Assert.That(violations, Is.Empty,
            "A `label_values(metric, label)` variable over an instrument that does not carry `label` offers no "
            + "values, so the selector can only ever be All. Source the variable from an instrument that carries "
            + "the label.\n" + string.Join("\n", violations));
    }

    /// <summary>
    /// Every selector the gate has to check must resolve to a documented
    /// instrument; an unresolvable one is reported rather than skipped.
    /// </summary>
    [Test]
    public void Every_checked_selector_resolves_to_a_documented_instrument()
    {
        var unresolved = ScanBundled().SelectMany(static r => r.Unresolved).Distinct(StringComparer.Ordinal).ToList();

        Assert.That(unresolved, Is.Empty,
            "These series names carry a variable-driven matcher or source a variable, but resolve to no instrument "
            + "row in docs/lattice.dashboards/metrics-to-panel-map.md, so their labels cannot be checked. Document "
            + "the instrument, or extend SeriesSuffixRegex if the exporter appends a unit word it does not know.\n"
            + string.Join("\n", unresolved));
    }

    // --------------------------------------------------- loud-on-empty scan

    /// <summary>The scan must find a population of matchers to check.</summary>
    [Test]
    public void The_scan_discovers_variable_driven_matchers()
    {
        var results = ScanBundled();

        Assert.Multiple(() =>
        {
            Assert.That(results.Sum(static r => r.Matchers.Count), Is.GreaterThanOrEqualTo(MinimumVariableMatchers),
                "The scan found too few variable-driven matchers to be reading the dashboards. A gate whose scan "
                + "matches nothing reports the same green as one that checked everything.");
            Assert.That(results.Sum(static r => r.Sources.Count), Is.GreaterThanOrEqualTo(LatticeDashboards.All.Count / 2),
                "The scan found too few label_values variables to be reading the templating blocks.");
            Assert.That(MetricDocTenantDimensionTests.ScannedRows(), Is.Not.Empty,
                "The documented tag sets this gate checks against were not read.");
        });
    }

    // ------------------------------------------------------ positive controls

    /// <summary>
    /// The detector must flag a matcher on a label the instrument lacks, and
    /// accept the same matcher on an instrument that carries it.
    /// </summary>
    [Test]
    public void Detector_flags_a_matcher_on_a_label_the_instrument_does_not_carry()
    {
        const string Json = """
            {
              "panels": [
                { "id": 1, "title": "unscoped", "targets": [ { "expr": "sum(rate(orleans_lattice_backup_captures_total{scope=~\"$scope\"}[5m]))" } ] },
                { "id": 2, "title": "scoped", "targets": [ { "expr": "sum(rate(orleans_lattice_backup_scheduler_failures_total{scope=~\"$scope\"}[5m]))" } ] }
              ]
            }
            """;

        var result = Scan("Synthetic", Json);

        Assert.Multiple(() =>
        {
            Assert.That(result.Matchers, Has.Count.EqualTo(2));
            Assert.That(result.Unresolved, Is.Empty);
            Assert.That(result.Violations, Has.Count.EqualTo(1));
            Assert.That(result.Violations[0], Does.Contain("orleans_lattice_backup_captures_total").And.Contain("scope"));
        });
    }

    /// <summary>
    /// The detector must flag a variable sourced from an instrument that lacks
    /// the label, and accept one sourced from an instrument that carries it.
    /// </summary>
    [Test]
    public void Detector_flags_a_variable_sourced_from_an_instrument_that_lacks_the_label()
    {
        const string Json = """
            {
              "templating": { "list": [
                { "name": "bad", "type": "query", "query": "label_values(orleans_lattice_backup_captures_total, scope)" },
                { "name": "good", "type": "query", "query": "label_values(orleans_lattice_backup_scope_last_run_status, scope)" },
                { "name": "silo", "type": "query", "query": "label_values(orleans_lattice_backup_captures_total, instance)" }
              ] }
            }
            """;

        var result = Scan("Synthetic", Json);

        Assert.Multiple(() =>
        {
            Assert.That(result.Sources, Has.Count.EqualTo(3));
            Assert.That(result.Unresolved, Is.Empty);
            Assert.That(result.Violations, Has.Count.EqualTo(1));
            Assert.That(result.Violations[0], Does.Contain("$bad"));
        });
    }

    /// <summary>
    /// Collector-supplied labels and negative matchers must not be checked: the
    /// scrape attaches the former, and the latter does not require the label.
    /// </summary>
    [Test]
    public void Detector_exempts_collector_labels_and_negative_matchers()
    {
        const string Json = """
            {
              "panels": [
                { "id": 1, "title": "exempt", "targets": [ { "expr": "sum(rate(orleans_lattice_backup_captures_total{cluster=~\"$cluster\",instance=~\"$silo\",scope!~\"$scope\"}[5m]))" } ] }
              ]
            }
            """;

        var result = Scan("Synthetic", Json);

        Assert.Multiple(() =>
        {
            Assert.That(result.Matchers, Is.Empty);
            Assert.That(result.Violations, Is.Empty);
        });
    }

    /// <summary>A series name maps to its instrument through the exporter's suffixes.</summary>
    [Test]
    public void Resolver_maps_exported_series_names_to_their_instrument()
    {
        Assert.Multiple(static () =>
        {
            Assert.That(ResolveInstrument("orleans_lattice_backup_captures_total"), Is.EqualTo("orleans.lattice.backup.captures"));
            Assert.That(ResolveInstrument("orleans_lattice_backup_capture_duration_milliseconds_bucket"), Is.EqualTo("orleans.lattice.backup.capture.duration"));
            Assert.That(ResolveInstrument("orleans_lattice_backup_entries_processed_total"), Is.EqualTo("orleans.lattice.backup.entries_processed"),
                "the longest documented name wins, not the first prefix that happens to fit");
            Assert.That(ResolveInstrument("orleans_lattice_backup_scope_last_run_status"), Is.EqualTo("orleans.lattice.backup.scope.last_run_status"));
            Assert.That(ResolveInstrument("orleans_lattice_no_such_instrument_total"), Is.Null);
        });
    }

    // ---------------------------------------------------------------- scan

    private static IReadOnlyList<ScanResult> ScanBundled() => BundledLazy.Value;

    private static readonly Lazy<IReadOnlyList<ScanResult>> BundledLazy = new(static () =>
        LatticeDashboards.All
            .Select(static kind => Scan(kind.ToString(), LatticeDashboards.GetGrafanaDashboardJson(kind)))
            .ToList());

    /// <summary>Scans one dashboard's JSON for variable matchers and variable sources.</summary>
    internal static ScanResult Scan(string dashboard, string json)
    {
        var matchers = new List<VariableMatcher>();
        var sources = new List<VariableSource>();

        using (var doc = JsonDocument.Parse(json))
        {
            Walk(doc.RootElement, dashboard, "dashboard", matchers);
            ReadVariables(doc.RootElement, dashboard, sources, matchers);
        }

        var violations = new List<string>();
        var unresolved = new List<string>();

        foreach (var m in matchers)
        {
            var labels = LabelsOf(m.Token, unresolved, $"{m.Dashboard} {m.Site}");
            if (labels is not null && !labels.Contains(m.Label))
            {
                violations.Add(
                    $"matcher {m.Dashboard} {m.Site}: {m.Token} is filtered by {m.Label}=~\"${m.Variable}\", "
                    + $"but its instrument carries only [{string.Join(", ", labels.Order(StringComparer.Ordinal))}]");
            }
        }

        foreach (var s in sources)
        {
            if (CollectorLabels.Contains(s.Label))
            {
                continue;
            }

            var labels = LabelsOf(s.Token, unresolved, $"{s.Dashboard} ${s.Variable}");
            if (labels is not null && !labels.Contains(s.Label))
            {
                violations.Add(
                    $"variable {s.Dashboard} ${s.Variable}: label_values({s.Token}, {s.Label}), "
                    + $"but its instrument carries only [{string.Join(", ", labels.Order(StringComparer.Ordinal))}]");
            }
        }

        return new ScanResult(matchers, sources, violations, unresolved);
    }

    private static IReadOnlySet<string>? LabelsOf(string token, List<string> unresolved, string where)
    {
        var instrument = ResolveInstrument(token);
        if (instrument is null || !DocumentedLabels().TryGetValue(instrument, out var labels))
        {
            unresolved.Add($"{where}: {token}");
            return null;
        }

        return labels;
    }

    private static void Walk(JsonElement node, string dashboard, string site, List<VariableMatcher> sink)
    {
        switch (node.ValueKind)
        {
            case JsonValueKind.Object:
                site = SiteLabel(node) ?? site;
                foreach (var property in node.EnumerateObject())
                {
                    if (property.NameEquals("expr") && property.Value.ValueKind == JsonValueKind.String)
                    {
                        ExtractMatchers(property.Value.GetString()!, dashboard, site, sink);
                        continue;
                    }

                    if (property.NameEquals("templating"))
                    {
                        continue;
                    }

                    Walk(property.Value, dashboard, site, sink);
                }

                break;

            case JsonValueKind.Array:
                foreach (var item in node.EnumerateArray())
                {
                    Walk(item, dashboard, site, sink);
                }

                break;
        }
    }

    private static void ReadVariables(
        JsonElement root, string dashboard, List<VariableSource> sources, List<VariableMatcher> matchers)
    {
        if (!root.TryGetProperty("templating", out var templating)
            || !templating.TryGetProperty("list", out var list)
            || list.ValueKind != JsonValueKind.Array)
        {
            return;
        }

        foreach (var variable in list.EnumerateArray())
        {
            if (!variable.TryGetProperty("name", out var nameElement)
                || nameElement.ValueKind != JsonValueKind.String
                || !variable.TryGetProperty("query", out var queryElement))
            {
                continue;
            }

            var query = queryElement.ValueKind switch
            {
                JsonValueKind.String => queryElement.GetString(),
                JsonValueKind.Object when queryElement.TryGetProperty("query", out var inner)
                    && inner.ValueKind == JsonValueKind.String => inner.GetString(),
                _ => null,
            };

            if (query is null)
            {
                continue;
            }

            var name = nameElement.GetString()!;
            var match = LabelValuesRegex.Match(query);
            if (match.Success)
            {
                sources.Add(new VariableSource(dashboard, name, match.Groups["token"].Value, match.Groups["label"].Value));
            }

            ExtractMatchers(query, dashboard, "variable:" + name, matchers);
        }
    }

    private static string? SiteLabel(JsonElement node)
    {
        if (node.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.Number
            && node.TryGetProperty("targets", out _))
        {
            var title = node.TryGetProperty("title", out var t) && t.ValueKind == JsonValueKind.String ? t.GetString() : null;
            return $"panel {id.GetRawText()} ({title})";
        }

        if (node.TryGetProperty("expr", out _)
            && node.TryGetProperty("name", out var name)
            && name.ValueKind == JsonValueKind.String)
        {
            return "annotation:" + name.GetString();
        }

        return null;
    }

    private static void ExtractMatchers(string expr, string dashboard, string site, List<VariableMatcher> sink)
    {
        foreach (Match selector in SelectorRegex.Matches(expr))
        {
            var token = selector.Groups["token"].Value;

            foreach (Match matcher in MatcherRegex.Matches(selector.Groups["body"].Value))
            {
                var label = matcher.Groups["label"].Value;
                var op = matcher.Groups["op"].Value;
                var variable = VariableReferenceRegex.Match(matcher.Groups["value"].Value);

                // A negative matcher does not require the label, and a collector
                // label is attached by the scrape rather than the instrument.
                if (!variable.Success || op is "!~" or "!=" || CollectorLabels.Contains(label))
                {
                    continue;
                }

                sink.Add(new VariableMatcher(dashboard, site, token, label, variable.Groups["name"].Value));
            }
        }
    }

    // -------------------------------------------------- instrument resolution

    private static readonly Lazy<IReadOnlyDictionary<string, IReadOnlySet<string>>> DocumentedLabelsLazy = new(static () =>
    {
        var map = new Dictionary<string, HashSet<string>>(StringComparer.Ordinal);
        foreach (var row in MetricDocTenantDimensionTests.ScannedRows())
        {
            if (!map.TryGetValue(row.Instrument, out var labels))
            {
                map[row.Instrument] = labels = new HashSet<string>(StringComparer.Ordinal);
            }

            labels.UnionWith(row.TagKeys);
        }

        return map.ToDictionary(static p => p.Key, static p => (IReadOnlySet<string>)p.Value, StringComparer.Ordinal);
    });

    private static IReadOnlyDictionary<string, IReadOnlySet<string>> DocumentedLabels() => DocumentedLabelsLazy.Value;

    /// <summary>
    /// Maps an exported series name onto the documented instrument it belongs to:
    /// the longest documented name whose sanitized form prefixes the series name,
    /// leaving only the exporter's unit word and type suffix.
    /// </summary>
    internal static string? ResolveInstrument(string token)
    {
        string? best = null;
        var bestLength = -1;

        foreach (var (sanitized, instrument) in SanitizedNamesLazy.Value)
        {
            if (sanitized.Length > bestLength
                && token.StartsWith(sanitized, StringComparison.Ordinal)
                && SeriesSuffixRegex.IsMatch(token[sanitized.Length..]))
            {
                best = instrument;
                bestLength = sanitized.Length;
            }
        }

        return best;
    }

    private static readonly Lazy<IReadOnlyList<(string Sanitized, string Instrument)>> SanitizedNamesLazy = new(static () =>
        DocumentedLabels().Keys
            .Select(static instrument => (Regex.Replace(instrument, "[^A-Za-z0-9:]+", "_"), instrument))
            .ToList());
}
