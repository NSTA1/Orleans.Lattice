using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice;
using Orleans.Lattice.Dashboards;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Drift-guard regression tests for the embedded Grafana dashboard JSON.
/// Each dashboard is parsed, every metric name referenced in panel
/// <c>expr</c> / <c>query</c> strings is extracted, and each referenced
/// token is asserted to be one of the canonical Prometheus forms a known
/// .NET instrument on <see cref="LatticeMetrics.Meter"/> or
/// <see cref="LatticeReplicationMetrics.Meter"/> would expose. A rename
/// in either meter fails this test before the dashboard ships stale.
/// </summary>
/// <remarks>
/// We intentionally use <em>forward</em> mapping (live instrument name to
/// expected PromQL token forms) rather than reverse mapping (PromQL token
/// to instrument name), because many instrument names embed underscores
/// (for example <c>orleans.lattice.replication.apply.dependency_wait_ms</c>
/// and <c>orleans.lattice.replication.wal.entries_appended</c>) and the
/// reverse direction is fundamentally ambiguous once dot-separated
/// segments and embedded-underscore segments are both translated to
/// underscores by the OpenTelemetry Prometheus exporter.
/// </remarks>
[TestFixture]
public sealed class DashboardJsonTests
{
    private static readonly Regex InstrumentTokenRegex =
        new(@"\borleans_lattice(?:_replication)?_[a-z0-9_]+\b", RegexOptions.Compiled);

    private static IReadOnlyDictionary<string, string> ExpectedTokenToMeter { get; } =
        BuildExpectedTokenToMeterMap();

    private static Dictionary<string, string> BuildExpectedTokenToMeterMap()
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var (instrumentName, meterName) in EnumerateLiveInstruments())
        {
            AddInstrumentForms(map, instrumentName, meterName);
        }

        // Some instruments — observable gauges in particular — are only created
        // when the host starts the subsystem that owns them, so a snapshot
        // MeterListener at test time does not see them. Both meter classes
        // expose the canonical name of every such instrument as a
        // <c>public const string ...Name</c> field; reflect over those so the
        // drift guard recognises tokens for instruments whose factories are
        // not statically wired.
        foreach (var name in EnumerateDocumentedInstrumentConstants(typeof(LatticeMetrics)))
        {
            AddInstrumentForms(map, name, LatticeMetrics.MeterName);
        }
        foreach (var name in EnumerateDocumentedInstrumentConstants(typeof(LatticeReplicationMetrics)))
        {
            AddInstrumentForms(map, name, LatticeReplicationMetrics.MeterName);
        }

        return map;
    }

    private static void AddInstrumentForms(Dictionary<string, string> map, string instrumentName, string meterName)
    {
        // OpenTelemetry's Prometheus exporter translates '.' to '_' and
        // preserves any underscores already present in the .NET name.
        var underscored = instrumentName.Replace('.', '_');

        // Counter: name + "_total"
        map[underscored + "_total"] = meterName;

        // Histogram (ms unit): name + "_milliseconds_{bucket|count|sum}"
        map[underscored + "_milliseconds_bucket"] = meterName;
        map[underscored + "_milliseconds_count"] = meterName;
        map[underscored + "_milliseconds_sum"] = meterName;

        // Histogram (s unit): name + "_seconds_{bucket|count|sum}"
        map[underscored + "_seconds_bucket"] = meterName;
        map[underscored + "_seconds_count"] = meterName;
        map[underscored + "_seconds_sum"] = meterName;

        // Histogram with no explicit unit (the .NET name itself encodes the unit,
        // e.g. ".apply.dependency_wait_ms"): the exporter appends the suffix
        // directly to the underscored name without inserting a unit segment.
        map[underscored + "_bucket"] = meterName;
        map[underscored + "_count"] = meterName;
        map[underscored + "_sum"] = meterName;

        // Counter / observable gauge with bytes unit ("By"): the exporter
        // appends "_bytes" (and "_bytes_total" for monotonic counters).
        map[underscored + "_bytes"] = meterName;
        map[underscored + "_bytes_total"] = meterName;

        // Gauge / observable / un-suffixed reference (some queries use the bare name)
        map[underscored] = meterName;
    }

    private static IEnumerable<string> EnumerateDocumentedInstrumentConstants(Type metricsType)
    {
        foreach (var field in metricsType.GetFields(System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.Static))
        {
            if (field.IsLiteral && !field.IsInitOnly && field.FieldType == typeof(string)
                && field.Name.EndsWith("Name", StringComparison.Ordinal)
                && field.GetRawConstantValue() is string value
                && value.StartsWith("orleans.lattice", StringComparison.Ordinal)
                && value.Contains('.', StringComparison.Ordinal)
                && !string.Equals(value, "orleans.lattice", StringComparison.Ordinal)
                && !string.Equals(value, "orleans.lattice.replication", StringComparison.Ordinal))
            {
                yield return value;
            }
        }
    }

    private static IEnumerable<(string Name, string MeterName)> EnumerateLiveInstruments()
    {
        // Force type-initialisers on the static metric classes so every instrument is registered.
        _ = LatticeMetrics.MeterName;
        _ = LatticeReplicationMetrics.MeterName;

        var collected = new List<(string Name, string MeterName)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, _) =>
        {
            if (ReferenceEquals(instrument.Meter, LatticeMetrics.Meter))
            {
                collected.Add((instrument.Name, LatticeMetrics.MeterName));
            }
            else if (ReferenceEquals(instrument.Meter, LatticeReplicationMetrics.Meter))
            {
                collected.Add((instrument.Name, LatticeReplicationMetrics.MeterName));
            }
        };
        listener.Start();
        return collected;
    }

    [Test]
    public void Every_dashboard_resource_can_be_loaded_and_is_well_formed_json()
    {
        foreach (var kind in LatticeDashboards.All)
        {
            var json = LatticeDashboards.GetGrafanaDashboardJson(kind);
            Assert.That(json, Is.Not.Null.And.Not.Empty,
                $"Dashboard '{kind}' returned empty content.");
            Assert.DoesNotThrow(() => JsonDocument.Parse(json),
                $"Dashboard '{kind}' is not well-formed JSON.");
        }
    }

    [Test]
    public void Every_dashboard_has_a_uid_and_title_and_panels()
    {
        foreach (var kind in LatticeDashboards.All)
        {
            var json = LatticeDashboards.GetGrafanaDashboardJson(kind);
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            Assert.That(root.TryGetProperty("uid", out var uid), Is.True, $"Dashboard '{kind}' is missing 'uid'.");
            Assert.That(uid.GetString(), Is.Not.Null.And.Not.Empty);
            Assert.That(root.TryGetProperty("title", out var title), Is.True, $"Dashboard '{kind}' is missing 'title'.");
            Assert.That(title.GetString(), Is.Not.Null.And.Not.Empty);
            Assert.That(root.TryGetProperty("panels", out var panels), Is.True, $"Dashboard '{kind}' is missing 'panels'.");
            Assert.That(panels.GetArrayLength(), Is.GreaterThan(0), $"Dashboard '{kind}' has no panels.");
        }
    }

    [TestCase(LatticeDashboardKind.Overview, "orleans.lattice")]
    [TestCase(LatticeDashboardKind.CommitPath, "orleans.lattice")]
    [TestCase(LatticeDashboardKind.Replication, "orleans.lattice.replication")]
    [TestCase(LatticeDashboardKind.AtomicWrites, "orleans.lattice")]
    public void Every_metric_token_referenced_in_dashboard_resolves_to_a_known_instrument(
        LatticeDashboardKind kind, string expectedMeter)
    {
        var json = LatticeDashboards.GetGrafanaDashboardJson(kind);
        var referencedTokens = ExtractInstrumentTokens(json);

        Assert.That(referencedTokens, Is.Not.Empty,
            $"Dashboard '{kind}' references no orleans_lattice instruments — that's almost certainly a bug.");

        var unknown = new List<string>();
        foreach (var token in referencedTokens)
        {
            if (!ExpectedTokenToMeter.TryGetValue(token, out var meterName))
            {
                unknown.Add($"{token} (no instrument on any known meter exposes that PromQL form)");
                continue;
            }

            // Replication dashboard may legitimately reference both meters in the
            // future; today, every Replication panel is on the replication meter
            // and Overview / CommitPath panels are on the core meter.
            if (kind != LatticeDashboardKind.Replication && meterName == LatticeReplicationMetrics.MeterName)
            {
                unknown.Add($"{token} (resolved to '{meterName}', expected '{expectedMeter}')");
            }
            else if (kind == LatticeDashboardKind.Replication && meterName != LatticeReplicationMetrics.MeterName)
            {
                unknown.Add($"{token} (resolved to '{meterName}', expected '{expectedMeter}')");
            }
        }

        Assert.That(unknown, Is.Empty,
            $"Dashboard '{kind}' references metric tokens that do not resolve to instruments on '{expectedMeter}':{Environment.NewLine}  - " +
            string.Join(Environment.NewLine + "  - ", unknown));
    }

    private static HashSet<string> ExtractInstrumentTokens(string json)
    {
        using var doc = JsonDocument.Parse(json);
        var tokens = new HashSet<string>(StringComparer.Ordinal);
        WalkForExpr(doc.RootElement, tokens);
        return tokens;
    }

    private static void WalkForExpr(JsonElement element, HashSet<string> tokens)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                foreach (var prop in element.EnumerateObject())
                {
                    if ((prop.Name == "expr" || prop.Name == "query") && prop.Value.ValueKind == JsonValueKind.String)
                    {
                        var s = prop.Value.GetString();
                        if (!string.IsNullOrEmpty(s))
                        {
                            foreach (Match m in InstrumentTokenRegex.Matches(s))
                            {
                                tokens.Add(m.Value);
                            }
                        }
                    }
                    WalkForExpr(prop.Value, tokens);
                }
                break;

            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    WalkForExpr(item, tokens);
                }
                break;
        }
    }
}
