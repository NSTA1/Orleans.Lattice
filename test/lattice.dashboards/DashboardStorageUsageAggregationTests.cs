using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice;
using Orleans.Lattice.Dashboards;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Asserts that every bundled panel reading a storage-usage gauge reduces it with
/// <c>max by (tree)</c> across silos before doing anything else with it.
/// </summary>
/// <remarks>
/// A tree's WAL-only and deep storage-usage aggregators are separate, independently
/// placed grains, and each publishes into its own host silo's sink, so on a
/// multi-silo cluster more than one silo can export the same tree's storage series.
/// The Overview footprint and cluster-total panels summed those series, which
/// reported a tree whose two aggregators sat on different silos at double its size.
/// </remarks>
[TestFixture]
public sealed class DashboardStorageUsageAggregationTests
{
    private static readonly string[] StorageUsageSeries =
    [
        PrometheusName(LatticeMetrics.StorageWalBytesName),
        PrometheusName(LatticeMetrics.StorageSnapshotBytesName),
        PrometheusName(LatticeMetrics.StorageLeafStateBytesName),
        PrometheusName(LatticeMetrics.StorageTotalBytesName),
        PrometheusName(LatticeMetrics.StorageUsageDeepPublishedName),
        PrometheusName(LatticeMetrics.StoragePolicyOverThresholdName),
    ];

    private static readonly Regex SeriesRegex = new(
        @"\b(" + string.Join("|", StorageUsageSeries.Select(Regex.Escape)) + @")\b",
        RegexOptions.Compiled);

    private static readonly Regex MaxByTreeOpening = new(
        @"max\s+by\s*\(\s*tree\s*\)\s*\(\s*$",
        RegexOptions.Compiled);

    [Test]
    public void Every_storage_usage_series_is_reduced_with_max_by_tree_across_silos()
    {
        var expressions = CollectExpressions();
        var referenced = new HashSet<string>(StringComparer.Ordinal);
        var offenders = new List<string>();

        foreach (var (site, expression) in expressions)
        {
            foreach (var violation in Violations(expression, referenced))
            {
                offenders.Add($"{site}: {violation} in '{expression}'");
            }
        }

        Assert.Multiple(() =>
        {
            Assert.That(
                referenced,
                Is.SupersetOf(StorageUsageSeries.Take(4)),
                "The bundled dashboards no longer chart the storage byte gauges this guard inspects, "
                + "so an empty offender list would prove nothing.");
            Assert.That(
                offenders,
                Is.Empty,
                "More than one silo can export a tree's storage-usage series, so a panel must take "
                + "max by (tree) across silos before summing or charting it. Offenders:"
                + Environment.NewLine + string.Join(Environment.NewLine, offenders));
        });
    }

    [Test]
    public void The_detector_flags_a_sum_and_admits_a_sum_of_per_tree_maxima()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                Violations("sum by (tree) (orleans_lattice_storage_wal_bytes{cluster=~\"$cluster\"})", null),
                Is.Not.Empty);
            Assert.That(
                Violations("sum(orleans_lattice_storage_total_bytes{cluster=~\"$cluster\"})", null),
                Is.Not.Empty);
            Assert.That(
                Violations("sum(max by (tree) (orleans_lattice_storage_total_bytes{cluster=~\"$cluster\"}))", null),
                Is.Empty);
            Assert.That(
                Violations("max by (tree) (orleans_lattice_storage_policy_over_threshold{cluster=~\"$cluster\"}) == 1", null),
                Is.Empty);
            Assert.That(
                Violations("sum by (tree) (orleans_lattice_storage_wal_stored_bytes{cluster=~\"$cluster\"})", null),
                Is.Empty,
                "A different series that merely shares the prefix is not a storage-usage sink gauge.");
        });
    }

    private static IEnumerable<string> Violations(string expression, HashSet<string>? referenced)
    {
        foreach (Match match in SeriesRegex.Matches(expression))
        {
            referenced?.Add(match.Value);
            if (!MaxByTreeOpening.IsMatch(expression[..match.Index]))
            {
                yield return $"'{match.Value}' is not reduced with max by (tree)";
            }
        }
    }

    private static List<(string Site, string Expression)> CollectExpressions()
    {
        var expressions = new List<(string, string)>();
        foreach (var kind in LatticeDashboards.All)
        {
            using var document = JsonDocument.Parse(LatticeDashboards.GetGrafanaDashboardJson(kind));
            Walk(document.RootElement, kind.ToString(), expressions);
        }

        return expressions;
    }

    private static void Walk(JsonElement node, string site, List<(string, string)> sink)
    {
        switch (node.ValueKind)
        {
            case JsonValueKind.Object:
                if (node.TryGetProperty("id", out var id) && id.ValueKind == JsonValueKind.Number)
                {
                    site = $"{site.Split(' ')[0]} panel {id.GetRawText()}";
                }

                foreach (var property in node.EnumerateObject())
                {
                    if ((property.NameEquals("expr") || property.NameEquals("query"))
                        && property.Value.ValueKind == JsonValueKind.String)
                    {
                        sink.Add((site, property.Value.GetString() ?? string.Empty));
                        continue;
                    }

                    Walk(property.Value, site, sink);
                }

                break;

            case JsonValueKind.Array:
                foreach (var element in node.EnumerateArray())
                {
                    Walk(element, site, sink);
                }

                break;
        }
    }

    private static string PrometheusName(string instrumentName) => instrumentName.Replace('.', '_');
}
