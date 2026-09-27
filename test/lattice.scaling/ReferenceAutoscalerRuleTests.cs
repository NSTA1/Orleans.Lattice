using System.Globalization;
using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Scaling.Tests;

/// <summary>
/// Guards the shipped reference autoscaler rules against the two ways they have
/// silently failed to scale out: a target value that can never ask for more
/// replicas, and a Prometheus query naming a series or label nothing exports.
/// </summary>
/// <remarks>
/// <see cref="ScalingSignal.ScaleValue"/> is the dominant compute pressure
/// (<c>0.0</c> to <c>1.0</c>) times the current replica count, so it never
/// exceeds that count, and KEDA's default <c>AverageValue</c> metric type asks for
/// <c>ceil(scaleValue / target)</c> replicas. A target of <c>1</c> therefore asks
/// for at most the current count at full saturation and can only hold or shrink
/// the pool. Every reference rule shipped <c>1</c> until this guard, while the
/// docs and the <c>ClusterScaling</c> sample said it must be below <c>1</c>.
/// </remarks>
[TestFixture]
public sealed class ReferenceAutoscalerRuleTests
{
    private static readonly string[] PackageRules =
    [
        "src/lattice.scaling/deploy/aca-scale-rule.bicep",
        "src/lattice.scaling/deploy/aca-scale-rule.json",
        "src/lattice.scaling/deploy/keda-scaledobject.yaml",
    ];

    private const string ComputeModule = "reference-architecture/bicep/modules/compute.bicep";
    private const string CollectorConfig = "reference-architecture/bicep/modules/scraper/otel-collector-config.yaml";

    // Matches the value assigned to a targetValue key in Bicep ('0.5'), JSON
    // ("targetValue": "0.5") and YAML (targetValue: "0.5"), and nowhere else:
    // the JSON comment key "//targetValue" is excluded by the leading anchor.
    private static readonly Regex TargetValueAssignment = new(
        """(?m)^\s*"?targetValue"?\s*:\s*["']([^"']+)["']""",
        RegexOptions.CultureInvariant);

    private static string Read(string relativePath) =>
        File.ReadAllText(Path.Combine(HygieneRepository.FindRepoRoot(), relativePath.Replace('/', Path.DirectorySeparatorChar)));

    private static double ParseInvariant(string value) =>
        double.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture);

    /// <summary>The replica count KEDA asks for from a fully saturated pool of <paramref name="replicas"/>.</summary>
    private static int DesiredAtSaturation(int replicas, double target) => (int)Math.Ceiling(replicas / target);

    [TestCaseSource(nameof(PackageRules))]
    public void Package_reference_rule_sets_a_target_value_that_can_scale_out(string relativePath)
    {
        var matches = TargetValueAssignment.Matches(Read(relativePath));

        Assert.That(matches, Has.Count.EqualTo(1), $"{relativePath} must assign targetValue exactly once");
        var target = ParseInvariant(matches[0].Groups[1].Value);
        Assert.Multiple(() =>
        {
            Assert.That(target, Is.GreaterThan(0.0).And.LessThan(1.0), $"{relativePath}: targetValue must be below 1 for the pool to grow");
            Assert.That(DesiredAtSaturation(1, target), Is.GreaterThan(1), $"{relativePath}: a saturated single replica must ask for another");
        });
    }

    [Test]
    public void Reference_architecture_scale_threshold_can_scale_out()
    {
        var threshold = ParseInvariant(ReadBicepStringParam(Read(ComputeModule), "siloScaleThreshold"));

        Assert.Multiple(() =>
        {
            Assert.That(threshold, Is.GreaterThan(0.0).And.LessThan(1.0));
            Assert.That(DesiredAtSaturation(1, threshold), Is.GreaterThan(1));
        });
    }

    [Test]
    public void Reference_architecture_scale_query_names_the_exported_scale_value_series()
    {
        var query = ReadBicepStringParam(Read(ComputeModule), "siloScaleQuery");

        // The Prometheus exposition of the gauge: dots become underscores and the
        // "{replica}" annotation unit adds no suffix.
        var series = LatticeScalingMetrics.ScaleValueName.Replace('.', '_');
        Assert.That(
            Regex.IsMatch(query, $@"(?<![A-Za-z0-9_:]){Regex.Escape(series)}(?![A-Za-z0-9_:])"),
            Is.True,
            $"the scale query '{query}' must select the {series} series");
    }

    [Test]
    public void Reference_architecture_scale_query_filters_only_on_labels_the_collector_stamps()
    {
        var query = ReadBicepStringParam(Read(ComputeModule), "siloScaleQuery");
        var stamped = ReadCollectorStaticLabels(Read(CollectorConfig));

        // job and instance are synthesised by the scrape itself; everything else a
        // matcher names must be a static label the collector adds, or the matcher
        // selects nothing and the scaler reads no data at all.
        var available = new Dictionary<string, string?>(stamped.ToDictionary(p => p.Key, p => (string?)p.Value), StringComparer.Ordinal)
        {
            ["job"] = null,
            ["instance"] = null,
        };

        var matchers = Regex.Matches(query, """([A-Za-z_][A-Za-z0-9_]*)\s*(=~|!=|!~|=)\s*"([^"]*)"\s*""");
        Assert.That(matchers, Is.Not.Empty, "the scale query should scope the series to the silo");
        foreach (Match matcher in matchers)
        {
            var label = matcher.Groups[1].Value;
            Assert.That(available.ContainsKey(label), Is.True, $"no exported series carries the label '{label}' the scale query filters on");
            if (matcher.Groups[2].Value == "=" && available[label] is { } value)
            {
                Assert.That(matcher.Groups[3].Value, Is.EqualTo(value), $"the collector stamps {label}=\"{value}\"");
            }
        }
    }

    [Test]
    public void ReadCollectorStaticLabels_finds_the_silo_head_label()
    {
        // Anti-vacuity control for the label check above: if the parser stopped
        // finding the collector's labels, that check would reject every query.
        Assert.That(ReadCollectorStaticLabels(Read(CollectorConfig)), Does.ContainKey("lattice_head").WithValue("silo"));
    }

    private static string ReadBicepStringParam(string bicep, string name)
    {
        var match = Regex.Match(bicep, $@"(?m)^param\s+{Regex.Escape(name)}\s+string\s*=\s*'((?:[^'\\]|\\.)*)'");
        Assert.That(match.Success, Is.True, $"{ComputeModule} must declare a string default for {name}");
        return match.Groups[1].Value.Replace("\\'", "'");
    }

    private static Dictionary<string, string> ReadCollectorStaticLabels(string yaml)
    {
        var labels = new Dictionary<string, string>(StringComparer.Ordinal);
        var lines = yaml.Replace("\r\n", "\n").Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            if (lines[i].Trim() != "labels:")
            {
                continue;
            }

            var indent = lines[i].Length - lines[i].TrimStart().Length;
            for (var j = i + 1; j < lines.Length; j++)
            {
                var line = lines[j];
                var trimmed = line.Trim();
                if (trimmed.Length == 0 || trimmed.StartsWith('#'))
                {
                    continue;
                }
                if (line.Length - line.TrimStart().Length <= indent)
                {
                    break;
                }

                var colon = trimmed.IndexOf(':');
                if (colon > 0)
                {
                    labels[trimmed[..colon].Trim()] = trimmed[(colon + 1)..].Trim().Trim('"', '\'');
                }
            }
        }

        return labels;
    }
}
