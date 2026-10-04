using System.Text.Json;
using System.Text.RegularExpressions;
using Orleans.Lattice;
using Orleans.Lattice.Dashboards;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Pins the description of the Grafana panel charting
/// <c>orleans.lattice.leaf.snapshot.coverage_repairs</c> to the instrument's
/// real outcome arms (issue #3221). Panel descriptions live in dashboard JSON,
/// which the markdown arm-arity gate does not scan, so an arm added to
/// <see cref="LatticeMetrics.CoverageRepairArms"/> could otherwise leave the
/// panel naming a stale count with nothing to catch it.
/// </summary>
[TestFixture]
public sealed class DashboardCoverageRepairsDescriptionTests
{
    private const string SeriesName = "orleans_lattice_leaf_snapshot_coverage_repairs_total";

    private static readonly string[] NumberWords =
        ["zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine", "ten", "eleven", "twelve"];

    private static readonly Regex ArmCountClaim = new(
        @"\b(zero|one|two|three|four|five|six|seven|eight|nine|ten|eleven|twelve)\s+(terminal\s+|outcome\s+)?arms\b",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

    [Test]
    public void Coverage_repairs_panel_exists_exactly_once_across_dashboards()
    {
        Assert.That(FindCoverageRepairsDescriptions(), Has.Count.EqualTo(1));
    }

    [Test]
    public void Coverage_repairs_description_names_every_outcome_arm()
    {
        var description = SingleDescription();

        foreach (var arm in LatticeMetrics.CoverageRepairArms)
        {
            var value = (string)arm.Value!;
            Assert.That(description, Does.Contain("outcome=" + value),
                $"The coverage_repairs panel description does not document the '{value}' arm.");
        }
    }

    [Test]
    public void Coverage_repairs_description_arm_counts_match_the_instrument()
    {
        var description = SingleDescription();
        var total = LatticeMetrics.CoverageRepairArms.Length;
        var terminal = total - 1;
        Assert.That(total, Is.LessThan(NumberWords.Length), "Extend NumberWords for the new arm count.");

        var allowed = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { NumberWords[total], NumberWords[terminal] };
        var claims = ArmCountClaim.Matches(description).Select(m => m.Groups[1].Value).ToArray();

        Assert.That(claims, Is.Not.Empty, "The description no longer states an arm count at all.");
        Assert.That(claims, Is.All.Matches<string>(c => allowed.Contains(c)),
            $"The description claims an arm count other than {NumberWords[total]} (all arms) or {NumberWords[terminal]} (terminal arms).");
        Assert.That(description, Does.Contain(NumberWords[terminal] + " terminal arms").IgnoreCase);
        Assert.That(description, Does.Contain("all " + NumberWords[total] + " arms").IgnoreCase);
    }

    [Test]
    public void Coverage_repairs_description_states_the_terminal_sum_is_exact_and_rearmed_co_occurs()
    {
        var description = SingleDescription();

        Assert.That(description, Does.Contain("EXACT invocation count"));
        Assert.That(description, Does.Contain("CO-OCCURS"));
        Assert.That(description, Does.Not.Contain("lower bound on invocations").IgnoreCase);
        Assert.That(description, Does.Not.Contain("deduplicated").IgnoreCase);
    }

    [Test]
    public void Coverage_repairs_description_does_not_claim_a_per_activation_exhaustion_budget()
    {
        var description = SingleDescription();

        Assert.That(description, Does.Not.Contain("once per activation").IgnoreCase);
        Assert.That(description, Does.Not.Contain("per-activation").IgnoreCase);
        Assert.That(description, Does.Contain("once per backoff cycle"));
    }

    private static string SingleDescription()
    {
        var descriptions = FindCoverageRepairsDescriptions();
        Assert.That(descriptions, Has.Count.EqualTo(1));
        return descriptions[0];
    }

    private static List<string> FindCoverageRepairsDescriptions()
    {
        var found = new List<string>();
        foreach (var kind in LatticeDashboards.All)
        {
            using var doc = JsonDocument.Parse(LatticeDashboards.GetGrafanaDashboardJson(kind));
            if (doc.RootElement.TryGetProperty("panels", out var panels))
            {
                CollectFromPanels(panels, found);
            }
        }

        return found;
    }

    private static void CollectFromPanels(JsonElement panels, List<string> found)
    {
        foreach (var panel in panels.EnumerateArray())
        {
            if (panel.TryGetProperty("panels", out var nested))
            {
                CollectFromPanels(nested, found);
            }

            if (!panel.TryGetProperty("targets", out var targets))
            {
                continue;
            }

            var charts = targets.EnumerateArray().Any(t =>
                t.TryGetProperty("expr", out var expr) && (expr.GetString() ?? string.Empty).Contains(SeriesName, StringComparison.Ordinal));
            if (charts)
            {
                found.Add(panel.TryGetProperty("description", out var d) ? d.GetString() ?? string.Empty : string.Empty);
            }
        }
    }
}
