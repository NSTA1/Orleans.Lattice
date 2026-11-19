using System.Text.RegularExpressions;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// The reference architecture's Grafana head provisions the bundled dashboards from
/// <c>reference-architecture/bicep/modules/observability.bicep</c>, which lists each
/// dashboard file by hand. This fixture fails when a bundled dashboard is missing
/// from that list, and when an entry is loaded through <c>loadTextContent</c>, whose
/// 131,072-character limit the larger dashboards exceed, so the module stops compiling.
/// </summary>
[TestFixture]
public sealed partial class ReferenceArchitectureDashboardProvisioningTests
{
    private static string ReadModule() => File.ReadAllText(Path.Combine(
        HygieneRepository.FindRepoRoot(), "reference-architecture", "bicep", "modules", "observability.bicep"));

    [GeneratedRegex(@"src/lattice\.dashboards/Grafana/(?<file>[A-Za-z]+\.json)")]
    private static partial Regex DashboardReference();

    [Test]
    public void The_observability_module_provisions_every_bundled_dashboard()
    {
        var provisioned = DashboardReference().Matches(ReadModule())
            .Select(match => "Orleans.Lattice.Dashboards.Grafana." + match.Groups["file"].Value)
            .ToHashSet(StringComparer.Ordinal);
        var bundled = LatticeDashboards.All.Select(LatticeDashboards.ResourceNameFor).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(bundled, Is.Not.Empty);
            Assert.That(bundled, Is.SubsetOf(provisioned));
        });
    }

    [Test]
    public void The_observability_module_loads_dashboards_as_json_not_text()
    {
        Assert.That(
            ReadModule(),
            Does.Not.Contain("loadTextContent('../../../src/lattice.dashboards/"),
            "loadTextContent is limited to 131,072 characters; load dashboards with string(loadJsonContent(...)).");
    }
}
