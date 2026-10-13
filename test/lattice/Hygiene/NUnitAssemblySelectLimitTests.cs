using System.Xml.Linq;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Tests.Hygiene;

/// <summary>
/// Pins the shared run settings that lift NUnit3TestAdapter's
/// <c>AssemblySelectLimit</c>. When a VSTest <c>--filter</c> selects more tests
/// than that limit (default 2000) the adapter abandons the filter and executes
/// the WHOLE assembly - Chaos and Tlc fixtures included - while reporting only
/// the selected tests, so the run looks right and silently costs the full
/// suite. The content-gate leg crossed the limit and went from about six to
/// about thirty-eight minutes with nothing in its results to say why.
/// </summary>
[TestFixture]
public sealed class NUnitAssemblySelectLimitTests
{
    /// <summary>
    /// Every project under <c>test/</c> picks the run settings up through
    /// <c>test/Directory.Build.props</c>, and those settings raise the limit
    /// above any selection a CI leg can make.
    /// </summary>
    [Test]
    public void Test_projects_lift_the_adapter_assembly_select_limit()
    {
        var testRoot = Path.Combine(HygieneRepository.FindRepoRoot(), "test");

        var props = XDocument.Load(Path.Combine(testRoot, "Directory.Build.props"));
        var settingsPath = props.Descendants("RunSettingsFilePath").Select(e => e.Value).SingleOrDefault();
        Assert.That(
            settingsPath,
            Is.EqualTo("$(MSBuildThisFileDirectory)nunit.runsettings"),
            "test/Directory.Build.props must point RunSettingsFilePath at test/nunit.runsettings");

        var settings = XDocument.Load(Path.Combine(testRoot, "nunit.runsettings"));
        var limit = settings.Root?.Element("NUnit")?.Element("AssemblySelectLimit")?.Value;
        Assert.That(limit, Is.Not.Null, "nunit.runsettings must set NUnit/AssemblySelectLimit");
        Assert.That(
            long.Parse(limit!, System.Globalization.CultureInfo.InvariantCulture),
            Is.GreaterThanOrEqualTo(int.MaxValue),
            "AssemblySelectLimit must not cap a filtered selection");
    }
}
