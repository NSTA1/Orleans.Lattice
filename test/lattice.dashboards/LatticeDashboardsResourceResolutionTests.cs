namespace Orleans.Lattice.Dashboards.Tests;

/// <summary>
/// Unit tests for the resource-resolution surface of <see cref="LatticeDashboards"/>:
/// the <see cref="LatticeDashboardKind"/> to embedded-resource-name mapping and the
/// out-of-range guard it applies to an undefined kind.
/// <para>
/// <see cref="DashboardJsonTests"/> asserts the <em>content</em> of every dashboard
/// that resolves. This fixture asserts the mapping itself, including the arm that
/// content-shaped tests can never reach: a kind outside the enum. That arm is the
/// difference between a future dashboard added to the enum but not to the switch
/// failing loudly at its call site and it silently resolving to some other
/// dashboard's JSON.
/// </para>
/// </summary>
[TestFixture]
public sealed class LatticeDashboardsResourceResolutionTests
{
    /// <summary>
    /// An integer that is deliberately not a defined <see cref="LatticeDashboardKind"/>.
    /// Enum values in .NET are not closed, so a caller can always produce one by cast
    /// (from a configuration value, a wire payload, or an off-by-one), which is exactly
    /// what the guard exists for.
    /// </summary>
    private const LatticeDashboardKind UndefinedKind = (LatticeDashboardKind)9999;

    [Test]
    public void GetGrafanaDashboardJson_throws_for_a_kind_outside_the_enum()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(
            () => LatticeDashboards.GetGrafanaDashboardJson(UndefinedKind));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.ParamName, Is.EqualTo("kind"));
            Assert.That(ex.ActualValue, Is.EqualTo(UndefinedKind));
        });
    }

    [Test]
    public void ResourceNameFor_throws_for_a_kind_outside_the_enum()
    {
        Assert.That(
            () => LatticeDashboards.ResourceNameFor(UndefinedKind),
            Throws.InstanceOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public void GetGrafanaDashboardJson_throws_for_the_next_unallocated_kind()
    {
        // The first value past the highest defined member is the one a newly added
        // dashboard would take. Until it is added to the switch, resolving it must
        // fail rather than fall through to another dashboard's resource.
        var next = (LatticeDashboardKind)(Enum.GetValues<LatticeDashboardKind>().Cast<int>().Max() + 1);

        Assert.That(
            () => LatticeDashboards.GetGrafanaDashboardJson(next),
            Throws.InstanceOf<ArgumentOutOfRangeException>(),
            "A kind not yet wired into ResourceNameFor must throw, not resolve to another dashboard.");
    }

    [Test]
    public void ResourceNameFor_maps_every_declared_kind_to_a_distinct_name()
    {
        var names = LatticeDashboards.All.Select(LatticeDashboards.ResourceNameFor).ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(names, Is.Unique, "Two dashboard kinds resolve to the same embedded resource.");
            Assert.That(names, Has.All.StartWith("Orleans.Lattice.Dashboards.Grafana."));
            Assert.That(names, Has.All.EndWith(".json"));
        });
    }

    [Test]
    public void ResourceNameFor_resolves_to_a_resource_that_is_actually_embedded()
    {
        var assembly = typeof(LatticeDashboards).Assembly;
        var embedded = assembly.GetManifestResourceNames();

        Assert.That(
            LatticeDashboards.All.Select(LatticeDashboards.ResourceNameFor),
            Is.SubsetOf(embedded),
            "A dashboard kind names a resource that is not embedded in the assembly, so "
            + "GetGrafanaDashboardJson would throw at runtime.");
    }

    [Test]
    public void All_reports_every_declared_kind()
    {
        Assert.That(LatticeDashboards.All, Is.EquivalentTo(Enum.GetValues<LatticeDashboardKind>()));
    }
}
