using NSubstitute;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// The catalogue's tree ownership probe: the installed version is judged as the install that
/// already owns its trees, so a live install never reads as one it cannot own.
/// </summary>
public sealed partial class LatticeAppCatalogTests
{
    [Test]
    public async Task DescribeFromSource_probes_the_installed_versions_trees_as_the_install_that_owns_them()
    {
        var harness = TwoSources(out _, out _);
        harness.Installs(CatalogHarness.Installed("crm", "1.0.0", "in-image", AppRegistryLifecycleState.Enabled) with
        {
            Provenance = new AppProvenance { Source = "in-image", Publisher = "recorded-publisher" },
        });

        var descriptor = await harness.Catalog.DescribeFromSourceAsync("in-image", "crm");

        Assert.That(descriptor!.Provenance.Publisher, Is.EqualTo("publisher-in-image"), "the description still carries the source's provenance");
        await harness.Registry.Received(1).GetTreeOwnershipConflictsAsync(
            Arg.Any<TenantId>(),
            Arg.Any<AppManifest>(),
            Arg.Is<AppProvenance>(p => p.Publisher == "recorded-publisher"),
            Arg.Any<CancellationToken>());
    }

    [TestCase("in-image", AppRegistryLifecycleState.Uninstalled, null)]
    [TestCase("feed", AppRegistryLifecycleState.Enabled, "1.0.0")]
    [TestCase("feed", AppRegistryLifecycleState.Enabled, "2.0.0")]
    public async Task DescribeFromSource_probes_a_version_no_live_install_owns_as_the_sources_publisher(
        string source, AppRegistryLifecycleState state, string? version)
    {
        var harness = TwoSources(out _, out _);
        harness.Installs(CatalogHarness.Installed("crm", "1.0.0", "in-image", state) with
        {
            Provenance = new AppProvenance { Source = "in-image", Publisher = "recorded-publisher" },
        });

        _ = await harness.Catalog.DescribeFromSourceAsync(source, "crm", version);

        await harness.Registry.Received(1).GetTreeOwnershipConflictsAsync(
            Arg.Any<TenantId>(),
            Arg.Any<AppManifest>(),
            Arg.Is<AppProvenance>(p => p.Publisher == "publisher-" + source),
            Arg.Any<CancellationToken>());
    }
}
