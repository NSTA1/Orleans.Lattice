using NSubstitute;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Apps.Sources;
using Orleans.Lattice.Apps.Tests;

namespace Orleans.Lattice.Api.Apps.Tests;

/// <summary>
/// Shared harness for the catalogue tests: named test sources composed into a real <see cref="AppSourceSet"/>,
/// substitutes for the registry and pipeline, a recording access gate and a configurable tenant resolver.
/// </summary>
internal sealed class CatalogHarness
{
    public IAppRegistry Registry { get; } = Substitute.For<IAppRegistry>();

    public IAppActivationPipeline Pipeline { get; } = Substitute.For<IAppActivationPipeline>();

    public RecordingAccessGate Gate { get; } = new();

    public ConfigurableTenantResolver Tenants { get; } = new();

    public List<IAppCatalogSource> Sources { get; } = [];

    public LatticeAppCatalog Catalog => new(new AppSourceSet(Sources), Registry, Pipeline, Gate, Tenants);

    public CatalogHarness()
    {
        Installs();
        Registry.GetTreeOwnershipConflictsAsync(Arg.Any<TenantId>(), Arg.Any<AppManifest>(), Arg.Any<AppProvenance>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult<IReadOnlyList<AppTreeOwnershipConflict>>([]));
    }

    public CatalogHarness With(params IAppCatalogSource[] sources)
    {
        Sources.AddRange(sources);
        return this;
    }

    /// <summary>Makes the registry list and return exactly these records for any tenant.</summary>
    public void Installs(params AppRegistryRecord[] records)
    {
        Registry.ListForTenantAsync(Arg.Any<TenantId>(), Arg.Any<CancellationToken>()).Returns(_ => ToAsync(records));
        Registry.GetAsync(Arg.Any<TenantId>(), Arg.Any<AppSlug>(), Arg.Any<CancellationToken>())
            .Returns(call => records.FirstOrDefault(r => r.Slug == call.ArgAt<AppSlug>(1)));
    }

    public void Deny() => Gate.Decision = LatticeAccessDecision.Deny("no");

    public static AppManifest Manifest(string slug, string version = "1.0.0") =>
        AppsControlHarness.Manifest(version, slug);

    public static AppManifest UiManifest(string slug, string version = "1.0.0", params AppUiBridgeDeclaration[] bridge) =>
        UiTestManifests.WithUi(Manifest(slug, version), bridge);

    public static AppRegistryRecord Installed(
        string slug,
        string version,
        string source,
        AppRegistryLifecycleState state = AppRegistryLifecycleState.Installed) =>
        AppsControlHarness.Record(state, version, slug: slug) with
        {
            Provenance = new AppProvenance { Source = source, Publisher = "publisher-" + source },
        };

    public void AssertNothingTouched()
    {
        Assert.That(Registry.ReceivedCalls(), Is.Empty, "registry");
        Assert.That(Pipeline.ReceivedCalls(), Is.Empty, "pipeline");
        foreach (var source in Sources.OfType<TestCatalogSource>())
        {
            Assert.That(source.Resolutions + source.Listings + source.AssetOpens, Is.Zero, source.Descriptor.Key);
        }
    }

    private static async IAsyncEnumerable<AppRegistryRecord> ToAsync(AppRegistryRecord[] records)
    {
        foreach (var record in records)
        {
            await Task.Yield();
            yield return record;
        }
    }
}
