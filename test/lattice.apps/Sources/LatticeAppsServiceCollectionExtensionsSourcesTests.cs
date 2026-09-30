using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Apps.Sources;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Wiring tests for the sources partial of <see cref="LatticeAppsServiceCollectionExtensions"/>: the
/// composed <see cref="AppSourceSet"/>, catalogue source registration, and the explicit asset prefix overload.
/// </summary>
[TestFixture]
public sealed class LatticeAppsServiceCollectionExtensionsSourcesTests
{
    private static ServiceCollection CoreServices()
    {
        var services = new ServiceCollection();
        var validator = Substitute.For<IValidateOptions<LatticeOptions>>();
        validator.Validate(Arg.Any<string?>(), Arg.Any<LatticeOptions>()).Returns(ValidateOptionsResult.Success);
        services.AddSingleton(validator);
        return services;
    }

    private static FakeAppAssembly Assembly(string slug) =>
        new(SourceTestManifests.ResourceName, SourceTestManifests.Minimal(slug, "1.0.0"));

    [Test]
    public void AddLatticeApps_serves_the_seam_from_a_set_composing_the_in_image_source()
    {
        var services = CoreServices();
        services.AddLatticeApps();
        services.AddLatticeApps();

        using var provider = services.BuildServiceProvider();
        var set = provider.GetRequiredService<AppSourceSet>();

        Assert.That(provider.GetRequiredService<IAppSource>(), Is.SameAs(set));
        Assert.That(set.IsValid, Is.True);
        Assert.That(set.Sources.Single(), Is.SameAs(provider.GetRequiredService<InImageAppSource>()));
        Assert.That(services.Count(d => d.ServiceType == typeof(IAppCatalogSource)), Is.EqualTo(1));
    }

    [Test]
    public async Task With_only_the_in_image_source_existing_seam_callers_see_identical_results()
    {
        var services = CoreServices();
        services.AddLatticeApps()
            .AddLatticeApp("notes", Assembly("notes"), SourceTestManifests.ResourceName)
            .AddLatticeApp("broken", new FakeAppAssembly(SourceTestManifests.ResourceName, "{ nope"), SourceTestManifests.ResourceName);
        using var provider = services.BuildServiceProvider();
        var seam = provider.GetRequiredService<IAppSource>();
        var direct = provider.GetRequiredService<InImageAppSource>();
        var notes = AppSlug.Parse("notes");

        Assert.That(await seam.ResolveAsync(notes), Is.SameAs(await direct.ResolveAsync(notes)));
        Assert.That(await seam.ResolveAsync(notes, AppVersion.Parse("1.0.0")), Is.SameAs(await direct.ResolveAsync(notes, AppVersion.Parse("1.0.0"))));
        Assert.That(await seam.ResolveAsync(AppSlug.Parse("broken")), Is.SameAs(await direct.ResolveAsync(AppSlug.Parse("broken"))));

        var mismatch = await seam.ResolveAsync(notes, AppVersion.Parse("2.0.0"));
        var expectedMismatch = await direct.ResolveAsync(notes, AppVersion.Parse("2.0.0"));
        Assert.That((mismatch.Status, mismatch.RequestedVersion, mismatch.AvailableVersion, mismatch.Errors.Single()),
            Is.EqualTo((expectedMismatch.Status, expectedMismatch.RequestedVersion, expectedMismatch.AvailableVersion, expectedMismatch.Errors.Single())));

        var missing = await seam.ResolveAsync(AppSlug.Parse("absent"));
        Assert.That(missing.Status, Is.EqualTo(AppSourceStatus.NotFound));
        Assert.That(missing.Errors.Single(), Is.EqualTo((await direct.ResolveAsync(AppSlug.Parse("absent"))).Errors.Single()));
    }

    [Test]
    public async Task AddLatticeAppSource_of_a_type_composes_it_once_after_the_in_image_source()
    {
        var services = CoreServices();
        services.AddLatticeApps()
            .AddLatticeAppSource<FakeDynamicAppSource>()
            .AddLatticeAppSource<FakeDynamicAppSource>();

        using var provider = services.BuildServiceProvider();
        var set = provider.GetRequiredService<AppSourceSet>();

        Assert.That(set.IsValid, Is.True);
        Assert.That(set.Sources.Select(s => s.Descriptor.Key), Is.EqualTo(new[] { InImageAppSource.SourceKey, FakeDynamicAppSource.DefaultKey }));
        Assert.That((await provider.GetRequiredService<IAppSource>().ResolveAsync(AppSlug.Parse("absent"))).Status, Is.EqualTo(AppSourceStatus.NotFound));
    }

    [Test]
    public async Task AddLatticeAppSource_instance_resolves_with_its_key_in_provenance()
    {
        var feed = new FakeDynamicAppSource().Add("tasks", "4.0.0");
        var services = CoreServices();
        services.AddLatticeApps().AddLatticeAppSource(feed);

        using var provider = services.BuildServiceProvider();
        var result = await provider.GetRequiredService<IAppSource>().ResolveAsync(AppSlug.Parse("tasks"));

        Assert.That(result.IsResolved, Is.True);
        Assert.That(result.Provenance!.Source, Is.EqualTo(FakeDynamicAppSource.DefaultKey));
        Assert.That(provider.GetRequiredService<AppSourceSet>().Sources, Does.Contain(feed));
    }

    [Test]
    public async Task A_duplicate_source_key_surfaces_on_resolution_never_on_startup()
    {
        var services = CoreServices();
        services.AddLatticeApps()
            .AddLatticeApp("notes", Assembly("notes"), SourceTestManifests.ResourceName)
            .AddLatticeAppSource(new FakeDynamicAppSource(InImageAppSource.SourceKey));

        using var provider = services.BuildServiceProvider();
        IAppSource? seam = null;
        Assert.DoesNotThrow(() => seam = provider.GetRequiredService<IAppSource>());
        var result = await seam!.ResolveAsync(AppSlug.Parse("notes"));

        Assert.That(result.Status, Is.EqualTo(AppSourceStatus.SourceMisconfigured));
        Assert.That(result.Errors.Single().Code, Is.EqualTo("duplicate-source"));
    }

    [Test]
    public void AddLatticeAppSource_validates_its_arguments()
    {
        var services = CoreServices();

        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeAppSource<FakeDynamicAppSource>());
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeAppSource(new FakeDynamicAppSource()));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeAppSource(null!));
        Assert.Throws<ArgumentNullException>(() => ((ISiloBuilder)null!).AddLatticeAppSource<FakeDynamicAppSource>());
        Assert.Throws<ArgumentNullException>(() => ((ISiloBuilder)null!).AddLatticeAppSource(new FakeDynamicAppSource()));
    }

    [Test]
    public void AddLatticeApp_with_a_prefix_records_it_on_the_registration()
    {
        var services = CoreServices();
        var assembly = Assembly("notes");

        var returned = services.AddLatticeApps().AddLatticeApp("notes", assembly, SourceTestManifests.ResourceName, "Bundle.");

        Assert.That(returned, Is.SameAs(services));
        using var provider = services.BuildServiceProvider();
        var registration = provider.GetRequiredService<IOptions<InImageAppSourceOptions>>().Value.Registrations.Single();
        Assert.That(registration.Slug, Is.EqualTo(AppSlug.Parse("notes")));
        Assert.That(registration.Assembly, Is.SameAs(assembly));
        Assert.That(registration.ManifestResourceName, Is.EqualTo(SourceTestManifests.ResourceName));
        Assert.That(registration.AssetResourcePrefix, Is.EqualTo("Bundle."));
    }

    [Test]
    public void AddLatticeApp_with_a_prefix_validates_its_arguments()
    {
        var services = CoreServices();
        var assembly = Assembly("notes");

        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeApp("notes", assembly, "r", "p."));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeApp(null!, assembly, "r", "p."));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeApp("notes", null!, "r", "p."));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeApp("notes", assembly, null!, "p."));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeApp("notes", assembly, "r", null!));
        Assert.Throws<FormatException>(() => services.AddLatticeApp("Not A Slug", assembly, "r", "p."));
        Assert.Throws<ArgumentNullException>(() => ((ISiloBuilder)null!).AddLatticeApp("notes", assembly, "r", "p."));
    }

    [Test]
    public void Silo_builder_overloads_delegate_to_the_service_collection()
    {
        var services = CoreServices();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        var feed = new FakeDynamicAppSource("other-feed");

        var returned = builder.AddLatticeApps()
            .AddLatticeAppSource<FakeDynamicAppSource>()
            .AddLatticeAppSource(feed)
            .AddLatticeApp("notes", Assembly("notes"), SourceTestManifests.ResourceName, "Bundle.");

        Assert.That(returned, Is.SameAs(builder));
        using var provider = services.BuildServiceProvider();
        var set = provider.GetRequiredService<AppSourceSet>();
        Assert.That(set.Sources.Select(s => s.Descriptor.Key),
            Is.EqualTo(new[] { InImageAppSource.SourceKey, FakeDynamicAppSource.DefaultKey, "other-feed" }));
        Assert.That(provider.GetRequiredService<IOptions<InImageAppSourceOptions>>().Value.Registrations.Single().AssetResourcePrefix,
            Is.EqualTo("Bundle."));
    }
}
