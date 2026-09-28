using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// Wiring tests for <see cref="LatticeAppsServiceCollectionExtensions"/>, over a bare service
/// collection (no silo): the ordering guard, idempotence, the registered services, in-image app
/// registration, and the replication intent with and without the replication add-on.
/// </summary>
[TestFixture]
public sealed class LatticeAppsServiceCollectionExtensionsTests
{
    private const string ReplicatedManifest = """
        {
          "identity": { "slug": "notes", "version": "1.0.0" },
          "trees": [{ "name": "records" }],
          "roles": [],
          "replication": [{ "tree": "records", "mergeMode": "LwwRegister" }],
          "subscriptions": [],
          "mcpTools": []
        }
        """;

    private static ServiceCollection CoreServices()
    {
        var services = new ServiceCollection();
        var validator = Substitute.For<IValidateOptions<LatticeOptions>>();
        validator.Validate(Arg.Any<string?>(), Arg.Any<LatticeOptions>()).Returns(ValidateOptionsResult.Success);
        services.AddSingleton(validator);
        return services;
    }

    private static int Count<T>(IServiceCollection services) => services.Count(d => d.ServiceType == typeof(T));

    [Test]
    public void AddLatticeApps_requires_AddLattice_first()
    {
        var error = Assert.Throws<InvalidOperationException>(() => new ServiceCollection().AddLatticeApps());

        Assert.That(error!.Message, Does.Contain("AddLattice()"));
    }

    [Test]
    public void AddLatticeApps_rejects_null_arguments()
    {
        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeApps());
        Assert.Throws<ArgumentNullException>(() => ((ISiloBuilder)null!).AddLatticeApps());
    }

    [Test]
    public void AddLatticeApps_registers_the_add_on_services()
    {
        var services = CoreServices();

        var returned = services.AddLatticeApps();

        Assert.That(returned, Is.SameAs(services));
        Assert.That(Count<IAppRegistry>(services), Is.EqualTo(1));
        Assert.That(Count<IAppRegistryStore>(services), Is.EqualTo(1));
        Assert.That(Count<AppInstallAuthorizer>(services), Is.EqualTo(1));
        Assert.That(Count<IAppRegistryProjection>(services), Is.EqualTo(1));
        Assert.That(Count<IMutationObserver>(services), Is.EqualTo(2), "the registry snapshot maintainer and the subscription router");
        Assert.That(Count<IAppSource>(services), Is.EqualTo(1));
        Assert.That(Count<IAppActivationPipeline>(services), Is.EqualTo(1));
        Assert.That(Count<IAppActivationStatusStore>(services), Is.EqualTo(1));
        Assert.That(Count<IAppTreeProvisioner>(services), Is.EqualTo(1));
        Assert.That(Count<AppActivationEngine>(services), Is.EqualTo(1));
        Assert.That(Count<AppActivationRunner>(services), Is.EqualTo(1));
        Assert.That(services.Any(d => d.ServiceType == typeof(IHostedService) && d.ImplementationType == typeof(AppStartupReconciler)), Is.True);
        Assert.That(Count<IPostConfigureOptions<LatticeReplicationOptions>>(services), Is.Zero);
        Assert.That(services.Any(d => d.ImplementationType == typeof(AppTreeOptionsConfigurator)), Is.True);
    }

    [Test]
    public void AddLatticeApps_is_idempotent_but_layers_configuration()
    {
        var services = CoreServices();
        services.AddLatticeApps(options => options.StartupRetryDelay = TimeSpan.FromSeconds(1));
        var count = services.Count;

        services.AddLatticeApps(options => options.ReconcileOnStartup = false);

        Assert.That(Count<IMutationObserver>(services), Is.EqualTo(2), "the registry snapshot maintainer and the subscription router");
        Assert.That(services.Count, Is.EqualTo(count + 1), "only the second configure delegate is added");
        using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptions<LatticeAppsOptions>>().Value;
        Assert.That(options.StartupRetryDelay, Is.EqualTo(TimeSpan.FromSeconds(1)));
        Assert.That(options.ReconcileOnStartup, Is.False);
    }

    [Test]
    public void AddLatticeApps_replaces_the_core_allow_all_ownership_guard_with_the_ledger_backed_one()
    {
        var services = CoreServices();
        var coreGuard = Substitute.For<ITreeOwnershipGuard>();
        services.AddSingleton(coreGuard);

        services.AddLatticeApps();
        services.AddLatticeApps();

        var guards = services.Where(d => d.ServiceType == typeof(ITreeOwnershipGuard)).ToArray();
        Assert.That(guards, Has.Length.EqualTo(1));
        Assert.That(guards[0].ImplementationType, Is.EqualTo(typeof(AppTreeOwnershipGuard)));
        Assert.That(Count<AppTreeOwnershipLedger>(services), Is.EqualTo(1));
        Assert.That(Count<IAppTreeLedgerStore>(services), Is.EqualTo(1));
        Assert.That(Count<IAppTreeFacts>(services), Is.EqualTo(1));
    }

    [Test]
    public void Invalid_options_are_rejected_on_resolution()
    {
        var services = CoreServices();
        services.AddLatticeApps(options => options.StartupRetryDelay = TimeSpan.Zero);
        using var provider = services.BuildServiceProvider();

        Assert.Throws<OptionsValidationException>(() => _ = provider.GetRequiredService<IOptions<LatticeAppsOptions>>().Value);
    }

    [Test]
    public void A_host_supplied_app_source_is_kept()
    {
        var services = CoreServices();
        var custom = Substitute.For<IAppSource>();
        services.AddSingleton(custom);

        services.AddLatticeApps();

        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IAppSource>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLatticeApp_registers_an_in_image_app_the_source_resolves()
    {
        var services = CoreServices();
        var assembly = new FakeAppAssembly(SourceTestManifests.ResourceName, ReplicatedManifest);

        services.AddLatticeApps().AddLatticeApp("notes", assembly, SourceTestManifests.ResourceName);

        using var provider = services.BuildServiceProvider();
        var registration = provider.GetRequiredService<IOptions<InImageAppSourceOptions>>().Value.Registrations.Single();
        Assert.That(registration.Slug, Is.EqualTo(AppSlug.Parse("notes")));
        Assert.That(registration.Assembly, Is.SameAs(assembly));
        var resolved = provider.GetRequiredService<IAppSource>().ResolveAsync(AppSlug.Parse("notes")).Result;
        Assert.That(resolved.IsResolved, Is.True);
    }

    [Test]
    public void AddLatticeApp_validates_its_arguments()
    {
        var services = CoreServices();
        var assembly = new FakeAppAssembly(SourceTestManifests.ResourceName, ReplicatedManifest);

        Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddLatticeApp("notes", assembly, "r"));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeApp(null!, assembly, "r"));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeApp("notes", null!, "r"));
        Assert.Throws<ArgumentNullException>(() => services.AddLatticeApp("notes", assembly, null!));
        Assert.Throws<FormatException>(() => services.AddLatticeApp("Not A Slug", assembly, "r"));
        Assert.Throws<ArgumentNullException>(() => ((ISiloBuilder)null!).AddLatticeApp("notes", assembly, "r"));
    }

    [Test]
    public void Silo_builder_overloads_delegate_to_the_service_collection()
    {
        var services = CoreServices();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);
        var assembly = new FakeAppAssembly(SourceTestManifests.ResourceName, ReplicatedManifest);

        var returned = builder.AddLatticeApps().AddLatticeApp("notes", assembly, SourceTestManifests.ResourceName);

        Assert.That(returned, Is.SameAs(builder));
        Assert.That(Count<IAppActivationPipeline>(services), Is.EqualTo(1));
        using var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IOptions<InImageAppSourceOptions>>().Value.Registrations, Has.Count.EqualTo(1));
    }

    [Test]
    public void Registering_an_app_does_not_enrol_its_trees_in_static_replication()
    {
        var services = CoreServices();
        services.ConfigureAll<LatticeReplicationOptions>(options => options.ReplicatedTrees =
            new Dictionary<string, LatticeMergeMode> { ["operator-tree"] = LatticeMergeMode.OrSet });
        services.AddLatticeApps()
            .AddLatticeApp("notes", new FakeAppAssembly(SourceTestManifests.ResourceName, ReplicatedManifest), SourceTestManifests.ResourceName);

        using var provider = services.BuildServiceProvider();
        var trees = provider.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>().Get("a/notes/records").ReplicatedTrees;

        Assert.That(trees!.Keys, Is.EquivalentTo(new[] { "operator-tree" }));
        Assert.That(trees["operator-tree"], Is.EqualTo(LatticeMergeMode.OrSet));
    }

    [Test]
    public void With_replication_absent_the_intent_is_never_resolved()
    {
        var services = CoreServices();
        var assembly = new FakeAppAssembly(SourceTestManifests.ResourceName, ReplicatedManifest);
        services.AddLatticeApps().AddLatticeApp("notes", assembly, SourceTestManifests.ResourceName);

        using var provider = services.BuildServiceProvider();
        _ = provider.GetRequiredService<IOptions<LatticeAppsOptions>>().Value;
        _ = provider.GetRequiredService<IOptions<InImageAppSourceOptions>>().Value;

        // Nothing resolved LatticeReplicationOptions, so the manifest was never even read for it.
        Assert.That(assembly.ResourceReads, Is.Zero);
    }

    [Test]
    public void Per_tree_soft_delete_duration_reaches_the_tree_options()
    {
        const string manifest = """
            {
              "identity": { "slug": "notes", "version": "1.0.0" },
              "trees": [{ "name": "records", "softDeleteDuration": "2.00:00:00" }],
              "roles": [],
              "subscriptions": [],
              "mcpTools": []
            }
            """;
        var services = CoreServices();
        services.AddLatticeApps().AddLatticeApp("notes", new FakeAppAssembly(SourceTestManifests.ResourceName, manifest), SourceTestManifests.ResourceName);

        using var provider = services.BuildServiceProvider();
        var monitor = provider.GetRequiredService<IOptionsMonitor<LatticeOptions>>();

        Assert.That(monitor.Get("a/notes/records").SoftDeleteDuration, Is.EqualTo(TimeSpan.FromDays(2)));
        Assert.That(monitor.Get("other").SoftDeleteDuration, Is.EqualTo(LatticeOptions.DefaultSoftDeleteDuration));
    }
}
