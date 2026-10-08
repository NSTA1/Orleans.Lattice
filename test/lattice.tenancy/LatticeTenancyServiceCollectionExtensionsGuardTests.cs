using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Hosting;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Replication;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Registration-time unit tests for
/// <see cref="LatticeTenancyServiceCollectionExtensions.AddLatticeTenancy"/> that
/// do not require a live silo: the three ordering guards (core, membership, and
/// auth must each be registered first, so enabling tenancy without auth or
/// membership fails fast), the success path (all deps present wires the registry
/// once), and the idempotent repeat-call path (a second call layers a supplied
/// configure delegate but performs the structural wiring only once).
/// </summary>
[TestFixture]
public sealed class LatticeTenancyServiceCollectionExtensionsGuardTests
{
    [Test]
    public void AddLatticeTenancy_without_core_throws()
    {
        var builder = new CovSiloBuilder();

        Assert.That(
            () => builder.AddLatticeTenancy(),
            Throws.InvalidOperationException.With.Message.Contains("AddLattice()"));
    }

    [Test]
    public void AddLatticeTenancy_without_membership_throws()
    {
        var builder = new CovSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());

        Assert.That(
            () => builder.AddLatticeTenancy(),
            Throws.InvalidOperationException.With.Message.Contains("AddLatticeMembership"));
    }

    [Test]
    public void AddLatticeTenancy_without_auth_throws()
    {
        var builder = new CovSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.Services.AddSingleton(Substitute.For<ILatticeMembershipDirectory>());

        Assert.That(
            () => builder.AddLatticeTenancy(),
            Throws.InvalidOperationException.With.Message.Contains("AddLatticeAuth"));
    }

    [Test]
    public void AddLatticeTenancy_with_all_dependencies_registers_the_registry_once()
    {
        var builder = NewBuilderWithDependencies();

        var result = builder.AddLatticeTenancy();

        Assert.That(result, Is.SameAs(builder));
        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ITenantRegistry)), Is.EqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(TenantRegistryInitializer)), Is.EqualTo(1));
            Assert.That(builder.Services.Any(d => d.ServiceType == typeof(TenancyRegistrationMarker)), Is.True);
        });
    }

    [Test]
    public void AddLatticeTenancy_maps_the_registry_to_the_lattice_implementation()
    {
        var builder = NewBuilderWithDependencies();

        builder.AddLatticeTenancy();

        var descriptor = builder.Services.Single(d => d.ServiceType == typeof(ITenantRegistry));
        Assert.That(descriptor.ImplementationType, Is.EqualTo(typeof(LatticeTenantRegistry)));
    }

    [Test]
    public void AddLatticeTenancy_registers_the_tenant_access_data_purge_once()
    {
        var builder = NewBuilderWithDependencies();

        builder.AddLatticeTenancy();
        builder.AddLatticeTenancy();

        var descriptor = builder.Services.Single(d => d.ServiceType == typeof(ITenantAccessDataPurge));
        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(
                descriptor.ImplementationFactory!(Substitute.For<IServiceProvider>()),
                Is.InstanceOf<TenantAccessDataPurge>());
        });
    }

    [Test]
    public void AddLatticeTenancy_wires_the_cross_silo_tenant_policy_epoch_once()
    {
        var builder = NewBuilderWithDependencies();

        builder.AddLatticeTenancy();
        builder.AddLatticeTenancy();

        Assert.Multiple(() =>
        {
            Assert.That(
                builder.Services.Single(d => d.ServiceType == typeof(ITenantPolicyEpochPublisher)).ImplementationType,
                Is.EqualTo(typeof(GrainTenantPolicyEpochPublisher)));
            Assert.That(
                builder.Services.Count(d =>
                    d.ServiceType == typeof(Microsoft.Extensions.Hosting.IHostedService)
                    && d.ImplementationType == typeof(TenantPolicyEpochSubscription)),
                Is.EqualTo(1),
                "every silo subscribes its snapshot to the epoch exactly once");
            Assert.That(
                builder.Services.Count(d => d.ServiceType == typeof(ITenantEpochSubscriber)),
                Is.EqualTo(3),
                "the compiled policy, residency and placement snapshots are each kept current by the epoch");
        });
    }

    [Test]
    public void AddLatticeTenancy_wires_registry_alias_changes_to_the_existing_policy_snapshot_once()
    {
        var builder = NewBuilderWithDependencies();
        builder.AddLatticeTenancy();
        var observers = builder.Services.Where(d => d.ServiceType == typeof(ITreeAliasObserver)).ToArray();
        builder.AddLatticeTenancy();
        using var policy = new CompiledTenantPolicySnapshotMaintainer(
            Substitute.For<ITenantRegistry>(), Substitute.For<ITenantPolicyEpochPublisher>(),
            TimeProvider.System,
            Microsoft.Extensions.Logging.Abstractions.NullLogger<CompiledTenantPolicySnapshotMaintainer>.Instance);
        var provider = Substitute.For<IServiceProvider>();
        provider.GetService(typeof(CompiledTenantPolicySnapshotMaintainer)).Returns(policy);

        var descriptor = observers.Single(d => d.ImplementationFactory is not null);
        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Where(d => d.ServiceType == typeof(ITreeAliasObserver)),
                Is.EqualTo(observers), "repeat registration must not duplicate alias observers");
            Assert.That(descriptor.Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            Assert.That(descriptor.ImplementationFactory!(provider), Is.SameAs(policy));
        });
    }

    [Test]
    public void AddLatticeTenancy_repeat_call_wires_structure_only_once()
    {
        var builder = NewBuilderWithDependencies();

        builder.AddLatticeTenancy();
        builder.AddLatticeTenancy();

        Assert.Multiple(() =>
        {
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ITenantRegistry)), Is.EqualTo(1));
            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(TenancyRegistrationMarker)), Is.EqualTo(1));
        });
    }

    [Test]
    public void AddLatticeTenancy_repeat_call_still_layers_configuration()
    {
        var builder = NewBuilderWithDependencies();

        builder.AddLatticeTenancy();
        builder.AddLatticeTenancy(o => o.SeedDefaultTenant = false);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeTenancyOptions>>()
            .Value;
        Assert.That(options.SeedDefaultTenant, Is.False);
    }

    [Test]
    public void ConfigureLatticeTenancy_layers_a_configuration_delegate()
    {
        var builder = NewBuilderWithDependencies();
        builder.AddLatticeTenancy();

        builder.ConfigureLatticeTenancy(o => o.EnableDurableHistoryView = false);

        var options = builder.Services
            .BuildServiceProvider()
            .GetRequiredService<IOptions<LatticeTenancyOptions>>()
            .Value;
        Assert.That(options.EnableDurableHistoryView, Is.False);
    }

    [Test]
    public void AddLatticeTenancy_replaces_every_core_tenancy_seam_it_claims_to()
    {
        var builder = NewBuilderWithDependencies();

        builder.AddLatticeTenancy();

        // Each of these seams is declared in core with a no-op default whose own
        // doc comment says "the tenancy package replaces it". Two of them were
        // never actually replaced, and nothing caught it: the context resolver
        // (so ComposeEffectiveTreeId always returned the caller's bare tree name
        // and every tenant shared one physical tree) and the enumeration filter
        // (so a tree-id enumeration was never pruned to the active tenant). This
        // test is the missing guard - it fails if either regresses to its core
        // no-op, whatever the registration order.
        //
        // Asserted on the registered descriptor rather than a resolved instance so
        // the check needs only the registration, not a fully-activatable cluster.
        var seams = new (Type Service, Type Expected)[]
        {
            (typeof(ITenantContextResolver), typeof(TenantContextResolver)),
            (typeof(ITenantEnumerationFilter), typeof(TenantEnumerationFilter)),
            (typeof(ITenantRegionVisibilityResolver), typeof(TenantRegionVisibilityResolver)),
        };

        Assert.Multiple(() =>
        {
            foreach (var (service, expected) in seams)
            {
                var descriptors = builder.Services.Where(d => d.ServiceType == service).ToList();

                Assert.That(descriptors, Has.Count.EqualTo(1),
                    $"{service.Name} must resolve exactly one implementation.");
                Assert.That(descriptors[0].ImplementationType, Is.EqualTo(expected),
                    $"{service.Name} must be replaced by the tenancy implementation, not left as the core no-op.");
                Assert.That(descriptors[0].Lifetime, Is.EqualTo(ServiceLifetime.Singleton));
            }

            Assert.That(builder.Services.Count(d => d.ServiceType == typeof(ITenantGateEnforcer)),
                Is.EqualTo(1), "the gate enforcer stays singly registered too");
        });
    }

    [Test]
    public void AddLatticeTenancy_registered_enumeration_filter_reports_itself_active()
    {
        var builder = NewBuilderWithDependencies();
        builder.AddLatticeTenancy();

        // A seam that resolves but reports itself inactive is as good as absent:
        // every choke point short-circuits on IsActive before calling it.
        var descriptor = builder.Services.Single(d => d.ServiceType == typeof(ITenantEnumerationFilter));
        var filter = (ITenantEnumerationFilter)Activator.CreateInstance(descriptor.ImplementationType!)!;

        Assert.That(filter.IsActive, Is.True);
    }

    [Test]
    public void AddLatticeTenancy_registered_region_visibility_resolver_reports_itself_active()
    {
        var builder = NewBuilderWithDependencies();
        builder.AddLatticeTenancy();

        // Same reasoning as the enumeration filter: the region-discovery surface
        // short-circuits on IsActive, so a resolver that resolves but reports
        // itself inactive would silently fail every tenant-scoped call closed.
        var resolver = (ITenantRegionVisibilityResolver)ActivatorUtilities.CreateInstance(
            new ServiceCollection()
                .AddSingleton(Substitute.For<ITenantRegistry>())
                .BuildServiceProvider(),
            builder.Services.Single(d => d.ServiceType == typeof(ITenantRegionVisibilityResolver))
                .ImplementationType!);

        Assert.That(resolver.IsActive, Is.True);
    }

    [Test]
    public void ConfigureLatticeTenancy_null_configure_throws()
    {
        var builder = new CovSiloBuilder();

        Assert.That(() => builder.ConfigureLatticeTenancy(null!), Throws.ArgumentNullException);
    }

    private static CovSiloBuilder NewBuilderWithDependencies()
    {
        var builder = new CovSiloBuilder();
        builder.Services.AddSingleton(Substitute.For<IValidateOptions<LatticeOptions>>());
        builder.Services.AddSingleton(Substitute.For<ILatticeMembershipDirectory>());
        builder.Services.AddSingleton(Substitute.For<ILatticeDecisionEngine>());
        return builder;
    }

    [TestCase(true, true)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(false, false)]
    public void AddLatticeTenancy_with_replication_enrols_registry_in_every_options_instance(bool replicationFirst, bool conflictingMap)
    {
        var builder = NewBuilderWithDependencies();
        if (replicationFirst)
        {
            builder.AddLatticeReplication(options => options.ClusterId = "registration-test");
        }

        builder.AddLatticeTenancy();
        builder.AddLatticeTenancy();
        if (!replicationFirst)
        {
            builder.AddLatticeReplication(options => options.ClusterId = "registration-test");
        }

        if (conflictingMap)
        {
            builder.Services.ConfigureAll<LatticeReplicationOptions>(options =>
                options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
                {
                    ["application"] = LatticeMergeMode.OrSet,
                    [TenantTreeNames.RegistryTree] = LatticeMergeMode.OrSet,
                });
        }

        using var services = builder.Services.BuildServiceProvider();
        var options = services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();
        foreach (var name in new[] { Options.DefaultName, TenantTreeNames.RegistryTree, "application" })
        {
            var trees = options.Get(name).ReplicatedTrees
                ?? throw new AssertionException("Tenancy must declare its definition registry for replication.");
            Assert.Multiple(() =>
            {
                Assert.That(trees[TenantTreeNames.RegistryTree], Is.EqualTo(LatticeMergeMode.LwwRegister));
                if (conflictingMap)
                {
                    Assert.That(trees["application"], Is.EqualTo(LatticeMergeMode.OrSet));
                }

                Assert.That(trees.ContainsKey(TenantTreeNames.UsageTree), Is.False);
                Assert.That(trees.ContainsKey(TenantTreeNames.OverageTree), Is.False);
            });
        }
    }

    [Test]
    public void AddLatticeTenancy_without_replication_does_not_enrol_registry()
    {
        var builder = NewBuilderWithDependencies();
        builder.AddLatticeTenancy();
        builder.Services.ConfigureAll<LatticeReplicationOptions>(options =>
            options.ReplicatedTrees = new Dictionary<string, LatticeMergeMode>
            {
                ["application"] = LatticeMergeMode.OrSet,
            });
        using var services = builder.Services.BuildServiceProvider();
        var options = services.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>();
        foreach (var name in new[] { Options.DefaultName, TenantTreeNames.RegistryTree, "application" })
        {
            Assert.That(options.Get(name).ReplicatedTrees!.Keys, Is.EquivalentTo(new[] { "application" }));
        }
    }

    /// <summary>A minimal <see cref="ISiloBuilder"/> backed by a plain service collection.</summary>
    private sealed class CovSiloBuilder : ISiloBuilder
    {
        public IServiceCollection Services { get; } = new ServiceCollection();

        public IConfiguration Configuration { get; } = new ConfigurationBuilder().Build();
    }
}
