using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Tests;

public class LatticeServiceCollectionExtensionsTests
{
    [Test]
    public void AddLattice_invokes_delegate_with_builder_and_storage_provider_name()
    {
        var builder = Substitute.For<ISiloBuilder>();
        string? capturedName = null;
        ISiloBuilder? capturedBuilder = null;

        builder.AddLattice((b, name) =>
        {
            capturedBuilder = b;
            capturedName = name;
        });

        Assert.That(capturedBuilder, Is.SameAs(builder));
        Assert.That(capturedName, Is.EqualTo(LatticeOptions.StorageProviderName));
    }

    [Test]
    public void AddLattice_returns_builder_for_fluent_chaining()
    {
        var builder = Substitute.For<ISiloBuilder>();

        var result = builder.AddLattice((_, _) => { });

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void AddWalStorage_throws_on_null_builder()
    {
        Assert.That(
            () => LatticeServiceCollectionExtensions.AddWalStorage(null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void AddWalStorage_returns_builder_for_fluent_chaining()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var result = builder.AddWalStorage();

        Assert.That(result, Is.SameAs(builder));
    }

    [Test]
    public void AddWalStorage_without_factory_registers_in_memory_provider()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddWalStorage();

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IWalStorageProvider>();
        Assert.That(resolved, Is.InstanceOf<InMemoryWalStorageProvider>());
    }

    [Test]
    public void AddWalStorage_with_factory_registers_supplied_factory_result()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var sentinel = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => sentinel);

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IWalStorageProvider>();
        Assert.That(resolved, Is.SameAs(sentinel));
    }

    [Test]
    public void AddWalStorage_no_factory_is_idempotent_first_baseline_wins()
    {
        // The no-factory overload installs the in-memory baseline via
        // TryAddSingleton. First call wins; a second baseline call is
        // a no-op. This is the contract AddLattice relies on when it
        // self-installs the baseline at the top of its own setup.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddWalStorage();
        builder.AddWalStorage();

        Assert.That(
            services.Count(d => d.ServiceType == typeof(IWalStorageProvider)),
            Is.EqualTo(1));
    }

    [Test]
    public void AddWalStorage_factory_after_baseline_replaces_in_memory_default()
    {
        // Regression: under the old TryAddSingleton path the factory was
        // silently dropped because AddLattice (or any earlier
        // AddWalStorage() call) had already installed the baseline.
        // Under the Replace contract the factory wins regardless of
        // order; this is the contract that makes
        // `siloBuilder.AddLattice(...).AddAzureTableWalStorage(...)`
        // work as a host expects.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddWalStorage(); // baseline (TryAdd)
        var sentinel = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => sentinel); // factory must replace baseline

        var provider = services.BuildServiceProvider();
        var resolved = provider.GetRequiredService<IWalStorageProvider>();
        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.SameAs(sentinel));
            Assert.That(
                services.Count(d => d.ServiceType == typeof(IWalStorageProvider)),
                Is.EqualTo(1),
                "Replace must produce a single descriptor, not stack a second one.");
        });
    }

    [Test]
    public void AddWalStorage_baseline_after_factory_does_not_displace_factory()
    {
        // TryAddSingleton on the baseline must not displace a previously
        // registered factory. This is the symmetry case: a host that
        // calls AddWalStorage(factory) before AddLattice still keeps
        // the factory because AddLattice's internal AddWalStorage() is
        // a TryAdd no-op.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var sentinel = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => sentinel);
        builder.AddWalStorage(); // baseline must no-op

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IWalStorageProvider>(), Is.SameAs(sentinel));
    }

    [Test]
    public void AddWalStorage_factory_is_last_call_wins()
    {
        // Two host-supplied factories: the second replaces the first.
        // This is the contract package-level overloads like
        // AddAzureTableWalStorage rely on when a host accidentally
        // configures the package twice.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var first = new InMemoryWalStorageProvider();
        var second = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => first);
        builder.AddWalStorage(_ => second);

        var provider = services.BuildServiceProvider();
        Assert.Multiple(() =>
        {
            Assert.That(provider.GetRequiredService<IWalStorageProvider>(), Is.SameAs(second));
            Assert.That(
                services.Count(d => d.ServiceType == typeof(IWalStorageProvider)),
                Is.EqualTo(1),
                "Replace must not stack descriptors.");
        });
    }

    [Test]
    public void AddLattice_registers_in_memory_cursor_registry_as_fallback()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLattice((_, _) => { });

        var provider = services.BuildServiceProvider();
        var registry = provider.GetService<IWalCursorRegistry>();
        Assert.That(
            registry,
            Is.InstanceOf<InMemoryWalCursorRegistry>(),
            "AddLattice must register the in-memory cursor registry as an always-on fallback so the saturation sampler's drain-lag input is never silently absent");

        // Singleton lifetime: same instance resolved twice.
        Assert.That(provider.GetService<IWalCursorRegistry>(), Is.SameAs(registry));
    }

    [Test]
    public void AddWalCursorRegistry_factory_wins_over_core_default()
    {
        var custom = Substitute.For<IWalCursorRegistry>();
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        // AddLattice registers the in-memory default first; a host that then
        // opts into a materialiser stack via AddWalCursorRegistry(factory) must
        // still win - the factory overload uses Replace, not TryAdd.
        builder.AddLattice((_, _) => { });
        builder.AddWalCursorRegistry(_ => custom);

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetRequiredService<IWalCursorRegistry>(), Is.SameAs(custom));
    }

    [Test]
    public void AddLattice_registers_IWalSaturationSignal_singleton()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLattice((_, _) => { });

        var provider = services.BuildServiceProvider();
        var signal = provider.GetService<IWalSaturationSignal>();
        Assert.That(signal, Is.Not.Null, "AddLattice must register the public saturation signal singleton");

        // Same instance resolved twice (singleton lifetime).
        Assert.That(provider.GetService<IWalSaturationSignal>(), Is.SameAs(signal));
    }

    [Test]
    public void AddLattice_registers_saturation_sampler_as_hosted_service()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLattice((_, _) => { });

        // Inspect the descriptors directly rather than materialising every
        // IHostedService: other hosted services (LatticeStorageUsagePoller,
        // CrdtShapeStartup, WalCommitLogWriterDrainer) require an
        // IGrainFactory that this DI container does not provide, so calling
        // GetServices<IHostedService>() would throw on them regardless of
        // whether the saturation sampler is registered.
        var samplerDescriptor = services.SingleOrDefault(d =>
            d.ServiceType == typeof(IHostedService)
            && d.ImplementationType == typeof(WalSaturationSampler));
        Assert.That(
            samplerDescriptor,
            Is.Not.Null,
            "AddLattice must register the WalSaturationSampler as an IHostedService so the per-silo cadence ticks under host lifecycle");
        Assert.That(
            samplerDescriptor!.Lifetime,
            Is.EqualTo(ServiceLifetime.Singleton),
            "the sampler must be a singleton so it owns the per-silo state");
    }

    [Test]
    public void AddLattice_registers_signal_and_concrete_type_as_same_instance()
    {
        // The public interface and the internal concrete type resolve
        // to the same singleton so callers reaching for either side
        // (public IWalSaturationSignal in production code, internal
        // WalSaturationSignal in tests + the sampler) see consistent
        // state.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLattice((_, _) => { });

        var provider = services.BuildServiceProvider();
        var iface = provider.GetRequiredService<IWalSaturationSignal>();
        var concrete = provider.GetRequiredService<WalSaturationSignal>();
        Assert.That(iface, Is.SameAs(concrete));
    }

    [Test]
    public void AddLattice_registers_WalSaturationObserverDispatcher_singleton()
    {
        // The dispatcher is the second-of-three saturation-surface
        // singletons (signal + dispatcher + sampler hosted service).
        // The sampler resolves it as a constructor dependency, so its
        // absence would fail container validation - but inspect the
        // descriptor directly to keep this test independent of the
        // sampler test above.
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLattice((_, _) => { });

        var dispatcherDescriptor = services.SingleOrDefault(d =>
            d.ServiceType == typeof(WalSaturationObserverDispatcher));
        Assert.That(
            dispatcherDescriptor,
            Is.Not.Null,
            "AddLattice must register the WalSaturationObserverDispatcher so DI-registered observers fan out on transitions");
        Assert.That(
            dispatcherDescriptor!.Lifetime,
            Is.EqualTo(ServiceLifetime.Singleton),
            "the dispatcher must be a singleton so the per-silo observer collection is stable");
    }

    [Test]
    public void AddLatticeWalStorageProvider_registers_keyed_provider_and_catalog_marker()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var secondary = new InMemoryWalStorageProvider();
        builder.AddWalStorage();
        builder.AddLatticeWalStorageProvider("secondary", _ => secondary);

        var provider = services.BuildServiceProvider();
        Assert.Multiple(() =>
        {
            Assert.That(provider.GetKeyedService<IWalStorageProvider>("secondary"), Is.SameAs(secondary));
            Assert.That(
                provider.GetServices<WalStorageProviderRegistration>().Select(r => r.Key),
                Does.Contain("secondary"));
        });
    }

    [Test]
    public void AddLatticeWalStorageProvider_keeps_baseline_default_provider_intact()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var baseline = new InMemoryWalStorageProvider();
        builder.AddWalStorage(_ => baseline);
        builder.AddLatticeWalStorageProvider("secondary", _ => new InMemoryWalStorageProvider());

        var provider = services.BuildServiceProvider();
        // Registering a named provider must not displace the default provider.
        Assert.That(provider.GetRequiredService<IWalStorageProvider>(), Is.SameAs(baseline));
    }

    [Test]
    public void AddLatticeWalStorageProvider_rejects_reserved_default_key()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        Assert.That(
            () => builder.AddLatticeWalStorageProvider(
                IWalStorageProviderCatalog.DefaultProviderKey, _ => new InMemoryWalStorageProvider()),
            Throws.ArgumentException);
    }

    [Test]
    public void AddLatticeWalStorageProvider_is_last_call_wins_for_same_key()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        var first = new InMemoryWalStorageProvider();
        var second = new InMemoryWalStorageProvider();
        builder.AddWalStorage();
        builder.AddLatticeWalStorageProvider("acct", _ => first);
        builder.AddLatticeWalStorageProvider("acct", _ => second);

        var provider = services.BuildServiceProvider();
        Assert.That(provider.GetKeyedService<IWalStorageProvider>("acct"), Is.SameAs(second));
    }
}

/// <summary>
/// Tests for the in-library shutdown log-demotion seam
/// (<see cref="LatticeServiceCollectionExtensions.LatticeShutdownLogFilter"/>):
/// the targeted Orleans transport tear-down warnings are demoted only while
/// the host is stopping, and left at Warning on a healthy host.
/// </summary>
public class LatticeShutdownLogFilterTests
{
    private static IHostApplicationLifetime Lifetime(bool stopping)
    {
        var lifetime = Substitute.For<IHostApplicationLifetime>();
        var cts = new CancellationTokenSource();
        if (stopping) cts.Cancel();
        lifetime.ApplicationStopping.Returns(cts.Token);
        return lifetime;
    }

    [Test]
    public void ShouldEmit_keeps_targeted_warning_when_host_is_healthy()
    {
        Assert.That(
            LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.ShouldEmit(
                "Orleans.Messaging", LogLevel.Warning, applicationStopping: false),
            Is.True);
    }

    [Test]
    public void ShouldEmit_suppresses_targeted_warning_when_application_stopping()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.ShouldEmit(
                    "Orleans.Messaging", LogLevel.Warning, applicationStopping: true),
                Is.False);
            Assert.That(
                LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.ShouldEmit(
                    "Orleans.Runtime.Placement.PlacementService", LogLevel.Warning, applicationStopping: true),
                Is.False);
        });
    }

    [Test]
    public void ShouldEmit_keeps_targeted_error_even_when_application_stopping()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.ShouldEmit(
                    "Orleans.Messaging", LogLevel.Error, applicationStopping: true),
                Is.True);
            Assert.That(
                LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.ShouldEmit(
                    "Orleans.Messaging", LogLevel.Critical, applicationStopping: true),
                Is.True);
        });
    }

    [Test]
    public void ShouldEmit_leaves_non_targeted_categories_untouched()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.ShouldEmit(
                    "Orleans.Lattice.BPlusTree.Grains.ShardRootGrain", LogLevel.Warning, applicationStopping: true),
                Is.True);
            Assert.That(
                LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.ShouldEmit(
                    null, LogLevel.Warning, applicationStopping: true),
                Is.True);
        });
    }

    [Test]
    public void IsDemotedCategory_matches_targeted_prefixes_only()
    {
        Assert.Multiple(() =>
        {
            Assert.That(LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.IsDemotedCategory("Orleans.Messaging"), Is.True);
            Assert.That(LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.IsDemotedCategory("Orleans.Messaging.GatewaySender"), Is.True);
            Assert.That(LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.IsDemotedCategory("Orleans.Runtime.Placement.PlacementService"), Is.True);
            Assert.That(LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.IsDemotedCategory("Orleans.Runtime.Catalog"), Is.False);
            Assert.That(LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.IsDemotedCategory(null), Is.False);
            Assert.That(LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.IsDemotedCategory(""), Is.False);
        });
    }

    [Test]
    public void Instance_ShouldEmit_resolves_lifetime_lazily_and_demotes_only_when_stopping()
    {
        var stoppingProvider = new ServiceCollection().AddSingleton(Lifetime(stopping: true)).BuildServiceProvider();
        var healthyProvider = new ServiceCollection().AddSingleton(Lifetime(stopping: false)).BuildServiceProvider();

        var stoppingFilter = new LatticeServiceCollectionExtensions.LatticeShutdownLogFilter(stoppingProvider);
        var healthyFilter = new LatticeServiceCollectionExtensions.LatticeShutdownLogFilter(healthyProvider);

        Assert.Multiple(() =>
        {
            Assert.That(stoppingFilter.ShouldEmit("Orleans.Messaging", LogLevel.Warning), Is.False);
            Assert.That(healthyFilter.ShouldEmit("Orleans.Messaging", LogLevel.Warning), Is.True);
            // Error survives in both states.
            Assert.That(stoppingFilter.ShouldEmit("Orleans.Messaging", LogLevel.Error), Is.True);
        });
    }

    [Test]
    public void Instance_ShouldEmit_keeps_warning_when_no_lifetime_registered()
    {
        // Non-hosted activation: no IHostApplicationLifetime in DI means the
        // host is never observed stopping, so warnings are kept.
        var provider = new ServiceCollection().BuildServiceProvider();
        var filter = new LatticeServiceCollectionExtensions.LatticeShutdownLogFilter(provider);
        Assert.That(filter.ShouldEmit("Orleans.Messaging", LogLevel.Warning), Is.True);
    }

    [Test]
    public void AddLattice_registers_a_shutdown_log_filter_rule_per_demoted_category()
    {
        var services = new ServiceCollection();
        var builder = Substitute.For<ISiloBuilder>();
        builder.Services.Returns(services);

        builder.AddLattice((_, _) => { });

        var provider = services.BuildServiceProvider();
        var filterOptions = provider.GetRequiredService<IOptions<LoggerFilterOptions>>().Value;
        foreach (var category in LatticeServiceCollectionExtensions.LatticeShutdownLogFilter.DemotedCategories)
        {
            Assert.That(
                filterOptions.Rules.Any(r => r.CategoryName == category && r.Filter is not null),
                Is.True,
                $"AddLattice must install a dynamic shutdown log filter rule for '{category}'");
        }
    }
}