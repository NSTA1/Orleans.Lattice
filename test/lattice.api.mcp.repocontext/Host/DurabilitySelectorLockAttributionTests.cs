using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.Serialization;
using Orleans.Serialization.Serializers;
using Orleans.Storage;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Pins where <see cref="DurabilitySelector.ConfigureDurability"/> installs the SQLite
/// lock-attribution decorator (issue #2431), and that installing it keeps the
/// provider Orleans actually uses and the lifecycle registration Orleans derives
/// from it.
/// </summary>
[TestFixture]
public sealed class DurabilitySelectorLockAttributionTests
{
    private sealed class CollectingSiloBuilder(IServiceCollection services, IConfiguration configuration)
        : ISiloBuilder
    {
        public IServiceCollection Services { get; } = services;

        public IConfiguration Configuration { get; } = configuration;
    }

    private static ServiceProvider Wire(
        Action<IServiceCollection>? before,
        params (string Key, string Value)[] pairs)
        => Wire(before, out _, pairs);

    private static ServiceProvider Wire(
        Action<IServiceCollection>? before,
        out IServiceCollection wired,
        params (string Key, string Value)[] pairs)
    {
        var dict = pairs.ToDictionary(p => p.Key, p => (string?)p.Value);
        IConfiguration configuration = new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
        var config = RepoContextHostConfiguration.FromConfiguration(configuration);

        var services = new ServiceCollection();
        services.AddLogging();

        // The silo-only services Orleans' provider factory and options post-configure
        // resolve; this collection is not a silo, so they are supplied here.
        services.AddSerializer();
        services.AddSingleton(Substitute.For<IGrainStorageSerializer>());
        services.AddSingleton(Substitute.For<IActivatorProvider>());
        before?.Invoke(services);

        new CollectingSiloBuilder(services, configuration).ConfigureDurability(config);
        wired = services;
        return services.BuildServiceProvider();
    }

    [Test]
    public void The_sqlite_arm_resolves_the_lattice_store_through_the_attributing_decorator()
    {
        using var provider = Wire(
            services => services.Configure<SiloMessagingOptions>(o => o.ResponseTimeout = TimeSpan.FromSeconds(30)),
            (RepoContextHostConfiguration.GrainStorageKey, "sqlite"),
            (RepoContextHostConfiguration.SqlitePathKey, "/mnt/data/repo.db"));

        var storage = provider.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

        Assert.That(storage, Is.InstanceOf<RepoContextLockAttributingGrainStorage>());
        var decorator = (RepoContextLockAttributingGrainStorage)storage;
        Assert.Multiple(() =>
        {
            Assert.That(decorator.Inner, Is.InstanceOf<AdoNetGrainStorage>(),
                "The decorator must wrap the provider AddAdoNetGrainStorage registered, not replace it.");
            Assert.That(decorator.BusyWindow, Is.EqualTo(TimeSpan.FromSeconds(15)),
                "The busy window is read from the provider's own resolved connection string, which the "
                + "host derives as half the 30 s request budget.");
            Assert.That(decorator.RetryPolicy, Is.SameAs(RepoContextGrainStorageLockRetryPolicy.PinStateWrites),
                "Issue #3761 item 6: the host re-issues pin-state writes that fail on a lock, so a "
                + "bulk-ingest convoy does not leave the published materialiser pin stale.");
        });
    }

    [Test]
    public void The_lifecycle_participant_orleans_registers_is_the_decorator()
    {
        using var provider = Wire(
            null,
            out var services,
            (RepoContextHostConfiguration.GrainStorageKey, "sqlite"),
            (RepoContextHostConfiguration.SqlitePathKey, "/mnt/data/repo.db"));

        // Only the factory registrations are invoked, one at a time: resolving every
        // participant would activate silo system targets this collection cannot build.
        var resolved = new List<object>();
        foreach (var descriptor in services.Where(d =>
            !d.IsKeyedService && d.ServiceType == typeof(ILifecycleParticipant<ISiloLifecycle>) && d.ImplementationFactory is not null))
        {
            try
            {
                resolved.Add(descriptor.ImplementationFactory!(provider));
            }
            catch (InvalidOperationException)
            {
            }
        }

        Assert.That(resolved.OfType<RepoContextLockAttributingGrainStorage>().Count(), Is.EqualTo(1),
            "Orleans registers the provider's lifecycle participant by casting the keyed storage. It must "
            + "still resolve - to the decorator, which forwards - or the provider never initialises.");
    }

    [Test]
    public void The_host_meter_is_used_when_the_host_registered_one()
    {
        using var hostMeter = new RepoContextGrainStorageLockMeter();
        using var provider = Wire(
            services => services.AddSingleton(hostMeter),
            (RepoContextHostConfiguration.GrainStorageKey, "sqlite"),
            (RepoContextHostConfiguration.SqlitePathKey, "/mnt/data/repo.db"));

        Assert.That(provider.GetRequiredService<RepoContextGrainStorageLockMeter>(), Is.SameAs(hostMeter),
            "The host constructs the meter eagerly so its series exist before the first scrape; the "
            + "durability wiring must record on that instance rather than a second one of its own.");
    }

    [Test]
    public void The_postgres_arm_is_not_decorated()
    {
        using var provider = Wire(
            null,
            (RepoContextHostConfiguration.GrainStorageKey, "postgres"),
            (RepoContextHostConfiguration.PostgresConnectionKey, "Host=db.internal;Database=repocontext"));

        var storage = provider.GetRequiredKeyedService<IGrainStorage>(LatticeOptions.StorageProviderName);

        Assert.That(storage, Is.InstanceOf<AdoNetGrainStorage>(),
            "The decorator classifies SQLite result codes, so it is installed on the SQLite arm only.");
    }

    [Test]
    public void Attribution_requires_a_registered_provider_to_wrap()
    {
        var services = new ServiceCollection();

        Assert.Multiple(() =>
        {
            Assert.Throws<InvalidOperationException>(() => services.AddSqliteLockAttribution("absent"));
            Assert.Throws<ArgumentNullException>(() => DurabilitySelector.AddSqliteLockAttribution(null!, "x"));
            Assert.Throws<ArgumentNullException>(() => services.AddSqliteLockAttribution(null!));
        });
    }

    [Test]
    public void An_instance_registration_is_wrapped_as_well_as_a_factory_one()
    {
        var inner = new LockAttributionTestSupport.ScriptedStorage();
        var services = new ServiceCollection();
        services.AddLogging();
        services.AddKeyedSingleton<IGrainStorage>("store", inner);
        services.Configure<AdoNetGrainStorageOptions>("store", o => o.ConnectionString = new SqliteConnectionStringBuilder
        {
            DataSource = "x.db",
            DefaultTimeout = 7,
        }.ToString());

        services.AddSqliteLockAttribution("store");
        using var provider = services.BuildServiceProvider();

        var decorator = (RepoContextLockAttributingGrainStorage)provider.GetRequiredKeyedService<IGrainStorage>("store");
        Assert.Multiple(() =>
        {
            Assert.That(decorator.Inner, Is.SameAs(inner));
            Assert.That(decorator.BusyWindow, Is.EqualTo(TimeSpan.FromSeconds(7)));
        });
    }
}
