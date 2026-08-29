using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit coverage for the per-store provider arms of
/// <see cref="DurabilitySelector.ConfigureDurability"/>.
///
/// Each store (grain storage, reminders, WAL) is independently switchable, so the
/// wiring is a matrix rather than three profiles - a PostgreSQL grain store beside
/// an Azure Table WAL is a supported combination. The risk this pins down is a
/// mis-wired arm that silently lands on the wrong provider or the wrong
/// connection string: the container would start clean and write durable state
/// somewhere the operator never configured. Each arm is asserted by resolving the
/// concrete options the Orleans provider will actually read.
/// </summary>
[TestFixture]
public sealed class DurabilitySelectorWiringTests
{
    private const string PostgresConnection = "Host=db.internal;Database=repocontext";
    private const string AzureConnection = "UseDevelopmentStorage=true";

    /// <summary>
    /// A minimal <see cref="ISiloBuilder"/> that only carries a service
    /// collection, which is all the durability wiring touches.
    /// </summary>
    private sealed class CollectingSiloBuilder(IServiceCollection services, IConfiguration configuration)
        : ISiloBuilder
    {
        public IServiceCollection Services { get; } = services;

        public IConfiguration Configuration { get; } = configuration;
    }

    private static ServiceProvider Wire(params (string Key, string Value)[] pairs)
    {
        var dict = pairs.ToDictionary(p => p.Key, p => (string?)p.Value);
        IConfiguration configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(dict)
            .Build();
        var config = RepoContextHostConfiguration.FromConfiguration(configuration);

        var services = new ServiceCollection();
        services.AddLogging();
        new CollectingSiloBuilder(services, configuration).ConfigureDurability(config);
        return services.BuildServiceProvider();
    }

    [Test]
    public void The_cluster_identity_is_applied_from_configuration()
    {
        using var provider = Wire(
            (RepoContextHostConfiguration.ClusterIdKey, "cluster-x"),
            (RepoContextHostConfiguration.ServiceIdKey, "service-y"));

        var options = provider.GetRequiredService<IOptions<ClusterOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.ClusterId, Is.EqualTo("cluster-x"));
            Assert.That(options.ServiceId, Is.EqualTo("service-y"));
        });
    }

    [Test]
    public void The_postgres_grain_storage_arm_wires_the_npgsql_invariant_and_connection()
    {
        using var provider = Wire(
            (RepoContextHostConfiguration.GrainStorageKey, "postgres"),
            (RepoContextHostConfiguration.PostgresConnectionKey, PostgresConnection));

        var options = ResolveAdoNetGrainStorage(provider);

        Assert.Multiple(() =>
        {
            Assert.That(options.Invariant, Is.EqualTo(DurabilitySelector.PostgresInvariantName));
            Assert.That(options.ConnectionString, Is.EqualTo(PostgresConnection));
        });
    }

    [Test]
    public void The_sqlite_grain_storage_arm_wires_the_sqlite_invariant_and_data_root_file()
    {
        using var provider = Wire(
            (RepoContextHostConfiguration.GrainStorageKey, "sqlite"),
            (RepoContextHostConfiguration.SqlitePathKey, "/mnt/data/repo.db"));

        var options = ResolveAdoNetGrainStorage(provider);

        Assert.Multiple(() =>
        {
            Assert.That(options.Invariant, Is.EqualTo(SqliteSchemaInitializer.InvariantName));
            Assert.That(options.ConnectionString, Does.Contain("/mnt/data/repo.db"));
        });
    }

    [Test]
    public void The_postgres_reminder_arm_wires_the_npgsql_invariant_and_connection()
    {
        using var provider = Wire(
            (RepoContextHostConfiguration.RemindersKey, "postgres"),
            (RepoContextHostConfiguration.PostgresConnectionKey, PostgresConnection));

        var options = provider.GetRequiredService<IOptions<AdoNetReminderTableOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.Invariant, Is.EqualTo(DurabilitySelector.PostgresInvariantName));
            Assert.That(options.ConnectionString, Is.EqualTo(PostgresConnection));
        });
    }

    [Test]
    public void The_sqlite_reminder_arm_wires_the_sqlite_invariant()
    {
        using var provider = Wire(
            (RepoContextHostConfiguration.RemindersKey, "sqlite"),
            (RepoContextHostConfiguration.SqlitePathKey, "/mnt/data/repo.db"));

        var options = provider.GetRequiredService<IOptions<AdoNetReminderTableOptions>>().Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.Invariant, Is.EqualTo(SqliteSchemaInitializer.InvariantName));
            Assert.That(options.ConnectionString, Does.Contain("/mnt/data/repo.db"));
        });
    }

    [Test]
    public void The_azure_wal_arm_wires_the_configured_table_name()
    {
        using var provider = Wire(
            (RepoContextHostConfiguration.WalProviderKey, "azure"),
            (RepoContextHostConfiguration.AzureConnectionKey, AzureConnection),
            (RepoContextHostConfiguration.AzureWalTableKey, "RepoWal"));

        var options = provider
            .GetRequiredService<IOptions<Orleans.Lattice.Storage.AzureTable.AzureTableWalStorageOptions>>()
            .Value;

        Assert.Multiple(() =>
        {
            Assert.That(options.TableName, Is.EqualTo("RepoWal"));
            Assert.That(options.ConnectionString, Is.EqualTo(AzureConnection));
        });
    }

    [Test]
    public void The_file_wal_arm_wires_the_wal_directory()
    {
        using var provider = Wire((RepoContextHostConfiguration.WalDirKey, "/mnt/data/wal"));

        var options = provider
            .GetRequiredService<IOptions<Orleans.Lattice.Storage.File.FileWalStorageOptions>>()
            .Value;

        Assert.That(options.RootDirectory, Is.EqualTo("/mnt/data/wal"));
    }

    /// <summary>
    /// Applies only the named <see cref="IConfigureOptions{TOptions}"/> delegates
    /// the wiring registered, which is exactly the code under test. Going through
    /// <see cref="IOptionsMonitor{TOptions}"/> would additionally run Orleans'
    /// post-configure step, which resolves silo-only services this collection
    /// deliberately does not contain.
    /// </summary>
    private static AdoNetGrainStorageOptions ResolveAdoNetGrainStorage(ServiceProvider provider)
    {
        var options = new AdoNetGrainStorageOptions();
        foreach (var configure in provider.GetServices<IConfigureOptions<AdoNetGrainStorageOptions>>())
        {
            if (configure is IConfigureNamedOptions<AdoNetGrainStorageOptions> named)
            {
                named.Configure(LatticeOptions.StorageProviderName, options);
            }
            else
            {
                configure.Configure(options);
            }
        }

        return options;
    }
}
