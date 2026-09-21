using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;
using Orleans.Storage;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Guards the coupling between <see cref="AdoNetGrainStorageOptions.DeleteStateOnClear"/>
/// and the <c>DeleteStorageKey</c> query in the embedded SQLite script. The two
/// are one change split across two files, and separating them is a startup
/// failure rather than a degraded path.
/// </summary>
/// <remarks>
/// <para>
/// Orleans resolves its query catalogue in <c>AdoNetGrainStorage.Init</c> by
/// running <see cref="AdoNetGrainStorage.DefaultInitializationQuery"/> and then
/// looking each key up by exact literal. When <c>DeleteStateOnClear</c> is set
/// and <c>DeleteStorageKey</c> is not among the returned rows it throws, which
/// takes the silo down at startup - so the failure mode of getting this wrong is
/// a container that will not boot, not a clear that quietly misbehaves.
/// </para>
/// <para>
/// This fixture reproduces that precondition rather than restating it. It
/// executes Orleans' own constant verbatim against a database initialised by the
/// real <see cref="SqliteSchemaInitializer"/>, and checks the resulting key set
/// against the option value the real wiring produces. Both halves are read from
/// the shipping artefacts, so it reddens if the query is renamed or dropped, if
/// the option is enabled on a branch whose schema lacks the key, or if Orleans
/// changes which keys it demands.
/// </para>
/// </remarks>
[TestFixture]
public sealed class SqliteDeleteOnClearContractTests
{
    private sealed class CollectingSiloBuilder(IServiceCollection services, IConfiguration configuration)
        : ISiloBuilder
    {
        public IServiceCollection Services { get; } = services;

        public IConfiguration Configuration { get; } = configuration;
    }

    private string _root = null!;
    private string _dbPath = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-contract-" + Guid.NewGuid().ToString("N"));
        _dbPath = Path.Combine(_root, "repocontext.db");
        new SqliteSchemaInitializer(_dbPath).Initialize();
    }

    [TearDown]
    public void TearDown()
    {
        SqliteConnection.ClearAllPools();
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Test]
    public void The_initialised_schema_satisfies_orleans_initialisation_query()
    {
        var keys = ResolveQueryKeysAsOrleansDoes();

        // Every key Orleans asks for must come back. ClearStorageKey is resolved
        // unconditionally even when the delete path is in use, so dropping it in
        // favour of the new query would fail startup just as surely.
        Assert.Multiple(() =>
        {
            Assert.That(keys, Does.Contain("WriteToStorageKey"));
            Assert.That(keys, Does.Contain("ReadFromStorageKey"));
            Assert.That(keys, Does.Contain("ClearStorageKey"));
            Assert.That(keys, Does.Contain("DeleteStorageKey"));
        });
    }

    [Test]
    public void The_sqlite_arm_never_enables_delete_on_clear_without_the_query_that_backs_it()
    {
        var deleteStateOnClear = ResolveSqliteGrainStorageOptions().DeleteStateOnClear;
        var keys = ResolveQueryKeysAsOrleansDoes();

        Assert.That(deleteStateOnClear, Is.True,
            "the SQLite branch is expected to delete cleared rows; see issue #3307.");
        Assert.That(keys, Does.Contain("DeleteStorageKey"),
            "DeleteStateOnClear is enabled, so AdoNetGrainStorage.Init will demand this key "
            + "and throw at silo startup if the embedded script stops defining it.");
    }

    [Test]
    public void A_schema_without_the_delete_query_would_be_caught_by_this_contract()
    {
        // Demonstrates that the assertion above is load-bearing rather than
        // vacuous: remove the key from the catalogue and the same check fails.
        using (var connection = Open())
        {
            using var command = connection.CreateCommand();
            command.CommandText = "DELETE FROM OrleansQuery WHERE QueryKey = 'DeleteStorageKey';";
            command.ExecuteNonQuery();
        }

        Assert.That(ResolveQueryKeysAsOrleansDoes(), Does.Not.Contain("DeleteStorageKey"));
    }

    private static AdoNetGrainStorageOptions ResolveSqliteGrainStorageOptions()
    {
        var dict = new Dictionary<string, string?>
        {
            [RepoContextHostConfiguration.GrainStorageKey] = "sqlite",
            [RepoContextHostConfiguration.SqlitePathKey] = "/mnt/data/repo.db",
        };
        IConfiguration configuration = new ConfigurationBuilder().AddInMemoryCollection(dict).Build();
        var config = RepoContextHostConfiguration.FromConfiguration(configuration);

        var services = new ServiceCollection();
        services.AddLogging();
        new CollectingSiloBuilder(services, configuration).ConfigureDurability(config);
        using var provider = services.BuildServiceProvider();

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

    private List<string> ResolveQueryKeysAsOrleansDoes()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = AdoNetGrainStorage.DefaultInitializationQuery;

        var keys = new List<string>(4);
        using var reader = command.ExecuteReader();
        while (reader.Read())
        {
            keys.Add(reader.GetString(0));
        }

        return keys;
    }

    private SqliteConnection Open()
    {
        var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        return connection;
    }
}
