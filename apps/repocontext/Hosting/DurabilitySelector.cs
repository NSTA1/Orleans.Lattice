using System.Data.Common;
using Azure.Data.Tables;
using Microsoft.Data.Sqlite;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Npgsql;
using Orleans.Configuration;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Storage.AzureTable;
using Orleans.Lattice.Storage.File;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Translates a resolved <see cref="RepoContextHostConfiguration"/> into the
/// concrete Orleans provider wiring on an <see cref="ISiloBuilder"/>: clustering,
/// grain storage, the reminder service, and the WAL - each independently
/// switchable so a profile can be mixed by per-store environment overrides. Every
/// relational store shares the one SQLite database file (or the one PostgreSQL
/// connection) on the mounted data root, and the ADO.NET invariant factories are
/// registered once at wiring time. The selector never silently degrades: the
/// configuration has already failed fast on a missing credential before this runs.
/// </summary>
public static class DurabilitySelector
{
    /// <summary>The Orleans ADO.NET invariant name for PostgreSQL (the Npgsql factory).</summary>
    public const string PostgresInvariantName = "Npgsql";

    private static int _factoriesRegistered;

    /// <summary>
    /// Applies the full durability wiring for <paramref name="config"/> to
    /// <paramref name="silo"/>: registers the ADO.NET provider factories, sets the
    /// cluster identity, and wires clustering, grain storage, reminders, and the
    /// WAL for the selected (or per-store overridden) providers.
    /// </summary>
    /// <param name="silo">The Orleans silo builder.</param>
    /// <param name="config">The resolved, validated host configuration.</param>
    /// <returns>The same <paramref name="silo"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="silo"/> or <paramref name="config"/> is null.</exception>
    public static ISiloBuilder ConfigureDurability(
        this ISiloBuilder silo,
        RepoContextHostConfiguration config)
    {
        ArgumentNullException.ThrowIfNull(silo);
        ArgumentNullException.ThrowIfNull(config);

        RegisterAdoNetFactories();

        silo.Configure<ClusterOptions>(options =>
        {
            options.ClusterId = config.ClusterId;
            options.ServiceId = config.ServiceId;
        });

        ConfigureClustering(silo, config);
        ConfigureGrainStorage(silo, config);
        ConfigureReminders(silo, config);
        ConfigureWal(silo, config);

        return silo;
    }

    /// <summary>
    /// Registers the SQLite and PostgreSQL ADO.NET provider factories with
    /// <see cref="DbProviderFactories"/> so Orleans can resolve them by invariant
    /// name. Idempotent: registration runs at most once per process.
    /// </summary>
    public static void RegisterAdoNetFactories()
    {
        if (Interlocked.Exchange(ref _factoriesRegistered, 1) == 1)
        {
            return;
        }

        DbProviderFactories.RegisterFactory(SqliteSchemaInitializer.InvariantName, SqliteFactory.Instance);
        DbProviderFactories.RegisterFactory(PostgresInvariantName, NpgsqlFactory.Instance);
    }

    private static void ConfigureClustering(ISiloBuilder silo, RepoContextHostConfiguration config)
    {
        switch (config.Clustering)
        {
            case ClusteringProvider.Azure:
                silo.UseAzureStorageClustering(options =>
                    options.TableServiceClient = new TableServiceClient(config.AzureConnectionString!));
                break;
            case ClusteringProvider.Localhost:
            default:
                silo.UseLocalhostClustering();
                break;
        }
    }

    private static void ConfigureGrainStorage(ISiloBuilder silo, RepoContextHostConfiguration config)
    {
        // AddLattice invokes the callback once with the single Lattice grain-
        // storage provider name, so every Lattice tree shares one durable backing
        // store on the mounted data root.
        silo.AddLattice((services, name) =>
        {
            switch (config.GrainStorage)
            {
                case RelationalStore.Postgres:
                    services.AddAdoNetGrainStorage(name, options =>
                    {
                        options.Invariant = PostgresInvariantName;
                        options.ConnectionString = config.PostgresConnectionString!;

                        // DeleteStateOnClear is deliberately left at Orleans'
                        // default of false here, unlike the Sqlite and Azure
                        // branches below. It is not an oversight and it is not a
                        // judgement that tombstoned rows are acceptable on
                        // Postgres - they are not - it is that this host cannot
                        // guarantee the query the option requires exists.
                        //
                        // Enabling it makes Orleans resolve the query key
                        // 'DeleteStorageKey' during AdoNetGrainStorage.Init and
                        // throw when it is absent, which fails SILO STARTUP
                        // rather than degrading at the first clear. The Sqlite
                        // branch can carry that coupling safely because this
                        // repository owns the schema and SqliteSchemaInitializer
                        // reapplies it INSERT OR REPLACE on every start, so the
                        // key cannot be missing. No Postgres schema ships here
                        // at all: the catalogue is provisioned by the operator
                        // from whichever Orleans script version they used, and
                        // this host never reads or repairs it. Turning the
                        // option on would therefore convert an absent query in a
                        // store outside this repository's control into a
                        // container that will not boot.
                        //
                        // That risk is live rather than theoretical. Orleans
                        // only aligned its PostgreSQL-Persistence.sql with its
                        // own PostgreSQL-Persistence-3.6.0.sql migration in
                        // dotnet/orleans#11247, so whether DeleteStorageKey is
                        // present depends on both the script version and the
                        // provisioning path taken.
                        //
                        // To opt in, apply a DeleteStorageKey definition to the
                        // OrleansQuery catalogue of the target database and set
                        // this to true. Prefer the scalar version-report shape
                        // used by this repository's SQLite script over upstream's
                        // DELETE ... RETURNING, which emits one row per deleted
                        // row and throws out of Orleans' SingleOrDefault() when a
                        // grain identity has acquired a duplicate row.
                    });
                    break;
                case RelationalStore.Azure:
                    // Azure Table grain storage has no IServiceCollection overload,
                    // so it is wired through the captured silo builder (same DI
                    // container) under the same provider name.
                    silo.AddAzureTableGrainStorage(name, options =>
                    {
                        options.TableServiceClient = new TableServiceClient(config.AzureConnectionString!);

                        // Delete the entity on clear rather than retaining it
                        // with a null payload. The Azure Table provider honours
                        // this directly against the table API, so unlike the
                        // relational providers there is no query catalogue and
                        // therefore no schema coupling to satisfy first. The
                        // growth this prevents is a property of the grain keys,
                        // not of the backing store - several grain types here are
                        // keyed generationally, so a retained row is never
                        // revisited and the population only grows - so the same
                        // defect would apply to an Azure deployment.
                        options.DeleteStateOnClear = true;
                    });
                    break;
                case RelationalStore.Sqlite:
                default:
                    services.AddAdoNetGrainStorage(name, options =>
                    {
                        options.Invariant = SqliteSchemaInitializer.InvariantName;

                        // Delete the grain row on clear rather than nulling its
                        // payload and keeping it. Without this, nothing in this
                        // deployment ever removes a grain-state row: measured on
                        // the deployed container, 29.8% of OrleansStorage rows
                        // were dead tombstones accruing at roughly 3,000/day.
                        //
                        // This is coupled to the DeleteStorageKey query in
                        // Persistence/Sqlite/SQLite-Persistence.sql and the two
                        // must never be separated - Orleans throws at silo
                        // startup when this is true and that key is absent. The
                        // coupling is safe on this branch specifically because
                        // SqliteSchemaInitializer reapplies the embedded script
                        // INSERT OR REPLACE on every start, so an existing
                        // database picks the query up on the next deploy with no
                        // migration step.
                        options.DeleteStateOnClear = true;
                    });
                    services.Services.AddOptions<AdoNetGrainStorageOptions>(name)
                        .Configure<IOptions<SiloMessagingOptions>>((options, messaging) =>
                            options.ConnectionString = SqliteSchemaInitializer.BuildConnectionString(
                                config.SqlitePath, messaging.Value.ResponseTimeout));
                    break;
            }
        });

        // Per-grain-type non-reentrancy queue depth, recorded at dispatch on
        // every outgoing grain call rather than only when something times out.
        //
        // This container's saturation investigation ran for two gate rounds
        // reading the only queue signal Orleans ships - the
        // "NonReentrancyQueueSize=" clause of the near-timeout diagnostic - and
        // that signal is censored twice over: it fires only for a request
        // already approaching the 30s deadline, and it describes that request's
        // own wait, so a grain type whose calls queue deeply but which does not
        // itself trip the timeout contributes no rows at all. On gate run 2 the
        // grain type carrying the deepest queues in the system contributed 0 of
        // 154 samples, and two independent extractions from those samples
        // agreed exactly that nothing was queueing. Enabling the observation
        // filter is what makes that population visible during a healthy run,
        // which is the run in which the question has to be answerable.
        silo.AddLatticeGrainCallObservation();
    }

    private static void ConfigureReminders(ISiloBuilder silo, RepoContextHostConfiguration config)
    {
        switch (config.Reminders)
        {
            case RelationalStore.Postgres:
                silo.UseAdoNetReminderService(options =>
                {
                    options.Invariant = PostgresInvariantName;
                    options.ConnectionString = config.PostgresConnectionString!;
                });
                break;
            case RelationalStore.Azure:
                silo.UseAzureTableReminderService(options =>
                    options.TableServiceClient = new TableServiceClient(config.AzureConnectionString!));
                break;
            case RelationalStore.Sqlite:
            default:
                silo.UseAdoNetReminderService(options =>
                {
                    options.Invariant = SqliteSchemaInitializer.InvariantName;
                });
                silo.Services.AddOptions<AdoNetReminderTableOptions>()
                    .Configure<IOptions<SiloMessagingOptions>>((options, messaging) =>
                        options.ConnectionString = SqliteSchemaInitializer.BuildConnectionString(
                            config.SqlitePath, messaging.Value.ResponseTimeout));
                break;
        }
    }

    private static void ConfigureWal(ISiloBuilder silo, RepoContextHostConfiguration config)
    {
        switch (config.Wal)
        {
            case WalProvider.Azure:
                silo.AddAzureTableWalStorage(options =>
                {
                    options.ConnectionString = config.AzureConnectionString!;
                    options.TableName = config.AzureWalTableName;
                });
                break;
            case WalProvider.File:
            default:
                silo.AddFileWalStorage(options =>
                {
                    options.RootDirectory = config.WalDirectory;
                    if (config.WalCompactionMaximumDeadBytes > 0)
                    {
                        options.CompactionMaximumDeadBytes = config.WalCompactionMaximumDeadBytes;
                    }
                });
                break;
        }
    }
}
