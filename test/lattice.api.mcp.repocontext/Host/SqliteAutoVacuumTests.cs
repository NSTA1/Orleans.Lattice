using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Tests for the SQLite <c>auto_vacuum</c> handling: <see cref="SqliteSchemaInitializer"/>
/// applies the configured mode to a new file for free and converts an existing file once,
/// and <see cref="SqliteAutoVacuumService.ReclaimStep"/> returns freed pages in bounded
/// batches so the file actually shrinks.
/// </summary>
[TestFixture]
public sealed class SqliteAutoVacuumTests
{
    private string _root = null!;
    private string _dbPath = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-sqlite-av-" + Guid.NewGuid().ToString("N"));
        _dbPath = Path.Combine(_root, "repocontext.db");
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
    public void A_new_database_adopts_incremental_mode_without_a_conversion()
    {
        var outcome = new SqliteSchemaInitializer(_dbPath).Initialize();

        Assert.Multiple(() =>
        {
            Assert.That(ReadAutoVacuum(), Is.EqualTo(2));
            Assert.That(outcome.Requested, Is.EqualTo(SqliteAutoVacuumMode.Incremental));
            Assert.That(outcome.Converted, Is.False, "a new file takes the mode before its first table");
        });
    }

    [Test]
    public void An_existing_database_is_converted_once_and_shrinks()
    {
        new SqliteSchemaInitializer(_dbPath, autoVacuum: SqliteAutoVacuumMode.None).Initialize();
        FillThenDelete();
        SqliteConnection.ClearAllPools();
        var bloated = new FileInfo(_dbPath).Length;

        var first = new SqliteSchemaInitializer(_dbPath).Initialize();
        SqliteConnection.ClearAllPools();
        var second = new SqliteSchemaInitializer(_dbPath).Initialize();

        Assert.Multiple(() =>
        {
            Assert.That(ReadAutoVacuum(), Is.EqualTo(2));
            Assert.That(first.Previous, Is.EqualTo(SqliteAutoVacuumMode.None));
            Assert.That(first.Converted, Is.True);
            Assert.That(first.BytesAfter, Is.LessThan(bloated / 2), "the conversion VACUUM must drop the freelist");
            Assert.That(second.Converted, Is.False, "a recorded mode must not be converted again");
        });
    }

    [Test]
    public void The_none_mode_leaves_an_existing_database_untouched()
    {
        new SqliteSchemaInitializer(_dbPath, autoVacuum: SqliteAutoVacuumMode.None).Initialize();

        var outcome = new SqliteSchemaInitializer(_dbPath, autoVacuum: SqliteAutoVacuumMode.None).Initialize();

        Assert.Multiple(() =>
        {
            Assert.That(ReadAutoVacuum(), Is.Zero);
            Assert.That(outcome.Converted, Is.False);
        });
    }

    [Test]
    public void A_reclaim_step_returns_at_most_its_bound_and_shrinks_the_freelist()
    {
        new SqliteSchemaInitializer(_dbPath).Initialize();
        FillThenDelete();
        var connectionString = SqliteSchemaInitializer.BuildConnectionString(_dbPath);

        var (before, after) = SqliteAutoVacuumService.ReclaimStep(connectionString, 8);
        var (_, drained) = SqliteAutoVacuumService.ReclaimStep(connectionString, int.MaxValue);

        Assert.Multiple(() =>
        {
            Assert.That(before, Is.GreaterThan(8), "precondition: the delete must free more than one step's bound");
            Assert.That(before - after, Is.EqualTo(8));
            Assert.That(drained, Is.Zero);
        });
    }

    [Test]
    public void A_reclaim_step_on_a_clean_database_is_a_no_op()
    {
        new SqliteSchemaInitializer(_dbPath).Initialize();

        var result = SqliteAutoVacuumService.ReclaimStep(SqliteSchemaInitializer.BuildConnectionString(_dbPath), 8);

        Assert.That(result, Is.EqualTo((0L, 0L)));
    }

    [Test]
    public void ReclaimStep_validates_its_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => SqliteAutoVacuumService.ReclaimStep(null!, 1), Throws.ArgumentNullException);
            Assert.That(() => SqliteAutoVacuumService.ReclaimStep("Data Source=x", 0),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [Test]
    public async Task The_service_stops_promptly_when_not_in_incremental_mode()
    {
        var config = RepoContextHostConfiguration.FromConfiguration(new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [RepoContextHostConfiguration.SqlitePathKey] = _dbPath,
                [RepoContextHostConfiguration.SqliteAutoVacuumKey] = "none",
            })
            .Build());
        var outcome = new SqliteAutoVacuumOutcome(
            SqliteAutoVacuumMode.None, SqliteAutoVacuumMode.None, false, 0, 0, TimeSpan.Zero);
        using var service = new SqliteAutoVacuumService(config, outcome, NullLogger<SqliteAutoVacuumService>.Instance);

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(service.ExecuteTask.IsCompletedSuccessfully, Is.True);
    }

    private void FillThenDelete()
    {
        using var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        Exec(connection, "CREATE TABLE IF NOT EXISTS Filler (Id INTEGER PRIMARY KEY, Payload BLOB);");
        using (var transaction = connection.BeginTransaction())
        {
            using var insert = connection.CreateCommand();
            insert.Transaction = transaction;
            insert.CommandText = "INSERT INTO Filler (Payload) VALUES (randomblob(8192));";
            for (var i = 0; i < 500; i++)
            {
                insert.ExecuteNonQuery();
            }

            transaction.Commit();
        }

        Exec(connection, "DELETE FROM Filler;");
        Exec(connection, "PRAGMA wal_checkpoint(TRUNCATE);");
    }

    private long ReadAutoVacuum()
    {
        using var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA auto_vacuum;";
        return (long)command.ExecuteScalar()!;
    }

    private static void Exec(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }
}
