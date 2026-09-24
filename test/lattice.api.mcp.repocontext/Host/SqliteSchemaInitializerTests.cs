using Microsoft.Data.Sqlite;
using Orleans.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="SqliteSchemaInitializer"/>: the embedded Orleans
/// ADO.NET schema applies against a fresh database file, is idempotent and
/// self-healing on a second run (re-applying corrected query definitions over an
/// existing file), the grain-storage write query manages no transaction of its
/// own, and the shared connection string carries the busy-timeout window.
/// </summary>
[TestFixture]
public sealed class SqliteSchemaInitializerTests
{
    private string _root = null!;
    private string _dbPath = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-sqlite-" + Guid.NewGuid().ToString("N"));
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
    public void Initialize_creates_the_orleans_schema_on_a_fresh_database()
    {
        new SqliteSchemaInitializer(_dbPath).Initialize();

        Assert.Multiple(() =>
        {
            Assert.That(File.Exists(_dbPath), Is.True);
            Assert.That(TableExists("OrleansQuery"), Is.True);
            Assert.That(TableExists("OrleansStorage"), Is.True);
            Assert.That(TableExists("OrleansRemindersTable"), Is.True);
        });
    }

    [Test]
    public void Initialize_is_idempotent_on_a_second_run()
    {
        var initializer = new SqliteSchemaInitializer(_dbPath);
        initializer.Initialize();

        Assert.That(() => initializer.Initialize(), Throws.Nothing);
        Assert.That(TableExists("OrleansQuery"), Is.True);
    }

    [Test]
    public void WriteToStorageKey_query_does_not_manage_transactions_manually()
    {
        new SqliteSchemaInitializer(_dbPath).Initialize();

        var queryText = ReadQueryText("WriteToStorageKey");

        Assert.Multiple(() =>
        {
            Assert.That(queryText, Does.Not.Contain("BEGIN TRANSACTION"),
                "The write query must not open a transaction: under connection pooling a "
                + "batch that fails before COMMIT leaks it onto the pooled connection, so its "
                + "next reuse fails with 'cannot start a transaction within a transaction'.");
            Assert.That(queryText, Does.Not.Contain("COMMIT"));
        });
    }

    [Test]
    public void Initialize_reapplies_query_definitions_over_an_existing_database()
    {
        var initializer = new SqliteSchemaInitializer(_dbPath);
        initializer.Initialize();

        // Simulate an older database whose stored query text predates a fix.
        OverwriteQueryText("WriteToStorageKey", "SELECT 'stale';");
        Assert.That(ReadQueryText("WriteToStorageKey"), Is.EqualTo("SELECT 'stale';"));

        // A redeploy must self-heal the stored definition rather than keep the stale one.
        initializer.Initialize();

        Assert.That(ReadQueryText("WriteToStorageKey"), Does.Contain("UPDATE OrleansStorage"),
            "Re-running the initializer over an existing file restores the current query text.");
    }

    [Test]
    public void Initialize_enables_wal_journal_mode()
    {
        new SqliteSchemaInitializer(_dbPath).Initialize();

        using var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA journal_mode;";
        var mode = (string)command.ExecuteScalar()!;

        Assert.That(mode, Is.EqualTo("wal").IgnoreCase);
    }

    [Test]
    public void BuildConnectionString_targets_the_file_with_a_busy_timeout()
    {
        var connectionString = SqliteSchemaInitializer.BuildConnectionString(_dbPath);

        var builder = new SqliteConnectionStringBuilder(connectionString);
        Assert.Multiple(() =>
        {
            Assert.That(builder.DataSource, Is.EqualTo(_dbPath));
            Assert.That(builder.DefaultTimeout, Is.GreaterThan(0));
            Assert.That(builder.DefaultTimeout,
                Is.EqualTo((int)(new SiloMessagingOptions().ResponseTimeout.TotalSeconds / 2)),
                "SQLite must leave headroom before Orleans times out the enclosing request.");
        });
    }

    [TestCase("")]
    [TestCase("   ")]
    public void Constructor_rejects_an_empty_path(string path)
        => Assert.That(() => new SqliteSchemaInitializer(path), Throws.ArgumentException);

    [TestCase(-1)]
    [TestCase(0)]
    [TestCase(1)]
    [TestCase(1.999)]
    public void BuildConnectionString_rejects_budgets_that_would_allow_unlimited_retries(double seconds)
    {
        var budget = TimeSpan.FromSeconds(seconds);
        Assert.Multiple(() =>
        {
            Assert.That(() => SqliteSchemaInitializer.BuildConnectionString(_dbPath, budget),
                Throws.TypeOf<ArgumentOutOfRangeException>());
            Assert.That(() => new SqliteSchemaInitializer(_dbPath, budget),
                Throws.TypeOf<ArgumentOutOfRangeException>());
        });
    }

    [TestCase(2, 1)]
    [TestCase(31.9, 15)]
    [TestCase(10_000_000, 2_147_483)]
    public void Initialize_uses_the_derived_timeout_for_commands_and_the_busy_pragma(
        double requestSeconds, int busySeconds)
    {
        var budget = TimeSpan.FromSeconds(requestSeconds);
        new SqliteSchemaInitializer(_dbPath, budget).Initialize();

        // Reuse the initializer's pooled connection to observe its per-connection PRAGMA.
        using var connection = new SqliteConnection(
            SqliteSchemaInitializer.BuildConnectionString(_dbPath, budget));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA busy_timeout;";
        Assert.Multiple(() =>
        {
            Assert.That(command.CommandTimeout, Is.EqualTo(busySeconds));
            Assert.That(Convert.ToInt64(command.ExecuteScalar()), Is.EqualTo(busySeconds * 1000L));
            Assert.That(busySeconds, Is.LessThan(requestSeconds));
        });
    }

    [Test]
    public void BuildConnectionString_contended_write_surfaces_sqlite_busy()
    {
        var budget = TimeSpan.FromSeconds(2);
        new SqliteSchemaInitializer(_dbPath, budget).Initialize();
        var connectionString = SqliteSchemaInitializer.BuildConnectionString(_dbPath, budget);
        using var writer = new SqliteConnection(connectionString);
        writer.Open();
        using var transaction = writer.BeginTransaction();
        using var contender = new SqliteConnection(connectionString);
        contender.Open();
        using var command = contender.CreateCommand();
        command.CommandText = "INSERT INTO OrleansQuery (QueryKey, QueryText) VALUES ('contender', 'SELECT 1');";

        var exception = Assert.Throws<SqliteException>(() => command.ExecuteNonQuery());

        Assert.That(exception!.SqliteErrorCode, Is.EqualTo(5), "A held write lock must surface SQLITE_BUSY.");
        Assert.That(command.CommandTimeout, Is.EqualTo(1));
    }

    [Test]
    public void Initialize_rejects_a_database_path_with_no_parent_directory()
    {
        // A filesystem root has no parent, so there is no directory the guard
        // could prove writable. Refusing beats opening a database at the root of
        // the container filesystem, which is outside the mounted data volume and
        // would not survive a restart.
        var root = Path.GetPathRoot(Path.GetTempPath())!;

        var ex = Assert.Throws<InvalidOperationException>(
            () => new SqliteSchemaInitializer(root).Initialize());

        Assert.That(ex!.Message, Does.Contain("Could not resolve a directory"));
    }

    private bool TableExists(string table)
    {
        using var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=$name;";
        command.Parameters.AddWithValue("$name", table);
        return Convert.ToInt64(command.ExecuteScalar()) > 0;
    }

    private string ReadQueryText(string queryKey)
    {
        using var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT QueryText FROM OrleansQuery WHERE QueryKey=$key;";
        command.Parameters.AddWithValue("$key", queryKey);
        return (string)command.ExecuteScalar()!;
    }

    private void OverwriteQueryText(string queryKey, string queryText)
    {
        using var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "UPDATE OrleansQuery SET QueryText=$text WHERE QueryKey=$key;";
        command.Parameters.AddWithValue("$text", queryText);
        command.Parameters.AddWithValue("$key", queryKey);
        command.ExecuteNonQuery();
    }
}
