using Microsoft.Data.Sqlite;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Unit tests for <see cref="SqliteSchemaInitializer"/>: the embedded Orleans
/// ADO.NET schema applies against a fresh database file, is idempotent on a second
/// run, and the shared connection string carries the busy-timeout window.
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
        });
    }

    [TestCase("")]
    [TestCase("   ")]
    public void Constructor_rejects_an_empty_path(string path)
        => Assert.That(() => new SqliteSchemaInitializer(path), Throws.ArgumentException);

    private bool TableExists(string table)
    {
        using var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=$name;";
        command.Parameters.AddWithValue("$name", table);
        return Convert.ToInt64(command.ExecuteScalar()) > 0;
    }
}
