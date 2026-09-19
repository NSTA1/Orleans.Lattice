using Microsoft.Data.Sqlite;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Behavioural tests for the grain-storage queries this host ships in
/// <c>SQLite-Persistence.sql</c>, exercised exactly as Orleans' ADO.NET provider
/// exercises them: run the stored batch, collect every <c>NewGrainStateVersion</c>
/// row across every result set, and treat the row count the way
/// <c>AdoNetGrainStorage</c> does with <c>SingleOrDefault()</c>.
/// </summary>
/// <remarks>
/// <para>
/// The load-bearing tests here are the two duplicate-row cases. Orleans reads the
/// version report with <c>SingleOrDefault()</c>, so a batch that returns two rows
/// throws <c>InvalidOperationException("Sequence contains more than one
/// element")</c> out of <c>WriteStateAsync</c> rather than surfacing a storage
/// conflict. Nothing in the schema prevents a grain identity from acquiring a
/// second row: <c>IX_OrleansStorage</c> is deliberately non-unique. Reporting the
/// version by selecting it back out of <c>OrleansStorage</c> therefore returns one
/// row per storage row, which makes such a grain permanently unwritable while it
/// still reads cleanly (<c>ReadFromStorageKey</c> caps itself with <c>LIMIT 1</c>).
/// </para>
/// <para>
/// These fixtures drive the real embedded script through a real SQLite file, so
/// they observe the query text that actually ships rather than a restatement of
/// it. A shape assertion over the SQL string would pass against any text that
/// merely looked right; only executing the batch against a duplicated row proves
/// the row count Orleans will see.
/// </para>
/// <para>
/// The upstream script this host's copy derives from has the defective form in
/// both queries and is tracked as dotnet/orleans#11303. These tests therefore
/// also guard the local divergence: if the vendored script is ever re-synced
/// from upstream before that issue is fixed, the two duplicate-row cases redden.
/// </para>
/// </remarks>
[TestFixture]
public sealed class SqliteGrainStorageQueryTests
{
    private const string ServiceId = "repo-context";
    private const string StateName = "leaf";
    private const long N0 = 0x0123456789ABCDEF;
    private const long N1 = 0x76543210FEDCBA98;

    private string _root = null!;
    private string _dbPath = null!;
    private string _writeSql = null!;
    private string _clearSql = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-query-" + Guid.NewGuid().ToString("N"));
        _dbPath = Path.Combine(_root, "repocontext.db");
        new SqliteSchemaInitializer(_dbPath).Initialize();
        _writeSql = StoredQuery("WriteToStorageKey");
        _clearSql = StoredQuery("ClearStorageKey");
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
    public void A_first_write_reports_version_one()
    {
        var versions = Write(version: null);

        Assert.Multiple(() =>
        {
            Assert.That(versions, Has.Count.EqualTo(1));
            Assert.That(versions[0], Is.EqualTo(1));
            Assert.That(StorageRowCount(), Is.EqualTo(1));
        });
    }

    [Test]
    public void Successive_writes_report_incrementing_versions()
    {
        Assert.Multiple(() =>
        {
            Assert.That(Write(version: null), Is.EqualTo(new[] { 1 }));
            Assert.That(Write(version: 1), Is.EqualTo(new[] { 2 }));
            Assert.That(Write(version: 2), Is.EqualTo(new[] { 3 }));
            Assert.That(StorageRowCount(), Is.EqualTo(1));
        });
    }

    [Test]
    public void A_write_against_a_stale_version_reports_that_stale_version_so_orleans_raises_a_conflict()
    {
        Write(version: null);
        Write(version: 1);

        // Orleans' CheckVersionInconsistency raises InconsistentStateException
        // when the reported version equals the version the grain already held.
        var versions = Write(version: 1);

        Assert.Multiple(() =>
        {
            Assert.That(versions, Has.Count.EqualTo(1));
            Assert.That(versions[0], Is.EqualTo(1));
        });
    }

    [Test]
    public void A_first_write_when_the_row_already_exists_reports_nothing_so_orleans_raises_a_conflict()
    {
        Write(version: null);

        // The losing half of a first-create race. Orleans maps "no row reported"
        // with a null grain version onto a conflict, which is what lets
        // TopologySeedPersist adopt the winner's row.
        Assert.That(Write(version: null), Is.Empty);
    }

    [Test]
    public void A_write_reports_exactly_one_row_when_the_grain_identity_has_a_duplicate_row()
    {
        Write(version: null);
        DuplicateEveryStorageRow();

        var versions = Write(version: 1);

        Assert.Multiple(() =>
        {
            Assert.That(StorageRowCount(), Is.EqualTo(2), "the duplicate must actually be present");
            Assert.That(versions, Has.Count.EqualTo(1),
                "Orleans reads this with SingleOrDefault(), so a second row would throw "
                + "InvalidOperationException('Sequence contains more than one element') out of "
                + "WriteStateAsync and leave the grain permanently unwritable.");
            Assert.That(versions[0], Is.EqualTo(2));
        });
    }

    [Test]
    public void A_clear_reports_exactly_one_row_when_the_grain_identity_has_a_duplicate_row()
    {
        Write(version: null);
        DuplicateEveryStorageRow();

        var versions = Clear(version: 1);

        Assert.Multiple(() =>
        {
            Assert.That(StorageRowCount(), Is.EqualTo(2), "the duplicate must actually be present");
            Assert.That(versions, Has.Count.EqualTo(1),
                "ClearStateAsync reads its version report with SingleOrDefault() too.");
            Assert.That(versions[0], Is.EqualTo(2));
        });
    }

    [Test]
    public void A_clear_reports_the_incremented_version()
    {
        Write(version: null);

        Assert.That(Clear(version: 1), Is.EqualTo(new[] { 2 }));
    }

    [Test]
    public void Concurrent_first_writes_create_exactly_one_row()
    {
        // The conditional INSERT is a single statement, so SQLite serialises the
        // existence check with the insert. This pins that: were it to stop
        // holding, the loser would land a second row and poison the grain.
        Parallel.For(0, 32, _ => Write(version: null));

        Assert.That(StorageRowCount(), Is.EqualTo(1));
    }

    private void DuplicateEveryStorageRow()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText =
            "INSERT INTO OrleansStorage (GrainIdHash, GrainIdN0, GrainIdN1, GrainTypeHash, GrainTypeString, "
            + "GrainIdExtensionString, ServiceId, PayloadBinary, ModifiedOn, Version) "
            + "SELECT GrainIdHash, GrainIdN0, GrainIdN1, GrainTypeHash, GrainTypeString, "
            + "GrainIdExtensionString, ServiceId, PayloadBinary, ModifiedOn, Version FROM OrleansStorage;";
        command.ExecuteNonQuery();
    }

    private List<int> Write(int? version) => Run(_writeSql, version, payload: true);

    private List<int> Clear(int? version) => Run(_clearSql, version, payload: false);

    private List<int> Run(string sql, int? version, bool payload)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.Parameters.AddWithValue("@GrainIdHash", 1234);
        command.Parameters.AddWithValue("@GrainIdN0", N0);
        command.Parameters.AddWithValue("@GrainIdN1", N1);
        command.Parameters.AddWithValue("@GrainTypeHash", 5678);
        command.Parameters.AddWithValue("@GrainTypeString", StateName);
        command.Parameters.AddWithValue("@GrainIdExtensionString", DBNull.Value);
        command.Parameters.AddWithValue("@ServiceId", ServiceId);
        command.Parameters.AddWithValue("@GrainStateVersion", version.HasValue ? version.Value : DBNull.Value);
        if (payload)
        {
            command.Parameters.AddWithValue("@PayloadBinary", new byte[] { 1, 2, 3 });
        }

        var reported = new List<int>(1);
        using var reader = command.ExecuteReader();
        do
        {
            while (reader.Read())
            {
                if (!reader.IsDBNull(0))
                {
                    reported.Add(Convert.ToInt32(reader.GetValue(0)));
                }
            }
        }
        while (reader.NextResult());

        return reported;
    }

    private long StorageRowCount()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM OrleansStorage;";
        return Convert.ToInt64(command.ExecuteScalar());
    }

    private string StoredQuery(string queryKey)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT QueryText FROM OrleansQuery WHERE QueryKey=$key;";
        command.Parameters.AddWithValue("$key", queryKey);
        return (string)command.ExecuteScalar()!;
    }

    private SqliteConnection Open()
    {
        var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        return connection;
    }
}
