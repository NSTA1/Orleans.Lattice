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
/// The load-bearing tests here are the three duplicate-row cases. Orleans reads the
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
/// The <c>DeleteStorageKey</c> cases carry the same weight for an additional
/// reason. The upstream PostgreSQL form of that query is
/// <c>DELETE ... RETURNING Version + 1</c>, which emits one row per deleted row
/// and would therefore reintroduce that wedge on any grain identity holding a
/// duplicate. This host's copy computes the version as a scalar instead, and
/// <see cref="A_delete_reports_exactly_one_row_when_the_grain_identity_has_a_duplicate_row"/>
/// is what pins the difference.
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
/// from upstream before that issue is fixed, the duplicate-row cases redden.
/// </para>
/// </remarks>
[TestFixture]
public sealed partial class SqliteGrainStorageQueryTests
{
    private const string ServiceId = "repo-context";
    private const string StateName = "leaf";
    private const long N0 = 0x0123456789ABCDEF;
    private const long N1 = 0x76543210FEDCBA98;

    private string _root = null!;
    private string _dbPath = null!;
    private string _writeSql = null!;
    private string _clearSql = null!;
    private string _deleteSql = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-query-" + Guid.NewGuid().ToString("N"));
        _dbPath = Path.Combine(_root, "repocontext.db");
        new SqliteSchemaInitializer(_dbPath).Initialize();
        _writeSql = StoredQuery("WriteToStorageKey");
        _clearSql = StoredQuery("ClearStorageKey");
        _deleteSql = StoredQuery("DeleteStorageKey");
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

    [Test]
    public void A_delete_removes_the_row_entirely()
    {
        Write(version: null);
        Assert.That(StorageRowCount(), Is.EqualTo(1), "the row must exist before it can be deleted");

        Delete(version: 1);

        // The row count is the assertion that matters. Asserting only that a read
        // comes back with no payload is strictly weaker: the ClearStorageKey
        // behaviour this replaces already satisfies that, because it nulls
        // PayloadBinary and keeps the row. Only a count of zero distinguishes the
        // two, which is the whole substance of the defect.
        Assert.That(StorageRowCount(), Is.EqualTo(0),
            "DeleteStorageKey must remove the row, not null its payload.");
    }

    [Test]
    public void A_clear_leaves_a_tombstone_row_behind_which_is_the_behaviour_delete_replaces()
    {
        Write(version: null);

        Clear(version: 1);

        // The contrast that gives the test above its meaning, pinned rather than
        // assumed. ClearStorageKey is retained in the script because Orleans
        // resolves it at Init unconditionally, so it stays executable and stays
        // wrong; enabling DeleteStateOnClear is what routes clears away from it.
        // Were this query ever changed to delete as well, this test reddens and
        // says so, rather than the pair silently becoming the same query.
        Assert.Multiple(() =>
        {
            Assert.That(StorageRowCount(), Is.EqualTo(1));
            Assert.That(PayloadLength(), Is.Null, "the retained row carries no payload bytes");
        });
    }

    [Test]
    public void A_delete_reports_the_incremented_version()
    {
        Write(version: null);

        // Orleans' ClearStateAsync feeds this straight into
        // CheckVersionInconsistency, which raises a conflict when the reported
        // version equals the one the grain already held. Reporting 1 back would
        // therefore turn every successful delete into an
        // InconsistentStateException.
        Assert.That(Delete(version: 1), Is.EqualTo(new[] { 2 }));
    }

    [Test]
    public void A_delete_against_a_stale_version_keeps_the_row_and_reports_the_stale_version()
    {
        Write(version: null);
        Write(version: 1);

        var versions = Delete(version: 1);

        Assert.Multiple(() =>
        {
            Assert.That(versions, Has.Count.EqualTo(1));
            Assert.That(versions[0], Is.EqualTo(1),
                "a reported version equal to the grain's own version is what makes Orleans "
                + "raise InconsistentStateException.");
            Assert.That(StorageRowCount(), Is.EqualTo(1),
                "a delete that lost the optimistic-concurrency check must not remove the row.");
        });
    }

    [Test]
    public void A_delete_reports_exactly_one_row_when_the_grain_identity_has_a_duplicate_row()
    {
        Write(version: null);
        DuplicateEveryStorageRow();

        var versions = Delete(version: 1);

        Assert.Multiple(() =>
        {
            Assert.That(versions, Has.Count.EqualTo(1),
                "ClearStateAsync reads this with SingleOrDefault(). Upstream's "
                + "DELETE ... RETURNING form emits one row per deleted row, so it would throw "
                + "InvalidOperationException('Sequence contains more than one element') here.");
            Assert.That(versions[0], Is.EqualTo(2));
            Assert.That(StorageRowCount(), Is.EqualTo(0),
                "both duplicated rows match the identity and the version, so both go.");
        });
    }

    [Test]
    public void A_grain_that_was_deleted_can_be_written_again_from_scratch()
    {
        Write(version: null);
        Delete(version: 1);

        // After a delete-on-clear Orleans sets ETag to null and RecordExists to
        // false, so the next write arrives on the first-write path rather than
        // the update path. The row must come back at version 1.
        Assert.Multiple(() =>
        {
            Assert.That(Write(version: null), Is.EqualTo(new[] { 1 }));
            Assert.That(StorageRowCount(), Is.EqualTo(1));
        });
    }

    private long? PayloadLength()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT LENGTH(PayloadBinary) FROM OrleansStorage LIMIT 1;";
        var value = command.ExecuteScalar();
        return value is null or DBNull ? null : Convert.ToInt64(value);
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

    private List<int> Delete(int? version) => Run(_deleteSql, version, payload: false);

    private List<int> Run(string sql, int? version, bool payload)
    {
        using var connection = Open();
        return RunOn(connection, sql, version, payload);
    }

    private static List<int> RunOn(SqliteConnection connection, string sql, int? version, bool payload, int? commandTimeoutSeconds = null)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        if (commandTimeoutSeconds.HasValue)
        {
            command.CommandTimeout = commandTimeoutSeconds.Value;
        }

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
