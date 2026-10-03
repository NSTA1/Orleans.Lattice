using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Issue #4391: <see cref="SqliteSnapshotOrphanSweep"/> deletes leaf and snapshot rows
/// that no live row reaches, and nothing else. Driven against a real SQLite file with
/// the real grain-storage schema, so every assertion is on rows that exist or do not.
/// </summary>
[TestFixture]
public sealed class SqliteSnapshotOrphanSweepTests
{
    private const string Service = "repo-context";
    private const int SegmentBytes = 256 * 1024;

    private string _root = null!;
    private string _dbPath = null!;

    [SetUp]
    public void SetUp()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-snapshot-sweep-" + Guid.NewGuid().ToString("N"));
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

    /// <summary>
    /// The leaf identities of a database seeded with every shape the sweep must tell
    /// apart. <see cref="Live"/> must survive any sweep; <see cref="Stranded"/> must go.
    /// </summary>
    private sealed record Seeded(Guid[] Live, Guid[] Stranded, Guid OtherService, string MalformedSegmentKey);

    private Seeded Seed()
    {
        using var connection = Open();

        // Live, routed by a shard root that names it as lowercase hex. Its own row
        // names the next leaf, which nothing else names.
        var routed = Guid.NewGuid();
        var onlyViaSibling = Guid.NewGuid();
        InsertLeaf(connection, routed, "tree-a", mentions: [Text(onlyViaSibling)]);
        InsertManifest(connection, routed);
        InsertSegment(connection, $"{routed:N}/g2/0");
        InsertSegment(connection, $"{routed:N}/g2/1");

        // Live only because a live leaf's row names it.
        InsertLeaf(connection, onlyViaSibling, "tree-a");
        InsertManifest(connection, onlyViaSibling);

        // Live through an upper-case mention in the same shard root.
        var upperCase = Guid.NewGuid();
        InsertLeaf(connection, upperCase, "tree-a");
        InsertManifest(connection, upperCase);
        InsertRow(connection, "shardroot", Guid.NewGuid(), null, Service,
            Bytes("root:", Text(routed), "/next:", Text(upperCase).ToUpperInvariant()));

        // Live through raw Guid bytes in an internal node, in .NET byte order.
        var rawMixed = Guid.NewGuid();
        InsertLeaf(connection, rawMixed, "tree-b");
        InsertManifest(connection, rawMixed);
        InsertRow(connection, "internal", Guid.NewGuid(), null, Service, [0x01, .. rawMixed.ToByteArray(), 0x02]);

        // Live through raw Guid bytes in big-endian order in a pin row.
        var rawBigEndian = Guid.NewGuid();
        InsertManifest(connection, rawBigEndian);
        InsertRow(connection, "wal-materialiser-pins", Guid.NewGuid(), null, Service, rawBigEndian.ToByteArray(bigEndian: true));

        // Live through the hyphenated form in another grain's key.
        var inKey = Guid.NewGuid();
        InsertManifest(connection, inKey);
        InsertRow(connection, "lattice-cursor", Guid.Empty, $"cursor/{inKey.ToString("D").ToUpperInvariant()}", Service, null);

        // A snapshot whose leaf row is missing but which a shard still routes to: it
        // may hold the only copy of that leaf's data, so it is kept.
        var routedWithoutRow = Guid.NewGuid();
        InsertManifest(connection, routedWithoutRow);
        InsertSegment(connection, $"{routedWithoutRow:N}/0");
        InsertRow(connection, "internal", Guid.NewGuid(), null, Service, Bytes("child:", Text(routedWithoutRow)));

        // Stranded: a manifest and segments with no leaf row, which nothing names.
        var orphanManifest = Guid.NewGuid();
        InsertManifest(connection, orphanManifest);
        InsertSegment(connection, $"{orphanManifest:N}/g3/0");
        InsertSegment(connection, $"{orphanManifest:N}/g3/1");

        // Stranded: a leaf row written back without a tree id after a clear (#4419).
        var zombie = Guid.NewGuid();
        InsertLeaf(connection, zombie, treeId: null);
        InsertManifest(connection, zombie);
        InsertSegment(connection, $"{zombie:N}/0");

        // Stranded: two leaf rows naming each other and nothing else. They must not keep
        // each other alive.
        var chainA = Guid.NewGuid();
        var chainB = Guid.NewGuid();
        InsertLeaf(connection, chainA, "purged-tree", mentions: [Text(chainB)]);
        InsertLeaf(connection, chainB, "purged-tree", mentions: [Text(chainA)]);
        InsertManifest(connection, chainA);

        // A segment of a leaf the shard root of ANOTHER service names: references do not
        // cross services, so it is stranded within its own.
        var otherService = Guid.NewGuid();
        InsertManifest(connection, otherService, service: "other-service");
        InsertSegment(connection, $"{otherService:N}/0", service: "other-service");
        InsertRow(connection, "shardroot", Guid.NewGuid(), null, Service, Bytes("root:", Text(otherService)));

        // A segment whose key has an unexpected shape is never touched.
        var malformed = "not-a-guid/0";
        InsertSegment(connection, malformed);

        using (var checkpoint = connection.CreateCommand())
        {
            checkpoint.CommandText = "PRAGMA wal_checkpoint(TRUNCATE);";
            checkpoint.ExecuteNonQuery();
        }

        return new Seeded(
            [routed, upperCase, onlyViaSibling, rawMixed, rawBigEndian, inKey, routedWithoutRow],
            [orphanManifest, zombie, chainA, chainB],
            otherService,
            malformed);
    }

    [Test]
    public void Report_mode_classifies_the_stranded_rows_and_deletes_nothing()
    {
        Seed();
        var before = CountRows();

        var outcome = SqliteSnapshotOrphanSweep.Run(_dbPath, SqliteSnapshotSweepMode.Report);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Mode, Is.EqualTo(SqliteSnapshotSweepMode.Report));
            Assert.That(outcome.StrandedLeaves, Is.EqualTo(5), "orphan manifest, zombie, both chain links, other-service leaf");
            Assert.That(outcome.LeafRows, Is.EqualTo(3), "the zombie and the two chain rows");
            Assert.That(outcome.ManifestRows, Is.EqualTo(4), "orphan, zombie, chain A, other-service");
            Assert.That(outcome.SegmentRows, Is.EqualTo(4), "two orphan, one zombie, one other-service");
            Assert.That(outcome.PayloadBytes, Is.GreaterThanOrEqualTo(4L * SegmentBytes));
            Assert.That(outcome.RowsDeleted, Is.Zero);
            Assert.That(CountRows(), Is.EqualTo(before), "report mode must not delete");
        });
    }

    [Test]
    public void Delete_mode_removes_exactly_the_stranded_rows()
    {
        var seeded = Seed();

        var outcome = SqliteSnapshotOrphanSweep.Run(_dbPath, SqliteSnapshotSweepMode.Delete);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.RowsDeleted, Is.EqualTo(outcome.LeafRows + outcome.ManifestRows + outcome.SegmentRows));
            Assert.That(outcome.RowsDeleted, Is.EqualTo(11));
            foreach (var leaf in seeded.Live)
            {
                Assert.That(RowsOwnedBy(leaf), Is.GreaterThan(0), $"live leaf {leaf:N} lost its rows");
            }

            Assert.That(RowsOwnedBy(seeded.Live[0]), Is.EqualTo(4), "a live leaf keeps its leaf row, manifest and every segment");
            foreach (var leaf in seeded.Stranded)
            {
                Assert.That(RowsOwnedBy(leaf), Is.Zero, $"stranded leaf {leaf:N} kept rows");
            }

            Assert.That(RowsOwnedBy(seeded.OtherService, "other-service"), Is.Zero);
            Assert.That(SegmentExists(seeded.MalformedSegmentKey), Is.True, "a key of unexpected shape is never deleted");
            Assert.That(CountRows("shardroot") + CountRows("internal"), Is.EqualTo(4), "topology rows are never touched");
        });
    }

    [Test]
    public void Delete_mode_returns_the_freed_pages_so_the_file_shrinks()
    {
        Seed();

        var outcome = SqliteSnapshotOrphanSweep.Run(_dbPath, SqliteSnapshotSweepMode.Delete);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.BytesAfter, Is.LessThan(outcome.BytesBefore - (3L * SegmentBytes)),
                "the deleted segments' pages must be returned to the filesystem, not left on the freelist");
            Assert.That(FreelistCount(), Is.Zero);
        });
    }

    [Test]
    public void A_second_sweep_finds_nothing()
    {
        Seed();
        SqliteSnapshotOrphanSweep.Run(_dbPath, SqliteSnapshotSweepMode.Delete);

        var second = SqliteSnapshotOrphanSweep.Run(_dbPath, SqliteSnapshotSweepMode.Delete);

        Assert.Multiple(() =>
        {
            Assert.That(second.StrandedLeaves, Is.Zero);
            Assert.That(second.RowsDeleted, Is.Zero);
        });
    }

    [Test]
    public void Deleting_one_row_per_batch_deletes_the_same_rows()
    {
        Seed();

        var outcome = SqliteSnapshotOrphanSweep.Run(_dbPath, SqliteSnapshotSweepMode.Delete, batchSize: 1);

        Assert.That(outcome.RowsDeleted, Is.EqualTo(11));
    }

    [Test]
    public void Off_mode_does_nothing()
    {
        Seed();
        var before = CountRows();

        var outcome = SqliteSnapshotOrphanSweep.Run(_dbPath, SqliteSnapshotSweepMode.Off);

        Assert.Multiple(() =>
        {
            Assert.That(outcome, Is.SameAs(SqliteSnapshotSweepOutcome.NotRun));
            Assert.That(CountRows(), Is.EqualTo(before));
        });
    }

    [Test]
    public void Run_validates_its_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => SqliteSnapshotOrphanSweep.Run(" ", SqliteSnapshotSweepMode.Report), Throws.ArgumentException);
            Assert.That(() => SqliteSnapshotOrphanSweep.Run(_dbPath, SqliteSnapshotSweepMode.Report, batchSize: 0),
                Throws.InstanceOf<ArgumentOutOfRangeException>());
        });
    }

    [TestCase("0123456789abcdef0123456789abcdef/7", true)]
    [TestCase("0123456789abcdef0123456789abcdef/g12/0", true)]
    [TestCase("0123456789abcdef0123456789abcdef", false)]
    [TestCase("0123456789abcdef0123456789abcdef/x1/0", false)]
    [TestCase("0123456789abcdef0123456789abcdef/g/0", false)]
    [TestCase("not-a-guid/0", false)]
    public void Segment_keys_parse_only_in_their_exact_shapes(string key, bool parses)
        => Assert.That(SqliteSnapshotOrphanSweep.TryParseSegmentOwner(key, out _), Is.EqualTo(parses));

    [Test]
    public void Key_columns_decode_to_the_guid_they_were_written_from()
    {
        var leaf = Guid.NewGuid();
        var (n0, n1) = KeyColumns(leaf);

        Assert.That(SqliteSnapshotOrphanSweep.GuidFromKeyColumns(n0, n1), Is.EqualTo(leaf));
    }

    [Test]
    public void SweepSnapshotStorage_runs_the_configured_mode_against_the_sqlite_store()
    {
        Seed();
        var config = RepoContextHostConfiguration.FromConfiguration(new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [RepoContextHostConfiguration.SqlitePathKey] = _dbPath,
                [RepoContextHostConfiguration.SqliteSnapshotSweepKey] = "report",
            })
            .Build());

        var outcome = RepoContextHostBuilder.SweepSnapshotStorage(config);

        Assert.Multiple(() =>
        {
            Assert.That(outcome.Mode, Is.EqualTo(SqliteSnapshotSweepMode.Report));
            Assert.That(outcome.StrandedLeaves, Is.EqualTo(5));
            Assert.That(() => RepoContextHostBuilder.SweepSnapshotStorage(null!), Throws.ArgumentNullException);
        });
    }

    [Test]
    public async Task The_reclaim_service_reports_the_sweep_outcome()
    {
        var config = RepoContextHostConfiguration.FromConfiguration(new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?>
            {
                [RepoContextHostConfiguration.SqlitePathKey] = _dbPath,
                [RepoContextHostConfiguration.SqliteAutoVacuumKey] = "none",
            })
            .Build());
        var vacuum = new SqliteAutoVacuumOutcome(
            SqliteAutoVacuumMode.None, SqliteAutoVacuumMode.None, false, 0, 0, TimeSpan.Zero);
        var sweep = new SqliteSnapshotSweepOutcome(
            SqliteSnapshotSweepMode.Delete, 10, 4, 1, 4, 2, 1234, 7, 9000, 4000, TimeSpan.FromMilliseconds(5));
        var logger = new CapturingLogger();
        using var service = new SqliteAutoVacuumService(config, vacuum, logger, snapshotSweep: sweep);

        await service.StartAsync(CancellationToken.None);
        await service.ExecuteTask!.WaitAsync(TimeSpan.FromSeconds(10));

        Assert.That(logger.Messages.Any(m => m.Contains("snapshot sweep", StringComparison.Ordinal)
            && m.Contains("7 row(s) deleted", StringComparison.Ordinal)), Is.True,
            string.Join(Environment.NewLine, logger.Messages));
    }

    [Test]
    public void A_shard_roots_leaf_access_statistics_do_not_keep_a_leaf_but_the_rest_of_the_row_does()
    {
        using (var connection = Open())
        {
            var counted = Guid.NewGuid();
            var routedAfter = Guid.NewGuid();
            var nestedName = Guid.NewGuid();
            InsertManifest(connection, counted);
            InsertManifest(connection, routedAfter);
            InsertManifest(connection, nestedName);

            // The statistics name all three. Only the pending-clear list after them, and
            // a nested property that merely shares the statistics' name, are references.
            var json =
                "{\"$type\":\"ShardRootState\",\"RootNodeId\":\"bplusinternal/00\","
                + $"\"LeafAccessModel\":{{\"Leaves\":{{\"$values\":[\"bplusleaf/{counted:N}\",\"bplusleaf/{routedAfter:N}\",\"bplusleaf/{nestedName:N}\"]}},\"Visits\":[1,2,3]}},"
                + $"\"PendingLeafClears\":[\"bplusleaf/{routedAfter:N}\"],"
                + $"\"Other\":{{\"LeafAccessModel\":\"bplusleaf/{nestedName:N}\"}}}}";
            InsertRow(connection, SqliteSnapshotOrphanSweep.ShardRootState, Guid.NewGuid(), null, Service, Bytes(json));

            var outcome = SqliteSnapshotOrphanSweep.Run(_dbPath, SqliteSnapshotSweepMode.Delete);

            Assert.Multiple(() =>
            {
                Assert.That(outcome.StrandedLeaves, Is.EqualTo(1));
                Assert.That(RowsOwnedBy(counted), Is.Zero, "named only by the access statistics");
                Assert.That(RowsOwnedBy(routedAfter), Is.EqualTo(1), "also named outside the statistics");
                Assert.That(RowsOwnedBy(nestedName), Is.EqualTo(1), "a nested property of the same name is not the statistics");
            });
        }
    }

    [TestCase("", false)]
    [TestCase("LGB1 binary", false)]
    [TestCase("{\"RootNodeId\":\"x\"}", false)]
    [TestCase("{\"LeafAccessModel\":", false)]
    [TestCase("{\"Other\":{\"LeafAccessModel\":1}}", false)]
    [TestCase("{\"A\":1,\"LeafAccessModel\":{\"Leaves\":[]},\"B\":2}", true)]
    public void Access_statistics_are_found_only_as_a_top_level_json_property(string payload, bool found)
    {
        var bytes = Bytes(payload);

        var result = SqliteSnapshotOrphanSweep.TryFindAccessStatistics(bytes, out var start, out var end);

        Assert.That(result, Is.EqualTo(found));
        if (found)
        {
            Assert.That(System.Text.Encoding.UTF8.GetString(bytes, start, end - start), Is.EqualTo("{\"Leaves\":[]}"));
        }
    }

    [Test]
    public void Mentions_are_found_in_every_encoding_and_only_for_candidates()
    {
        var a = Guid.NewGuid();
        var b = Guid.NewGuid();
        var c = Guid.NewGuid();
        var d = Guid.NewGuid();
        var stranger = Guid.NewGuid();
        var candidates = new HashSet<Guid> { a, b, c, d };
        byte[] data =
        [
            .. Bytes("x", a.ToString("N"), "-"),
            .. Bytes(b.ToString("D").ToUpperInvariant(), ";"),
            .. c.ToByteArray(),
            .. d.ToByteArray(bigEndian: true),
            .. Bytes(stranger.ToString("N")),
        ];
        var found = new HashSet<Guid>();

        SqliteSnapshotOrphanSweep.FindMentions(data, candidates, found);

        Assert.That(found, Is.EquivalentTo(new[] { a, b, c, d }));
    }

    private sealed class CapturingLogger : ILogger<SqliteAutoVacuumService>
    {
        public List<string> Messages { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => Messages.Add(formatter(state, exception));
    }

    private SqliteConnection Open()
    {
        var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        return connection;
    }

    private static string Text(Guid leaf) => leaf.ToString("N");

    private static byte[] Bytes(params string[] parts) => System.Text.Encoding.UTF8.GetBytes(string.Concat(parts));

    private static (long N0, long N1) KeyColumns(Guid leaf)
    {
        var bytes = leaf.ToByteArray();
        return (BitConverter.ToInt64(bytes, 0), BitConverter.ToInt64(bytes, 8));
    }

    private static void InsertLeaf(SqliteConnection connection, Guid leaf, string? treeId, string[]? mentions = null)
    {
        var payload = treeId is null
            ? new byte[] { 0x4C, 0x47, 0x42, 0x31, 0x20, 0xC1, 0x01 }
            : Bytes(treeId, "|", string.Join("|", mentions ?? []));
        InsertRow(connection, SqliteSnapshotOrphanSweep.LeafState, leaf, null, Service, payload);
    }

    private static void InsertManifest(SqliteConnection connection, Guid leaf, string service = Service)
        => InsertRow(connection, SqliteSnapshotOrphanSweep.ManifestState, leaf, null, service, new byte[64]);

    private static void InsertSegment(SqliteConnection connection, string key, string service = Service)
        => InsertRow(connection, SqliteSnapshotOrphanSweep.SegmentState, Guid.Empty, key, service, new byte[SegmentBytes]);

    private static void InsertRow(
        SqliteConnection connection, string type, Guid key, string? extension, string service, byte[]? payload)
    {
        var (n0, n1) = KeyColumns(key);
        using var command = connection.CreateCommand();
        command.CommandText =
            "INSERT INTO OrleansStorage (GrainIdHash, GrainIdN0, GrainIdN1, GrainTypeHash, GrainTypeString, "
            + "GrainIdExtensionString, ServiceId, PayloadBinary, ModifiedOn, Version) "
            + "VALUES (0, $n0, $n1, 0, $type, $ext, $service, $payload, '2026-10-01 00:00:00', 1);";
        command.Parameters.AddWithValue("$n0", n0);
        command.Parameters.AddWithValue("$n1", n1);
        command.Parameters.AddWithValue("$type", type);
        command.Parameters.AddWithValue("$ext", (object?)extension ?? DBNull.Value);
        command.Parameters.AddWithValue("$service", service);
        command.Parameters.AddWithValue("$payload", (object?)payload ?? DBNull.Value);
        command.ExecuteNonQuery();
    }

    private long CountRows(string? type = null)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = type is null
            ? "SELECT COUNT(*) FROM OrleansStorage;"
            : "SELECT COUNT(*) FROM OrleansStorage WHERE GrainTypeString = $t;";
        if (type is not null)
        {
            command.Parameters.AddWithValue("$t", type);
        }

        return (long)command.ExecuteScalar()!;
    }

    private long RowsOwnedBy(Guid leaf, string service = Service)
    {
        var (n0, n1) = KeyColumns(leaf);
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT COUNT(*) FROM OrleansStorage WHERE ServiceId = $s AND ("
            + "(GrainTypeString IN ($leaf, $manifest) AND GrainIdN0 = $n0 AND GrainIdN1 = $n1) "
            + "OR (GrainTypeString = $segment AND GrainIdExtensionString LIKE $prefix));";
        command.Parameters.AddWithValue("$s", service);
        command.Parameters.AddWithValue("$leaf", SqliteSnapshotOrphanSweep.LeafState);
        command.Parameters.AddWithValue("$manifest", SqliteSnapshotOrphanSweep.ManifestState);
        command.Parameters.AddWithValue("$segment", SqliteSnapshotOrphanSweep.SegmentState);
        command.Parameters.AddWithValue("$n0", n0);
        command.Parameters.AddWithValue("$n1", n1);
        command.Parameters.AddWithValue("$prefix", leaf.ToString("N") + "/%");
        return (long)command.ExecuteScalar()!;
    }

    private bool SegmentExists(string key)
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM OrleansStorage WHERE GrainIdExtensionString = $k;";
        command.Parameters.AddWithValue("$k", key);
        return (long)command.ExecuteScalar()! == 1;
    }

    private long FreelistCount()
    {
        using var connection = Open();
        using var command = connection.CreateCommand();
        command.CommandText = "PRAGMA freelist_count;";
        return (long)command.ExecuteScalar()!;
    }
}
