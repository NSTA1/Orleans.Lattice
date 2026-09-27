using Microsoft.Data.Sqlite;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using NSubstitute;
using Orleans.Configuration;
using Orleans.Lattice.Api.Mcp.RepoContext.Host;
using Orleans.Lattice.BPlusTree;
using Orleans.Runtime;
using Orleans.Serialization.Serializers;
using Orleans.Storage;
using static Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host.LockAttributionTestSupport;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Host;

/// <summary>
/// Provokes a real SQLite write convoy against Orleans' real ADO.NET grain storage and
/// this host's real schema, and checks every failure in it is attributed.
/// </summary>
/// <remarks>
/// <para>
/// Issue #2431 notes the lock storm is not currently reproducing, so any work on it
/// needs a way to provoke it rather than wait for it. This is that provocation, made
/// deterministic: a second connection holds the database write lock with
/// <c>BEGIN IMMEDIATE</c>, so every writer queued behind it waits out the busy window
/// and fails with the same <c>SQLITE_BUSY</c> the deployed container logged. Nothing
/// about the failure is simulated - it is raised by Microsoft.Data.Sqlite inside
/// <see cref="AdoNetGrainStorage"/>, executing the embedded <c>WriteToStorageKey</c>
/// query - so these tests also pin the exception shape the classifier relies on.
/// </para>
/// <para>
/// The request budget is two seconds, which the host derives into a one-second busy
/// window, so each contended write costs about one second rather than fifteen.
/// </para>
/// </remarks>
public sealed partial class RepoContextLockAttributingGrainStorageTests
{
    private static readonly TimeSpan ShortRequestBudget = TimeSpan.FromSeconds(2);

    private string _root = null!;
    private string _dbPath = null!;

    [SetUp]
    public void SetUpDatabase()
    {
        _root = Path.Combine(Path.GetTempPath(), "repocontext-lock-" + Guid.NewGuid().ToString("N"));
        _dbPath = Path.Combine(_root, "repocontext.db");
        new SqliteSchemaInitializer(_dbPath).Initialize();
    }

    [TearDown]
    public void TearDownDatabase()
    {
        SqliteConnection.ClearAllPools();
        if (Directory.Exists(_root))
        {
            Directory.Delete(_root, recursive: true);
        }
    }

    [Test]
    public async Task A_provoked_write_convoy_is_attributed_write_by_write_against_a_held_write_lock()
    {
        var logger = new RecordingLogger();
        using var meter = new RepoContextGrainStorageLockMeter();
        using var recorder = new MeterRecorder(meter);
        var storage = await StartOverRealProviderAsync(meter, logger);
        var grains = new[] { Grain("convoy-1"), Grain("convoy-2"), Grain("convoy-3") };

        List<Exception> failures;
        using (var holder = HoldWriteLock())
        {
            var writes = grains
                .Select(grain => Task.Run(() => storage.WriteStateAsync(
                    "leaf", grain, new GrainState<string> { State = "payload" })))
                .ToArray();

            failures = [];
            foreach (var write in writes)
            {
                try
                {
                    await write;
                }
                catch (Exception failure)
                {
                    failures.Add(failure);
                }
            }
        }

        var entries = logger.Entries;
        Assert.Multiple(() =>
        {
            Assert.That(failures, Has.Count.EqualTo(3),
                "Control: the held lock must actually have failed every queued write, otherwise "
                + "nothing here was contended and the attribution below is vacuous.");
            Assert.That(failures, Has.All.InstanceOf<SqliteException>(),
                "Orleans' ADO.NET provider rethrows Microsoft.Data.Sqlite's exception unwrapped. "
                + "If that changes the classifier's cause-chain walk still finds it, but this is "
                + "the shape the deployed container sees today.");
            Assert.That(entries, Has.Count.EqualTo(3), "One attribution line per failed write.");
            Assert.That(entries.Select(e => e["GrainId"]), Is.EquivalentTo(grains.Select(g => g.ToString())),
                "Every line names the grain whose own write failed.");
            Assert.That(entries.Select(e => e["SqliteErrorCode"]), Has.All.EqualTo(SqliteLockClassifier.SqliteBusy));
            Assert.That(entries.Select(e => e["BusyWindowMs"]), Has.All.EqualTo(1_000L),
                "The window is read from the provider's own connection string.");
            Assert.That(entries.Select(e => (long)e["ElapsedMs"]!), Has.All.GreaterThanOrEqualTo(1_000L));
            Assert.That(entries.Select(e => e["Wait"]), Has.All.EqualTo(RepoContextGrainStorageLockMeter.WaitExhausted),
                "A writer queued behind a held lock waits the whole window out. This is the "
                + "convoy-exhaustion signature the issue hypothesised and never measured.");
            Assert.That(recorder.Sum(
                    RepoContextGrainStorageLockMeter.LockFailuresCounterName,
                    (RepoContextGrainStorageLockMeter.OperationTag, "write"),
                    (RepoContextGrainStorageLockMeter.WaitTag, RepoContextGrainStorageLockMeter.WaitExhausted)),
                Is.EqualTo(3d));
            Assert.That(meter.Convoy.WritesInFlight, Is.Zero);
        });

        await storage.WriteStateAsync("leaf", grains[0], new GrainState<string> { State = "payload" });

        Assert.Multiple(() =>
        {
            Assert.That(logger.Entries, Has.Count.EqualTo(3),
                "Control: once the lock is released the same write succeeds and is not attributed, "
                + "so the lines above were caused by the held lock and not by the decorator.");
            Assert.That(StoredRowCount(), Is.EqualTo(1L));
        });
    }

    [Test]
    public async Task The_real_provider_initialises_through_the_decorator_lifecycle()
    {
        using var meter = new RepoContextGrainStorageLockMeter();
        var storage = await StartOverRealProviderAsync(meter, new RecordingLogger());

        await storage.WriteStateAsync("leaf", Grain("init"), new GrainState<string> { State = "v1" });

        Assert.That(StoredRowCount(), Is.EqualTo(1L),
            "The provider loads its query catalogue in its lifecycle stage, which it now reaches "
            + "only through the decorator's forwarded Participate. A write landing proves it did.");
    }

    private async Task<RepoContextLockAttributingGrainStorage> StartOverRealProviderAsync(
        RepoContextGrainStorageLockMeter meter, RecordingLogger logger)
    {
        DurabilitySelector.RegisterAdoNetFactories();
        var connectionString = SqliteSchemaInitializer.BuildConnectionString(_dbPath, ShortRequestBudget);
        var options = new AdoNetGrainStorageOptions
        {
            Invariant = SqliteSchemaInitializer.InvariantName,
            ConnectionString = connectionString,
            GrainStorageSerializer = new Utf8StringSerializer(),
            DeleteStateOnClear = true,
        };
        var provider = new AdoNetGrainStorage(
            Substitute.For<IActivatorProvider>(),
            NullLogger<AdoNetGrainStorage>.Instance,
            Options.Create(options),
            Options.Create(new ClusterOptions { ClusterId = "repo-context", ServiceId = "repo-context" }),
            LatticeOptions.StorageProviderName);
        var storage = new RepoContextLockAttributingGrainStorage(
            provider,
            meter,
            logger,
            TimeSpan.FromSeconds(new SqliteConnectionStringBuilder(connectionString).DefaultTimeout));

        ILifecycleObserver? observer = null;
        var lifecycle = Substitute.For<ISiloLifecycle>();
        lifecycle
            .Subscribe(Arg.Any<string>(), Arg.Any<int>(), Arg.Do<ILifecycleObserver>(o => observer = o))
            .Returns(Substitute.For<IDisposable>());
        storage.Participate(lifecycle);

        Assert.That(observer, Is.Not.Null, "The decorator must forward Participate to the provider.");
        await observer!.OnStart(CancellationToken.None);
        return storage;
    }

    private IDisposable HoldWriteLock()
    {
        // Unpooled, and rolled back explicitly before it closes: Microsoft.Data.Sqlite's
        // pool does not roll back a transaction opened in SQL text, so a pooled holder
        // would hand the storage a connection still inside BEGIN IMMEDIATE.
        var holder = new SqliteConnection(new SqliteConnectionStringBuilder
        {
            DataSource = _dbPath,
            Pooling = false,
        }.ToString());
        holder.Open();
        Execute(holder, "BEGIN IMMEDIATE;");
        return new LockRelease(holder);
    }

    private static void Execute(SqliteConnection connection, string sql)
    {
        using var command = connection.CreateCommand();
        command.CommandText = sql;
        command.ExecuteNonQuery();
    }

    private sealed class LockRelease(SqliteConnection holder) : IDisposable
    {
        public void Dispose()
        {
            Execute(holder, "ROLLBACK;");
            holder.Dispose();
        }
    }

    private long StoredRowCount()
    {
        using var connection = new SqliteConnection(SqliteSchemaInitializer.BuildConnectionString(_dbPath));
        connection.Open();
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT COUNT(*) FROM OrleansStorage;";
        return Convert.ToInt64(command.ExecuteScalar());
    }

    private sealed class Utf8StringSerializer : IGrainStorageSerializer
    {
        public BinaryData Serialize<T>(T input) => BinaryData.FromString((string)(object)input!);

        public T Deserialize<T>(BinaryData input) => (T)(object)input.ToString();
    }
}
