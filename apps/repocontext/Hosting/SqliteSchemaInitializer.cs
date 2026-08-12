using System.Reflection;
using Microsoft.Data.Sqlite;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Owns and applies the SQLite ADO.NET invariant schema (the Orleans query,
/// grain-storage, and reminders tables) for the <see cref="DurabilityProfile.Local"/>
/// and any SQLite-backed profile. The three scripts are embedded in this
/// assembly; the initializer applies them idempotently against the single
/// database file on the mounted data root, having first proven the data path is
/// writable by the non-root runtime UID (fail-fast).
/// </summary>
/// <remarks>
/// The database is opened in <c>WAL</c> journal mode (persisted on the file) with
/// a per-connection busy timeout, matching the connection string
/// <see cref="BuildConnectionString"/> hands to Orleans' grain storage and
/// reminder providers, so a single writer and Orleans' pooled readers never
/// collide with an immediate <c>SQLITE_BUSY</c>.
/// </remarks>
public sealed class SqliteSchemaInitializer
{
    /// <summary>
    /// The Orleans ADO.NET invariant name Orleans' <c>DbConstantsStore</c>
    /// recognizes for SQLite. The connection factory registered under this
    /// invariant is the modern <c>Microsoft.Data.Sqlite</c> provider (see
    /// <see cref="DurabilitySelector.RegisterAdoNetFactories"/>); Orleans keys its
    /// SQL-dialect constants by this string, so it must match one of Orleans'
    /// known invariants rather than the concrete provider's assembly name.
    /// </summary>
    public const string InvariantName = "System.Data.SQLite";

    private const int BusyTimeoutSeconds = 30;

    private static readonly string[] ScriptResourceNames =
    {
        "Persistence.Sqlite.SQLite-Main.sql",
        "Persistence.Sqlite.SQLite-Persistence.sql",
        "Persistence.Sqlite.SQLite-Reminders.sql",
    };

    private readonly string _databasePath;

    /// <summary>Creates an initializer for the SQLite database at <paramref name="databasePath"/>.</summary>
    /// <param name="databasePath">The absolute path to the SQLite database file on the data root.</param>
    /// <exception cref="ArgumentException"><paramref name="databasePath"/> is null or whitespace.</exception>
    public SqliteSchemaInitializer(string databasePath)
    {
        if (string.IsNullOrWhiteSpace(databasePath))
        {
            throw new ArgumentException("The SQLite database path must not be empty.", nameof(databasePath));
        }

        _databasePath = databasePath;
    }

    /// <summary>
    /// Builds the shared SQLite connection string used by both this initializer
    /// and the Orleans ADO.NET providers: the file data source plus a command
    /// timeout that Microsoft.Data.Sqlite honours as a busy-retry window, so
    /// Orleans' pooled connections wait out a transient lock rather than failing.
    /// </summary>
    /// <param name="databasePath">The SQLite database file path.</param>
    /// <returns>The connection string.</returns>
    public static string BuildConnectionString(string databasePath)
        => new SqliteConnectionStringBuilder
        {
            DataSource = databasePath,
            DefaultTimeout = BusyTimeoutSeconds,
            Pooling = true,
        }.ToString();

    /// <summary>
    /// Ensures the database directory exists and is writable, then applies the
    /// schema scripts once (idempotent: a second run against a populated file is a
    /// no-op). Configures <c>WAL</c> journal mode on first creation.
    /// </summary>
    /// <exception cref="InvalidOperationException">The data path is missing or not writable.</exception>
    public void Initialize()
    {
        EnsureWritableDirectory();

        using var connection = new SqliteConnection(BuildConnectionString(_databasePath));
        connection.Open();

        ExecutePragmas(connection);

        if (SchemaAlreadyApplied(connection))
        {
            return;
        }

        using var transaction = connection.BeginTransaction();
        foreach (var script in LoadScripts())
        {
            using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = script;
            command.ExecuteNonQuery();
        }

        transaction.Commit();
    }

    private void EnsureWritableDirectory()
    {
        var directory = Path.GetDirectoryName(Path.GetFullPath(_databasePath));
        if (string.IsNullOrEmpty(directory))
        {
            throw new InvalidOperationException(
                $"Could not resolve a directory for the SQLite database path '{_databasePath}'.");
        }

        DataPathGuard.EnsureDirectoryWritable(directory, "SQLite data");
    }

    private static void ExecutePragmas(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText =
            $"PRAGMA journal_mode=WAL; PRAGMA synchronous=NORMAL; PRAGMA busy_timeout={BusyTimeoutSeconds * 1000};";
        command.ExecuteNonQuery();
    }

    private static bool SchemaAlreadyApplied(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText =
            "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='OrleansQuery';";
        var count = Convert.ToInt64(command.ExecuteScalar());
        return count > 0;
    }

    private static IEnumerable<string> LoadScripts()
    {
        var assembly = typeof(SqliteSchemaInitializer).Assembly;
        foreach (var suffix in ScriptResourceNames)
        {
            yield return ReadResource(assembly, suffix);
        }
    }

    private static string ReadResource(Assembly assembly, string suffix)
    {
        var name = assembly.GetManifestResourceNames()
            .FirstOrDefault(n => n.EndsWith(suffix, StringComparison.Ordinal))
            ?? throw new InvalidOperationException(
                $"Embedded SQLite script '{suffix}' was not found in the host assembly.");

        using var stream = assembly.GetManifestResourceStream(name)
            ?? throw new InvalidOperationException($"Embedded SQLite script '{name}' could not be opened.");
        using var reader = new StreamReader(stream);
        return reader.ReadToEnd();
    }
}
