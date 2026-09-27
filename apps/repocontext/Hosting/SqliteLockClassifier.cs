using Microsoft.Data.Sqlite;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// Recognises a SQLite lock failure - <c>SQLITE_BUSY</c> or <c>SQLITE_LOCKED</c>,
/// surfaced by Microsoft.Data.Sqlite as "database is locked" - anywhere in an
/// exception's cause chain.
/// </summary>
/// <remarks>
/// Orleans' ADO.NET grain storage rethrows the provider exception unwrapped today,
/// but the walk follows inner exceptions and every branch of an
/// <see cref="AggregateException"/> so that a wrapping layer added later does not
/// silently turn every lock failure into an unattributed one. The walk is bounded,
/// so a cyclic or pathologically deep chain cannot hang the failure path.
/// </remarks>
public static class SqliteLockClassifier
{
    /// <summary>The SQLite primary result code for <c>SQLITE_BUSY</c>.</summary>
    public const int SqliteBusy = 5;

    /// <summary>The SQLite primary result code for <c>SQLITE_LOCKED</c>.</summary>
    public const int SqliteLocked = 6;

    private const int MaxDepth = 16;

    /// <summary>
    /// Finds the first <see cref="SqliteException"/> in <paramref name="exception"/>'s
    /// cause chain whose primary result code is <see cref="SqliteBusy"/> or
    /// <see cref="SqliteLocked"/>.
    /// </summary>
    /// <param name="exception">The failure to classify. May be null.</param>
    /// <param name="lockFailure">The lock failure found, or null.</param>
    /// <returns><see langword="true"/> when a lock failure was found.</returns>
    public static bool TryFind(Exception? exception, out SqliteException? lockFailure)
    {
        lockFailure = Find(exception, 0);
        return lockFailure is not null;
    }

    private static SqliteException? Find(Exception? exception, int depth)
    {
        if (exception is null || depth >= MaxDepth)
        {
            return null;
        }

        if (exception is SqliteException sqlite
            && sqlite.SqliteErrorCode is SqliteBusy or SqliteLocked)
        {
            return sqlite;
        }

        if (exception is AggregateException aggregate)
        {
            foreach (var inner in aggregate.InnerExceptions)
            {
                var found = Find(inner, depth + 1);
                if (found is not null)
                {
                    return found;
                }
            }

            return null;
        }

        return Find(exception.InnerException, depth + 1);
    }
}
