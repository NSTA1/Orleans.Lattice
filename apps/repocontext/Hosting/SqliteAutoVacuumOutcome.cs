namespace Orleans.Lattice.Api.Mcp.RepoContext.Host;

/// <summary>
/// What <see cref="SqliteSchemaInitializer.Initialize"/> found and did about the
/// database's <c>auto_vacuum</c> mode on this start.
/// </summary>
/// <param name="Requested">The mode the host was configured to apply.</param>
/// <param name="Previous">The mode the database file carried before this start.</param>
/// <param name="Converted">
/// <see langword="true"/> when an existing database had to be rewritten with a one-time
/// <c>VACUUM</c> to adopt <paramref name="Requested"/>.
/// </param>
/// <param name="BytesBefore">The database file size before any conversion.</param>
/// <param name="BytesAfter">The database file size after any conversion.</param>
/// <param name="Elapsed">How long the conversion took; zero when none ran.</param>
public sealed record SqliteAutoVacuumOutcome(
    SqliteAutoVacuumMode Requested,
    SqliteAutoVacuumMode Previous,
    bool Converted,
    long BytesBefore,
    long BytesAfter,
    TimeSpan Elapsed);
