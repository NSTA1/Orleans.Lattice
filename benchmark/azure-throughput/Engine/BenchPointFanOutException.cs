namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Thrown by the per-entry point modes when some, but not necessarily all, of
/// a flush unit's entries failed. Each point entry is its own
/// <c>ILattice</c> call with its own outcome, so the ingest engine books the
/// entries that landed as written and retries or fails only
/// <see cref="FailedEntries"/>, instead of booking the whole unit as failed
/// because one call in it threw.
/// </summary>
public sealed class BenchPointFanOutException : Exception
{
    /// <summary>
    /// Creates the exception.
    /// </summary>
    /// <param name="succeeded">How many entries of the unit completed.</param>
    /// <param name="failedEntries">The entries whose call failed, in unit order.</param>
    /// <param name="firstFailure">The first failure observed, which classifies the retry.</param>
    public BenchPointFanOutException(
        int succeeded,
        IReadOnlyList<KeyValuePair<string, byte[]>> failedEntries,
        Exception firstFailure)
        : base($"{failedEntries?.Count ?? 0} point call(s) of the flush unit failed; {succeeded} completed.", firstFailure)
    {
        ArgumentNullException.ThrowIfNull(failedEntries);
        ArgumentNullException.ThrowIfNull(firstFailure);
        Succeeded = succeeded;
        FailedEntries = failedEntries;
    }

    /// <summary>How many entries of the unit completed.</summary>
    public int Succeeded { get; }

    /// <summary>The entries whose call failed, in unit order.</summary>
    public IReadOnlyList<KeyValuePair<string, byte[]>> FailedEntries { get; }
}
