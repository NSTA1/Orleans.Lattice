namespace Orleans.Lattice;

/// <summary>
/// Thrown by a zero-observable-writes snapshot cursor's
/// <c>Next*Async</c> when the WAL prefix the snapshot's replay
/// depends on has been trimmed past the captured offset. The WAL
/// retention pin (held via <see cref="IWalCursorRegistry"/>) is
/// refreshed on every successful step, so a cursor only sees this
/// exception when it has been idle past
/// <see cref="LatticeOptions.MaxCursorSnapshotPinTtl"/> or an
/// operator forced a trim. Callers should open a fresh snapshot
/// cursor against a current <see cref="LatticeSnapshotCoordinate"/>;
/// the existing cursor's persisted state is left intact and a
/// subsequent <c>CloseAsync</c> still cleans it up.
/// </summary>
public sealed class LatticeSnapshotExpiredException : InvalidOperationException
{
    /// <summary>
    /// Initialises a new instance with the specified message.
    /// </summary>
    public LatticeSnapshotExpiredException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified message and inner
    /// exception.
    /// </summary>
    public LatticeSnapshotExpiredException(string message, Exception innerException) : base(message, innerException) { }
}
