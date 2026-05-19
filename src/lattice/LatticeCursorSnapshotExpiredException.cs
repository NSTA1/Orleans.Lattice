namespace Orleans.Lattice;

/// <summary>
/// Thrown by a <see cref="LatticeCursorSpec.PointInTime"/> cursor's
/// <c>Next*Async</c> / <c>DeleteRangeStepAsync</c> when the registry-side
/// pin securing the cursor's saga-decision snapshot has expired without a
/// refresh. The pin's hard cap is
/// <see cref="LatticeOptions.MaxCursorSnapshotPinTtl"/>; every successful
/// step refreshes it, so a cursor only sees this exception when it has
/// been idle past the cap (or its reminder fired late). Callers should
/// open a fresh cursor; the existing cursor's persisted state is left
/// intact and a subsequent <c>CloseAsync</c> still cleans it up.
/// </summary>
public sealed class LatticeCursorSnapshotExpiredException : InvalidOperationException
{
    /// <summary>
    /// Initialises a new instance with the specified message.
    /// </summary>
    public LatticeCursorSnapshotExpiredException(string message) : base(message) { }

    /// <summary>
    /// Initialises a new instance with the specified message and inner
    /// exception.
    /// </summary>
    public LatticeCursorSnapshotExpiredException(string message, Exception innerException) : base(message, innerException) { }
}