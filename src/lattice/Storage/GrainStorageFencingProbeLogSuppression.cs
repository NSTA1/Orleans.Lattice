using Microsoft.Extensions.Logging;

namespace Orleans.Lattice;

/// <summary>
/// Marks the asynchronous flow of a running <see cref="GrainStorageFencingProbe"/>
/// so that <see cref="GrainStorageFencingProbeLogFilter"/> can drop the Warning,
/// Error and Critical records the grain storage provider writes while it serves
/// the probe (issue #5442).
/// </summary>
/// <remarks>
/// <para>
/// The probe makes the provider reject a write on purpose, and providers such as
/// Orleans' Azure Table storage log every rejected write at Error before they
/// throw. Lattice catches the exception and classifies it, so the record would
/// otherwise read as a fault on every healthy silo start.
/// </para>
/// <para>
/// The mark is an <see cref="AsyncLocal{T}"/>, so it covers only the probe's own
/// calls into the provider: records from any other work in the silo are never
/// dropped. It is also bounded in time: <see cref="Scope.Dispose"/> closes it, so
/// work the provider started on the probe's flow and that outlives the probe is
/// no longer covered. Nothing a dropped record could report is lost: every
/// outcome of the probe's storage calls becomes a verdict that
/// <see cref="GrainStorageFencingCheck"/> logs itself, a provider that accepts
/// the stale write is reported as unfenced, and a provider fault is reported as
/// inconclusive together with the exception.
/// </para>
/// </remarks>
internal static class GrainStorageFencingProbeLogSuppression
{
    private static readonly AsyncLocal<Scope?> Current = new();

    // Open-scope count, read before the AsyncLocal so a log call made while no
    // probe runs (every call after silo start) costs one volatile read.
    private static int _openScopes;

    /// <summary>
    /// Opens a suppression scope on the calling asynchronous flow. Dispose it when
    /// the probe finishes.
    /// </summary>
    internal static Scope Enter()
    {
        var scope = new Scope(Current.Value);
        Interlocked.Increment(ref _openScopes);
        Current.Value = scope;
        return scope;
    }

    /// <summary>
    /// Whether a record at <paramref name="level"/> is dropped: it is Warning or
    /// above and is being written on the flow of a probe that is still running.
    /// </summary>
    internal static bool ShouldSuppress(LogLevel level)
        => level is >= LogLevel.Warning and < LogLevel.None
            && Volatile.Read(ref _openScopes) > 0
            && Current.Value is { IsOpen: true };

    /// <summary>
    /// A suppression scope opened by <see cref="Enter"/>.
    /// </summary>
    internal sealed class Scope : IDisposable
    {
        private readonly Scope? _previous;
        private int _open = 1;

        internal Scope(Scope? previous) => _previous = previous;

        /// <summary>Whether the scope has not been disposed yet.</summary>
        internal bool IsOpen => Volatile.Read(ref _open) == 1;

        /// <summary>
        /// Closes the scope for every flow that captured it and restores the
        /// enclosing scope on the calling flow. Idempotent.
        /// </summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _open, 0) == 0)
            {
                return;
            }

            Interlocked.Decrement(ref _openScopes);
            if (ReferenceEquals(Current.Value, this))
            {
                Current.Value = _previous;
            }
        }
    }
}
