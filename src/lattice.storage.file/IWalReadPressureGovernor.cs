namespace Orleans.Lattice.Storage.File;

/// <summary>
/// The seam through which <see cref="FileWalShard"/> asks "how much can this
/// process afford to read right now?" and obtains the buffers to read into.
/// <para>
/// It exists so the read path's behaviour under memory pressure is a
/// <b>deterministic, injectable</b> property rather than something that can
/// only be observed by actually exhausting a heap. The production
/// implementation (<see cref="GcWalReadPressureGovernor"/>) reads the garbage
/// collector's own view of occupancy; a test supplies a scripted one and gets
/// the same code path with none of the timing or GC dependence.
/// </para>
/// </summary>
internal interface IWalReadPressureGovernor
{
    /// <summary>
    /// Narrows a configured per-read byte ceiling to what the process can
    /// currently afford. Never widens it, and never returns less than one
    /// byte. Implementations must not allocate.
    /// </summary>
    /// <param name="configuredMaxBytes">The configured ceiling, at least 1.</param>
    long NarrowBudget(long configuredMaxBytes);

    /// <summary>
    /// Allocates a read buffer of exactly <paramref name="byteCount"/> bytes,
    /// throwing <see cref="OutOfMemoryException"/> when it cannot be
    /// satisfied.
    /// </summary>
    byte[] Allocate(int byteCount);
}
