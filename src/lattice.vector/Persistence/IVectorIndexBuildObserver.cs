namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// Receives per-slice stage timings from a <see cref="DurableVectorIndex"/>
/// build.
/// <para>
/// <b>This is an observer seam rather than an instrument because
/// <c>Orleans.Lattice.Vector</c> publishes no metrics at all.</b> It declares no
/// <c>Meter</c> and no instruments, and it should stay that way: it is a library
/// consumed by hosts that own their own meters, so emitting from here would put
/// a metrics class in a package whose consumers have not asked for one and would
/// fix the instrument names at the wrong layer. The consuming package implements
/// this and publishes on its own meter.
/// </para>
/// <para>
/// Implementations are called on the build path and must not throw, block, or
/// retain the timings.
/// </para>
/// </summary>
public interface IVectorIndexBuildObserver
{
    /// <summary>
    /// Called after a successful ingest slice has written its checkpoint. A slice
    /// that faults is reported by the fault path and does not call the observer.
    /// </summary>
    /// <param name="timings">Where that slice's time went.</param>
    void OnSliceCompleted(in VectorIndexBuildSliceTimings timings);
}
