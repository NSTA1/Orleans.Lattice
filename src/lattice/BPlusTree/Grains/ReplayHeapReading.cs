namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// One heap occupancy reading taken by <see cref="ReplayHeapPressure"/>: how much
/// managed memory the process is currently holding, and the ceiling above which
/// the runtime will throw rather than grow (issue #2862).
/// <para>
/// The two figures travel together because a judgement about either alone is
/// meaningless. An occupancy of 7 GiB is comfortable under a 32 GiB ceiling and
/// terminal under a 9 GiB one, and the deployment that raised this issue had a
/// ceiling of exactly 9 GiB - 75% of its 12 GiB container grant - while nothing
/// in the replay path knew that.
/// </para>
/// </summary>
/// <param name="InUseBytes">
/// Approximate managed bytes currently allocated, as
/// <see cref="GC.GetTotalMemory(bool)"/> reports them without forcing a
/// collection.
/// </param>
/// <param name="CeilingBytes">
/// The resolved heap ceiling in bytes, or a non-positive value when no ceiling is
/// known to this process. <b>Non-positive means unknown, never zero</b>: a reader
/// that treated it as a ceiling of zero would judge every process to be
/// catastrophically over its limit.
/// </param>
internal readonly record struct ReplayHeapReading(long InUseBytes, long CeilingBytes);
