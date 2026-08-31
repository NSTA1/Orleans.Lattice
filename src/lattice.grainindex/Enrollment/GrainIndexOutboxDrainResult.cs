namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// What one pass of the pending-projection outbox drain did.
/// </summary>
/// <param name="Scanned">The outstanding entries the pass looked at.</param>
/// <param name="Applied">The entries whose index batch landed and was confirmed.</param>
/// <param name="Failed">
/// The entries that could not be applied and were left in place for a later
/// pass. A failing entry never blocks the ones behind it.
/// </param>
/// <param name="Skipped">
/// The entries belonging to an index this silo does not declare. They are left
/// untouched for a silo that does, rather than discarded.
/// </param>
internal readonly record struct GrainIndexOutboxDrainResult(
    int Scanned,
    int Applied,
    int Failed,
    int Skipped)
{
    /// <summary>Whether the pass found nothing to do.</summary>
    public bool IsEmpty => Scanned == 0;
}
