namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// The durable record that one grain is enrolled in one index: the "seen"
/// marker the backfill reads, carrying the projection the index is known to
/// hold for that grain.
/// </summary>
/// <remarks>
/// <para>
/// The marker and the confirmed projection are deliberately the same record.
/// Splitting them would double the registry writes on the mutation path, and
/// they are written at exactly the same moment anyway: a grain is enrolled
/// precisely when its entries have landed, and the entries that landed are what
/// the next diff must be taken against.
/// </para>
/// <para>
/// Carrying the projection is what makes re-activation free. Without it an
/// activating grain would have to assume it had contributed nothing, re-write
/// every entry it already owns, and tombstone nothing - correct, but a write
/// per property per activation. With it an unchanged grain re-projects to an
/// empty plan and touches no tree at all.
/// </para>
/// </remarks>
[GenerateSerializer]
[Immutable]
[Alias(TypeAliases.GrainIndexEnrollmentRecord)]
internal sealed class GrainIndexEnrollmentRecord
{
    /// <summary>Initialises a record.</summary>
    /// <param name="projection">The projection the index is known to hold. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="projection"/> is <c>null</c>.</exception>
    public GrainIndexEnrollmentRecord(GrainIndexProjection projection)
    {
        ArgumentNullException.ThrowIfNull(projection);
        Projection = projection;
    }

    /// <summary>
    /// The entries the index holds for this grain as of the last confirmed
    /// write. It is the baseline the next projection diffs against.
    /// </summary>
    [Id(0)] public GrainIndexProjection Projection { get; }
}
