namespace Orleans.Lattice.Operations;

/// <summary>
/// The progress sink a coordinated operation reports through: the current phase
/// and the whole units completed within it. Engines receive it explicitly from
/// <see cref="LatticeOperationRunner"/>, or read it ambiently from
/// <see cref="LatticeOperationProgress.Current"/> when they sit behind an
/// interface that cannot grow a parameter.
/// </summary>
internal interface ILatticeOperationProgress
{
    /// <summary>
    /// Reports progress. Reports are coalesced, so calling this per unit is cheap;
    /// a phase change, a changed total and the final unit are always written
    /// through. Throws <see cref="OperationCanceledException"/> once cancellation
    /// of the operation has been requested, so a report point is also a prompt
    /// cancellation point.
    /// </summary>
    /// <param name="phase">The phase name. Must not be <c>null</c> or empty.</param>
    /// <param name="completedUnits">Units of the phase completed.</param>
    /// <param name="totalUnits">The phase total, or <see langword="null"/> when unknown.</param>
    /// <param name="unitName">What the units count, or <see langword="null"/>.</param>
    /// <returns>A task that completes when the report has been accepted.</returns>
    ValueTask ReportAsync(string phase, long completedUnits = 0, long? totalUnits = null, string? unitName = null);
}
