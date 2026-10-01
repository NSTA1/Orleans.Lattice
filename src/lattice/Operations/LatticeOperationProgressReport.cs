namespace Orleans.Lattice.Operations;

/// <summary>A progress report from a runner to an operation's tracking grain.</summary>
/// <param name="Phase">The phase name.</param>
/// <param name="CompletedUnits">Units of the phase completed.</param>
/// <param name="TotalUnits">The phase total, or <see langword="null"/> when unknown.</param>
/// <param name="UnitName">What the units count, or <see langword="null"/>.</param>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationProgressReport)]
[Immutable]
internal readonly record struct LatticeOperationProgressReport(
    [property: Id(0)] string Phase,
    [property: Id(1)] long CompletedUnits,
    [property: Id(2)] long? TotalUnits,
    [property: Id(3)] string? UnitName);
