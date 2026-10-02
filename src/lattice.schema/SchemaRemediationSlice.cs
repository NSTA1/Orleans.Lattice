namespace Orleans.Lattice.Schema;

/// <summary>
/// What one <see cref="ILatticeSchemaRemediationGrain.RunSliceAsync"/> call left
/// behind: the remediation's status after the slice, and how many values the
/// current phase will process in all, when that is known.
/// </summary>
/// <param name="Report">The status after the slice.</param>
/// <param name="PhaseTotal">
/// The current phase's total: <c>null</c> during the dry run, whose total is not
/// known until it ends, and the dry run's count during the build.
/// </param>
[GenerateSerializer]
[Alias(SchemaTypeAliases.SchemaRemediationSlice)]
[Immutable]
internal readonly record struct SchemaRemediationSlice(
    [property: Id(0)] LatticeSchemaRemediationReport Report,
    [property: Id(1)] int? PhaseTotal);
