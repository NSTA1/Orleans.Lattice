namespace Orleans.Lattice.Operations;

/// <summary>The arguments of <see cref="ILatticeOperationGrain.BeginAsync"/>.</summary>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationBeginRequest)]
[Immutable]
internal sealed record LatticeOperationBeginRequest
{
    /// <summary>The operation kind.</summary>
    [Id(0)] public required string Kind { get; init; }

    /// <summary>The effective trees the operation targets.</summary>
    [Id(1)] public IReadOnlyList<string> TreeIds { get; init; } = [];

    /// <summary>The ordered phase names the operation will report, or empty when undeclared.</summary>
    [Id(2)] public IReadOnlyList<string> Phases { get; init; } = [];

    /// <summary>The silo whose runner executes the operation.</summary>
    [Id(3)] public SiloAddress? RunnerSilo { get; init; }

    /// <summary>Opaque, client-defined attributes recorded with the operation.</summary>
    [Id(4)] public IReadOnlyDictionary<string, string> Attributes { get; init; } = LatticeOperationRecord.EmptyResult;
}
