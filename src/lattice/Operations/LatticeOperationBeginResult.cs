namespace Orleans.Lattice.Operations;

/// <summary>The answer to <see cref="ILatticeOperationGrain.BeginAsync"/>.</summary>
/// <param name="Created">
/// <see langword="true"/> when this call created the operation;
/// <see langword="false"/> when one with the same id already existed and is
/// returned unchanged (an idempotent start).
/// </param>
/// <param name="Record">The operation's record.</param>
[GenerateSerializer]
[Alias(TypeAliases.LatticeOperationBeginResult)]
[Immutable]
internal sealed record LatticeOperationBeginResult(
    [property: Id(0)] bool Created,
    [property: Id(1)] LatticeOperationRecord Record);
