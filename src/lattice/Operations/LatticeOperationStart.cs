namespace Orleans.Lattice.Operations;

/// <summary>What <see cref="LatticeOperationRunner.StartAsync"/> starts.</summary>
internal sealed record LatticeOperationStart
{
    /// <summary>The tenant that owns the operation.</summary>
    public required string TenantId { get; init; }

    /// <summary>The operation id; validated by <see cref="LatticeOperationKey.ThrowIfInvalid"/>.</summary>
    public required string OperationId { get; init; }

    /// <summary>The operation kind.</summary>
    public required string Kind { get; init; }

    /// <summary>The effective trees the operation targets.</summary>
    public IReadOnlyList<string> TreeIds { get; init; } = [];

    /// <summary>The ordered phase names the operation reports, or empty when undeclared.</summary>
    public IReadOnlyList<string> Phases { get; init; } = [];

    /// <summary>Opaque, client-defined attributes recorded with the operation (see <see cref="LatticeOperationRecord.Attributes"/>).</summary>
    public IReadOnlyDictionary<string, string> Attributes { get; init; } = LatticeOperationRecord.EmptyResult;
}
