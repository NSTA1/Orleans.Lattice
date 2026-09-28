namespace Orleans.Lattice;

/// <summary>The allocation-free default for hosts without an ownership provider.</summary>
internal sealed class NullTreeOwnershipGuard : ITreeOwnershipGuard
{
    internal static readonly NullTreeOwnershipGuard Instance = new();

    public ValueTask<TreeOwnershipDecision> AuthorizeAliasAsync(
        string logicalTreeId,
        string physicalTreeId,
        string? derivedFrom,
        CancellationToken cancellationToken = default)
        => new(TreeOwnershipDecision.Allow());
}
