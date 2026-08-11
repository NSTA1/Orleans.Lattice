namespace Orleans.Lattice.Api.Mcp;

/// <summary>The mutation a remove-wins (RW) set write tool applies.</summary>
public enum CrdtRwSetOp
{
    /// <summary>Add the element to the set for the writer (a concurrent unobserved remove still wins).</summary>
    Add,

    /// <summary>Remove-wins remove of the element for the writer (dominates a concurrent unobserved add).</summary>
    Remove,
}
