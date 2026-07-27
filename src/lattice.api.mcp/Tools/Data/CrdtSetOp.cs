namespace Orleans.Lattice.Api.Mcp;

/// <summary>The mutation an OR-Set write tool applies.</summary>
public enum CrdtSetOp
{
    /// <summary>Add the element to the set for the writer (add-wins on a concurrent remove).</summary>
    Add,

    /// <summary>Observed-remove the element (only the adds this writer has seen are removed).</summary>
    Remove,
}
