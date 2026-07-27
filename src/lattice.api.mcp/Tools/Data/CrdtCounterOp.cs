namespace Orleans.Lattice.Api.Mcp;

/// <summary>The mutation a PN-counter write tool applies.</summary>
public enum CrdtCounterOp
{
    /// <summary>Add <c>amount</c> to the counter for the writer.</summary>
    Increment,

    /// <summary>Subtract <c>amount</c> from the counter for the writer.</summary>
    Decrement,
}
