namespace Orleans.Lattice.Api.Mcp;

/// <summary>The mutation a flag write tool applies to an OR-Flag or RW-Flag.</summary>
public enum CrdtFlagOp
{
    /// <summary>Set the flag on for the writer.</summary>
    Enable,

    /// <summary>Set the flag off for the writer.</summary>
    Disable,
}
