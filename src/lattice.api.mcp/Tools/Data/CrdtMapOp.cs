namespace Orleans.Lattice.Api.Mcp;

/// <summary>The mutation an OR-Map write tool applies.</summary>
public enum CrdtMapOp
{
    /// <summary>Put the value under the field for the writer (recursive per-field merge).</summary>
    Set,

    /// <summary>Observed-remove the field.</summary>
    Remove,
}
