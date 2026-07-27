namespace Orleans.Lattice.Api.Mcp;

/// <summary>The mutation a Sequence (RGA) write tool applies.</summary>
public enum CrdtSequenceOp
{
    /// <summary>Insert the value at the given position for the writer.</summary>
    InsertAt,

    /// <summary>Tombstone the element at the given position.</summary>
    RemoveAt,
}
