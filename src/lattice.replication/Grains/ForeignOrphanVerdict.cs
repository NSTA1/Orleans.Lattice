namespace Orleans.Lattice.Replication.Grains;

/// <summary>What the third-origin reconcile does with a receiver row the export lacks (issue #4549).</summary>
internal enum ForeignOrphanVerdict
{
    /// <summary>Held at the source, so the export's silence proves nothing and it is kept.</summary>
    Keep,

    /// <summary>The source applied the write before the export opened, so it deleted the key.</summary>
    Delete,

    /// <summary>At or above the origin's watermark: kept, and the reconcile owes a retry.</summary>
    Owed,
}
