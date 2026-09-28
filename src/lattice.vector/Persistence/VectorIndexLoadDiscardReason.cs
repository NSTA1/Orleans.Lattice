namespace Orleans.Lattice.Vector.Persistence;

/// <summary>Why a full load discarded derived durable index state.</summary>
public enum VectorIndexLoadDiscardReason
{
    /// <summary>No durable state was discarded.</summary>
    None = 0,

    /// <summary>A required record was absent, corrupt, or in an unsupported format.</summary>
    UnloadableRecord = 1,

    /// <summary>The restored vector or identifier count contradicted the committed count.</summary>
    CountMismatch = 2,

    /// <summary>The stored dimensionality or distance metric differed from the configured space.</summary>
    EmbeddingSpaceChange = 3,
}
