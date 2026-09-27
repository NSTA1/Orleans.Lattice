namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The role a piece of text plays when it is embedded, so an
/// <see cref="IEmbeddingProvider"/> backed by an asymmetric embedding model can
/// apply the matching query/passage prefix and produce vectors in the matching
/// sub-space; for such a model, mixing the two roles silently degrades recall.
/// The shipped Onyx provider forwards the role on the wire, but neither the Onyx
/// model server nor the bundled ONNX embedding server applies a prefix for it, so
/// its passage and query vectors share one encoding.
/// </summary>
public enum EmbeddingTextType
{
    /// <summary>
    /// A stored document chunk that will be indexed and later retrieved. Use this
    /// role when vectorising repository content during bootstrap so the vector is
    /// stamped for the passage side of the model.
    /// </summary>
    Passage = 0,

    /// <summary>
    /// A search query embedded to retrieve matching passages. Use this role when
    /// turning a semantic search request into a query vector.
    /// </summary>
    Query = 1,
}
