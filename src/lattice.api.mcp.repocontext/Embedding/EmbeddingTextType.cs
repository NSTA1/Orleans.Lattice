namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The role a piece of text plays when it is embedded, so an
/// <see cref="IEmbeddingProvider"/> can apply the correct query/passage prefix
/// and produce vectors in the matching sub-space. Asymmetric embedding models
/// (the shipped Onyx default among them) encode a stored document differently
/// from a search query, so mixing the two roles silently degrades recall.
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
