namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Which source strategy supplies the content an index generation is built from.
/// The strategy is chosen per repository by <see cref="RepoContextIndexSourceGate"/>
/// and the two are mutually exclusive: a repository is either walked from a mounted
/// workspace or fetched from a git remote, never both.
/// </summary>
internal enum RepoContextSourceKind
{
    /// <summary>
    /// The default: the indexer walks a read-only tree mounted under the workspace
    /// boundary. Whoever mounts the volume decides what is indexed, and deletion is
    /// inferred from absence on disk.
    /// </summary>
    MountedWorkspace = 0,

    /// <summary>
    /// Opt-in and hub-only: the indexer fetches a configured git ref into a staging
    /// work tree and indexes the resolved commit, so every generation is anchored to
    /// a verifiable commit SHA and deletion is exact.
    /// </summary>
    GitRemote = 1,
}
