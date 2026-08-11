namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The family a repository-context record belongs to. Derived from the record's
/// hierarchical key (see <see cref="RepoContextKeys"/>) and used to route a
/// record onto its dedicated named Lattice tree via
/// <see cref="RepoContextTrees.ForKind(RepoContextRecordKind)"/>.
/// <para>
/// This discriminator is an in-memory routing/parse convenience only; it is not
/// itself persisted on the wire, so it carries no Orleans serialization alias.
/// </para>
/// </summary>
internal enum RepoContextRecordKind
{
    /// <summary>A repository root structural node (<c>repo/{repoId}</c>).</summary>
    Repo,

    /// <summary>A package / module / directory structural node (<c>repo/{repoId}/pkg/{path}</c>).</summary>
    Package,

    /// <summary>A source-file structural node (<c>repo/{repoId}/file/{path}</c>).</summary>
    File,

    /// <summary>A symbol record (<c>repo/{repoId}/symbol/{fqName}</c>).</summary>
    Symbol,

    /// <summary>An agent-authored memory record (<c>repo/{repoId}/mem/{topic}/{id}</c>).</summary>
    Memory,
}
