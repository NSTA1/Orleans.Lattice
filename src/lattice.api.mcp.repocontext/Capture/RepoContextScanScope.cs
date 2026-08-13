namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The ordered range a <c>repocontext_scan</c> or <c>repocontext_list_topics</c>
/// call walks. Each scope resolves to exactly one named Lattice tree and one key
/// prefix (see <see cref="RepoContextKeys"/>), so a scan is an unambiguous,
/// single-tree ordered range read.
/// <para>
/// This selector is an in-memory routing convenience for the capture tools; it is
/// never persisted on the wire, so it carries no Orleans serialization alias.
/// </para>
/// </summary>
internal enum RepoContextScanScope
{
    /// <summary>All file structural nodes under <c>repo/{repoId}/file/</c> (optionally under a directory).</summary>
    Files,

    /// <summary>All package structural nodes under <c>repo/{repoId}/pkg/</c> (optionally under a directory).</summary>
    Packages,

    /// <summary>All symbol records under <c>repo/{repoId}/symbol/</c>.</summary>
    Symbols,

    /// <summary>All agent memory records under <c>repo/{repoId}/mem/</c>.</summary>
    Memory,

    /// <summary>All agent memory records under a single topic <c>repo/{repoId}/mem/{topic}/</c>.</summary>
    MemoryTopic,
}
