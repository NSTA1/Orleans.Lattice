namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A single source file discovered by <see cref="RepoTreeWalker"/> during a
/// bootstrap scan: its repository-relative path, a content digest, its size, and
/// a best-effort detected language. This is an in-memory scan artefact only - it
/// never crosses an Orleans wire (the persisted shape is <see cref="FileNode"/>),
/// so it carries no serialization attributes.
/// </summary>
/// <param name="RelativePath">The file path relative to the repository root,
/// always using <c>'/'</c> as the separator so it maps directly onto the
/// hierarchical key grammar in <see cref="RepoContextKeys"/>.</param>
/// <param name="Digest">A stable, lower-case hex content digest of the file's
/// bytes (see <see cref="FileDigest"/>). Two scans of unchanged content produce
/// the same digest, which is what makes a re-run a no-op.</param>
/// <param name="SizeBytes">The file's size in bytes.</param>
/// <param name="Language">A best-effort language identifier derived from the file
/// extension, or the empty string when the extension is not recognised.</param>
internal readonly record struct RepoFileEntry(
    string RelativePath,
    string Digest,
    long SizeBytes,
    string Language);
