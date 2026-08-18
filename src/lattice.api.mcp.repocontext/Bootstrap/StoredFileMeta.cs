namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The reconcile-relevant facts already stored for a file, read from its
/// <see cref="FileNode"/> before a walk so the walk can decide - per file, without
/// reading it - whether the file is unchanged. It carries the stored content
/// <see cref="Digest"/> and <see cref="Language"/> (reused verbatim when the file is
/// skipped) plus the two quantities the stat fast-path compares: the stored
/// <see cref="SizeBytes"/> and <see cref="IngestWallTicks"/>, the wall-clock
/// component of the hybrid logical clock the file was last ingested at (recovered
/// from the register order key, so no extra field is persisted).
/// <para>
/// This is an in-memory reconcile artefact only - it never crosses an Orleans wire
/// (the persisted shape is <see cref="FileNode"/>), so it carries no serialization
/// attributes.
/// </para>
/// </summary>
/// <param name="Digest">The stored content digest, in whatever algorithm shape it
/// was written (see <see cref="FileDigest"/>).</param>
/// <param name="Language">The stored best-effort language identifier.</param>
/// <param name="SizeBytes">The stored file size in bytes, or a negative value when
/// no size was recorded (which forces the fast-path to re-hash).</param>
/// <param name="IngestWallTicks">The wall-clock tick component of the ingest hybrid
/// logical clock - the anchor the fast-path treats as "last indexed at". A file
/// whose on-disk modification time is strictly older than this and whose size is
/// unchanged is assumed unchanged without a read. Zero when no anchor was
/// recovered (which forces a re-hash).</param>
/// <param name="DeclaredSymbols">The fully-qualified names of the symbols the
/// stored file node last declared. Read from the file node's
/// <see cref="FileNode.DeclaredSymbols"/> register so the symbol reconciler knows,
/// for a changed or removed file, which symbols that file no longer declares.
/// Empty when none were recorded.</param>
/// <param name="SymbolsProcessed">Whether the stored file node carries the
/// <see cref="FileNode.SymbolsProcessed"/> marker - that is, whether its symbols
/// have already been extracted. False for a node written before symbol extraction
/// existed, which is what makes the background back-fill pick it up. A file that
/// declares no symbols but has been processed is still <see langword="true"/>.</param>
/// <param name="ContentProcessed">Whether the stored file node carries the
/// <see cref="FileNode.ContentProcessed"/> marker - that is, whether its searchable
/// body text has already been projected into the content tree. False for a node
/// written before the content projection existed, which is what makes the background
/// content back-fill pick it up.</param>
/// <param name="TokenCount">The stored BPE token count from the file node's
/// <see cref="FileNode.TokenCount"/> register, or a negative value when none was
/// recorded (a node written before the register existed). A negative value makes the
/// content back-fill re-select the file so its token count is computed, and is what a
/// carry-forward rewrite treats as "no count to preserve".</param>
/// <param name="CrossReferenced">Whether the stored file node carries the
/// <see cref="FileNode.CrossReferenced"/> marker - that is, whether its declared
/// symbols' outbound references have already been projected into the reverse
/// cross-reference index. False for a node written before the reverse index existed,
/// which - combined with <see cref="SymbolsProcessed"/> being true - is what makes the
/// background cross-reference back-fill force-seed it from the stored symbol
/// records.</param>
internal readonly record struct StoredFileMeta(
    string Digest,
    string Language,
    long SizeBytes,
    long IngestWallTicks,
    IReadOnlyList<string> DeclaredSymbols,
    bool SymbolsProcessed = false,
    bool ContentProcessed = false,
    long TokenCount = -1,
    bool CrossReferenced = false);
