namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The outcome of one symbol-reconcile pass: how many distinct symbol records were
/// written (upserted) this run, and, for every file whose structural node the
/// bootstrap is about to rewrite, the fully-qualified names that file now declares -
/// so the file node's <see cref="FileNode.DeclaredSymbols"/> register can be stamped
/// atomically with the rest of the node.
/// </summary>
/// <param name="SymbolsCaptured">The number of distinct symbols upserted (written as
/// live) during the pass.</param>
/// <param name="DeclaredByPath">The declared fully-qualified names keyed by
/// repository-relative file path, covering exactly the added, updated, and
/// back-filled files whose nodes are being rewritten (and therefore the files whose
/// node should be stamped as symbol-processed).</param>
internal readonly record struct SymbolReconcileResult(
    int SymbolsCaptured,
    IReadOnlyDictionary<string, IReadOnlyList<string>> DeclaredByPath);
