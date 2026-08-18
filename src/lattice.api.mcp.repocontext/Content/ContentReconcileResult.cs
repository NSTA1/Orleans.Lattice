namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The outcome of one content-reconcile pass: the repository-relative paths whose
/// searchable body text was successfully projected into the
/// <see cref="RepoContextTrees.Content"/> tree this run. The bootstrap stamps the
/// <see cref="FileNode.ContentProcessed"/> marker on exactly these files' nodes, so
/// a file that could not be read this pass is left unmarked and retried by the
/// content back-fill on a later pass.
/// </summary>
/// <param name="ProcessedPaths">The repository-relative paths whose content record
/// was written (or confirmed empty) this pass, and whose file node should therefore
/// be stamped as content-processed.</param>
/// <param name="ContentCaptured">The number of content records written live during
/// the pass.</param>
/// <param name="TokenCountsByPath">The BPE token count of the decoded body text for
/// each path in <paramref name="ProcessedPaths"/>, computed once while the body was
/// in hand. The bootstrap stamps these onto the matching file nodes'
/// <see cref="FileNode.TokenCount"/> register, so budgets read the count rather than
/// recomputing it per call.</param>
internal readonly record struct ContentReconcileResult(
    IReadOnlySet<string> ProcessedPaths,
    int ContentCaptured,
    IReadOnlyDictionary<string, int> TokenCountsByPath);
