namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The outcome of a materialised-view reconcile: view anti-entropy that builds the
/// expected view from current source state into a shadow generation, compares it
/// against the live view via a content digest, and swaps the shadow in (repairing the
/// view) only when they diverge.
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeViewReconcileResult)]
[Immutable]
public sealed record TreeViewReconcileResult
{
    /// <summary>The logical view name this reconcile targeted.</summary>
    [Id(0)] public required string ViewName { get; init; }

    /// <summary>The source tree id the view is derived from and authorized against.</summary>
    [Id(1)] public required string SourceTreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when drift was detected and repaired (the shadow was
    /// swapped in); <see langword="false"/> when the view already matched the source
    /// and no swap was needed.
    /// </summary>
    [Id(2)] public bool DriftRepaired { get; init; }
}
