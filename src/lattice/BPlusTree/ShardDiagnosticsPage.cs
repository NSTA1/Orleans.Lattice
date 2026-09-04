namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// One work-bounded batch of a shard diagnostics walk (see
/// <see cref="IShardRootGrain.GetDiagnosticsBoundedAsync"/>).
/// <para>
/// The shard aggregates key counts across a bounded number of leaves and then
/// returns, releasing the non-reentrant shard so other traffic can interleave.
/// <see cref="ResumeFromInclusive"/> is the key the caller passes back as the
/// next batch's resume position, or <see langword="null"/> when this shard's
/// chain is complete. The caller sums <see cref="ShardDiagnosticReport.LiveKeys"/>
/// and <see cref="ShardDiagnosticReport.Tombstones"/> across the batches, so the
/// reported totals are unchanged - only the number of shard calls it takes
/// differs (issue 1972).
/// </para>
/// <para>
/// <b><see cref="Report"/>'s O(1) fields are authoritative only on the first
/// batch</b> - the one requested with a <see langword="null"/> resume position.
/// Depth, hotness, and the lifecycle flags are shard-level facts that a resumed
/// batch would have to re-descend the tree to recompute for an identical
/// answer, so a resumed batch leaves them at their defaults and only its key
/// counts carry information. A batch is therefore not a complete report on its
/// own; the driver assembles one. That is the same contract as
/// <see cref="ShardCountPage"/>, where a partial count is a <b>wrong</b> answer
/// rather than a short one, which is why this carries a resume position instead
/// of a "has more" flag: the caller must be unable to mistake a bounded batch
/// for a complete result.
/// </para>
/// <para>
/// This weakens no guarantee the diagnostics surface offered. The report was
/// already a sample rather than a snapshot - it carries its own
/// <c>SampledAt</c> and is served through a cache with a
/// <c>DiagnosticsCacheTtl</c> - so counts drawn from several turns are the same
/// kind of answer the single-call walk produced.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ShardDiagnosticsPage)]
[Immutable]
internal readonly record struct ShardDiagnosticsPage
{
    /// <summary>
    /// This batch's contribution. Key counts cover only the leaves this batch
    /// visited; the remaining fields are populated on the first batch and left
    /// at their defaults on a resumed one.
    /// </summary>
    [Id(0)] public ShardDiagnosticReport Report { get; init; }

    /// <summary>
    /// The key to resume from, as the next batch's resume position, or
    /// <see langword="null"/> when this shard's chain has been walked to its
    /// end. A non-null value is always strictly greater than the resume
    /// position the batch was called with, so the walk cannot stall re-counting
    /// the same leaves - which would also double-count them.
    /// </summary>
    [Id(1)] public string? ResumeFromInclusive { get; init; }
}
