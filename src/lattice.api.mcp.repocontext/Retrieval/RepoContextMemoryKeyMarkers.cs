namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The outcome of one resumable scan of a repository's embedded-memory-key
/// markers: the keys observed so far, and whether the scan reached the end of the
/// marker range.
/// <para>
/// The markers are the memory arm's backstop for a lost source-id membership
/// flag, and the recorded half of the orphan set (recorded - live). Loading them
/// is a range scan over the membership tree, which is the tree whose saturation
/// the backstop exists to tolerate - so on a large, leaf-fragmented tree the load
/// can exceed the shard's page-fill ceiling and be defeated by exactly the
/// pressure it was meant to survive (issue #2071). Reporting the partial set
/// alongside <see cref="Complete"/> is what lets a caller use the two halves
/// differently, which is the whole point of separating them:
/// </para>
/// <list type="bullet">
/// <item>
/// <description>
/// As a <b>skip signal</b> a partial set is always safe. A marker is only ever
/// written after the corresponding vectors landed, so a key that is present is
/// genuine evidence; a key that is missing merely re-embeds idempotently. So the
/// keys are usable on every pass, complete or not.
/// </description>
/// </item>
/// <item>
/// <description>
/// As the <b>recorded half of the orphan sweep</b> a partial set is not safe -
/// subtracting the live keys from an incomplete recorded set is fine, but a
/// caller must never mistake an incomplete set for the whole one. The sweep runs
/// only when <see cref="Complete"/> is true.
/// </description>
/// </item>
/// </list>
/// </summary>
/// <param name="Keys">
/// The memory record keys recorded as embedded that the scan has observed. When
/// <see cref="Complete"/> is false this is a prefix of the full set, carried
/// forward across passes rather than discarded.
/// </param>
/// <param name="Complete">
/// <see langword="true"/> when the scan walked the marker range to its end, so
/// <see cref="Keys"/> is the authoritative recorded set; <see langword="false"/>
/// when a page read failed and the walk will resume from its continuation token
/// on the next call.
/// </param>
/// <param name="Passes">
/// How many calls the current walk of the range has taken, counting the one that
/// produced this result. A completed scan reporting more than one pass is direct
/// evidence that banked progress was resumed rather than discarded, which is the
/// behaviour issue #2071 turns on.
/// </param>
/// <param name="Fault">
/// The fault that stopped an incomplete scan, so the caller can log the real
/// cause rather than a bare "incomplete"; <see langword="null"/> when the scan
/// completed.
/// </param>
internal sealed record RepoContextMemoryKeyMarkers(
    IReadOnlySet<string> Keys,
    bool Complete,
    int Passes,
    Exception? Fault);
