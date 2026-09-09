namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// A bounded membership point-probe's answer: the source identifiers it found,
/// paired with how many of the probed keys the store's read-path access gate
/// removed before fan-out (issue #2277).
/// </summary>
/// <remarks>
/// <para>
/// A bare <see cref="IReadOnlySet{T}"/> of found identifiers is the shape this
/// item was opened on. It cannot express the difference between the two reasons an
/// identifier is missing from it - never embedded, or embedded but hidden from this
/// caller by an access gate - and every consumer reads the missing case as the
/// first. On a gated deployment that misclassifies an embedded source as a gap and
/// re-embeds it on every pass forever, with a clean log at every layer.
/// </para>
/// <para>
/// Carrying the count in the return type rather than documenting the hazard is
/// deliberate: it makes each consumer's decision to classify on absence an explicit
/// one, checked by <see cref="AbsenceIsConclusive"/>, instead of a silent default.
/// </para>
/// </remarks>
/// <param name="SourceIds">The source identifiers the probe found among the candidates.</param>
/// <param name="PrunedByAccessGate">How many probed keys the read-path access gate removed before fan-out.</param>
internal readonly record struct RepoContextProbedSourceIds(
    IReadOnlySet<string> SourceIds,
    int PrunedByAccessGate)
{
    /// <summary>
    /// Whether a candidate's <em>absence</em> from <see cref="SourceIds"/> may be
    /// read as "not embedded". True exactly when the gate pruned nothing. When
    /// false the probe is incomplete rather than negative, and the caller must
    /// decline to classify for that page rather than treat it as authoritative.
    /// </summary>
    public bool AbsenceIsConclusive => PrunedByAccessGate == 0;

    /// <summary>
    /// Whether <paramref name="sourceId"/> was found by the probe. Answering this
    /// affirmatively is always sound; it is only the negative answer that
    /// <see cref="AbsenceIsConclusive"/> qualifies.
    /// </summary>
    /// <param name="sourceId">The source identifier to test.</param>
    public bool Contains(string sourceId) => SourceIds.Contains(sourceId);
}
