namespace Orleans.Lattice.Replication;

/// <summary>
/// Result of <see cref="ISagaParticipant.PrepareAsync"/>. Carries the
/// participant's binary vote plus an optional human-readable detail describing
/// why the participant reached that vote. A plain in-process value type: it is
/// never serialized over a grain boundary, so it carries no Orleans alias.
/// </summary>
/// <param name="Vote">
/// The participant's vote. <see cref="SagaVote.Commit"/> when the resource set
/// prepared cleanly and is holding the prepared state; <see cref="SagaVote.Abort"/>
/// when the participant could not prepare (a precondition failed or a genuine
/// error occurred and the participant self-compensated).
/// </param>
/// <param name="Detail">
/// Optional detail describing the vote (for example an abort reason). May be
/// <see langword="null"/>.
/// </param>
internal readonly record struct SagaParticipantPrepareResult(SagaVote Vote, string? Detail = null);
