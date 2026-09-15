namespace Orleans.Lattice.Vector.Persistence;

/// <summary>
/// What a <see cref="DurableVectorIndex"/> can honestly say about itself right
/// now: the phase it is in, how much of the corpus it holds, and whether it got
/// there by loading durable state or by rebuilding.
/// <para>
/// This is the signal a readiness probe and a retrieval-path attribution are
/// built from. It is deliberately free of wall-clock time and of any estimate:
/// every field is a count the index actually knows, so a consumer never has to
/// present a guess as a fact.
/// </para>
/// </summary>
/// <param name="Phase">How far the build has got.</param>
/// <param name="Generation">The index generation these figures describe.</param>
/// <param name="VectorsIndexed">The number of vectors the index currently holds.</param>
/// <param name="VectorsExpected">
/// The number of vectors the store of record held when the build last counted
/// it, or <c>0</c> when it has not been counted. A bound for reporting only;
/// nothing depends on it for correctness.
/// </param>
/// <param name="PartitionsPersisted">The number of partitions whose durable form is current.</param>
/// <param name="PartitionsTotal">The number of partitions the index has, or <c>0</c> when untrained.</param>
/// <param name="RestoredFromDurableState">
/// Whether this state came from durable records rather than from a rebuild. A
/// consumer reporting a cold start uses this to tell "loaded in" from "recomputed".
/// </param>
/// <param name="SlicesDeadlined">
/// How many ingest slices this index instance has stopped because their
/// wall-clock budget was spent, rather than because they filled their work
/// budget or exhausted the source.
/// </param>
/// <param name="SlicesDeadlinedWithoutProgress">
/// How many of <paramref name="SlicesDeadlined"/> banked nothing, because the
/// source yielded no item at all before the budget was spent.
/// <para>
/// This is the field that discriminates the two ways a build can fail to
/// converge, which are otherwise indistinguishable from outside: a build whose
/// slices are merely SHORT still advances its cursor every slice, so this stays
/// at <c>0</c> while <see cref="VectorsIndexed"/> climbs and the remedy is to
/// tune the budget. A build whose source cannot deliver its first item advances
/// nothing, so this climbs in step with <paramref name="SlicesDeadlined"/> while
/// <see cref="VectorsIndexed"/> stays put - and no budget will ever fix that,
/// because the read the build is waiting on cannot complete. Reporting only that
/// a deadline fired conflates the two and sends the next investigation at the
/// wrong layer.
/// </para>
/// </param>
public readonly record struct VectorIndexBuildProgress(
    VectorIndexBuildPhase Phase,
    long Generation,
    int VectorsIndexed,
    int VectorsExpected,
    int PartitionsPersisted,
    int PartitionsTotal,
    bool RestoredFromDurableState,
    int SlicesDeadlined = 0,
    int SlicesDeadlinedWithoutProgress = 0)
{
    /// <summary>
    /// Whether every ingest slice stopped by its deadline also banked nothing,
    /// with at least one such slice observed. That is the signature of a build
    /// that is bounded but WEDGED: the budget is being enforced, and the source
    /// is nonetheless not delivering, so the build cannot converge however long
    /// it is left running.
    /// <para>
    /// It is deliberately conjunctive. A build that has banked some slices and
    /// stalled on others is contended, which time and a quieter box may cure; a
    /// build that has banked none of them is blocked on a read that does not
    /// complete, which they will not.
    /// </para>
    /// </summary>
    public bool IsStarvedBySource =>
        SlicesDeadlined > 0 && SlicesDeadlinedWithoutProgress == SlicesDeadlined;

    /// <summary>
    /// Whether the index answers from its partitioning. While this is
    /// <see langword="false"/> searches are still <i>exact</i>, by exhaustive
    /// scan, and must not be reported as degraded.
    /// <para>
    /// Both conditions are load-bearing, and the partition count is the one that
    /// is easy to omit. <see cref="VectorIndexBuildPhase.Ready"/> means the build
    /// pipeline ran to the end; it does not mean the pipeline produced a
    /// partitioning. <c>VectorIndex.Train()</c> returns <see langword="false"/>
    /// and drops any previous partitioning when the corpus is below
    /// <c>MinimumTrainingCount</c> or resolves to fewer than two partitions, and
    /// the build reaches <see cref="VectorIndexBuildPhase.Ready"/> anyway -
    /// correctly, because the build really is finished and the index really is
    /// serving, exhaustively and exactly. Reporting that state as answering from
    /// a partitioning it does not have would be the one thing this type exists
    /// not to do.
    /// </para>
    /// </summary>
    public bool IsReady => Phase == VectorIndexBuildPhase.Ready && PartitionsTotal > 0;

    /// <summary>
    /// The fraction of the store of record the index currently holds, in
    /// <c>[0, 1]</c>. Reports <c>1</c> once the build has finished, and when the
    /// expected count is unknown, so a caller never renders a progress bar that
    /// implies knowledge the index does not have.
    /// <para>
    /// This deliberately tests the phase rather than <see cref="IsReady"/>, and
    /// the divergence is the point: partitioning has nothing to do with how much
    /// of the corpus was ingested. A build that finished without partitioning
    /// still ingested all of it, so reporting a fraction below <c>1</c> for it
    /// would be a fresh false signal. The two properties answer different
    /// questions and only ever agreed by accident, so do not "restore
    /// consistency" by routing this back through <see cref="IsReady"/>.
    /// </para>
    /// </summary>
    public double IngestedFraction =>
        Phase == VectorIndexBuildPhase.Ready || VectorsExpected <= 0
            ? 1d
            : Math.Clamp((double)VectorsIndexed / VectorsExpected, 0d, 1d);
}
