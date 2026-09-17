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
/// Both figures are LIFETIME totals for this index instance, and neither is ever
/// reset. They are the right shape for a post-mortem count - "how much of this
/// build's time went on deadlines that bought nothing" - and they are the wrong
/// shape for a verdict about the present, which is what
/// <see cref="IsStarvedBySource"/> now reads
/// <see cref="EmptyDeadlinesSinceLastAdvance"/> for instead. See that property
/// for why.
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
    /// How many ingest slices have been stopped by their deadline having banked
    /// nothing SINCE THE BUILD LAST BANKED ANYTHING. Reset to <c>0</c> by any
    /// slice that consumes at least one item, whether that slice was deadlined or
    /// ran to its work budget.
    /// <para>
    /// This is the present-tense counterpart of
    /// <see cref="SlicesDeadlinedWithoutProgress"/>, and the difference between
    /// them is the whole reason it exists. The lifetime pair can say what a build
    /// has been through; only a figure that is cleared by progress can say what it
    /// is doing now, which is the question a starvation warning is answering.
    /// </para>
    /// </summary>
    public int EmptyDeadlinesSinceLastAdvance { get; init; }

    /// <summary>
    /// Whether the build is stalled on a source that is not delivering: at least
    /// one ingest slice has been deadlined empty-handed, and nothing has been
    /// banked since. That is the signature of a build that is bounded but WEDGED -
    /// the budget is being enforced, the source is nonetheless not answering, and
    /// the build cannot converge however long it is left running.
    /// <para>
    /// It clears the moment a slice banks anything, and that is load-bearing
    /// rather than incidental. A build that has banked some slices and stalled on
    /// others is contended, which time and a quieter box may cure; a build that is
    /// banking nothing at all is blocked on a read that does not complete, which
    /// they will not.
    /// </para>
    /// <para>
    /// <b>This deliberately no longer reads the lifetime pair, and restoring that
    /// would restore the defect.</b> The predicate used to be
    /// <c>SlicesDeadlined &gt; 0 &amp;&amp; SlicesDeadlinedWithoutProgress == SlicesDeadlined</c>:
    /// a present-tense claim assembled out of two counters that are never reset.
    /// Because a slice that completes inside its budget touches NEITHER counter,
    /// an advancing build cannot move that equality at all - the only event that
    /// could was a future slice that was both deadlined AND productive. So a build
    /// that took a run of empty deadlines early and then advanced perfectly for
    /// the rest of its life went on reporting starvation forever, while the
    /// accompanying warning asserted "the build is not advancing" and "raising the
    /// budget will not help" over a corpus climbing a hundred vectors a tick. The
    /// mirror-image false negative was there too: one deadlined-but-productive
    /// slice broke the equality permanently, so a build that wedged afterwards
    /// could never report starvation at all.
    /// </para>
    /// <para>
    /// A build wedged from cold behaves identically under both forms, because for
    /// such a build the current run IS its lifetime - which is why every fixture
    /// written against the old predicate still holds. The two forms diverge only
    /// where the old one was wrong.
    /// </para>
    /// </summary>
    public bool IsStarvedBySource => EmptyDeadlinesSinceLastAdvance > 0;

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
