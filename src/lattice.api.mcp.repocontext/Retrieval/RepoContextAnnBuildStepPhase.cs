namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Where in an approximate-index build coordinator tick the step was when it was
/// counted. A closed set of six, exhaustive over the tick: four phases a step can
/// be attempted in, plus the two that only a fault can reach.
/// <para>
/// <b>Why this dimension exists.</b> Without it a faulted read of the ingest
/// corpus and a faulted write of the trained index are THE SAME SERIES VALUE, and
/// they imply opposite conclusions. A fault on the ingest read says the corpus
/// could not be read - an independent defect. A fault in the persist says the
/// trained index could not be written, and that write lands in the very tree a
/// corpus-read defect would already have named, so the two are one defect counted
/// twice. Epic #2368 could not tell those apart from telemetry and had to read an
/// 8 MiB container log by hand to place a single fault (issue #2855). The
/// <c>phase</c> tag is what makes the placement readable from the series.
/// </para>
/// <para>
/// <b>The values are resolved where the step runs, never inferred.</b>
/// <see cref="RepoContextAnnIndexHandle"/> writes the phase it is entering before
/// it calls the index, and rewrites it from the index's own reported phase at the
/// fault site, so a step that trains and then persists in one call is attributed
/// to whichever half actually threw rather than to the half it started in. The
/// coordinator supplies <see cref="Coordinating"/> itself for a tick that never
/// reached the step at all.
/// </para>
/// </summary>
internal enum RepoContextAnnBuildStepPhase
{
    /// <summary>
    /// The tick faulted outside the build step: resolving the run credential,
    /// completing the coordinator, probing the corpus gate, or writing the durable
    /// coordinator state. No build step was attempted, so no index phase can be
    /// named - which is itself the finding, because it says the coordinator is
    /// failing before it ever reaches the plane.
    /// <para>
    /// Zero, and therefore the value a default-constructed probe would hold. That
    /// is deliberate and safe in exactly one direction: the coordinator supplies
    /// this value explicitly for a tick it knows did not step, and the handle
    /// overwrites it with <see cref="Opening"/> the moment a step begins, so a
    /// stale zero can only ever under-claim knowledge rather than mislabel a fault
    /// as belonging to a phase it did not reach.
    /// </para>
    /// </summary>
    Coordinating = 0,

    /// <summary>
    /// The step faulted opening or restoring the durable index, before any build
    /// work was attempted. Distinct from <see cref="Ingesting"/>: nothing was read
    /// from the store of record, so a fault here is about the index's own durable
    /// state rather than about the corpus.
    /// </summary>
    Opening = 1,

    /// <summary>
    /// The step was reading the ingest corpus out of the store of record -
    /// counting it, or streaming a slice of it into the index. <b>This is the read
    /// side of the DoD-1b discrimination.</b> A fault here says the corpus could
    /// not be read, which is a defect of the projection or the leaf chain behind
    /// it and is independent of whether the index could be written.
    /// <para>
    /// Covers <see cref="Vector.Persistence.VectorIndexBuildPhase.NotStarted"/> as
    /// well as <see cref="Vector.Persistence.VectorIndexBuildPhase.Ingesting"/>,
    /// because the step taken from <c>NotStarted</c> counts the source, which is a
    /// read of the same corpus by the same path.
    /// </para>
    /// </summary>
    Ingesting = 2,

    /// <summary>
    /// The step was training the partitioning over vectors already banked in
    /// memory. Pure computation against data the index already holds: it reads
    /// nothing and writes nothing durable, so a fault here is neither a corpus
    /// read nor an index persist.
    /// </summary>
    Training = 3,

    /// <summary>
    /// The step was writing the trained index back to its durable store.
    /// <b>This is the write side of the DoD-1b discrimination.</b> A fault here
    /// says the index could not be persisted, and that write goes into the same
    /// tree a corpus-read defect would already have named, so a fault on this arm
    /// is a reason to treat the two criteria as one defect rather than two.
    /// </summary>
    Persisting = 4,

    /// <summary>
    /// The build had already reached
    /// <see cref="Vector.Persistence.VectorIndexBuildPhase.Ready"/> and the step
    /// was catching the served index up with the store of record, or maintaining
    /// it. A fault here does not stop a build - there is no build left to stop -
    /// so it says a SERVING plane is drifting, which is a different and quieter
    /// failure from one that never started serving.
    /// </summary>
    Reconciling = 5,
}
