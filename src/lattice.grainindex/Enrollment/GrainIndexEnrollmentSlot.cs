namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// One activated grain's enrolment state in one index: the key it is filed
/// under, the projection the index is known to hold, and the write it has
/// recorded but not yet confirmed.
/// </summary>
/// <remarks>
/// A mutable struct held in an array the state object allocates once per
/// activation, so tracking a grain across its whole activation costs one array
/// and no per-write allocation. It is only ever reached through an indexer on
/// that array, never copied into a local, which is what keeps the mutation
/// visible.
/// </remarks>
internal struct GrainIndexEnrollmentSlot
{
    /// <summary>The grain's encoded key, computed once at activation.</summary>
    public string GrainKey;

    /// <summary>
    /// The projection the index is known to hold for this grain. Every plan is
    /// diffed against it rather than against the last <i>attempted</i>
    /// projection, so a write that follows a failed one subsumes it instead of
    /// assuming it landed.
    /// </summary>
    public GrainIndexProjection Confirmed;

    /// <summary>
    /// The outbox entry recorded for the write currently in flight, or
    /// <c>null</c> when this grain has nothing outstanding for this index.
    /// </summary>
    public GrainIndexPendingProjection? Pending;

    /// <summary>
    /// Whether the registry already holds a seen marker for this grain, so the
    /// backfill will skip it. A first enrolment writes the marker even when the
    /// projection is empty, which is the only case where an empty plan still
    /// costs a registry write.
    /// </summary>
    public bool Enrolled;

    /// <summary>Initialises a slot from what the registry reported at activation.</summary>
    /// <param name="grainKey">The grain's encoded key.</param>
    /// <param name="confirmed">The projection the index holds, or an empty one.</param>
    /// <param name="enrolled">Whether a seen marker already exists.</param>
    public GrainIndexEnrollmentSlot(string grainKey, GrainIndexProjection confirmed, bool enrolled)
    {
        GrainKey = grainKey;
        Confirmed = confirmed;
        Enrolled = enrolled;
        Pending = null;
    }
}
