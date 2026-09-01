namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Brings one dormant grain into existence so the activation-path enrolment
/// hook indexes it. The step the background backfill takes for every key its
/// <see cref="IGrainKeySource"/> yields that is not already indexed.
/// </summary>
/// <remarks>
/// <para>
/// A grain index is populated by the grain itself: an <c>[Indexed]</c> state
/// object projects and records its grain when the activation reaches the
/// <c>Activate</c> lifecycle stage. The backfill therefore does not need to read
/// anybody's state - it needs the grain to exist for a moment. That is this
/// seam's whole job.
/// </para>
/// <para>
/// The default implementation addresses the grain through the index's own key
/// codec and asks the runtime to deactivate it when idle. The call is what
/// activates it - the activation lifecycle, and so the enrolment, completes
/// before the call is dispatched - and the deactivation is what keeps a crawl
/// over a large dormant population from pinning every grain it touches in
/// memory.
/// </para>
/// <para>
/// It is a replaceable seam because "make this grain exist" is host policy: a
/// deployment that would rather warm its grains, or that addresses them through
/// a facade, registers its own singleton and the crawl is unchanged.
/// </para>
/// </remarks>
public interface IGrainIndexBackfillActivator
{
    /// <summary>
    /// Activates the grain that <paramref name="grainKey"/> names in the index
    /// <paramref name="definition"/> describes.
    /// </summary>
    /// <param name="definition">The index whose grain is being onboarded. Must not be <c>null</c>.</param>
    /// <param name="grainKey">The encoded grain key to activate. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the activation.</param>
    /// <returns>A task that completes once the grain has activated.</returns>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    Task ActivateAsync(
        IGrainIndexDefinition definition,
        string grainKey,
        CancellationToken cancellationToken);
}
