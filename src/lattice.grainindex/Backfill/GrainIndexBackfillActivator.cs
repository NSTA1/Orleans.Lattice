using Orleans.Core.Internal;

namespace Orleans.Lattice.GrainIndex.Backfill;

/// <summary>
/// The default <see cref="IGrainIndexBackfillActivator"/>: it addresses the
/// grain through the index's own key codec and asks the runtime to deactivate it
/// once idle.
/// </summary>
/// <remarks>
/// <para>
/// The call is what does the work. Dispatching any message to a grain reference
/// activates the grain, and Orleans completes the activation lifecycle - which
/// is where an <c>[Indexed]</c> state object projects and records itself -
/// before the message runs. By the time the call returns, the grain has enrolled.
/// </para>
/// <para>
/// Asking for deactivation rather than merely pinging is what makes the crawl
/// safe over a large dormant population: a backfill that left every grain it
/// touched activated would fill the silo with activations nothing is using, and
/// idle collection would only reclaim them much later.
/// </para>
/// <para>
/// A grain that is <i>already</i> active is not re-activated by this, so it does
/// not re-project. That is the right behaviour for the crawl's job: an active
/// grain has already been through its activation and is therefore already
/// recorded, so the crawl skips it before reaching here. It does mean a rebuild
/// leaves a currently-active grain's entries to be refreshed by its next write
/// rather than by the crawl.
/// </para>
/// <para>
/// The grain reference is resolved per key because a reference is per grain;
/// nothing else here allocates, and the factory is captured once.
/// </para>
/// </remarks>
internal sealed class GrainIndexBackfillActivator : IGrainIndexBackfillActivator
{
    private readonly IGrainFactory _grainFactory;

    /// <summary>Initialises the activator.</summary>
    /// <param name="grainFactory">Addresses the grains to onboard. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException"><paramref name="grainFactory"/> is <c>null</c>.</exception>
    public GrainIndexBackfillActivator(IGrainFactory grainFactory)
    {
        ArgumentNullException.ThrowIfNull(grainFactory);
        _grainFactory = grainFactory;
    }

    /// <inheritdoc />
    public async Task ActivateAsync(
        IGrainIndexDefinition definition,
        string grainKey,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(grainKey);
        cancellationToken.ThrowIfCancellationRequested();

        var grain = definition.KeyCodec.Resolve(_grainFactory, grainKey);
        await grain.AsReference<IGrainManagementExtension>().DeactivateOnIdle().ConfigureAwait(true);
    }
}
