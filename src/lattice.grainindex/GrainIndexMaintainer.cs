using Microsoft.Extensions.Options;
using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Keeps one grain's index entries in step with its state, by applying a
/// projection diff to the index's backing lattice tree as a single
/// all-or-nothing batch.
/// </summary>
/// <remarks>
/// <para>
/// The batch is the point. A grain whose <c>Age</c> moves from 17 to 18 needs
/// its old entry tombstoned and its new one written together: apply them
/// separately and a concurrent scan sees the grain at both ages, or at neither.
/// The mixed atomic write on <see cref="ILattice"/> exists for exactly this
/// re-key retraction, so the maintainer routes upserts and tombstones through
/// it in one call.
/// </para>
/// <para>
/// An unchanged re-projection never reaches the tree at all: an empty plan
/// short-circuits before the round trip, which is what makes re-enrolling an
/// unchanged grain - on every activation, say - free.
/// </para>
/// </remarks>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
/// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
public sealed class GrainIndexMaintainer<TGrain, TState>
    where TGrain : IGrain
{
    /// <summary>
    /// Initialises a maintainer over an explicit tree, which is the form to use
    /// when the caller already holds the index's <see cref="ILattice"/>
    /// reference.
    /// </summary>
    /// <param name="definition">The index definition. Must not be <c>null</c>.</param>
    /// <param name="tree">The index's backing lattice tree. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexMaintainer(GrainIndexDefinition<TGrain, TState> definition, ILattice tree)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(tree);
        Projector = new GrainIndexProjector<TGrain, TState>(definition);
        Tree = tree;
    }

    /// <summary>
    /// Initialises a maintainer that resolves the index's backing tree from the
    /// index's named options.
    /// </summary>
    /// <remarks>
    /// The tree name is read once, here, rather than per call: it is validated
    /// at startup and is not a runtime-tunable knob, so re-reading it on the
    /// projection path would buy nothing.
    /// </remarks>
    /// <param name="definition">The index definition. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">The grain factory used to resolve the tree. Must not be <c>null</c>.</param>
    /// <param name="options">The per-index options monitor, read by index name. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexMaintainer(
        GrainIndexDefinition<TGrain, TState> definition,
        IGrainFactory grainFactory,
        IOptionsMonitor<GrainIndexOptions> options)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(options);
        Projector = new GrainIndexProjector<TGrain, TState>(definition);
        Tree = grainFactory.GetGrain<ILattice>(options.Get(definition.Name).TreeName);
    }

    /// <summary>The projector that computes this index's entries.</summary>
    public GrainIndexProjector<TGrain, TState> Projector { get; }

    /// <summary>The index's backing lattice tree.</summary>
    public ILattice Tree { get; }

    /// <summary>
    /// Projects <paramref name="state"/>, reconciles it against
    /// <paramref name="previous"/>, and applies the result atomically.
    /// </summary>
    /// <param name="previous">
    /// The projection last written for this grain. Pass
    /// <see cref="GrainIndexProjection.Empty(string)"/> when the grain has
    /// never been indexed. Must not be <c>null</c>.
    /// </param>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="state">The grain state to project. Must not be <c>null</c>.</param>
    /// <param name="operationId">
    /// An optional idempotency key for the atomic batch. Supply a stable value
    /// and reuse it across retries of the <i>same</i> plan so a transport
    /// timeout re-attaches to the original batch instead of re-running it;
    /// leave it <c>null</c> to have one generated per call.
    /// </param>
    /// <param name="cancellationToken">Cancels before the batch is submitted.</param>
    /// <returns>
    /// The projection now held in the tree. Persist it as the baseline for the
    /// next update.
    /// </returns>
    /// <exception cref="ArgumentNullException">Any required argument is <c>null</c>.</exception>
    public async Task<GrainIndexProjection> UpdateAsync(
        GrainIndexProjection previous,
        string grainKey,
        TState state,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        var plan = Projector.Plan(previous, grainKey, state);
        await ApplyAsync(plan, operationId, cancellationToken).ConfigureAwait(false);
        return plan.Projection;
    }

    /// <summary>
    /// Projects <paramref name="state"/> for <paramref name="grainId"/>,
    /// reconciles it against <paramref name="previous"/>, and applies the
    /// result atomically.
    /// </summary>
    /// <param name="previous">The projection last written for this grain. Must not be <c>null</c>.</param>
    /// <param name="grainId">The indexed grain's identity.</param>
    /// <param name="state">The grain state to project. Must not be <c>null</c>.</param>
    /// <param name="operationId">An optional idempotency key for the atomic batch.</param>
    /// <param name="cancellationToken">Cancels before the batch is submitted.</param>
    /// <returns>The projection now held in the tree.</returns>
    /// <exception cref="ArgumentNullException">Any required argument is <c>null</c>.</exception>
    /// <exception cref="GrainIndexKeyEncodingException">The grain's key cannot be encoded by the definition's codec.</exception>
    public Task<GrainIndexProjection> UpdateAsync(
        GrainIndexProjection previous,
        GrainId grainId,
        TState state,
        string? operationId = null,
        CancellationToken cancellationToken = default) =>
        UpdateAsync(
            previous,
            Projector.Definition.KeyCodec.Encode(grainId),
            state,
            operationId,
            cancellationToken);

    /// <summary>
    /// Withdraws a grain from the index entirely, tombstoning every entry it
    /// contributed. Use it when the grain is deleted or its state cleared.
    /// </summary>
    /// <param name="previous">The projection last written for the grain. Must not be <c>null</c>.</param>
    /// <param name="operationId">An optional idempotency key for the atomic batch.</param>
    /// <param name="cancellationToken">Cancels before the batch is submitted.</param>
    /// <returns>A task that completes when the entries are gone.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="previous"/> is <c>null</c>.</exception>
    public Task RemoveAsync(
        GrainIndexProjection previous,
        string? operationId = null,
        CancellationToken cancellationToken = default) =>
        ApplyAsync(GrainIndexUpdatePlan.Removing(previous), operationId, cancellationToken);

    /// <summary>
    /// Applies <paramref name="plan"/> to the index's tree as a single
    /// all-or-nothing batch, so no reader observes a half-moved entry.
    /// </summary>
    /// <param name="plan">The plan to apply. Must not be <c>null</c>.</param>
    /// <param name="operationId">
    /// An optional idempotency key. When <c>null</c> a fresh one is generated,
    /// which is the right default for a plan computed from freshly-read state:
    /// a content-derived key would make a legitimate later batch over the same
    /// keys re-attach to this one instead of running.
    /// </param>
    /// <param name="cancellationToken">Cancels before the batch is submitted.</param>
    /// <returns>A task that completes when the batch commits, or immediately when the plan is empty.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="plan"/> is <c>null</c>.</exception>
    public Task ApplyAsync(
        GrainIndexUpdatePlan plan,
        string? operationId = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(plan);

        if (plan.IsEmpty)
            return Task.CompletedTask;

        return GrainIndexPlanApplier.ApplyAsync(
            Tree,
            plan,
            operationId ?? Guid.NewGuid().ToString("N"),
            cancellationToken);
    }
}
