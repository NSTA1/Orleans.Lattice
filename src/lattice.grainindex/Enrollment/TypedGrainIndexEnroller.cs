using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// The concrete enrolment path for one index, closed over the grain interface
/// the index was declared against.
/// </summary>
/// <remarks>
/// <para>
/// One instance per index per silo, shared by every grain in it: it holds the
/// projector, the tree reference, and the projection mode, all resolved once at
/// construction. Nothing here is per grain, so nothing here is re-resolved,
/// re-compiled, or re-read on a write.
/// </para>
/// <para>
/// It deliberately does not remember any grain's baseline. That belongs to the
/// activation that is writing, which holds it in its own slot and hands it back
/// on the next plan - a shared per-grain cache here would have to be invalidated
/// by every other silo's writes to be correct.
/// </para>
/// </remarks>
/// <typeparam name="TGrain">The indexed grain interface type.</typeparam>
/// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
internal sealed class TypedGrainIndexEnroller<TGrain, TState> : GrainIndexEnroller<TState>
    where TGrain : IGrain
{
    private readonly GrainIndexMaintainer<TGrain, TState> _maintainer;
    private readonly IGrainIndexEnrollmentStore _store;

    /// <summary>Initialises an enroller over an explicit tree.</summary>
    /// <param name="definition">The index definition. Must not be <c>null</c>.</param>
    /// <param name="tree">The index's backing lattice tree. Must not be <c>null</c>.</param>
    /// <param name="store">The registry-backed enrolment bookkeeping. Must not be <c>null</c>.</param>
    /// <param name="mode">When this index publishes entries relative to the state write.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public TypedGrainIndexEnroller(
        GrainIndexDefinition<TGrain, TState> definition,
        ILattice tree,
        IGrainIndexEnrollmentStore store,
        GrainIndexProjectionMode mode)
    {
        ArgumentNullException.ThrowIfNull(definition);
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(store);

        _maintainer = new GrainIndexMaintainer<TGrain, TState>(definition, tree);
        _store = store;
        IndexName = definition.Name;
        Mode = mode;
    }

    /// <inheritdoc />
    public override string IndexName { get; }

    /// <inheritdoc />
    public override GrainIndexProjectionMode Mode { get; }

    /// <summary>The maintainer that applies this index's plans.</summary>
    internal GrainIndexMaintainer<TGrain, TState> Maintainer => _maintainer;

    /// <inheritdoc />
    public override bool AppliesTo(object? grainInstance) => grainInstance is TGrain;

    /// <inheritdoc />
    public override string EncodeKey(GrainId grainId) =>
        _maintainer.Projector.Definition.KeyCodec.Encode(grainId);

    /// <inheritdoc />
    public override async Task<GrainIndexProjection?> ReadBaselineAsync(
        string grainKey,
        CancellationToken cancellationToken)
    {
        var record = await _store
            .ReadEnrollmentAsync(IndexName, grainKey, cancellationToken)
            .ConfigureAwait(true);

        return record?.Projection;
    }

    /// <inheritdoc />
    public override GrainIndexUpdatePlan Plan(
        GrainIndexProjection previous,
        string grainKey,
        TState state) =>
        _maintainer.Projector.Plan(previous, grainKey, state);

    /// <inheritdoc />
    public override async Task<GrainIndexPendingProjection> BeginAsync(
        GrainIndexUpdatePlan plan,
        string grainKey,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(plan);
        ArgumentNullException.ThrowIfNull(grainKey);

        // The idempotency key is generated once, here, and reused by every
        // retry of this exact plan. Deriving it from the plan's content instead
        // would make a grain that returns to a value it held before re-attach to
        // that earlier batch and silently do nothing.
        var pending = new GrainIndexPendingProjection(
            IndexName,
            grainKey,
            Guid.NewGuid().ToString("N"),
            plan);

        await _store.WritePendingAsync(pending, cancellationToken).ConfigureAwait(true);
        return pending;
    }

    /// <inheritdoc />
    public override async Task CommitAsync(
        GrainIndexPendingProjection pending,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(pending);

        await _maintainer
            .ApplyAsync(pending.Plan, pending.OperationId, cancellationToken)
            .ConfigureAwait(true);

        await _store
            .CompleteAsync(IndexName, pending.GrainKey, pending.Plan.Projection, cancellationToken)
            .ConfigureAwait(true);
    }

    /// <inheritdoc />
    public override Task MarkEnrolledAsync(
        string grainKey,
        GrainIndexProjection projection,
        CancellationToken cancellationToken) =>
        _store.CompleteAsync(IndexName, grainKey, projection, cancellationToken);

    /// <inheritdoc />
    public override Task WithdrawAsync(string grainKey, CancellationToken cancellationToken) =>
        _store.WithdrawAsync(IndexName, grainKey, cancellationToken);
}
