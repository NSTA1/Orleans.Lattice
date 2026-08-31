using Orleans.Runtime;

namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// The enrolment path for one index, seen without its grain interface type.
/// </summary>
/// <remarks>
/// <para>
/// An indexed state object knows the state type it holds but not the grain
/// interface the index was declared over - the two are chosen independently, and
/// several indexes over the same state may target different grain interfaces. It
/// therefore works against this facade, and
/// <see cref="TypedGrainIndexEnroller{TGrain, TState}"/> supplies the grain
/// type. Closing that generic happens once per index at silo setup, never on
/// the write path.
/// </para>
/// <para>
/// The four-step shape - read the baseline, plan, record the intent, apply and
/// confirm - is deliberately exposed rather than hidden behind a single method,
/// because the grain's own state commit has to happen <i>between</i> recording
/// the intent and applying it. That ordering is the whole reason the outbox
/// closes the window it does.
/// </para>
/// </remarks>
/// <typeparam name="TState">The grain-state type the index projects from.</typeparam>
internal abstract class GrainIndexEnroller<TState>
{
    /// <summary>The logical name of the index this enrols into.</summary>
    public abstract string IndexName { get; }

    /// <summary>
    /// The index's pre-built telemetry tag, so a recording site on the write
    /// path never has to build one.
    /// </summary>
    public abstract KeyValuePair<string, object?> IndexTag { get; }

    /// <summary>When this index publishes entries relative to the state write.</summary>
    public abstract GrainIndexProjectionMode Mode { get; }

    /// <summary>
    /// Whether <paramref name="grainInstance"/> implements the grain interface
    /// this index was declared over.
    /// </summary>
    /// <param name="grainInstance">The activating grain, which may be <c>null</c>.</param>
    /// <returns><c>true</c> when the index applies to the grain.</returns>
    public abstract bool AppliesTo(object? grainInstance);

    /// <summary>Encodes a grain identity the way this index's entries key it.</summary>
    /// <param name="grainId">The grain's identity.</param>
    /// <returns>The encoded grain key.</returns>
    /// <exception cref="GrainIndexKeyEncodingException">The identity cannot be encoded by this index's codec.</exception>
    public abstract string EncodeKey(GrainId grainId);

    /// <summary>
    /// Reads the projection the index is known to hold for a grain, or
    /// <c>null</c> when the grain has never been enrolled.
    /// </summary>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The confirmed projection, or <c>null</c>.</returns>
    public abstract Task<GrainIndexProjection?> ReadBaselineAsync(
        string grainKey,
        CancellationToken cancellationToken);

    /// <summary>
    /// Projects <paramref name="state"/> and reconciles it against
    /// <paramref name="previous"/>. Allocates no upsert or tombstone list when
    /// nothing the index projects has changed.
    /// </summary>
    /// <param name="previous">The confirmed projection. Must not be <c>null</c>.</param>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="state">The grain state to project.</param>
    /// <returns>The plan that brings the index in line with the state.</returns>
    public abstract GrainIndexUpdatePlan Plan(
        GrainIndexProjection previous,
        string grainKey,
        TState state);

    /// <summary>
    /// Durably records <paramref name="plan"/> in the outbox before it is
    /// attempted, so a failure or a silo stop after this point is recoverable
    /// rather than invisible.
    /// </summary>
    /// <param name="plan">The plan about to be applied. Must not be <c>null</c>.</param>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>The recorded entry, to be handed back to <see cref="CommitAsync"/>.</returns>
    public abstract Task<GrainIndexPendingProjection> BeginAsync(
        GrainIndexUpdatePlan plan,
        string grainKey,
        CancellationToken cancellationToken);

    /// <summary>
    /// Applies a recorded plan to the index tree and, once it has committed,
    /// records the resulting projection as confirmed and clears the outbox
    /// entry.
    /// </summary>
    /// <param name="pending">The entry returned by <see cref="BeginAsync"/>. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels before the batch is submitted.</param>
    /// <returns>A task that completes when the entries and the marker are durable.</returns>
    public abstract Task CommitAsync(
        GrainIndexPendingProjection pending,
        CancellationToken cancellationToken);

    /// <summary>
    /// Records a grain as enrolled without an index write, for the one case
    /// where a grain contributes no entries yet still has to be marked so the
    /// backfill does not keep revisiting it.
    /// </summary>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="projection">The projection the index holds. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the marker is durable.</returns>
    public abstract Task MarkEnrolledAsync(
        string grainKey,
        GrainIndexProjection projection,
        CancellationToken cancellationToken);

    /// <summary>
    /// Removes a grain's seen marker and any outstanding outbox entry, for a
    /// grain that has left the index.
    /// </summary>
    /// <param name="grainKey">The encoded grain key. Must not be <c>null</c>.</param>
    /// <param name="cancellationToken">Cancels the write.</param>
    /// <returns>A task that completes when the markers are gone.</returns>
    public abstract Task WithdrawAsync(string grainKey, CancellationToken cancellationToken);
}
