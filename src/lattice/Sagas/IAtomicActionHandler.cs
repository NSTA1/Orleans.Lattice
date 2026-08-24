namespace Orleans.Lattice;

/// <summary>
/// The invocation context passed to a custom atomic-action handler's forward and
/// compensating effects. Carries the saga's idempotency key
/// (<see cref="OperationId"/>), the step's serialized <see cref="Args"/>, an
/// <see cref="IGrainFactory"/> for the handler to reach other grains, and the
/// <see cref="CancellationToken"/> for the current effect.
/// <para>
/// The same <see cref="OperationId"/> and <see cref="Args"/> are presented to both
/// the forward and the compensating effect, and both may run more than once across
/// a crash-resume, so a well-behaved handler makes each effect idempotent keyed on
/// <see cref="OperationId"/>.
/// </para>
/// </summary>
public interface IAtomicActionContext
{
    /// <summary>
    /// The saga's operation id (idempotency key). Stable across every forward and
    /// compensating invocation of this saga, including after a crash-resume; use it
    /// to make each effect idempotent.
    /// </summary>
    string OperationId { get; }

    /// <summary>
    /// The step's opaque, Orleans-serializable argument payload, exactly as the
    /// caller supplied it when building the plan. The same bytes are presented to
    /// the forward and the compensating effect.
    /// </summary>
    ReadOnlyMemory<byte> Args { get; }

    /// <summary>
    /// A grain factory the handler can use to invoke other grains as part of its
    /// forward or compensating effect.
    /// </summary>
    IGrainFactory GrainFactory { get; }

    /// <summary>The cancellation token for the current effect invocation.</summary>
    CancellationToken CancellationToken { get; }
}

/// <summary>
/// A caller-registered, named pair of forward and compensating effects an
/// <see cref="IAtomicActionGrain"/> saga can run as a custom step. A handler is
/// registered once at silo start through <c>AddLatticeAtomicAction</c> and is
/// resolved by its <see cref="HandlerId"/>; a saga step never carries the handler
/// itself, only its id and args, which is what makes a plan safe to persist and
/// replay.
/// <para>
/// <b>Compensation is the caller's contract.</b> <see cref="CompensateAsync"/> must
/// fully and idempotently undo the effect of a successful <see cref="ForwardAsync"/>
/// for the same <see cref="IAtomicActionContext.OperationId"/> and args. If it
/// cannot, the saga parks in
/// <see cref="AtomicActionStatus.CompensationFailed"/> and requires operator
/// intervention.
/// </para>
/// </summary>
public interface IAtomicActionHandler
{
    /// <summary>
    /// The stable id this handler is registered and resolved under. Must match the
    /// id used to register the handler and the id a plan step names.
    /// </summary>
    string HandlerId { get; }

    /// <summary>
    /// An opaque version tag for the handler's effect contract. The coordinator
    /// stamps this into each step when the saga starts and re-checks it on a
    /// crash-resume; if a redeploy changes a handler's tag while a saga is in
    /// flight, the saga parks rather than replaying a changed effect. Bump the tag
    /// whenever the forward/compensate semantics change in a way that is unsafe to
    /// replay against a partially completed saga.
    /// </summary>
    string VersionTag { get; }

    /// <summary>
    /// Runs the step's forward effect. Should be idempotent keyed on
    /// <see cref="IAtomicActionContext.OperationId"/> so a crash-resume that
    /// re-invokes it does not double-apply. A thrown exception pivots the saga into
    /// reverse-order compensation of the already-committed steps.
    /// </summary>
    /// <param name="context">The invocation context (operation id, args, grain factory, cancellation).</param>
    /// <returns>A task that completes when the forward effect has committed.</returns>
    Task ForwardAsync(IAtomicActionContext context);

    /// <summary>
    /// Runs the step's compensating effect, undoing a previously successful
    /// <see cref="ForwardAsync"/> for the same operation id and args. Must be
    /// idempotent. A thrown exception (after the coordinator's retry budget) parks
    /// the saga in <see cref="AtomicActionStatus.CompensationFailed"/>.
    /// </summary>
    /// <param name="context">The invocation context (operation id, args, grain factory, cancellation).</param>
    /// <returns>A task that completes when the compensating effect has committed.</returns>
    Task CompensateAsync(IAtomicActionContext context);
}
