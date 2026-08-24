namespace Orleans.Lattice;

/// <summary>
/// A public, generic, all-or-nothing atomic-action coordinator (a saga / TCC
/// coordinator), keyed by a caller-supplied operation id. It runs an ordered plan
/// of steps - each a forward effect paired with a compensating effect - and commits
/// all-or-nothing: if a forward step faults, every already-committed step is
/// compensated in strict reverse order, so the action leaves no partial effect
/// behind. It generalizes the key-only atomic write to arbitrary caller-defined
/// effects registered as named handlers, and offers a built-in tree-write step that
/// delegates to the verified atomic-write machinery for atomicity.
/// <para>
/// Resolve a coordinator by operation id through the grain factory, for example
/// <c>grainFactory.GetGrain&lt;IAtomicActionGrain&gt;("order-4711")</c>. The
/// operation id is the idempotency key: re-issuing the same plan under the same id
/// after the saga is terminal returns the memoized outcome without re-running any
/// effect. Build a plan with <see cref="AtomicActionPlanBuilder"/>, and register
/// custom handlers at silo start with <c>AddLatticeAtomicAction</c>.
/// </para>
/// <para>
/// <b>Atomicity, precisely.</b> A built-in tree-write step inherits the tree's
/// verified atomic-write guarantee (a single-tree write is atomic; a cross-tree
/// write routes through the cross-tree two-phase-commit coordinator), so it either
/// fully applies or fully rolls back to its captured pre-image. A custom step
/// provides best-effort, eventually-consistent saga compensation whose correctness
/// depends on the caller's compensating effect honouring its contract. The saga
/// does not make custom steps two-phase; it makes the whole plan all-or-nothing by
/// compensation.
/// </para>
/// </summary>
[Alias(TypeAliases.IAtomicActionGrain)]
public interface IAtomicActionGrain : IGrainWithStringKey
{
    /// <summary>
    /// Runs the supplied plan all-or-nothing under this grain's operation id and
    /// returns the terminal <see cref="AtomicActionOutcome"/>. Runs each step's
    /// forward effect in order; on a forward fault, compensates every
    /// already-committed step in strict reverse order and returns
    /// <see cref="AtomicActionStatus.Compensated"/>. The saga is durable: it
    /// checkpoints after every step transition and resumes from its persisted state
    /// after a crash, reaching its terminal outcome exactly once.
    /// <para>
    /// Idempotent per operation id: a repeat call after the saga is terminal
    /// returns the memoized outcome without re-running effects. A repeat call while
    /// the saga is still running returns the outcome once it settles.
    /// </para>
    /// </summary>
    /// <param name="plan">The ordered plan of forward/compensating steps to run.</param>
    /// <returns>
    /// The terminal outcome: <see cref="AtomicActionStatus.Committed"/> when every
    /// forward step committed, or <see cref="AtomicActionStatus.Compensated"/> when
    /// a forward fault was cleanly rolled back.
    /// </returns>
    /// <exception cref="System.ArgumentNullException"><paramref name="plan"/> is <see langword="null"/>.</exception>
    /// <exception cref="System.ArgumentException">
    /// The plan is empty, exceeds <see cref="LatticeOptions.MaxAtomicActionSteps"/>,
    /// a custom step has an empty handler id, or a step is malformed for its kind.
    /// </exception>
    /// <exception cref="AtomicActionHandlerNotRegisteredException">
    /// A custom step names a handler id that is not registered on this silo. Handler
    /// resolution fails closed.
    /// </exception>
    /// <exception cref="System.ArgumentOutOfRangeException">
    /// A custom step's args payload exceeds
    /// <see cref="LatticeOptions.MaxAtomicActionArgsBytes"/>.
    /// </exception>
    /// <exception cref="CompensationFailedException">
    /// A forward step faulted and a compensating effect itself faulted; the saga
    /// parked in <see cref="AtomicActionStatus.CompensationFailed"/>.
    /// </exception>
    Task<AtomicActionOutcome> ExecuteAsync(AtomicActionPlan plan);

    /// <summary>
    /// Returns a point-in-time <see cref="AtomicActionOutcome"/> if the saga under
    /// this operation id has reached a terminal state, or <see langword="null"/> if
    /// no saga has run under this id or one is still in flight. For observability
    /// and idempotent status polling; never starts or mutates a saga.
    /// </summary>
    /// <returns>The memoized terminal outcome, or <see langword="null"/>.</returns>
    Task<AtomicActionOutcome?> TryGetOutcomeAsync();
}
