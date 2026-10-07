using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which alias swap a <see cref="SagaCopyBindingModel"/> run interleaves with the
/// saga.
/// </summary>
public enum SagaCopyBindingSwap
{
    /// <summary>
    /// An online resize's flip from T to R. T mirrors every mutation into R
    /// throughout, so a saga bound to T may stay bound across the flip (#4369).
    /// </summary>
    ResizeFlip,

    /// <summary>
    /// A resize undo's swap from R back to T. R mirrors nothing into T, so a saga
    /// bound to R must re-bind to T and re-dispatch there (#4357, #4358).
    /// </summary>
    UndoSwap,
}

/// <summary>
/// Which fix a <see cref="SagaCopyBindingModel"/> run removes.
/// </summary>
public enum SagaCopyBindingGuard
{
    /// <summary>The shipping design.</summary>
    None,

    /// <summary>The routing tier places the batch on whichever copy its cached pair names, ignoring the binding (#4358).</summary>
    RouterIgnoresBinding,

    /// <summary>The pre-decision check re-binds without asking where the bound copy mirrors (#4369, before #4376).</summary>
    PreDecisionIgnoresMirror,

    /// <summary>
    /// The routing tier refuses a bound dispatch whenever the tree moved, and the
    /// execute phase then re-binds, both without asking where the bound copy
    /// mirrors (#4454 before its fix; the specification's
    /// <c>SagaBatchOnOneCopyRebindIgnoresMirror</c>).
    /// </summary>
    RebindIgnoresMirror,

    /// <summary>
    /// The pre-decision check stays bound whenever the tree moved, as if the bound
    /// copy always mirrored into the resolved one, so a saga bound to a copy an
    /// undo discards decides there.
    /// </summary>
    PreDecisionAlwaysStaysBound,

    /// <summary>A dispatch the routing tier refused never makes the saga re-bind, so the saga stops making progress.</summary>
    RefusalNeverRebinds,
}

/// <summary>
/// The assertions a <see cref="SagaCopyBindingModel"/> run checks at the commit
/// decision.
/// </summary>
[Flags]
public enum SagaCopyBindingAssertions
{
    /// <summary>Check nothing.</summary>
    None = 0,

    /// <summary>Every key of the batch has a prepared bucket on the bound copy.</summary>
    BatchOnBoundCopy = 1,

    /// <summary>No copy that can still serve the tree holds a bucket, except the bound copy and the copy it mirrors into.</summary>
    NoBucketOffTheBoundCopy = 2,

    /// <summary>The copy the saga decides on can still serve the tree: it is the alias, or it mirrors into the alias.</summary>
    BoundCopyLive = 4,

    /// <summary>
    /// A dispatch the routing tier refused leaves the saga bound to the copy the
    /// tree resolves to, so its next dispatch is admitted: a refusal makes progress.
    /// </summary>
    RefusalMakesProgress = 8,

    /// <summary>Every assertion.</summary>
    All = BatchOnBoundCopy | NoBucketOffTheBoundCopy | BoundCopyLive | RefusalMakesProgress,
}

/// <summary>
/// A Coyote concurrency model of an atomic-write saga's binding to a physical copy
/// (<c>AtomicWriteState.BoundPhysicalTreeId</c>) across one alias swap. The saga
/// dispatches its batch through stateless routing activations whose cached pair
/// may predate the swap, then checks the binding immediately before its decision.
/// <para>
/// Every binding decision is the real <see cref="SagaCopyBinding"/> rule: the
/// routing tier's <see cref="SagaCopyBinding.AdmitsDispatch"/> against the cached
/// pair and <see cref="SagaCopyBinding.DispatchCopy"/> against a refreshed one, the
/// execute phase's <see cref="SagaCopyBinding.RebindsAfterRefusal"/> and
/// <see cref="SagaCopyBinding.AfterRefusal"/>, and the pre-decision
/// <see cref="SagaCopyBinding.BeforeDecision"/>. This is the implementation-level
/// counterpart of the shard-ownership specification's <c>SagaPrepare</c>,
/// <c>SagaRebindOnRefusal</c>, <c>SagaRebindBeforeDecision</c> and
/// <c>SagaDecide</c>, and its assertions are that specification's
/// <c>SagaBatchOnOneCopy</c>.
/// </para>
/// <para>
/// <b>Scope.</b> The model leaves out the shard-level fence and redirect that
/// <see cref="ResizeFenceModel"/> drives, so the binding rules are checked on
/// their own: with both layers present each masks the other's removal, which the
/// specification's <c>SagaBatchOnOneCopyRouterIgnoresBinding</c> mutation records.
/// A dispatch is one routing-tier call that places the whole remaining batch on
/// one copy, as <c>ILattice.SetManyAsync</c> does, unless
/// <c>partialDispatch</c> is set: a transient shard failure can then stop it part
/// way, which is where #4454 lived - the routing tier refused the rest of the
/// batch and the execute phase re-bound, leaving the prepares already taken on a
/// copy that mirrors into the new one.
/// </para>
/// </summary>
public sealed class SagaCopyBindingModel : ICoyoteModel
{
    private const string T = "tree";
    private const string R = "tree/resized/op";

    private readonly int _keyCount;
    private readonly SagaCopyBindingSwap _swap;
    private readonly SagaCopyBindingGuard _guard;
    private readonly bool _partialDispatch;
    private readonly SagaCopyBindingAssertions _assertions;

    /// <summary>Creates the model.</summary>
    public SagaCopyBindingModel(
        int keyCount,
        SagaCopyBindingSwap swap,
        SagaCopyBindingGuard guard,
        bool partialDispatch = false,
        SagaCopyBindingAssertions assertions = SagaCopyBindingAssertions.All)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(keyCount, 2);
        _keyCount = keyCount;
        _swap = swap;
        _guard = guard;
        _partialDispatch = partialDispatch;
        _assertions = assertions;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // The copy the tree resolves to before the swap, and after it. Before an
        // undo the resized copy R is live; before a flip the old copy T is.
        var from = _swap == SagaCopyBindingSwap.ResizeFlip ? T : R;
        var to = _swap == SagaCopyBindingSwap.ResizeFlip ? R : T;
        var alias = from;
        var swapped = false;

        // Routers that cached a pair before the swap name `from`; after it, a
        // router may hold either. A router holding the copy the tree left before
        // this resize began is refused by that copy's fence or redirect, which
        // this model leaves out (see Scope), so it is not offered.
        var bucket = new Dictionary<string, bool[]>
        {
            [T] = new bool[_keyCount],
            [R] = new bool[_keyCount],
        };

        var bound = alias;
        var dispatched = 0;
        var decided = false;

        // Ends a run whose refusals make no progress, which RefusalMakesProgress
        // reports at the first such refusal; the fixed design never reaches it.
        var attempts = 0;
        var attemptBound = (4 * _keyCount) + 4;

        while (!decided)
        {
            if (++attempts > attemptBound)
            {
                return;
            }

            if (!swapped && runtime.RandomBoolean())
            {
                alias = to;
                swapped = true;
                continue;
            }

            if (dispatched < _keyCount)
            {
                Dispatch(cached: swapped && runtime.RandomBoolean() ? from : alias);
                continue;
            }

            var verdict = SagaCopyBinding.BeforeDecision(
                bound,
                alias,
                _guard switch
                {
                    SagaCopyBindingGuard.PreDecisionIgnoresMirror => null,
                    // Pretends the bound copy mirrors into whatever the tree resolves to.
                    SagaCopyBindingGuard.PreDecisionAlwaysStaysBound => alias,
                    _ => MirrorDestination(bound),
                });
            if (verdict == SagaCopyBindingVerdict.Rebind)
            {
                bound = alias;
                dispatched = 0;
                continue;
            }

            decided = true;
        }

        CheckAtDecision();

        void Dispatch(string cached)
        {
            // The routing tier checks its cached pair, then re-reads the registry
            // once and places the batch on the copy DispatchCopy names - the bound
            // copy itself when it mirrors into the resolved one (#4454) - or
            // refuses.
            var copy = cached;
            var admitted = _guard == SagaCopyBindingGuard.RouterIgnoresBinding
                || SagaCopyBinding.AdmitsDispatch(bound, copy);
            if (!admitted)
            {
                var placed = SagaCopyBinding.DispatchCopy(bound, alias, KnownMirror(bound));
                if (placed is null)
                {
                    // The refusal names the bound copy as the stale one; the saga
                    // resolves afresh and stays bound or re-binds.
                    if (_guard != SagaCopyBindingGuard.RefusalNeverRebinds
                        && SagaCopyBinding.RebindsAfterRefusal(bound, bound)
                        && SagaCopyBinding.AfterRefusal(bound, alias, KnownMirror(bound)) == SagaCopyBindingVerdict.Rebind)
                    {
                        bound = alias;
                        dispatched = 0;
                    }

                    if ((_assertions & SagaCopyBindingAssertions.RefusalMakesProgress) != 0)
                    {
                        // Progress: the next dispatch is admitted, on the alias or on a
                        // bound copy that mirrors into it.
                        Specification.Assert(
                            bound == alias || MirrorDestination(bound) == alias,
                            $"a refused dispatch left the saga bound to {bound}, which the routing tier keeps refusing while the tree resolves to {alias}");
                    }

                    return;
                }

                copy = placed;
            }

            var end = _keyCount;
            if (_partialDispatch && dispatched < _keyCount - 1 && runtime.RandomBoolean())
            {
                // A transient shard failure: the keys before it are prepared, the
                // rest are not, and the execute phase will retry the remainder.
                end = dispatched + 1;
            }

            for (var key = dispatched; key < end; key++)
            {
                bucket[copy][key] = true;
                if (MirrorDestination(copy) is { } mirror)
                {
                    bucket[mirror][key] = true;
                }
            }

            dispatched = end;
        }

        string? MirrorDestination(string copy) =>
            _swap == SagaCopyBindingSwap.ResizeFlip && copy == T ? R : null;

        // Where the binding rules are told the bound copy mirrors: the truth,
        // unless the run removes the #4454 fix.
        string? KnownMirror(string copy) =>
            _guard == SagaCopyBindingGuard.RebindIgnoresMirror ? null : MirrorDestination(copy);

        void CheckAtDecision()
        {
            if ((_assertions & SagaCopyBindingAssertions.BoundCopyLive) != 0)
            {
                // An undone resize discards R, so a saga that decides bound to R
                // after the undo has its acknowledged commit discarded with it.
                var boundLive = bound == alias || MirrorDestination(bound) == alias;
                Specification.Assert(
                    boundLive,
                    $"the saga decided bound to {bound}, which the tree no longer resolves to and which mirrors nothing into {alias}");
            }

            if ((_assertions & SagaCopyBindingAssertions.BatchOnBoundCopy) != 0)
            {
                Specification.Assert(
                    bucket[bound].All(b => b),
                    $"the saga decided on {bound} without its whole batch prepared there");
            }

            if ((_assertions & SagaCopyBindingAssertions.NoBucketOffTheBoundCopy) != 0)
            {
                foreach (var copy in new[] { T, R })
                {
                    // An undone resize discards R, so R can no longer serve the tree.
                    var live = !(copy == R && _swap == SagaCopyBindingSwap.UndoSwap && swapped);
                    var allowed = copy == bound || copy == MirrorDestination(bound);
                    Specification.Assert(
                        !live || allowed || !bucket[copy].Any(b => b),
                        $"the saga decided bound to {bound} while {copy}, which can still serve the tree, holds part of its batch");
                }
            }
        }
    }
}
