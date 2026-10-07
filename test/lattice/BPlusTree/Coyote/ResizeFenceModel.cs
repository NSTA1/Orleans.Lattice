using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which fix a <see cref="ResizeFenceModel"/> run removes. <see cref="None"/> is
/// the shipping design.
/// </summary>
public enum ResizeFenceGuard
{
    /// <summary>The shipping design: fence before flip, the bound saga admitted, the fence kept after a landed flip.</summary>
    None,

    /// <summary>
    /// The routing tier does not hand the saga's binding to the fenced copy's
    /// shards, as before #4376, so a fenced shard refuses the bound batch.
    /// </summary>
    BoundSagaRefusedByFence,

    /// <summary>
    /// The resize lifts the old copy's fence after any failed flip, without
    /// asking the registry whether the flip landed anyway.
    /// </summary>
    LiftIgnoresRegistry,

    /// <summary>The alias flips before the old copy is fenced, as before #4362.</summary>
    FlipBeforeFence,

    /// <summary>
    /// A fenced shard admits a stale router's call as if it were the bound saga's
    /// prepared batch: the fence's refusal arm is gone, which is what makes the
    /// fence a fence (#4362).
    /// </summary>
    FenceAdmitsAStaleCall,
}

/// <summary>
/// The assertions a <see cref="ResizeFenceModel"/> run checks. A specificity test
/// disables exactly one, to show a guard is caught by the assertion it targets
/// and no other.
/// </summary>
[Flags]
public enum ResizeFenceAssertions
{
    /// <summary>Check nothing (used only with a single flag removed).</summary>
    None = 0,

    /// <summary>The saga's batch lands on every shard of the old copy or on none (#4369).</summary>
    BatchWholeOnOldCopy = 1,

    /// <summary>
    /// A router whose cached pair names the old copy is never served by it after
    /// the resized copy took a write (#4362). Whether a fenced shard serves the
    /// router's call is decided by <see cref="ResizeFence.AdmitsBoundSaga"/>.
    /// </summary>
    NoStaleReadAfterFlip = 2,

    /// <summary>Both assertions.</summary>
    All = BatchWholeOnOldCopy | NoStaleReadAfterFlip,
}

/// <summary>
/// A Coyote concurrency model of an online resize's swap of physical copy T for
/// the resized copy R, interleaved with an atomic-write saga bound to T that is
/// dispatching its batch shard by shard, with a client writing through the
/// alias, and with a router whose cached pair still names T.
/// <para>
/// Every admission decision is the real <see cref="ResizeFence"/> rule:
/// <see cref="ResizeFence.AdmitsBoundSaga"/> decides whether a fenced shard of T
/// takes the saga's prepare, and <see cref="ResizeFence.LiftsFenceAfterFailedFlip"/>
/// decides whether a failed flip lifts T's fence. This is the implementation-level
/// counterpart of the shard-ownership specification's <c>ResizeFence</c>,
/// <c>ResizeFlip</c>, <c>ResizeFlipRefused</c> and <c>SagaPrepare</c> actions.
/// </para>
/// <para>
/// A flip has three outcomes, each a controlled choice: it lands; it is refused
/// (the alias does not move); or it fails in transport after landing (the alias
/// moved, but the coordinator saw a failure). The third is the one
/// <c>LiftFenceUnlessSwappedAsync</c> exists for. Refusals are bounded so every
/// schedule terminates.
/// </para>
/// </summary>
public sealed class ResizeFenceModel : ICoyoteModel
{
    private const string Old = "tree";
    private const string Resized = "tree/resized/op";
    private const int MaxFailedFlips = 2;

    private readonly int _shardCount;
    private readonly ResizeFenceGuard _guard;
    private readonly ResizeFenceAssertions _assertions;

    /// <summary>
    /// Creates the model for an old copy of <paramref name="shardCount"/> shards,
    /// removing the fix <paramref name="guard"/> names, and checking
    /// <paramref name="assertions"/>.
    /// </summary>
    public ResizeFenceModel(int shardCount, ResizeFenceGuard guard, ResizeFenceAssertions assertions = ResizeFenceAssertions.All)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(shardCount, 2);
        _shardCount = shardCount;
        _guard = guard;
        _assertions = assertions;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        var fenced = new bool[_shardCount];
        var preparedOnOld = new bool[_shardCount];
        var alias = Old;
        var resizedTookWrite = false;

        var sagaNext = 0;
        var fenceNext = 0;
        var failedFlips = 0;
        var flipped = false;

        while (sagaNext < _shardCount || !flipped || fenceNext < _shardCount)
        {
            var sagaCanStep = sagaNext < _shardCount;
            var resizeCanStep = !flipped || fenceNext < _shardCount;
            if (sagaCanStep && (!resizeCanStep || runtime.RandomBoolean()))
            {
                var shard = sagaNext++;
                var admitted = !fenced[shard]
                    || ResizeFence.AdmitsBoundSaga(
                        rejecting: true,
                        // A prepared batch, never a terminal: the terminal arm of
                        // the rule is pinned by ResizeFenceTests.
                        directTerminal: false,
                        preparedScope: true,
                        boundPhysicalTreeId: _guard == ResizeFenceGuard.BoundSagaRefusedByFence ? null : Old,
                        physicalTreeId: Old);
                preparedOnOld[shard] = admitted;
            }
            else
            {
                ResizeStep();
            }

            if (alias == Resized && runtime.RandomBoolean())
            {
                resizedTookWrite = true;
            }

            ProbeStaleRouter(runtime.RandomBoolean() ? 0 : _shardCount - 1);
        }

        if ((_assertions & ResizeFenceAssertions.BatchWholeOnOldCopy) != 0)
        {
            var held = preparedOnOld.Count(p => p);
            Specification.Assert(
                held == 0 || held == _shardCount,
                $"the bound batch landed on {held} of {_shardCount} shards of the old copy (a partial batch an undo would re-expose)");
        }

        void ResizeStep()
        {
            if (_guard == ResizeFenceGuard.FlipBeforeFence && alias == Old && !flipped)
            {
                Flip();
                return;
            }

            if (fenceNext < _shardCount)
            {
                fenced[fenceNext++] = true;
                return;
            }

            Flip();
        }

        void Flip()
        {
            // A swap resumed after its flip landed skips the flip (SwapAliasAsync
            // compares the current alias with the resized copy first).
            if (alias == Resized)
            {
                flipped = true;
                return;
            }

            if (failedFlips >= MaxFailedFlips || runtime.RandomBoolean())
            {
                alias = Resized;
                flipped = true;
                return;
            }

            failedFlips++;
            if (runtime.RandomBoolean())
            {
                // The flip failed in transport after the registry took it.
                alias = Resized;
            }

            var lift = _guard == ResizeFenceGuard.LiftIgnoresRegistry
                || ResizeFence.LiftsFenceAfterFailedFlip(resolvedPhysicalTreeId: alias, resizedPhysicalTreeId: Resized);
            if (lift)
            {
                // ExitRejectingAsync on every shard; the next phase tick fences again.
                Array.Clear(fenced);
                fenceNext = 0;
            }
        }

        void ProbeStaleRouter(int shard)
        {
            if ((_assertions & ResizeFenceAssertions.NoStaleReadAfterFlip) == 0)
            {
                return;
            }

            // The stale router's call is routed through the logical alias, so it is
            // never a direct terminal; it is a read, a plain write, or a prepare of
            // another saga bound to the resized copy, never the bound saga's.
            var prepared = runtime.RandomBoolean();
            var served = !fenced[shard]
                || ResizeFence.AdmitsBoundSaga(
                    rejecting: true,
                    directTerminal: false,
                    preparedScope: prepared,
                    boundPhysicalTreeId: _guard == ResizeFenceGuard.FenceAdmitsAStaleCall ? Old : (prepared ? Resized : null),
                    physicalTreeId: Old);
            Specification.Assert(
                !(alias == Resized && resizedTookWrite && served),
                $"a router whose cached pair names the old copy was served by shard {shard} after the resized copy took a write it never mirrors back");
        }
    }
}
