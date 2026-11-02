using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which retirement design a <see cref="WalPinRetirementModel"/> run drives, so
/// the guard tests can prove the fence and its ordering are load-bearing.
/// </summary>
public enum WalPinRetirementMode
{
    /// <summary>
    /// The fix (issue #5358): the retirement takes its token before it reads
    /// the leaf, and the removal goes through
    /// <see cref="Orleans.Lattice.WalOrphanPinRetirementCore.MayRemove"/>.
    /// </summary>
    Fenced,

    /// <summary>
    /// The guard removed: the removal is unconditional by consumer id, as
    /// production was before issue #5358.
    /// </summary>
    Unfenced,

    /// <summary>
    /// The fence with its ordering broken: the token is taken after the leaf
    /// read, so a merge between the read and the token is not fenced.
    /// </summary>
    TokenAfterRead,
}

/// <summary>
/// A Coyote concurrency model of the WAL GC's orphaned materialiser pin
/// retirement racing a purge-cleared leaf being populated again under the same
/// grain id (issue #5358), executing the production decision core
/// (<see cref="Orleans.Lattice.WalOrphanPinRetirementCore.Decide"/> and
/// <see cref="Orleans.Lattice.WalOrphanPinRetirementCore.MayRemove"/>).
/// <para>
/// The scenario starts where the defect does: the leaf's row has been cleared
/// by a purge and its pin survived (its own retirement was interrupted). The
/// retirement takes a token, reads the leaf's durable row, decides through the
/// core and removes through the fence. Concurrently, and only if the scheduler
/// chooses, the leaf is re-created: its tree id is persisted, then its birth
/// seed merges into the surviving entry (changing nothing a read could see),
/// then it takes a write it has not checkpointed. The scheduler interleaves the
/// two at every step.
/// </para>
/// <para>
/// The safety property is the <c>NoTrimPastLiveNeed</c> invariant of
/// <c>spec/wal/WalPinRetirement.tla</c>: once both are done, a leaf holding an
/// un-checkpointed write must still hold its pin, or the trim the floor then
/// permits loses the write.
/// </para>
/// </summary>
public sealed class WalPinRetirementModel : ICoyoteModel
{
    private readonly WalPinRetirementMode _mode;

    /// <summary>Creates the model for the chosen retirement <paramref name="mode"/>.</summary>
    /// <param name="mode">The retirement design to drive.</param>
    public WalPinRetirementModel(WalPinRetirementMode mode)
    {
        _mode = mode;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        var epoch = Guid.NewGuid();
        long mergeSequence = 1;
        long? lastMerged = 1;
        var pinPresent = true;

        var husk = true;
        var recreate = runtime.RandomBoolean();
        var leafStep = 0;
        var needsReplay = false;

        WalMaterialiserPinToken? token = null;
        WalGcBlockingPinState? read = null;
        var retirementStep = 0;

        while (retirementStep < 3 || (recreate && leafStep < 3))
        {
            var advanceRetirement = retirementStep < 3
                && (!(recreate && leafStep < 3) || runtime.RandomBoolean());
            if (advanceRetirement)
            {
                switch (retirementStep)
                {
                    case 0:
                        if (_mode == WalPinRetirementMode.TokenAfterRead)
                        {
                            read = ReadLeaf(husk);
                        }
                        else
                        {
                            token = new WalMaterialiserPinToken(epoch, mergeSequence);
                        }

                        break;
                    case 1:
                        if (_mode == WalPinRetirementMode.TokenAfterRead)
                        {
                            token = new WalMaterialiserPinToken(epoch, mergeSequence);
                        }
                        else
                        {
                            read = ReadLeaf(husk);
                        }

                        break;
                    default:
                        // The gate verdict and the budget are pinned to the
                        // arms that remove (#2319): every other arm keeps the
                        // pin, which cannot produce the violation hunted here,
                        // and WalOrphanPinRetirementCoreTests covers them. The
                        // pin grain's epoch is pinned too: a reactivation
                        // between token and removal refuses, the safe direction.
                        var decision = Orleans.Lattice.WalOrphanPinRetirementCore.Decide(
                            read!.Value, Orleans.Lattice.ConsumerIdVerdict.LeafPublished, budgetAvailable: true);
                        if (decision == Orleans.Lattice.OrphanPinRemovalDecision.Retire
                            && (_mode == WalPinRetirementMode.Unfenced
                                || Orleans.Lattice.WalOrphanPinRetirementCore.MayRemove(token!.Value, epoch, lastMerged)))
                        {
                            pinPresent = false;
                            lastMerged = null;
                        }

                        break;
                }

                retirementStep++;
            }
            else
            {
                switch (leafStep)
                {
                    case 0:
                        // SetTreeIdAsync persists the tree id before the seed.
                        husk = false;
                        break;
                    case 1:
                        // The birth seed merges a Zero block pin; into a
                        // surviving entry the monotone merge changes nothing.
                        lastMerged = ++mergeSequence;
                        pinPresent = true;
                        break;
                    default:
                        needsReplay = true;
                        break;
                }

                leafStep++;
            }
        }

        Specification.Assert(
            !needsReplay || pinPresent,
            $"the re-created leaf holds an un-checkpointed write but its pin was retired ({_mode}): "
            + "the trim floor no longer holds for it and the write can be trimmed");
    }

    private static WalGcBlockingPinState ReadLeaf(bool husk) =>
        husk ? WalGcBlockingPinState.Orphaned : WalGcBlockingPinState.CheckpointedUncovered;
}
