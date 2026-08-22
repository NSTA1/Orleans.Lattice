using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// A Coyote concurrency model of the multi-key atomic-visibility read gate,
/// driving the <b>production</b> <see cref="AtomicVisibilityGate.ResolveKey"/>
/// rule under systematic schedule exploration. It reproduces the reshard
/// split-view race (#1584): a reader fans out to two keys that both carry a
/// prepared mutation from the same saga, while the per-tree transaction
/// registry transitions that saga InFlight -> Committed.
/// <para>
/// The <paramref name="useSnapshot"/> toggle chooses between the two designs:
/// </para>
/// <list type="bullet">
///   <item><description>
///     <c>true</c> - the fix: the reader resolves <b>both</b> keys against a
///     single captured registry decision (as <c>SnapshotPendingForReadAsync</c> /
///     <c>TxDecisionView</c> do), so the commit cannot fall mid-fan-out. The
///     observation is always all-or-nothing.
///   </description></item>
///   <item><description>
///     <c>false</c> - the regression: the reader reads the registry live once
///     per key, so a commit landing between the two reads makes one key surface
///     its prepared value while the sibling still hides it - a split view.
///   </description></item>
/// </list>
/// The safety property (both keys visible, or neither) is asserted with the
/// same <see cref="AtomicVisibilityGate"/> the leaf grain runs, so a violation
/// Coyote finds is a violation of the code that actually drives Orleans.
/// </summary>
public sealed class AtomicCommitVisibilityModel(bool useSnapshot) : ICoyoteModel
{
    public void Run(ICoyoteRuntime runtime)
    {
        // Registry decision for the saga, flipped from InFlight to Committed at
        // an explored point. Monotonic: once committed, it stays committed.
        var committed = false;

        void MaybeCommit()
        {
            if (!committed && runtime.RandomBoolean())
            {
                committed = true;
            }
        }

        // The commit may already have landed before the reader begins its fan-out.
        MaybeCommit();

        TxStatus statusForKey1;
        TxStatus statusForKey2;

        if (useSnapshot)
        {
            // One consistent registry view resolves every key of the fan-out.
            var snapshot = committed ? TxStatus.Committed : TxStatus.InFlight;
            statusForKey1 = snapshot;
            statusForKey2 = snapshot;
        }
        else
        {
            // Live per-key reads: the commit can fall between the two.
            statusForKey1 = committed ? TxStatus.Committed : TxStatus.InFlight;
            MaybeCommit();
            statusForKey2 = committed ? TxStatus.Committed : TxStatus.InFlight;
        }

        // Both keys carry a live (non-tombstone) prepared value from a saga that
        // is not an already-terminal orphan, so the gate surfaces the prepared
        // value exactly when the saga is committed.
        var key1Visible = AtomicVisibilityGate.ResolveKey(statusForKey1, alreadyTerminal: false, preparedHiddenByTombstoneOrExpiry: false)
            == PendingReadOutcome.SurfacePrepared;
        var key2Visible = AtomicVisibilityGate.ResolveKey(statusForKey2, alreadyTerminal: false, preparedHiddenByTombstoneOrExpiry: false)
            == PendingReadOutcome.SurfacePrepared;

        Specification.Assert(
            key1Visible == key2Visible,
            $"atomic-visibility split: key1Visible={key1Visible}, key2Visible={key2Visible} " +
            "(a reader observed one key of the saga but not the other)");
    }
}
