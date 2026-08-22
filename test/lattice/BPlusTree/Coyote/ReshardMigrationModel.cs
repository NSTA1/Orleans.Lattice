using Microsoft.Coyote.Runtime;
using Microsoft.Coyote.Specifications;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.Testing.Coyote;

namespace Orleans.Lattice.Tests.BPlusTree.Coyote;

/// <summary>
/// Which orphan guard (if any) a <see cref="ReshardMigrationModel"/> run keeps in
/// place, so the safety test can prove the guards are load-bearing by removing each
/// one and asserting Coyote re-finds the #1584 regression.
/// </summary>
public enum ReshardGuardMode
{
    /// <summary>
    /// The fix: both orphan guards active. The write-side terminal disposition is
    /// the real <see cref="MigrationTerminalCore.DecideBucketAction"/> (a late
    /// terminal re-delivery discards the orphan bucket), and the read-side gate
    /// feeds the real terminal-landed flag to
    /// <see cref="AtomicVisibilityGate.ResolveKey"/> (a surviving orphan bucket
    /// falls through to the authoritative projected value). No schedule shadows a
    /// later saga's value.
    /// </summary>
    Guarded,

    /// <summary>
    /// The read-side guard removed: the reader resolves an orphan bucket with
    /// <c>alreadyTerminal = false</c>, so <see cref="AtomicVisibilityGate"/>
    /// surfaces the stale prepare-time value of an already-terminal saga. A late
    /// orphan bucket on some keys but not others then tears the read.
    /// </summary>
    NoReadGuard,

    /// <summary>
    /// The write-side guard removed: a late terminal re-delivery <b>drains</b> the
    /// orphan bucket into projected state instead of discarding it
    /// (<see cref="MigrationTerminalBucketAction.DrainCommit"/> in place of
    /// <see cref="MigrationTerminalBucketAction.DiscardOrphan"/>), stamping an old
    /// saga round's value over the current one - the reshard <c>unknown-round</c>
    /// signature.
    /// </summary>
    NoWriteGuard,
}

/// <summary>
/// A Coyote concurrency model of the online-reshard migration protocol's
/// interaction with the atomic-commit saga - the source of the #1584 split-view
/// class - driving the <b>production</b> reshard cores under systematic schedule
/// exploration. The saga-decision registry is a real
/// <see cref="TxRegistryDecisionCore"/>, the write-side terminal disposition is the
/// real <see cref="MigrationTerminalCore.DecideBucketAction"/>, and the reader's
/// per-key visibility decision is the real
/// <see cref="AtomicVisibilityGate.ResolveKey"/> rule fed the real terminal-landed
/// flag. Because the model executes the same code Orleans runs, a violation Coyote
/// finds is a violation of the shipping migration path.
/// <para>
/// The scenario models a destination leaf that has received two saga rounds during
/// a migration - round <c>S1</c> (value <c>V1</c>) then round <c>S2</c> (value
/// <c>V2</c>), each a committed write across every key - so projected state has
/// converged to <c>V2</c> and both terminals have landed. A <b>late</b>
/// shadow-forwarded (or split-sweep-replayed) prepare of the earlier <c>S1</c> then
/// re-buckets a scheduler-chosen subset of keys with <c>S1</c>'s stale prepare-time
/// value, and the model interleaves the delivery orders of {orphan shadow-forward
/// prepare, duplicate <c>S1</c> terminal broadcast, cross-migration backstop,
/// reader fan-out}. This is exactly where the <c>alreadyTerminal</c> input to
/// <see cref="AtomicVisibilityGate"/> comes from: the orphan bucket is for a saga
/// whose terminal has already landed.
/// </para>
/// <para>
/// The safety properties are (a) <b>no split view</b> - the reader observes every
/// key with the same saga generation (zero-or-all), and (b) <b>no orphan shadows a
/// later saga's value</b> - no key is ever observed with the stale <c>V1</c> once
/// <c>V2</c> is the authoritative projected value. The concurrent-commit torn read
/// (a reader straddling a single saga's mid-fan-out commit/drain) is covered
/// deterministically by <see cref="AtomicCommitVisibilityModel"/>; this model
/// isolates the orphan-guard and cross-migration-backstop interleavings that only
/// arise once a migration has moved a saga's keys to a destination leaf.
/// </para>
/// <para>
/// <b>Relation to the chaos backstop.</b> These same interleavings are covered
/// today only probabilistically by the CI-only chaos test
/// <c>ReshardTopologyTests.Continuous_reader_observes_zero_or_all_keys_through_mid_saga_reshard</c>
/// (issue #1584). This model makes them <b>deterministic</b>: the relative
/// delivery orders of {late shadow-forward prepare, duplicate terminal broadcast,
/// cross-migration LWW backstop, multi-key reader fan-out} against a saga whose
/// terminal has already landed are exhaustively interleaved by the Coyote
/// scheduler rather than sampled by chance, so the orphan-guard regression is
/// caught in seconds by a systematic model instead of eventually by a slow
/// probabilistic reshard run.
/// </para>
/// </summary>
public sealed class ReshardMigrationModel : ICoyoteModel
{
    private const int Pre = 0;
    private const int V1 = 1;
    private const int V2 = 2;

    private readonly int _keyCount;
    private readonly ReshardGuardMode _mode;

    /// <summary>
    /// Creates the model for a <paramref name="keyCount"/>-key destination leaf
    /// under the chosen <paramref name="mode"/>.
    /// </summary>
    public ReshardMigrationModel(int keyCount, ReshardGuardMode mode)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(keyCount, 2);
        _keyCount = keyCount;
        _mode = mode;
    }

    /// <inheritdoc />
    public void Run(ICoyoteRuntime runtime)
    {
        // The real recording-side core holds both sagas' decisions.
        var core = new TxRegistryDecisionCore(new Dictionary<Guid, TxStatus>(), 0L);
        var s1 = Guid.NewGuid();
        var s2 = Guid.NewGuid();

        // Destination-leaf state: the authoritative projected value per key, the
        // late orphan bucket per key (Empty = none), and the set of sagas whose
        // terminal has landed here (the leaf's _recentlyTerminal).
        var projected = new int[_keyCount];
        Array.Fill(projected, Pre);
        var bucket = new Guid[_keyCount];
        Array.Fill(bucket, Guid.Empty);
        var terminalApplied = new HashSet<Guid>();

        // Establish the two committed rounds: S1 backstops V1 into every key, then
        // S2 backstops V2 over them. Both terminals land, so projected == V2 and
        // terminalApplied == { S1, S2 } before the orphan/reader interleave.
        core.Apply(s1, TxStatus.Committed);
        ApplyTerminalToAll(s1, V1, projected, bucket, terminalApplied);
        core.Apply(s2, TxStatus.Committed);
        ApplyTerminalToAll(s2, V2, projected, bucket, terminalApplied);

        // The late S1 orphan prepare, its duplicate terminal, the backstop, and the
        // reader fan-out interleave. The reader resolves each key exactly once; the
        // orphan re-bucketing and the duplicate S1 terminal are delivered between
        // key resolutions in an order the runtime explores.
        var observed = new int[_keyCount];
        for (var i = 0; i < _keyCount; i++)
        {
            MaybeOrphanRebucket(s1, bucket, runtime);
            MaybeDuplicateTerminal(s1, projected, bucket, terminalApplied, runtime);
            observed[i] = ObserveKey(i, core, s1, projected, bucket, terminalApplied);
        }

        AssertNoSplitOrOrphanShadow(observed);
    }

    /// <summary>
    /// Applies a saga terminal to every key through the write-side disposition
    /// core, mirroring <c>BPlusLeafGrain.ApplyTxTerminalAsync</c>: a drainable
    /// bucket is drained, an orphan or aborted bucket is discarded, and a key with
    /// no bucket is backstopped with the committed value. Marks the saga
    /// terminal-landed afterwards, exactly as the grain marks <c>_recentlyTerminal</c>.
    /// </summary>
    private void ApplyTerminalToAll(
        Guid txid,
        int value,
        int[] projected,
        Guid[] bucket,
        HashSet<Guid> terminalApplied)
    {
        for (var i = 0; i < projected.Length; i++)
        {
            ApplyTerminalToKey(i, txid, value, projected, bucket, terminalApplied);
        }

        terminalApplied.Add(txid);
    }

    /// <summary>
    /// The per-key terminal disposition. The bucket fate is the production
    /// <see cref="MigrationTerminalCore.DecideBucketAction"/> rule, except under
    /// <see cref="ReshardGuardMode.NoWriteGuard"/>, where an orphan bucket is
    /// drained rather than discarded to reintroduce the stale-restamp regression.
    /// </summary>
    private void ApplyTerminalToKey(
        int i,
        Guid txid,
        int value,
        int[] projected,
        Guid[] bucket,
        HashSet<Guid> terminalApplied)
    {
        var hadPending = bucket[i] == txid;
        var alreadyTerminal = terminalApplied.Contains(txid);
        var action = MigrationTerminalCore.DecideBucketAction(hadPending, alreadyTerminal, committed: true);

        if (_mode == ReshardGuardMode.NoWriteGuard && action == MigrationTerminalBucketAction.DiscardOrphan)
        {
            // The removed write-side guard: drain the orphan bucket (value = the
            // saga's stale prepare-time value) over the authoritative projected
            // state instead of discarding it.
            action = MigrationTerminalBucketAction.DrainCommit;
        }

        switch (action)
        {
            case MigrationTerminalBucketAction.DrainCommit:
                projected[i] = value;
                bucket[i] = Guid.Empty;
                break;
            case MigrationTerminalBucketAction.DiscardOrphan:
            case MigrationTerminalBucketAction.DiscardAborted:
                bucket[i] = Guid.Empty;
                break;
            case MigrationTerminalBucketAction.None:
            default:
                // No bucket to flip: the cross-migration LWW backstop writes the
                // committed value directly into projected state.
                projected[i] = value;
                break;
        }
    }

    /// <summary>
    /// The late orphan shadow-forward prepare: for each key with no live bucket,
    /// the runtime decides whether S1's prepare is (re-)delivered here now, planting
    /// an orphan bucket whose terminal has already landed.
    /// </summary>
    private static void MaybeOrphanRebucket(Guid s1, Guid[] bucket, ICoyoteRuntime runtime)
    {
        for (var i = 0; i < bucket.Length; i++)
        {
            if (bucket[i] == Guid.Empty && runtime.RandomBoolean())
            {
                bucket[i] = s1;
            }
        }
    }

    /// <summary>
    /// A duplicate S1 terminal broadcast reaching the leaf after the orphan bucket
    /// landed: for each key that currently carries the orphan, the runtime decides
    /// whether the terminal is re-delivered now, running the write-side disposition
    /// core over the orphan bucket.
    /// </summary>
    private void MaybeDuplicateTerminal(
        Guid s1,
        int[] projected,
        Guid[] bucket,
        HashSet<Guid> terminalApplied,
        ICoyoteRuntime runtime)
    {
        for (var i = 0; i < bucket.Length; i++)
        {
            if (bucket[i] == s1 && runtime.RandomBoolean())
            {
                ApplyTerminalToKey(i, s1, V1, projected, bucket, terminalApplied);
            }
        }
    }

    /// <summary>
    /// Resolves the value the reader observes for key <paramref name="i"/>. A key
    /// carrying the S1 orphan bucket consults the production read gate
    /// (<see cref="AtomicVisibilityGate.ResolveKey"/>) with the real terminal-landed
    /// flag, except under <see cref="ReshardGuardMode.NoReadGuard"/>, where the flag
    /// is forced <see langword="false"/> so the gate surfaces the stale orphan
    /// value. A key with no bucket serves projected state.
    /// </summary>
    private int ObserveKey(
        int i,
        TxRegistryDecisionCore core,
        Guid s1,
        int[] projected,
        Guid[] bucket,
        HashSet<Guid> terminalApplied)
    {
        if (bucket[i] != s1)
        {
            return projected[i];
        }

        var alreadyTerminal = _mode != ReshardGuardMode.NoReadGuard && terminalApplied.Contains(s1);
        var outcome = AtomicVisibilityGate.ResolveKey(
            core.Resolve(s1),
            alreadyTerminal,
            preparedHiddenByTombstoneOrExpiry: false);

        // The orphan bucket carries S1's stale prepare-time value (V1). Surfacing
        // it is exactly the regression; falling through serves the authoritative
        // projected value.
        return outcome == PendingReadOutcome.SurfacePrepared ? V1 : projected[i];
    }

    /// <summary>
    /// The combined safety assertion: the reader observes no split view (every key
    /// the same generation) and never a stale orphan value (no key observes the
    /// superseded <c>V1</c> once <c>V2</c> is authoritative). A single failing key
    /// captures both the <c>split (pre, post)</c> and <c>unknown-round</c> shapes
    /// the reshard chaos suite catches.
    /// </summary>
    private static void AssertNoSplitOrOrphanShadow(int[] observed)
    {
        var first = observed[0];
        for (var i = 1; i < observed.Length; i++)
        {
            Specification.Assert(
                observed[i] == first,
                $"reshard split view across {observed.Length} keys: key0={first}, " +
                $"key{i}={observed[i]} (a reader observed different saga generations)");
        }

        for (var i = 0; i < observed.Length; i++)
        {
            Specification.Assert(
                observed[i] == V2,
                $"reshard orphan shadowed a later saga's value: key{i}={observed[i]} " +
                $"but the authoritative projected value is V2={V2}");
        }
    }
}
