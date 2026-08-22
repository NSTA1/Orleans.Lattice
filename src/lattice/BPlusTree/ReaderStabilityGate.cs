namespace Orleans.Lattice.BPlusTree;

/// <summary>
/// The pure, dependency-free reader-side stability rule for a multi-key read
/// resolved against a versioned registry view. It is the reader-side companion to
/// <see cref="TxRegistryDecisionCore"/> (the recording side) and
/// <see cref="AtomicVisibilityGate"/> / <see cref="TxDecisionView"/> (the per-key
/// read gate): the <c>LatticeGrain</c> multi-shard read fan-outs execute these
/// rules, and the Coyote atomic-commit model drives the same rules, so the
/// no-torn-read property proven by the model is a property of the code that runs.
/// <para>
/// A multi-key read captures a registry decision snapshot (a map paired with the
/// <see cref="TxRegistryDecisionCore.Revision"/> that produced it - see
/// <see cref="TxDecisionSnapshot"/>), resolves every key of the fan-out against
/// that single view, and then must decide whether the read it computed is still
/// authoritative. The cheap decision is <see cref="IsRevisionStable(long, long)"/>:
/// the read is stable iff the registry's revision after resolving all keys equals
/// the revision captured with the snapshot. When the revision moved, a decision
/// mutated during the fan-out; the reader disambiguates with
/// <see cref="IsSnapshotStable"/> (only an
/// <see cref="TxStatus.InFlight"/>-&gt;<see cref="TxStatus.Committed"/>
/// transition actually invalidates the read) and, if still unstable, retries
/// under a fresh snapshot.
/// </para>
/// </summary>
internal static class ReaderStabilityGate
{
    /// <summary>
    /// The cheap revision probe: a multi-key read resolved against a snapshot
    /// captured at <paramref name="capturedRevision"/> is stable iff the
    /// registry's revision observed after the fan-out
    /// (<paramref name="revisionAfterFanOut"/>) is unchanged. Because the
    /// recording side (<see cref="TxRegistryDecisionCore.Apply(Guid, TxStatus)"/>)
    /// advances the revision on every decision-map mutation, an equal revision is
    /// proof that no decision changed during the fan-out, so the snapshot the read
    /// resolved against is still authoritative.
    /// </summary>
    public static bool IsRevisionStable(long capturedRevision, long revisionAfterFanOut) =>
        revisionAfterFanOut == capturedRevision;

    /// <summary>
    /// The disambiguation rule used when the revision moved during the fan-out:
    /// the read computed under <paramref name="snap1"/> is still consistent given
    /// a fresh <paramref name="snap2"/> iff no saga transitioned
    /// <see cref="TxStatus.InFlight"/>-&gt;<see cref="TxStatus.Committed"/> between
    /// them. The check is asymmetric - every <see cref="TxStatus.Committed"/>
    /// entry in <paramref name="snap2"/> must already be Committed in
    /// <paramref name="snap1"/>.
    /// <para>
    /// The asymmetry is the whole point. Per-leaf drain into the runtime entry
    /// cache on commit is irreversible - once a leaf has flipped a saga's prepared
    /// keys into the cache it has no record they came from a saga, so a stale
    /// <see cref="TxStatus.InFlight"/> snapshot can no longer gate visibility on
    /// that leaf. A reader whose <paramref name="snap1"/> was taken before the
    /// commit but whose fan-out reaches some leaves after their drain therefore
    /// observes drained leaves serving post-saga entries while sibling undrained
    /// leaves consult <paramref name="snap1"/>'s InFlight and fall through to
    /// pre-saga entries - a split observation. <paramref name="snap2"/>'s Committed
    /// entry reveals the transition and the caller retries with the fresh snapshot
    /// in scope.
    /// </para>
    /// <para>
    /// <see cref="TxStatus.Aborted"/> transitions and registry forgets
    /// (<paramref name="snap1"/> has the txid, <paramref name="snap2"/> does not)
    /// are atomic-safe by construction and do not invalidate the read. A
    /// <see langword="null"/> <paramref name="snap2"/> (registry RPC failure) is
    /// treated as stable so the read completes rather than retrying indefinitely.
    /// The parameters are the concrete <see cref="Dictionary{TKey, TValue}"/> the
    /// registry snapshot produces so the iteration below uses the struct
    /// enumerator and allocates nothing on this reader path.
    /// </para>
    /// </summary>
    public static bool IsSnapshotStable(
        Dictionary<Guid, TxStatus>? snap1,
        Dictionary<Guid, TxStatus>? snap2)
    {
        if (snap2 is null)
        {
            return true;
        }

        foreach (var (txid, status) in snap2)
        {
            if (status == TxStatus.Committed)
            {
                if (snap1 is null
                    || !snap1.TryGetValue(txid, out var s1)
                    || s1 != TxStatus.Committed)
                {
                    return false;
                }
            }
        }

        return true;
    }
}
