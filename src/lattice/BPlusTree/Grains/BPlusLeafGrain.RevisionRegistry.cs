using System.Collections.Concurrent;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Same-silo revision registry partial for <see cref="BPlusLeafGrain"/>.
/// <para>
/// Exposes a process-wide monotonic counter per leaf <see cref="GrainId"/>
/// that the local <see cref="LeafCacheGrain"/> activation can read
/// synchronously to skip its <see cref="BPlusLeafGrain.GetDeltaSinceAsync"/>
/// cross-grain refresh call when nothing has advanced on the primary
/// since the cache last refreshed. Cross-silo callers do not see
/// entries another silo''s activations populate, so the cache''s
/// <see cref="ConcurrentDictionary{TKey, TValue}.TryGetValue"/> returns
/// <c>false</c> on those silos and the cache falls through to the
/// existing cross-grain refresh path; correctness is therefore preserved
/// in multi-silo deployments.
/// </para>
/// <para>
/// The cookie is bumped from inside each existing <c>state.State.Version.Tick</c>
/// site on the leaf -- all writes, deletes, expiries, and split-rebalance
/// publications -- so any state advance the cache could observe via the
/// existing RPC is also observable via the cookie. A bump is publish-
/// after-apply (the cookie is monotonic and is updated under the same
/// activation''s single-threaded scheduler turn that mutates the state),
/// so the cookie-cache fast-path has the same race shape as the existing
/// RPC fast-path: any in-flight write that has not yet bumped the cookie
/// would also not yet be visible to a hypothetical RPC that beat it to
/// the leaf''s mailbox. The semantic effect on a same-silo cache is
/// equivalent to the existing RPC behaviour, with the cross-grain
/// dispatch elided.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Process-wide same-silo revision registry. Keyed by leaf
    /// <see cref="GrainId"/>; the value is a per-leaf monotonic counter
    /// bumped on every state-advancing operation. Absence of an entry
    /// implies "primary leaf is not activated on this silo" -- the only
    /// silo whose activation populates the entry -- which forces the
    /// cache to its existing cross-grain refresh path.
    /// </summary>
    private static readonly ConcurrentDictionary<GrainId, long> LeafRevisionRegistry = new();

    /// <summary>Per-activation monotonic revision counter for this leaf.</summary>
    private long _localRevisionCounter;

    /// <summary>
    /// Reads the same-silo revision cookie for <paramref name="leafId"/>.
    /// Returns <c>true</c> iff the primary leaf is currently activated
    /// on the calling silo and has bumped the cookie at least once.
    /// Cookies start at <c>1</c> after the first bump and increase
    /// monotonically; deactivation removes the entry, so a re-activation
    /// also re-starts at <c>1</c>. A reader compares the returned value
    /// against its own last-observed cookie; the comparison covers both
    /// "no advance since last observation" and the rarer "primary was
    /// re-activated and has not yet caught up to the previously-observed
    /// cookie" -- both correctly force a cross-grain refresh.
    /// </summary>
    internal static bool TryGetLeafRevision(GrainId leafId, out long revision)
        => LeafRevisionRegistry.TryGetValue(leafId, out revision);

    /// <summary>
    /// Increments the per-activation revision counter and publishes it
    /// to <see cref="LeafRevisionRegistry"/>. Called from every
    /// state-advancing site on this leaf (each <c>state.State.Version.Tick</c>).
    /// </summary>
    private void BumpLocalRevision()
    {
        var rev = unchecked(++_localRevisionCounter);
        LeafRevisionRegistry[context.GrainId] = rev;
    }
}

