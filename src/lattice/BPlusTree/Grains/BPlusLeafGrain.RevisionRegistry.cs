using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Same-silo revision registry partial for <see cref="BPlusLeafGrain"/>.
/// <para>
/// Exposes a process-wide monotonic counter per leaf <see cref="GrainId"/>
/// that the local <see cref="LeafCacheGrain"/> activation can read
/// synchronously to skip its <see cref="BPlusLeafGrain.GetDeltaSinceAsync"/>
/// cross-grain refresh call when nothing has advanced on the primary
/// since the cache last refreshed. Cross-silo callers do not see
/// entries another silo's activations populate, so the cache's
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
/// activation's single-threaded scheduler turn that mutates the state),
/// so the cookie-cache fast-path has the same race shape as the existing
/// RPC fast-path: any in-flight write that has not yet bumped the cookie
/// would also not yet be visible to a hypothetical RPC that beat it to
/// the leaf's mailbox. The semantic effect on a same-silo cache is
/// equivalent to the existing RPC behaviour, with the cross-grain
/// dispatch elided.
/// </para>
/// <para>
/// Implementation: each activation publishes a <see cref="StrongBox{T}"/>
/// of <see cref="long"/> into the registry exactly once (on first bump);
/// every subsequent bump is a single <see cref="Interlocked.Increment(ref long)"/>
/// on the published box's mutable field. The hot path therefore performs
/// no <see cref="ConcurrentDictionary{TKey, TValue}"/> indexer assignment
/// per tick - only an atomic increment of an already-resolved field
/// reference - which keeps tight write loops (e.g. <see cref="SetManyAsync"/>
/// over a thousand-key batch) allocation-free. Readers go through the
/// dict lookup once per refresh, which is the cheap path; only the
/// per-write side ever runs in a tight loop.
/// </para>
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    /// <summary>
    /// Process-wide same-silo revision registry. Keyed by leaf
    /// <see cref="GrainId"/>; the value is a heap-allocated
    /// <see cref="StrongBox{T}"/> wrapping a per-leaf monotonic counter
    /// bumped on every state-advancing operation. The wrapper is shared
    /// between the leaf's bumper and the cache's reader so the bumper
    /// can <see cref="Interlocked.Increment(ref long)"/> the field
    /// directly without a per-tick dict indexer assignment. Absence of
    /// an entry implies "primary leaf is not activated on this silo" --
    /// the only silo whose activation populates the entry -- which
    /// forces the cache to its existing cross-grain refresh path.
    /// </summary>
    private static readonly ConcurrentDictionary<GrainId, StrongBox<long>> LeafRevisionRegistry = new();

    /// <summary>
    /// Per-activation cached reference to this leaf's revision box.
    /// Lazily resolved on first <see cref="BumpLocalRevision"/> via
    /// <see cref="ConcurrentDictionary{TKey, TValue}.GetOrAdd(TKey, System.Func{TKey, TValue})"/>;
    /// thereafter every bump is a single
    /// <see cref="Interlocked.Increment(ref long)"/> on the box's
    /// mutable <c>Value</c> field with no dictionary touch.
    /// </summary>
    private StrongBox<long>? _localRevisionBox;

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
    {
        if (LeafRevisionRegistry.TryGetValue(leafId, out var box))
        {
            revision = Interlocked.Read(ref box.Value);
            return true;
        }

        revision = 0;
        return false;
    }

    /// <summary>
    /// Atomically increments this activation's published revision
    /// counter. Called from every state-advancing site on this leaf
    /// (each <c>state.State.Version.Tick</c>). The first call lazily
    /// publishes the activation's <see cref="StrongBox{T}"/> into the
    /// process-wide registry via
    /// <see cref="ConcurrentDictionary{TKey, TValue}.GetOrAdd(TKey, System.Func{TKey, TValue})"/>;
    /// every subsequent call reads-and-writes <c>box.Value</c> directly
    /// (Orleans guarantees only one foreground turn touches a given
    /// activation at a time, so the local read+write is race-free
    /// against itself; the write uses <see cref="Volatile.Write(ref long, long)"/>
    /// to publish a release-store so the cross-grain reader on a
    /// different scheduler thread observes the bumped value through its
    /// matching <see cref="Interlocked.Read(ref long)"/>). No dictionary
    /// touch and no atomic increment in the steady-state path keeps tight
    /// write loops (e.g. <see cref="SetManyAsync"/> over a thousand-key
    /// batch) free of both per-call allocations and per-call full barriers.
    /// <see cref="MethodImplOptions.AggressiveInlining"/> is applied
    /// because tight write loops call this method once per key and the
    /// call-site overhead would otherwise dominate the tick.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void BumpLocalRevision()
    {
        var box = _localRevisionBox ??=
            LeafRevisionRegistry.GetOrAdd(context.GrainId, static _ => new StrongBox<long>(0L));
        Volatile.Write(ref box.Value, box.Value + 1);
    }
}

