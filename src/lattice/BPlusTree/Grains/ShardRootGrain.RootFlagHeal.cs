using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Activation-time repair for shard roots whose persisted <c>RootIsLeaf</c> flag
/// is baked <c>true</c> over a root that is actually a <c>BPlusInternalGrain</c>
/// (issue 899 / issue 1883).
/// <para>
/// <b>Why the population exists.</b> <c>ShardRootState.RootIsLeaf</c> carried a
/// <c>= true</c> property initializer over a member whose "off" value is the CLR
/// type default. The grain-storage serializer omits a member equal to the type
/// default, so a correctly-written <c>false</c> was dropped from the blob and the
/// initializer resurrected it as <c>true</c> on load. Any subsequent state write
/// then persisted that <c>true</c> LITERALLY, baking the lie into the blob. A
/// census of a pristine pre-epic production volume found every one of the 160
/// internal-rooted shard roots presenting <c>RootIsLeaf == true</c> at runtime,
/// of which 96 had the value baked into the blob itself.
/// </para>
/// <para>
/// <b>Why a repair is still needed after the POCO fix.</b> Removing the
/// initializer makes the round trip lossless, which self-heals every shard whose
/// blob never contained the member - it now reconstructs as the <c>false</c> that
/// was written. It heals nothing for a shard that was re-saved after a bad reload,
/// because that blob literally contains <c>"RootIsLeaf":true</c> and there is no
/// value left to reconstruct. Those shards need an actual write.
/// </para>
/// <para>
/// <b>Ordering.</b> This repair is correct only on top of the POCO fix. Before it,
/// writing <c>false</c> meant the serializer omitted the member and the
/// initializer returned <c>true</c> on the next load - a no-op that looks like it
/// worked. Re-introducing a non-default initializer on either flag would silently
/// disable this repair;
/// <c>PersistedStateDefaultInitializerContractTests</c> is the guard that stops
/// that happening.
/// </para>
/// </summary>
internal sealed partial class ShardRootGrain
{
    /// <summary>
    /// Corrects a persisted <c>RootIsLeaf</c> flag that claims a leaf root over an
    /// internal-typed root node, once per activation, before any operation turn
    /// runs.
    /// <para>
    /// <b>Where it runs, and why here.</b> The condition is non-recurring: the
    /// promotion sites that consumed the lying flag to decide what to persist now
    /// decide by node type, and the POCO round trip is lossless, so nothing mints
    /// new instances. That makes this a ONE-TIME repair of a static population, and
    /// an activation hook is the shape that fits: it is <c>O(1)</c> per activation
    /// rather than a standing sweep, it is self-limiting (once a shard is corrected
    /// the condition never holds again, so the population drains monotonically to
    /// zero and stays there), and it needs no operator action, which the epic's
    /// drop-in-upgrade constraint requires. Activation is also the only point at
    /// which no other turn can be running on this activation, so the repair cannot
    /// interleave with traffic and needs no gate of its own beyond the one
    /// <see cref="WriteShardStateAsync"/> already takes.
    /// </para>
    /// <para>
    /// <b>Detection is by TYPE, never by the flag.</b> The predicate is "the raw
    /// flag claims a leaf root, but <see cref="RootIsLeafTyped"/> refuses it" - and
    /// <see cref="RootIsLeafTyped"/> refuses exactly when the persisted root id does
    /// not address a leaf grain. A shard with no root yet is excluded explicitly:
    /// its flag is not consulted by anything and writing <c>false</c> there would
    /// be a spurious storage write on every fresh shard.
    /// </para>
    /// <para>
    /// <b>Under a non-runtime grain factory this is a no-op</b>, deliberately and by
    /// the same mechanism as every other issue-899 guard: <c>IsLeafGrainId</c>
    /// answers <see langword="true"/> for every id when the leaf grain type cannot
    /// be resolved, so <see cref="RootIsLeafTyped"/> is simply the raw flag and the
    /// predicate below is never satisfied. A test fixture that does not make the
    /// leaf-type probe resolvable therefore cannot observe this repair at all,
    /// which is why the regression fixture carries a leg that fails when the probe
    /// is dead.
    /// </para>
    /// <para>
    /// <b>What the repair is worth.</b> The shipped containment (deciding both
    /// promotion guards by <see cref="RootIsLeafTyped"/>, plus the clamp that forces
    /// a new root's <c>childrenAreLeaves</c> bit from the surviving child's actual
    /// type) stops the lying flag throwing, but leaves it in place. One consumer
    /// still samples it raw: <c>PromoteRootAsync</c> persists
    /// <c>PendingPromotionRootWasLeaf = state.State.RootIsLeaf</c>, so a lying flag
    /// is copied into the promotion intent and only caught downstream by the clamp.
    /// Correcting the flag removes that exposure at its source rather than
    /// containing it on every future promotion.
    /// </para>
    /// </summary>
    private Task HealBakedRootIsLeafFlagAsync()
    {
        // Ordered so the overwhelmingly common cases return without touching the
        // leaf-grain-type probe: a healthy internal-rooted shard exits on the first
        // branch, and a shard with no root yet on the second. Zero allocation on
        // every path that does not repair - the whole fast path is a bool read, a
        // nullable struct check, and a GrainType comparison, returning the cached
        // completed task.
        //
        // This method is deliberately NOT async: the checks below must all sit
        // BEFORE any suspension point, or every shard-root activation in the fleet
        // allocates a task for a repair that, the population having drained, can
        // never fire again. ShardRootGrainRootFlagHealTests
        // .Activation_allocates_no_task_when_there_is_nothing_to_repair pins that.
        if (!state.State.RootIsLeaf)
            return Task.CompletedTask;

        if (state.State.RootNodeId is not { } rootId)
            return Task.CompletedTask;

        // Healthy (the flag agrees with the node type), or the leaf-grain-type
        // probe is unresolvable and every issue-899 guard - this one included -
        // degrades to a no-op.
        if (RootIsLeafTyped)
            return Task.CompletedTask;

        return HealBakedRootIsLeafFlagSlowAsync(rootId);
    }

    private async Task HealBakedRootIsLeafFlagSlowAsync(GrainId rootId)
    {
        state.State.RootIsLeaf = false;
        try
        {
            await WriteShardStateAsync();
        }
        catch (Exception ex)
        {
            // Restore the in-memory value to what storage still holds, so the two
            // do not diverge (the Class B revert every shard-root write site uses),
            // and leave the repair to the next activation. Swallowing is deliberate:
            // this runs inside activation, and a shard whose flag is merely wrong is
            // fully serviceable because every consumer that could throw on it is
            // already decided by node type. Failing activation over a cosmetic
            // repair would turn a contained defect into an outage.
            state.State.RootIsLeaf = true;
            logger.LogWarning(
                ex,
                "ShardRootGrain {ShardId} could not persist the repair of an inconsistent RootIsLeaf flag over "
                + "internal root node {RootNodeId} (issue 899 / issue 1883); the flag is left as persisted and the "
                + "repair will be retried on the next activation. Reads and writes are unaffected - every path that "
                + "could act on the flag is decided by node type.",
                context.GrainId,
                rootId);
            return;
        }

        logger.LogWarning(
            "ShardRootGrain {ShardId} repaired an inconsistent persisted RootIsLeaf flag (issue 899 / issue 1883): "
            + "the flag claimed a leaf root while root node {RootNodeId} addresses an internal grain, and has been "
            + "corrected to false. This is a one-time repair of state baked by a pre-fix serializer round trip; a "
            + "shard is repaired at most once, so this warning is not expected to recur for the same shard.",
            context.GrainId,
            rootId);
    }
}
