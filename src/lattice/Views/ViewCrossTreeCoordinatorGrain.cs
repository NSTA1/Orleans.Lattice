using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.Views;

/// <summary>
/// Default <see cref="IViewCrossTreeCoordinatorGrain"/>. One activation per
/// cross-tree <c>operationId</c> rendezvouses every participating view's ready
/// slice and, once the wait set completes, flips them all jointly through a
/// single cross-tree atomic write so view readers observe them flip together.
/// See <see cref="IViewCrossTreeCoordinatorGrain"/> for the contract and the
/// deadlock-freedom / wait-set rationale.
/// <para>
/// <b>Crash safety.</b> The decision is persisted in two durable steps mirroring
/// the receiver-side barrier: when the wait set completes the coordinator first
/// persists the joint-flip intent (<see cref="ViewCrossTreeCoordinatorState.Decided"/>),
/// then issues the idempotent joint cross-tree write (keyed
/// <c>mv-xt-{operationId}</c> so a redelivery re-attaches rather than
/// double-applying), then persists <see cref="ViewCrossTreeCoordinatorState.Applied"/>.
/// A crash between the two persists is healed by a redelivered registration,
/// which re-drives the idempotent write before returning the applied decision -
/// so a crash mid-apply never exposes a partial set of views.
/// </para>
/// </summary>
internal sealed class ViewCrossTreeCoordinatorGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<ViewCrossTreeCoordinatorGrain> logger,
    [PersistentState("view-cross-tree", LatticeOptions.StorageProviderName)]
    IPersistentState<ViewCrossTreeCoordinatorState> state)
    : TtlGrain<ViewCrossTreeCoordinatorGrain>(context, reminderRegistry, logger), IViewCrossTreeCoordinatorGrain
{
    private const string RetentionReminderName = "view-cross-tree-retention";

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => optionsMonitor.CurrentValue.AtomicWriteRetention;

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        Logger.LogInformation(
            "View cross-tree coordinator {Key}: retention window expired; clearing state.",
            GrainContext.GrainId.Key);
        await state.ClearStateAsync();
    }

    /// <summary>
    /// The deterministic, idempotent operation id for the joint cross-tree flip,
    /// derived from the coordinator key so a redelivery (or a crash-recovery
    /// re-drive) re-attaches to the completed saga rather than minting a fresh
    /// one. The coordinator key has no '/' (validated at the source cross-tree
    /// write), so the derived id is a valid cross-tree operation id.
    /// </summary>
    private string JointOperationId => $"mv-xt-{GrainContext.GrainId.Key}";

    /// <inheritdoc />
    public async Task<ViewCrossTreeDecision> RegisterReadyAsync(ViewCrossTreeReadiness readiness)
    {
        ArgumentNullException.ThrowIfNull(readiness);
        ArgumentException.ThrowIfNullOrEmpty(readiness.ViewName);
        ArgumentException.ThrowIfNullOrEmpty(readiness.ViewTreeId);
        ArgumentNullException.ThrowIfNull(readiness.WaitSet);
        ArgumentNullException.ThrowIfNull(readiness.Upserts);

        // Already applied and durable: a redelivered registration re-confirms the
        // joint flip is committed. Return the memoized decision without re-issuing.
        if (state.State.Applied)
        {
            return ViewCrossTreeDecision.Committed;
        }

        // Terminally degraded by an earlier participant timeout: no joint flip
        // will ever be issued, so this registrant flips its own slice locally.
        if (state.State.Degraded)
        {
            return ViewCrossTreeDecision.DegradedResult;
        }

        if (state.State.WaitSet.Count == 0)
        {
            // First registration: freeze the (canonicalised) wait set.
            state.State.WaitSet = readiness.WaitSet
                .Distinct(StringComparer.Ordinal)
                .OrderBy(static v => v, StringComparer.Ordinal)
                .ToList();
            state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
        }
        else if (!WaitSetMatches(readiness.WaitSet))
        {
            // Config drift between two registrations of the same operation must not
            // shrink or grow the joint barrier.
            throw new InvalidOperationException(
                $"View cross-tree coordinator '{GrainContext.GrainId.Key}' received a registration for view " +
                $"'{readiness.ViewName}' whose wait set differs from the frozen wait set; the participant view " +
                "set must be stable across every registration of a cross-tree operation.");
        }

        if (!state.State.WaitSet.Contains(readiness.ViewName))
        {
            throw new InvalidOperationException(
                $"View cross-tree coordinator '{GrainContext.GrainId.Key}' received a registration for view " +
                $"'{readiness.ViewName}' that is absent from the frozen wait set.");
        }

        // Record (or idempotently overwrite) this view's ready slice.
        state.State.Slices[readiness.ViewName] = new ViewCrossTreeSlice
        {
            ViewName = readiness.ViewName,
            ViewTreeId = readiness.ViewTreeId,
            Upserts = readiness.Upserts,
        };

        // The wait set completes when every participant view has registered.
        var complete = true;
        foreach (var waitView in state.State.WaitSet)
        {
            if (!state.State.Slices.ContainsKey(waitView))
            {
                complete = false;
                break;
            }
        }

        if (!complete && !state.State.Decided)
        {
            // Persist the recorded slice so the rendezvous survives a crash, then
            // tell the caller to keep waiting.
            await state.WriteStateAsync();
            return ViewCrossTreeDecision.NotReady;
        }

        // The wait set is complete (or a prior call already decided). Persist the
        // joint-flip intent before issuing the idempotent write so a crash mid
        // apply is re-driven by a redelivered registration rather than leaving a
        // partial set of views.
        if (!state.State.Decided)
        {
            state.State.Decided = true;
            await state.WriteStateAsync();
        }

        await IssueJointFlipAsync();

        state.State.Applied = true;
        await state.WriteStateAsync();

        // Arm one-shot retention cleanup now that the decision is terminal.
        await SlideTtlAsync();
        return ViewCrossTreeDecision.Committed;
    }

    /// <summary>
    /// Issues the joint cross-tree flip across every participant view tree whose
    /// slice has at least one upsert, idempotently keyed by
    /// <see cref="JointOperationId"/>. Empty slices contribute nothing to flip,
    /// so they are excluded; a single non-empty slice falls back to the
    /// single-tree atomic write (the cross-tree primitive requires two or more
    /// trees). Retraction deletes are not carried - each maintainer applies its
    /// own after observing the applied decision.
    /// </summary>
    private async Task IssueJointFlipAsync()
    {
        // Authorise the joint flip's writes to participant view trees (propagates
        // through the cross-tree atomic-write saga via RequestContext).
        using var viewWriteScope = ViewWriteContext.BeginScope();
        var batches = new List<LatticeTreeBatch>(state.State.Slices.Count);
        foreach (var slice in state.State.Slices.Values)
        {
            if (slice.Upserts.Count > 0)
            {
                batches.Add(new LatticeTreeBatch(slice.ViewTreeId, slice.Upserts));
            }
        }

        switch (batches.Count)
        {
            case 0:
                // Every participant view's slice is empty: nothing to flip, but the
                // joint guarantee is trivially satisfied.
                return;
            case 1:
                // One non-empty slice: a cross-tree write needs two or more trees,
                // so flip the single tree atomically (still all-or-nothing for it).
                await grainFactory
                    .GetGrain<ILattice>(batches[0].TreeId)
                    .SetManyAtomicAsync(batches[0].Entries, JointOperationId);
                return;
            default:
                await grainFactory.SetManyAtomicAsync(batches, JointOperationId);
                return;
        }
    }

    /// <inheritdoc />
    public async Task<ViewCrossTreeDecision> RegisterDegradedAsync(string viewName)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);

        // The joint flip already committed (a late degrade lost the race): tell the
        // caller it is committed so it applies the joint result rather than
        // double-writing its slice locally.
        if (state.State.Applied)
        {
            return ViewCrossTreeDecision.Committed;
        }

        // Terminally degrade: no joint flip will ever be issued. Persist before
        // returning so a redelivery (or another participant's registration) sees
        // the terminal decision and also flips locally - never jointly.
        if (!state.State.Degraded)
        {
            state.State.Degraded = true;
            await state.WriteStateAsync();
            await SlideTtlAsync();
        }

        return ViewCrossTreeDecision.DegradedResult;
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="incoming"/> is the same set
    /// (ignoring order and duplicates) as the frozen wait set. Both wait sets are
    /// tiny (one entry per participant view), so the linear membership scans are
    /// cheaper than allocating a <see cref="HashSet{T}"/> per later registration.
    /// </summary>
    private bool WaitSetMatches(IReadOnlyList<string> incoming)
    {
        var frozen = state.State.WaitSet;
        foreach (var view in frozen)
        {
            if (!Contains(incoming, view))
            {
                return false;
            }
        }

        foreach (var view in incoming)
        {
            if (!frozen.Contains(view))
            {
                return false;
            }
        }

        return true;
    }

    private static bool Contains(IReadOnlyList<string> list, string value)
    {
        for (var i = 0; i < list.Count; i++)
        {
            if (string.Equals(list[i], value, StringComparison.Ordinal))
            {
                return true;
            }
        }

        return false;
    }
}
