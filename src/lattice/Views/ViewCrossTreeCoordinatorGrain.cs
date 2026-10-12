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
/// single cross-tree atomic write. Independent consumer reads may straddle the
/// publication; there is no shared cross-view snapshot acquisition contract.
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
/// <para>
/// A second, mandatory ready set repairs the publication using freshly admitted
/// source outcomes, never by replaying the original frozen slices. Repair
/// contributions remain immutable while callers hold their publication/rebind
/// fences. Retention begins only after all participants acknowledge durable
/// checkpoint retirement, so a lost response cannot release registration closure.
/// </para>
/// </summary>
internal sealed class ViewCrossTreeCoordinatorGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    IViewJointRepairAdmissionGate admissionGate,
    ILogger<ViewCrossTreeCoordinatorGrain> logger,
    [PersistentState("view-cross-tree", LatticeOptions.StorageProviderName)]
    IPersistentState<ViewCrossTreeCoordinatorState> state)
    : TtlGrain<ViewCrossTreeCoordinatorGrain>(context, reminderRegistry, logger),
        IViewCrossTreeCoordinatorGrain, IViewJointRepairCapable
{
    private const string RetentionReminderName = "view-cross-tree-retention";
    private bool _protocolObserved;
    private int? _durableProtocolVersion;

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => optionsMonitor.CurrentValue.AtomicWriteRetention;

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        ValidateProtocolVersion();
        if (state.State.RepairProtocolVersion == ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion
            && state.State.Decided && !RepairClosed)
        {
            Logger.LogWarning(
                "View cross-tree coordinator {Key}: retaining publication while participant repair checkpoints remain open.",
                GrainContext.GrainId.Key);
            return;
        }

        Logger.LogInformation(
            "View cross-tree coordinator {Key}: retention window expired; clearing state.",
            GrainContext.GrainId.Key);
        await state.ClearStateAsync();
        _durableProtocolVersion = null;
        _protocolObserved = true;
    }

    /// <summary>
    /// The deterministic, idempotent operation id for the joint cross-tree flip,
    /// derived from the coordinator key so a redelivery (or a crash-recovery
    /// re-drive) re-attaches to the completed saga rather than minting a fresh
    /// one. The coordinator key has no '/' (validated at the source cross-tree
    /// write), so the derived id is a valid cross-tree operation id.
    /// </summary>
    private string JointOperationId => $"mv-xt-{GrainContext.GrainId.Key}";

    private bool RepairClosed => state.State.RepairApplied
        && state.State.WaitSet.All(view => state.State.RepairAcknowledged.Contains(view));

    private ViewCrossTreeDecision OriginalCommittedDecision =>
        state.State.RepairProtocolVersion == ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion
            ? ViewCrossTreeDecision.Committed
            : ViewCrossTreeDecision.LegacyCommitted;

    private void ValidateProtocolVersion()
    {
        if (state.State.RepairProtocolVersion is not (0 or ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion))
        {
            throw new InvalidOperationException(
                $"Unsupported view cross-tree repair protocol {state.State.RepairProtocolVersion}; use a compatible host.");
        }
        if (!_protocolObserved)
        {
            _durableProtocolVersion = state.RecordExists
                && (state.State.RepairProtocolVersion != 0 || state.State.WaitSet.Count != 0
                    || state.State.Slices.Count != 0 || state.State.Decided
                    || state.State.Applied || state.State.Degraded || state.State.StartedAtTicks != 0)
                ? state.State.RepairProtocolVersion
                : null;
            _protocolObserved = true;
        }
    }

    private async Task PersistStateAsync()
    {
        await state.WriteStateAsync();
        _durableProtocolVersion = state.State.RepairProtocolVersion;
    }

    /// <inheritdoc />
    public Task<int?> GetRepairProtocolVersionAsync()
    {
        ValidateProtocolVersion();
        return Task.FromResult(_durableProtocolVersion);
    }

    private void RequireCurrentRepairProtocol()
    {
        ValidateProtocolVersion();
        if (state.State.RepairProtocolVersion != ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion)
        {
            throw new InvalidOperationException(
                "Legacy cross-tree operations finish under their original contract and cannot certify fresh repair.");
        }
    }

    /// <inheritdoc />
    public async Task<ViewCrossTreeDecision> RegisterReadyAsync(ViewCrossTreeReadiness readiness)
    {
        ArgumentNullException.ThrowIfNull(readiness);
        ArgumentException.ThrowIfNullOrEmpty(readiness.ViewName);
        ArgumentException.ThrowIfNullOrEmpty(readiness.ViewTreeId);
        ArgumentNullException.ThrowIfNull(readiness.WaitSet);
        ArgumentNullException.ThrowIfNull(readiness.Upserts);
        ValidateProtocolVersion();
        if (readiness.RepairProtocolVersion is not (0 or ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion))
        {
            throw new InvalidOperationException("Unsupported participant repair protocol; use a compatible host.");
        }
        if (_durableProtocolVersion is null)
        {
            if (!await admissionGate.IsOpenAsync())
            {
                Logger.LogDebug(
                    "View cross-tree coordinator {Key}: new repair admission awaits compatible cluster membership.",
                    GrainContext.GrainId.Key);
                return ViewCrossTreeDecision.NotReady;
            }
            state.State.RepairProtocolVersion = ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion;
        }

        if (state.State.Degraded)
        {
            return ViewCrossTreeDecision.DegradedResult;
        }

        if (state.State.RepairProtocolVersion == ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion
            && readiness.RepairProtocolVersion != ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion)
        {
            if (state.State.WaitSet.Count == 0)
            {
                state.State.WaitSet = CanonicalStringSet.SortedDistinct(readiness.WaitSet);
                state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
            }
            if (!WaitSetMatches(readiness.WaitSet) || !state.State.WaitSet.Contains(readiness.ViewName))
            {
                throw new InvalidOperationException("An incompatible caller must still name the frozen participant set.");
            }
            await PersistStateAsync();
            return ViewCrossTreeDecision.NotReady;
        }

        // Already applied and durable: a redelivered registration re-confirms the
        // joint flip is committed. Return the memoized decision without re-issuing.
        if (state.State.Applied)
        {
            return OriginalCommittedDecision;
        }

        if (state.State.WaitSet.Count == 0)
        {
            // First registration: freeze the (canonicalised) wait set.
            state.State.WaitSet = CanonicalStringSet.SortedDistinct(readiness.WaitSet);
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

        // The persisted decision fixes the payload and physical targets of the
        // idempotent write. A retry must not replace them with later readiness.
        if (!state.State.Decided)
        {
            state.State.Slices[readiness.ViewName] = new ViewCrossTreeSlice
            {
                ViewName = readiness.ViewName,
                ViewTreeId = readiness.ViewTreeId,
                Upserts = readiness.Upserts,
                Deletes = readiness.Deletes ?? [],
            };
        }

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
            await PersistStateAsync();
            return ViewCrossTreeDecision.NotReady;
        }

        // The wait set is complete (or a prior call already decided). Persist the
        // joint-flip intent before issuing the idempotent write so a crash mid
        // apply is re-driven by a redelivered registration rather than leaving a
        // partial set of views.
        if (!state.State.Decided)
        {
            state.State.Decided = true;
        }

        await CompleteOriginalPublicationAsync();
        if (state.State.RepairProtocolVersion == 0)
        {
            await SlideTtlAsync();
        }

        // Current-protocol publication is not registration closure: participants
        // still owe fresh repair and durable checkpoint acknowledgement.
        return OriginalCommittedDecision;
    }

    private async Task CompleteOriginalPublicationAsync()
    {
        await PersistStateAsync();
        await IssueJointFlipAsync(state.State.Slices, JointOperationId);

        var persisted = false;
        state.State.Applied = true;
        try
        {
            await PersistStateAsync();
            persisted = true;
        }
        finally
        {
            if (!persisted)
            {
                state.State.Applied = false;
            }
        }
    }

    /// <summary>
    /// Issues the joint cross-tree flip across every participant view tree whose
    /// slice has at least one upsert or delete, idempotently keyed by the supplied
    /// <paramref name="operationId"/>. Empty slices contribute nothing to flip,
    /// so they are excluded; a single non-empty slice falls back to the
    /// single-tree atomic write (the cross-tree primitive requires two or more
    /// trees). Each slice's retraction deletes ride inside the joint flip
    /// alongside its upserts, so a re-key projection's old-key delete and
    /// new-key upsert flip atomically.
    /// </summary>
    private async Task IssueJointFlipAsync(
        Dictionary<string, ViewCrossTreeSlice> slices,
        string operationId)
    {
        // Authorise the joint flip's writes to participant view trees (propagates
        // through the cross-tree atomic-write saga via RequestContext).
        using var viewWriteScope = ViewWriteContext.BeginScope();
        var batches = new List<LatticeTreeBatch>(slices.Count);
        foreach (var slice in slices.Values)
        {
            if (slice.Upserts.Count == 0 && slice.Deletes.Count == 0)
            {
                continue;
            }

            batches.Add(BuildMixedBatch(slice));
        }

        switch (batches.Count)
        {
            case 0:
                // Every participant view's slice is empty: nothing to flip, but the
                // joint guarantee is trivially satisfied.
                return;
            case 1:
                // One non-empty slice: a cross-tree write needs two or more trees,
                // so flip the single tree atomically (still all-or-nothing for it),
                // carrying its upserts and retraction deletes in one mixed op.
                var only = slices.Values.First(
                    s => s.Upserts.Count > 0 || s.Deletes.Count > 0);
                await grainFactory
                    .GetGrain<ILattice>(only.ViewTreeId)
                    .SetManyAtomicAsync(only.Upserts, only.Deletes, operationId);
                return;
            default:
                await grainFactory.SetManyAtomicAsync(batches, operationId);
                return;
        }
    }

    /// <inheritdoc />
    public async Task<ViewCrossTreeDecision> RegisterRepairReadyAsync(ViewCrossTreeReadiness readiness)
    {
        ArgumentNullException.ThrowIfNull(readiness);
        ArgumentException.ThrowIfNullOrEmpty(readiness.ViewName);
        ArgumentException.ThrowIfNullOrEmpty(readiness.ViewTreeId);
        ArgumentNullException.ThrowIfNull(readiness.WaitSet);
        ArgumentNullException.ThrowIfNull(readiness.Upserts);
        ArgumentNullException.ThrowIfNull(readiness.Deletes);
        RequireCurrentRepairProtocol();
        if (readiness.RepairProtocolVersion != ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion)
        {
            throw new InvalidOperationException("Joint repair requires current-protocol durable participant holds.");
        }
        if (!StringComparer.Ordinal.Equals(readiness.OperationId, GrainContext.GrainId.Key.ToString())
            || !WaitSetMatches(readiness.WaitSet)
            || !state.State.WaitSet.Contains(readiness.ViewName))
        {
            throw new InvalidOperationException(
                "Joint repair readiness must name the original operation and its frozen participant set.");
        }

        if (!state.State.Applied || state.State.Degraded)
        {
            throw new InvalidOperationException("Joint repair requires a durably committed original publication.");
        }

        if (state.State.RepairApplied)
        {
            return ViewCrossTreeDecision.Committed;
        }

        if (!state.State.RepairDecided && !state.State.RepairSlices.ContainsKey(readiness.ViewName))
        {
            state.State.RepairSlices[readiness.ViewName] = new ViewCrossTreeSlice
            {
                ViewName = readiness.ViewName,
                ViewTreeId = readiness.ViewTreeId,
                Upserts = readiness.Upserts.Select(entry =>
                    new KeyValuePair<string, byte[]>(entry.Key, (byte[])entry.Value.Clone())).ToList(),
                Deletes = readiness.Deletes.ToArray(),
            };
        }

        if (!state.State.RepairDecided)
        {
            if (state.State.WaitSet.Any(view => !state.State.RepairSlices.ContainsKey(view)))
            {
                await PersistStateAsync();
                return ViewCrossTreeDecision.NotReady;
            }
            state.State.RepairDecided = true;
        }

        // Persist again on a decided retry: a failed intent write can leave the
        // in-memory flag set even though no durable intent exists.
        await PersistStateAsync();
        await IssueJointFlipAsync(state.State.RepairSlices, $"mv-repair-xt-{GrainContext.GrainId.Key}");
        var persisted = false;
        state.State.RepairApplied = true;
        try
        {
            await PersistStateAsync();
            persisted = true;
        }
        finally
        {
            if (!persisted)
            {
                state.State.RepairApplied = false;
            }
        }
        return ViewCrossTreeDecision.Committed;
    }

    /// <inheritdoc />
    public Task<ViewCrossTreeReadiness?> GetRepairReadinessAsync(string viewName)
    {
        ValidateRepairParticipant(viewName);
        if (!state.State.RepairSlices.TryGetValue(viewName, out var slice))
        {
            return Task.FromResult<ViewCrossTreeReadiness?>(null);
        }

        return Task.FromResult<ViewCrossTreeReadiness?>(new ViewCrossTreeReadiness
        {
            OperationId = GrainContext.GrainId.Key.ToString(),
            ViewName = slice.ViewName,
            ViewTreeId = slice.ViewTreeId,
            WaitSet = state.State.WaitSet.ToArray(),
            Upserts = slice.Upserts.Select(entry =>
                new KeyValuePair<string, byte[]>(entry.Key, (byte[])entry.Value.Clone())).ToList(),
            Deletes = slice.Deletes.ToArray(),
            RepairProtocolVersion = ViewCrossTreeCoordinatorState.CurrentRepairProtocolVersion,
        });
    }

    /// <inheritdoc />
    public async Task AcknowledgeRepairAsync(string viewName)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ValidateProtocolVersion();
        if (state.State.WaitSet.Count == 0 && !state.State.Decided && !state.State.Degraded)
        {
            Logger.LogDebug(
                "View cross-tree coordinator {Key}: checkpoint acknowledgement for {ViewName} after retention cleanup.",
                GrainContext.GrainId.Key, viewName);
            return;
        }

        ValidateRepairParticipant(viewName);
        if (!state.State.RepairApplied)
        {
            throw new InvalidOperationException("Cannot retire a participant before joint repair commits.");
        }

        if (!state.State.RepairAcknowledged.Contains(viewName))
        {
            state.State.RepairAcknowledged.Add(viewName);
            var persisted = false;
            try
            {
                await PersistStateAsync();
                persisted = true;
            }
            finally
            {
                if (!persisted)
                {
                    state.State.RepairAcknowledged.Remove(viewName);
                }
            }
        }

        if (RepairClosed)
        {
            await SlideTtlAsync();
        }
    }

    private void ValidateRepairParticipant(string viewName)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        RequireCurrentRepairProtocol();
        if (!state.State.Applied || !state.State.WaitSet.Contains(viewName))
        {
            throw new InvalidOperationException("Repair requires an original committed participant.");
        }
    }

    /// <summary>
    /// Builds the cross-tree batch for a single view slice, unioning the slice's
    /// upserts and retraction deletes into one mixed <see cref="LatticeTreeBatch"/>
    /// (the deletes carry an empty value buffer and a true entry-delete flag) so
    /// the whole slice flips atomically inside the joint write.
    /// </summary>
    private static LatticeTreeBatch BuildMixedBatch(ViewCrossTreeSlice slice)
    {
        if (slice.Deletes.Count == 0)
        {
            return new LatticeTreeBatch(slice.ViewTreeId, slice.Upserts);
        }

        var entries = new List<KeyValuePair<string, byte[]>>(slice.Upserts.Count + slice.Deletes.Count);
        entries.AddRange(slice.Upserts);
        var entryDeletes = new List<bool>(entries.Capacity);
        for (var i = 0; i < slice.Upserts.Count; i++) entryDeletes.Add(false);
        foreach (var key in slice.Deletes)
        {
            entries.Add(new KeyValuePair<string, byte[]>(key, Array.Empty<byte>()));
            entryDeletes.Add(true);
        }

        return new LatticeTreeBatch(slice.ViewTreeId, entries, Predicate: null, EntryDeltas: null, EntryDeletes: entryDeletes);
    }

    /// <inheritdoc />
    public async Task<ViewCrossTreeDecision> RegisterDegradedAsync(string viewName)
    {
        ArgumentException.ThrowIfNullOrEmpty(viewName);
        ValidateProtocolVersion();

        // The joint flip already committed (a late degrade lost the race): tell the
        // caller it is committed so it applies the joint result rather than
        // double-writing its slice locally.
        if (state.State.Applied)
        {
            return OriginalCommittedDecision;
        }

        if (state.State.Decided)
        {
            // The joint write may already have committed despite a lost
            // response. Never certify per-tree fallback after durable intent.
            await CompleteOriginalPublicationAsync();
            if (state.State.RepairProtocolVersion == 0)
            {
                await SlideTtlAsync();
            }
            return OriginalCommittedDecision;
        }

        // Terminally degrade: no joint flip will ever be issued. Persist before
        // returning so a redelivery (or another participant's registration) sees
        // the terminal decision and also flips locally - never jointly.
        if (!state.State.Degraded)
        {
            state.State.Degraded = true;
            await PersistStateAsync();
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
            if (!OrdinalStrings.Contains(incoming, view))
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
}
