using System.Globalization;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Receiver-side coordinator for a replicated cross-tree atomic write. See
/// <see cref="ILatticeCrossTreeReceiverGrain"/> for the contract and rationale.
/// One activation per <c>(originClusterId, operationId)</c> (this grain's
/// compound key). Purely reactive: it is driven entirely by the per-tree
/// terminals that arrive over replication - it never runs a saga and never
/// calls back into another grain, so it cannot participate in a circular wait.
/// <para>
/// Crash recovery rides on replication's own at-least-once redelivery: every
/// <see cref="NotifyTerminalAsync"/> persists before returning and returns the
/// full finalize set whenever decided, so a redelivered terminal re-heals
/// materialization idempotently. A one-shot retention reminder clears the
/// persisted state once the configured <see cref="LatticeOptions.AtomicWriteRetention"/>
/// elapses after the decision.
/// </para>
/// </summary>
internal sealed class LatticeCrossTreeReceiverGrain(
    IGrainContext context,
    IReminderRegistry reminderRegistry,
    IOptionsMonitor<LatticeOptions> optionsMonitor,
    ILogger<LatticeCrossTreeReceiverGrain> logger,
    [PersistentState("cross-tree-receiver", LatticeOptions.StorageProviderName)]
    IPersistentState<CrossTreeReceiverState> state)
    : TtlGrain<LatticeCrossTreeReceiverGrain>(context, reminderRegistry, logger), ILatticeCrossTreeReceiverGrain
{
    private const string RetentionReminderName = "cross-tree-receiver-retention";

    /// <summary>
    /// In-memory marker, <c>true</c> only while a freshly-computed decision is
    /// mid-flight in <see cref="NotifyTerminalAsync"/> and therefore not yet
    /// durable (the in-memory <see cref="CrossTreeReceiverState.Decided"/> has
    /// been set but <c>WriteStateAsync</c> has not yet
    /// completed). The <c>[AlwaysInterleave]</c> <see cref="GetDecisionAsync"/>
    /// and the redelivery early-return must <b>not</b> publish a decision in
    /// this window: the caller (<c>TxRegistryGrain.ResolveReceiverDelegatedAsync</c>)
    /// durably caches the returned verdict and drops its delegation, so a
    /// decision that a crash then loses before the write lands would become a
    /// permanent partial cross-tree view. On a fresh activation this is
    /// <c>false</c>, so a decision loaded from durable storage is published
    /// immediately - it is durable by construction. A failed write leaves it
    /// <c>true</c>, so the next (redelivered) terminal re-drives the persist.
    /// </summary>
    private bool _decisionAwaitingPersist;

    /// <inheritdoc />
    protected override string TtlReminderName => RetentionReminderName;

    /// <inheritdoc />
    protected override TimeSpan ResolveTtl() => optionsMonitor.CurrentValue.AtomicWriteRetention;

    /// <inheritdoc />
    protected override async Task OnTtlExpiredAsync()
    {
        Logger.LogInformation(
            "Cross-tree receiver {Key}: retention window expired; clearing state.",
            GrainContext.GrainId.Key);
        await state.ClearStateAsync();
    }

    /// <summary>
    /// Builds the compound grain key for a receiver coordinator from the source
    /// cluster id and the cross-tree operation id.
    /// <para>
    /// The key is length-prefixed - the decimal character length of
    /// <paramref name="originClusterId"/>, an underscore, then the two halves
    /// concatenated - rather than joined by a delimiter character. A
    /// length prefix keeps the two halves unambiguous regardless of the
    /// characters either contains (the property the old ASCII Unit Separator
    /// delimiter was chosen for), while producing a key that is free of any
    /// control character. That matters because this grain persists via a
    /// storage provider: Azure Table grain storage carries the primary key into
    /// the request URL and the Partition/Row key columns, both of which reject
    /// control chars 0x00-0x1F, so a raw Unit Separator (0x1F) in the key made
    /// the receiver fail to activate with HTTP 400 on a real Azure deployment.
    /// </para>
    /// <para>
    /// The encoding is a pure deterministic function of its two inputs so every
    /// region derives an identical key for the same operation. Changing the
    /// encoding changes this grain's identity; that is acceptable here because
    /// the previous (control-char) key was non-functional on the Azure Table
    /// path, so there is no live durable state to stay compatible with.
    /// </para>
    /// </summary>
    [GrainKeyBuilder]
    public static string ComputeKey(string originClusterId, string operationId)
    {
        ArgumentException.ThrowIfNullOrEmpty(originClusterId);
        ArgumentException.ThrowIfNullOrEmpty(operationId);
        return string.Concat(
            originClusterId.Length.ToString(CultureInfo.InvariantCulture),
            "_",
            originClusterId,
            operationId);
    }

    /// <inheritdoc />
    public async Task<CrossTreeReceiverDecision> NotifyTerminalAsync(CrossTreeReceiverTerminal terminal)
    {
        ArgumentNullException.ThrowIfNull(terminal);
        ArgumentException.ThrowIfNullOrEmpty(terminal.TreeId);
        ArgumentNullException.ThrowIfNull(terminal.WaitSet);
        ArgumentNullException.ThrowIfNull(terminal.ObservedSourceShards);

        // Already decided AND durable: a redelivered terminal re-heals
        // materialization. Return the full finalize set without re-persisting.
        // While a decision is awaiting persistence (a prior write failed mid
        // flight) we must NOT short-circuit here - fall through and re-drive
        // the persist so the verdict only escapes once durable.
        if (state.State.Decided && !_decisionAwaitingPersist)
        {
            return BuildDecision();
        }

        if (state.State.WaitSet.Count == 0)
        {
            // First terminal: freeze the wait set and identity. The wait set is
            // canonicalized (ordinal-sorted, de-duplicated) so the exact-match
            // validation below is order-insensitive.
            state.State.WaitSet = CanonicalStringSet.SortedDistinct(terminal.WaitSet);
            state.State.OriginClusterId = terminal.OriginClusterId;
            state.State.OperationId = terminal.OperationId;
            state.State.StartedAtTicks = DateTime.UtcNow.Ticks;
        }
        else
        {
            // Later terminal: the wait set must be identical to the frozen one.
            // Config drift on the receiver between two terminals of the same
            // operation cannot be allowed to shrink or grow the barrier.
            if (!WaitSetMatches(terminal.WaitSet))
            {
                throw new InvalidOperationException(
                    $"Cross-tree receiver '{GrainContext.GrainId.Key}' received a terminal for tree " +
                    $"'{terminal.TreeId}' whose wait set differs from the frozen wait set; the replicated " +
                    "participant set must be stable across every terminal of a cross-tree operation.");
            }
        }

        if (!state.State.WaitSet.Contains(terminal.TreeId))
        {
            // A terminal whose own tree is not in the wait set can never help
            // complete the barrier and signals a protocol violation.
            throw new InvalidOperationException(
                $"Cross-tree receiver '{GrainContext.GrainId.Key}' received a terminal for tree " +
                $"'{terminal.TreeId}' that is absent from the frozen wait set.");
        }

        // Record (or idempotently overwrite) this tree's terminal.
        state.State.Arrived[terminal.TreeId] = terminal;

        // The barrier completes when every wait-set tree has arrived. The
        // global verdict is commit iff every arrived terminal voted commit. A
        // plain loop avoids the method-group delegate the LINQ All overload
        // would allocate on every terminal.
        var complete = true;
        foreach (var waitTree in state.State.WaitSet)
        {
            if (!state.State.Arrived.ContainsKey(waitTree))
            {
                complete = false;
                break;
            }
        }
        if (complete)
        {
            state.State.Decided = true;
            state.State.Committed = state.State.Arrived.Values.All(t => t.Committed);
            // Mark the decision non-durable until the write below completes, so
            // an interleaving GetDecisionAsync cannot publish it early.
            _decisionAwaitingPersist = true;
        }

        // Persist BEFORE returning so the decision (and the arrival that may yet
        // complete it on a later terminal) is durable - the registration that
        // preceded this call is thereby linearized against durable state.
        await state.WriteStateAsync();

        // The write landed: the decision (if any) is now durable and may be
        // published to readers.
        _decisionAwaitingPersist = false;

        if (!state.State.Decided)
        {
            return CrossTreeReceiverDecision.InFlight;
        }

        // Arm one-shot retention cleanup now that the decision is terminal.
        await SlideTtlAsync();
        return BuildDecision();
    }

    /// <inheritdoc />
    public Task<TxStatus> GetDecisionAsync()
    {
        // Only publish a decision that is durable: a decision still awaiting its
        // persist (set in memory by an in-flight NotifyTerminalAsync) reads as
        // InFlight so the caller never durably caches a verdict a crash could
        // lose. A decision loaded from storage on activation has the marker
        // false and is published immediately.
        var status = state.State.Decided && !_decisionAwaitingPersist
            ? (state.State.Committed ? TxStatus.Committed : TxStatus.Aborted)
            : TxStatus.InFlight;
        return Task.FromResult(status);
    }

    /// <summary>
    /// Builds the decided <see cref="CrossTreeReceiverDecision"/> from the
    /// persisted arrivals. Returns the full per-tree finalize set so a
    /// redelivered terminal re-heals every tree's materialization.
    /// </summary>
    private CrossTreeReceiverDecision BuildDecision()
    {
        var finalize = new List<CrossTreeReceiverTreeFinalize>(state.State.Arrived.Count);
        foreach (var t in state.State.Arrived.Values)
        {
            finalize.Add(new CrossTreeReceiverTreeFinalize
            {
                TreeId = t.TreeId,
                TransactionId = t.TransactionId,
                ObservedSourceShards = t.ObservedSourceShards,
                TerminalHlc = t.TerminalHlc,
                OriginClusterId = t.OriginClusterId,
            });
        }
        return new CrossTreeReceiverDecision
        {
            Decided = true,
            Committed = state.State.Committed,
            TreesToFinalize = finalize,
        };
    }

    /// <summary>
    /// Returns <c>true</c> when <paramref name="incoming"/> is the same set
    /// (ignoring order and duplicates) as the frozen wait set. The frozen wait
    /// set is already de-duplicated, so equality holds iff every frozen tree is
    /// present in <paramref name="incoming"/> and every incoming tree is present
    /// in the frozen set. Both wait sets are tiny (one entry per replicated
    /// participant), so the linear membership scans are cheaper than allocating
    /// a <see cref="HashSet{T}"/> and a method-group delegate per later terminal.
    /// </summary>
    private bool WaitSetMatches(IReadOnlyList<string> incoming)
    {
        var frozen = state.State.WaitSet;
        foreach (var tree in frozen)
        {
            if (!Contains(incoming, tree)) return false;
        }
        foreach (var tree in incoming)
        {
            if (!frozen.Contains(tree)) return false;
        }
        return true;
    }

    private static bool Contains(IReadOnlyList<string> list, string value)
    {
        for (var i = 0; i < list.Count; i++)
        {
            if (string.Equals(list[i], value, StringComparison.Ordinal)) return true;
        }
        return false;
    }
}
