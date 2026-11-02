using Microsoft.Extensions.Logging;
using Orleans.Lattice.BPlusTree.State;
using Orleans.Runtime;
using Orleans.Timers;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// The tree's range-delete fence (epic #5605, member 2); see
/// <see cref="ITreeRangeDeleteFenceGrain"/> for the protocol and design note section
/// 2, "Range delete: fenced at the tree", for why the fence is tree-level: a range
/// delete's record is replayed by every overlapping leaf of every shard, so a
/// shard-scoped fence fails after a partial fan-out.
/// </summary>
/// <remarks>
/// <para>
/// <b>One generation for every walk.</b> Every shard's walk of a fenced delete is
/// stamped with the tree's range-delete generation, so the close's one re-claim per
/// WAL partition fences every shard's walk at once, and the heads it returns are
/// valid for every shard's marker (<c>ReadPositionHonestRangeDeletePerShardClose</c>).
/// </para>
/// <para>
/// <b>Atomic open check (revision 5, B1).</b> The grain is non-reentrant: a close runs
/// from its first re-claim to its last marker close without another request of this
/// grain interleaving, and it marks its intent closing, durably, before the first
/// re-claim. <see cref="GetOpenIntentGenerationAsync"/> answers from that one state
/// (<c>ReadPositionHonestRestampDuringClose</c>).
/// </para>
/// <para>
/// <b>Generations are claimed before they are handed out.</b> An intent is handed a
/// generation only once it is durable at every partition, so a walk's stamped append
/// never meets a partition with no entry for it except after a WAL shard restart,
/// where the leaf re-presents the same generation. A close raises the generation
/// durably before it re-claims it, so a close interrupted mid-way is re-driven by the
/// reminder and finishes claiming the same generation.
/// </para>
/// <para>
/// The marker calls it makes go to <c>[AlwaysInterleave]</c> shard-root methods, so a
/// shard root that is itself waiting on this grain in a walk's open check can never
/// deadlock against a close.
/// </para>
/// </remarks>
internal sealed class TreeRangeDeleteFenceGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    IReminderRegistry reminderRegistry,
    LatticeOptionsResolver optionsResolver,
    ILogger<TreeRangeDeleteFenceGrain> logger,
    [PersistentState("tree-range-delete-fence", LatticeOptions.StorageProviderName)]
    IPersistentState<TreeRangeDeleteFenceState> state) : ITreeRangeDeleteFenceGrain, IRemindable, IGrainBase
{
    /// <summary>The name of the per-tree reminder that closes intents past their deadline.</summary>
    internal const string ReminderName = "range-delete-fence";

    /// <summary>How often the reminder ticks while an intent is open.</summary>
    internal static readonly TimeSpan ReminderPeriod = TimeSpan.FromMinutes(1);

    /// <summary>How many times a claim is re-issued above a refusal before the fence gives up.</summary>
    internal const int MaxClaimRounds = 8;

    private bool _reminderRegistered;

    IGrainContext IGrainBase.GrainContext => context;

    private string TreeId => context.GrainId.Key.ToString()!;

    /// <summary>The marker token a range-delete intent opens on every shard root.</summary>
    internal static string MarkerToken(Guid intentId) => string.Concat("rd:", intentId.ToString("N"));

    /// <inheritdoc />
    public async Task<long> OpenIntentAsync(Guid intentId, string startInclusive, string endExclusive, long deadlineUtcTicks, int[] shards)
    {
        ArgumentNullException.ThrowIfNull(startInclusive);
        ArgumentNullException.ThrowIfNull(endExclusive);
        ArgumentNullException.ThrowIfNull(shards);
        if (intentId == Guid.Empty)
        {
            throw new ArgumentException("A range-delete intent needs an id.", nameof(intentId));
        }

        if (state.State.Intents.TryGetValue(intentId, out var existing))
        {
            if (existing.Closing)
            {
                throw new InvalidOperationException($"Range-delete intent {intentId:N} of tree '{TreeId}' is already closing.");
            }

            return state.State.Generation;
        }

        await EnsureGenerationClaimedAsync();

        // Persisted before any marker opens, so a crash leaves an intent the reminder
        // closes, closing whatever markers it had opened.
        state.State.Intents[intentId] = new TreeRangeDeleteIntent
        {
            IntentId = intentId,
            StartInclusive = startInclusive,
            EndExclusive = endExclusive,
            DeadlineUtcTicks = deadlineUtcTicks,
            Shards = (int[])shards.Clone(),
        };
        await state.WriteStateAsync();
        await EnsureReminderAsync();

        var token = MarkerToken(intentId);
        var opens = new Task[shards.Length];
        for (var i = 0; i < shards.Length; i++)
        {
            opens[i] = ShardRoot(shards[i]).OpenHandOffMarkerAsync(token, startInclusive, endExclusive);
        }

        await Task.WhenAll(opens);
        LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindRangeDelete, opened: true, TreeId);
        return state.State.Generation;
    }

    /// <inheritdoc />
    public Task<long?> GetOpenIntentGenerationAsync(Guid intentId) =>
        Task.FromResult<long?>(
            state.State.Intents.TryGetValue(intentId, out var intent) && !intent.Closing
                ? state.State.Generation
                : null);

    /// <inheritdoc />
    public async Task CloseIntentAsync(Guid intentId)
    {
        if (state.State.Intents.TryGetValue(intentId, out var intent))
        {
            await CloseCoreAsync(intent);
        }
    }

    /// <inheritdoc />
    public async Task ReceiveReminder(string reminderName, TickStatus status)
    {
        if (!string.Equals(reminderName, ReminderName, StringComparison.Ordinal))
        {
            return;
        }

        var now = DateTime.UtcNow.Ticks;
        foreach (var intent in state.State.Intents.Values.ToArray())
        {
            if (intent.Closing || intent.DeadlineUtcTicks <= now)
            {
                try
                {
                    await CloseCoreAsync(intent);
                }
                catch (Exception ex)
                {
                    // Left open, and re-driven on the next tick: an open intent costs
                    // latency (its markers hold the overlapping leaves), never data.
                    logger.LogWarning(
                        ex,
                        "Tree {TreeId}: could not close range-delete intent {IntentId}; the next reminder tick retries (epic #5605).",
                        TreeId,
                        intent.IntentId);
                }
            }
        }

        if (state.State.Intents.Count == 0)
        {
            await UnregisterReminderAsync();
        }
    }

    private async Task CloseCoreAsync(TreeRangeDeleteIntent intent)
    {
        if (!intent.Closing)
        {
            // Rule 1: closing, durably, before the first re-claim, with the generation
            // the close will claim raised in the same write, so a re-driven close claims
            // the same one.
            intent.Closing = true;
            state.State.Generation = Math.Max(state.State.Generation, state.State.ClaimedGeneration) + 1;
            await state.WriteStateAsync();
        }

        var heads = await ClaimEverywhereAsync();
        var token = MarkerToken(intent.IntentId);
        var closes = new Task[intent.Shards.Length];
        for (var i = 0; i < intent.Shards.Length; i++)
        {
            closes[i] = ShardRoot(intent.Shards[i]).CloseHandOffMarkerAsync(token, heads);
        }

        await Task.WhenAll(closes);
        state.State.Intents.Remove(intent.IntentId);
        await state.WriteStateAsync();
        LatticeMetrics.RecordHandOffMarker(LatticeMetrics.HandOffKindRangeDelete, opened: false, TreeId);
        if (state.State.Intents.Count == 0)
        {
            await UnregisterReminderAsync();
        }
    }

    /// <summary>
    /// Ensures the current generation is claimed at every partition before an intent
    /// is handed it: the first intent claims generation 1, and a close interrupted
    /// mid-claim is finished.
    /// </summary>
    private async Task EnsureGenerationClaimedAsync()
    {
        if (state.State.Generation == 0)
        {
            state.State.Generation = 1;
            await state.WriteStateAsync();
        }

        if (state.State.ClaimedGeneration < state.State.Generation)
        {
            await ClaimEverywhereAsync();
        }
    }

    /// <summary>
    /// Claims <see cref="TreeRangeDeleteFenceState.Generation"/> at every WAL partition
    /// of the tree and returns the claim heads, indexed by partition. A refusal means a
    /// partition already holds a higher generation (this state was lost or reset): the
    /// fence raises its generation above it and claims again, everywhere.
    /// </summary>
    private async Task<long[]> ClaimEverywhereAsync()
    {
        var partitions = await optionsResolver.GetWalPartitionsAsync(TreeId);
        for (var round = 1; ; round++)
        {
            var generation = state.State.Generation;
            var claims = new Task<WalAppendFenceClaimResult>[partitions];
            for (var p = 0; p < partitions; p++)
            {
                claims[p] = grainFactory
                    .GetGrain<IWalShardGrain>(string.Concat(TreeId, "/", p.ToString(System.Globalization.CultureInfo.InvariantCulture)))
                    .ClaimRoutingGenerationAsync(WalRoutingInitiator.TreeRangeDelete, generation, CancellationToken.None);
            }

            var results = await Task.WhenAll(claims);
            var refusedAbove = 0L;
            var heads = new long[partitions];
            for (var p = 0; p < partitions; p++)
            {
                if (results[p].Accepted)
                {
                    heads[p] = results[p].Head;
                }
                else
                {
                    refusedAbove = Math.Max(refusedAbove, results[p].Refusal);
                }
            }

            if (refusedAbove == 0)
            {
                state.State.ClaimedGeneration = Math.Max(state.State.ClaimedGeneration, generation);
                await state.WriteStateAsync();
                return heads;
            }

            if (round >= MaxClaimRounds)
            {
                throw new InvalidOperationException(
                    $"Tree '{TreeId}': the range-delete generation could not be claimed at every WAL partition after {MaxClaimRounds} rounds.");
            }

            state.State.Generation = Math.Max(state.State.Generation, refusedAbove) + 1;
            await state.WriteStateAsync();
        }
    }

    private IShardRootGrain ShardRoot(int shardIndex) =>
        grainFactory.GetGrain<IShardRootGrain>(
            string.Concat(TreeId, "/", shardIndex.ToString(System.Globalization.CultureInfo.InvariantCulture)));

    private async Task EnsureReminderAsync()
    {
        if (_reminderRegistered)
        {
            return;
        }

        await reminderRegistry.RegisterOrUpdateReminder(context.GrainId, ReminderName, ReminderPeriod, ReminderPeriod);
        _reminderRegistered = true;
    }

    private async Task UnregisterReminderAsync()
    {
        var reminder = await reminderRegistry.GetReminder(context.GrainId, ReminderName);
        if (reminder is not null)
        {
            await reminderRegistry.UnregisterReminder(context.GrainId, reminder);
        }

        _reminderRegistered = false;
    }
}
