namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Snapshot-capture decision gate partial for <see cref="TxRegistryGrain"/>
/// (issue #4485).
/// <para>
/// <b>The defect.</b> A snapshot captures each physical shard's baseline at its
/// own moment, and a saga's terminal broadcast is one append per shard. A
/// capture taken while a multi-shard saga was broadcasting could therefore hold
/// one key post-saga and another pre-saga, and so could every backup built on
/// it.
/// </para>
/// <para>
/// <b>The gate.</b> A capture holds a lease-fenced gate on every registry key
/// of the tree while it captures. Under the gate no NEW decision can be
/// recorded (writes and prepares are never blocked), and the registry
/// snapshots its local decisions once, as D0. Every terminal anywhere follows a
/// durable local decision (<see cref="ITxRegistryGrain.GetStatusForTerminalAsync"/>
/// is the read every terminal-applying sweep uses), so every commit terminal a
/// baseline can contain belongs to a saga Committed in D0, and every such saga
/// had all of its prepares acknowledged before its decision. Resolving each
/// still-pending bucket against D0 therefore puts every saga on one side of the
/// capture on every shard.
/// </para>
/// <para>
/// <b>Held in memory only.</b> A registry reactivation drops every hold. That
/// re-admits decisions, and the capture learns of it when
/// <see cref="ReleaseCaptureGateAsync"/> (or a D0 lookup) reports the hold
/// lost, so it fails closed rather than accepting a cut a decision may have
/// crossed.
/// </para>
/// </summary>
internal sealed partial class TxRegistryGrain
{
    /// <summary>
    /// How long past its expiry a lapsed, never-released hold is remembered
    /// before it is pruned. It only needs to outlive the crashed or stalled
    /// capture's own release call, which then reports the lapse.
    /// </summary>
    internal static readonly TimeSpan LapsedHoldRetention = TimeSpan.FromMinutes(10);

    private Dictionary<Guid, CaptureHold>? _captureHolds;

    /// <summary>One capture's hold on this registry.</summary>
    private sealed class CaptureHold
    {
        public TxRegistryCaptureGateMode Mode;
        public DateTimeOffset ExpiresAt;
        public Dictionary<Guid, TxStatus>? Decisions;
    }

    /// <inheritdoc />
    public async Task AcquireCaptureGateAsync(Guid token, TxRegistryCaptureGateMode mode, TimeSpan lease)
    {
        if (token == Guid.Empty)
            throw new ArgumentException("A capture gate token must not be empty.", nameof(token));
        if (lease <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(lease), lease, "A capture gate lease must be positive.");

        var now = TimeProvider.GetUtcNow();
        PruneLapsedHolds(now);

        var holds = _captureHolds ??= new Dictionary<Guid, CaptureHold>();
        if (holds.TryGetValue(token, out var hold))
        {
            // A lapsed hold is never revived: decisions may have been recorded
            // while it was down, so a cut taken under it is already invalid.
            if (now > hold.ExpiresAt)
                throw GateLapsed();
            if (mode > hold.Mode) hold.Mode = mode;
            if (now + lease > hold.ExpiresAt) hold.ExpiresAt = now + lease;
        }
        else
        {
            hold = new CaptureHold { Mode = mode, ExpiresAt = now + lease };
            holds[token] = hold;
        }

        if (hold.Mode != TxRegistryCaptureGateMode.Gate || hold.Decisions is not null)
            return;

        // The hold is in force from the line above, synchronously, so no Mark
        // that has not yet passed its gate check can record a decision from
        // here on. A Mark that passed it earlier may still be riding a group
        // commit: the read-committed loop waits for it, and if its write failed
        // it rolled back and its caller's retry is refused.
        while (true)
        {
            var decisions = CaptureLocalDecisions();
            if (await WhenAllDurableAsync())
            {
                if (TimeProvider.GetUtcNow() > hold.ExpiresAt || !IsHeld(token, hold))
                    throw GateLapsed();
                hold.Decisions = decisions;
                return;
            }
        }
    }

    /// <inheritdoc />
    public Task<bool> RenewCaptureGateAsync(Guid token, TimeSpan lease)
    {
        if (lease <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(lease), lease, "A capture gate lease must be positive.");

        var now = TimeProvider.GetUtcNow();
        if (_captureHolds is null || !_captureHolds.TryGetValue(token, out var hold) || now > hold.ExpiresAt)
            return Task.FromResult(false);

        if (now + lease > hold.ExpiresAt) hold.ExpiresAt = now + lease;
        return Task.FromResult(true);
    }

    /// <inheritdoc />
    public Task<bool> ReleaseCaptureGateAsync(Guid token)
    {
        var now = TimeProvider.GetUtcNow();
        if (_captureHolds is null || !_captureHolds.Remove(token, out var hold))
            return Task.FromResult(false);

        return Task.FromResult(now <= hold.ExpiresAt);
    }

    /// <inheritdoc />
    public Task<Dictionary<Guid, TxStatus>> GetCaptureGateStatusManyAsync(Guid token, IReadOnlyList<Guid> txids)
    {
        ArgumentNullException.ThrowIfNull(txids);

        var now = TimeProvider.GetUtcNow();
        if (_captureHolds is null
            || !_captureHolds.TryGetValue(token, out var hold)
            || now > hold.ExpiresAt
            || hold.Decisions is not { } decisions)
        {
            throw GateLapsed();
        }

        var result = new Dictionary<Guid, TxStatus>(txids.Count);
        foreach (var txid in txids)
        {
            result[txid] = decisions.TryGetValue(txid, out var status) ? status : TxStatus.InFlight;
        }

        return Task.FromResult(result);
    }

    /// <inheritdoc />
    public async Task<TxStatus> GetStatusForTerminalAsync(Guid txid)
    {
        while (true)
        {
            var status = await ReadStatusForTerminalAsync(txid);
            if (await WhenDurableAsync(txid))
            {
                return status;
            }
        }
    }

    /// <inheritdoc />
    public async Task<Dictionary<Guid, TxStatus>> GetStatusManyForTerminalAsync(IReadOnlyList<Guid> txids)
    {
        ArgumentNullException.ThrowIfNull(txids);
        while (true)
        {
            var result = new Dictionary<Guid, TxStatus>(txids.Count);
            foreach (var txid in txids)
            {
                result[txid] = await ReadStatusForTerminalAsync(txid);
            }

            if (await WhenDurableAsync(txids))
            {
                return result;
            }
        }
    }

    /// <summary>
    /// The unguarded body of <see cref="GetStatusForTerminalAsync"/>: identical
    /// to the reader's answer for a locally decided or undecided txid, and for a
    /// delegated txid a verdict only once it is durably cached here.
    /// </summary>
    private async ValueTask<TxStatus> ReadStatusForTerminalAsync(Guid txid)
    {
        if (IsTombstoneExpired(txid))
        {
            return state.State.Decisions.ContainsKey(txid)
                ? TxStatus.Indeterminate
                : TxStatus.InFlight;
        }

        if (state.State.Decisions.TryGetValue(txid, out var status))
        {
            return status;
        }

        return await ResolveAnyDelegatedAsync(txid, terminalIntent: true);
    }

    /// <summary>
    /// Whether a live capture hold refuses new decisions (and delegated-verdict
    /// caching), with the remaining lease of the longest such hold.
    /// </summary>
    private bool IsDecisionGated(out TimeSpan retryAfter) =>
        IsHoldActive(TxRegistryCaptureGateMode.Gate, out retryAfter);

    /// <summary>
    /// Whether a live capture hold refuses new cross-tree delegation
    /// registrations, with the remaining lease of the longest such hold.
    /// </summary>
    private bool IsRegistrationFenced(out TimeSpan retryAfter) =>
        IsHoldActive(TxRegistryCaptureGateMode.Fence, out retryAfter);

    private bool IsHoldActive(TxRegistryCaptureGateMode atLeast, out TimeSpan retryAfter)
    {
        retryAfter = TimeSpan.Zero;
        if (_captureHolds is null || _captureHolds.Count == 0)
            return false;

        var now = TimeProvider.GetUtcNow();
        var active = false;
        foreach (var (_, hold) in _captureHolds)
        {
            if (hold.Mode < atLeast || now > hold.ExpiresAt)
                continue;
            active = true;
            var remaining = hold.ExpiresAt - now;
            if (remaining > retryAfter) retryAfter = remaining;
        }

        return active;
    }

    private bool IsHeld(Guid token, CaptureHold hold) =>
        _captureHolds is not null
        && _captureHolds.TryGetValue(token, out var current)
        && ReferenceEquals(current, hold);

    private void PruneLapsedHolds(DateTimeOffset now)
    {
        if (_captureHolds is null || _captureHolds.Count == 0)
            return;

        List<Guid>? stale = null;
        foreach (var (token, hold) in _captureHolds)
        {
            if (now > hold.ExpiresAt + LapsedHoldRetention)
                (stale ??= []).Add(token);
        }

        if (stale is null)
            return;
        foreach (var token in stale)
            _captureHolds.Remove(token);
    }

    /// <summary>
    /// A copy of this registry's LOCAL decisions, expired tombstones reported as
    /// <see cref="TxStatus.Indeterminate"/>, with no coordinator dialled. A
    /// delegated txid with no local decision is absent, so it resolves as
    /// <see cref="TxStatus.InFlight"/>: under invariant I1 no terminal for it can
    /// exist anywhere while the gate holds.
    /// </summary>
    private Dictionary<Guid, TxStatus> CaptureLocalDecisions()
    {
        var now = TimeProvider.GetUtcNow();
        var retention = Retention;
        var result = new Dictionary<Guid, TxStatus>(state.State.Decisions.Count);
        foreach (var (txid, status) in state.State.Decisions)
        {
            result[txid] = IsTombstoneExpiredAt(txid, now, retention)
                ? TxStatus.Indeterminate
                : status;
        }

        return result;
    }

    private TxDecisionGateRefusedException GateLapsed() =>
        new(GrainKey, TxDecisionGateRefusal.GateLapsed, TimeSpan.Zero);

    private void ThrowIfDecisionGated()
    {
        if (IsDecisionGated(out var retryAfter))
            throw new TxDecisionGateRefusedException(GrainKey, TxDecisionGateRefusal.DecisionGated, retryAfter);
    }

    private void ThrowIfRegistrationFenced()
    {
        if (IsRegistrationFenced(out var retryAfter))
            throw new TxDecisionGateRefusedException(GrainKey, TxDecisionGateRefusal.RegistrationFenced, retryAfter);
    }

    /// <summary>
    /// Adds the delegated verdicts a held decision gate kept from being cached
    /// to a reader's snapshot, so a reader still sees the coordinator's decision
    /// while no new local decision is recorded.
    /// </summary>
    private static void MergeUncachedVerdicts(Dictionary<Guid, TxStatus> snapshot, Dictionary<Guid, TxStatus>? uncached)
    {
        if (uncached is null)
            return;
        foreach (var (txid, status) in uncached)
            snapshot.TryAdd(txid, status);
    }
}
