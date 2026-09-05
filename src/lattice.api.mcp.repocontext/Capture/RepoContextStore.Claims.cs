using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// The claim and fencing half of the capture adapter: the
/// <c>repocontext_claim</c>, <c>_renew_claim</c>, <c>_release_claim</c>, and
/// <c>_claim_status</c> tools, and the fencing check the record write path applies.
/// <para>
/// It introduces no lock, lease, queue, or fencing scheme of its own. Mutual
/// exclusion, FIFO fairness, bounded leases, expiry reclaim, and the monotonically
/// increasing fencing token all come from the core
/// <see cref="ILatticeLockGrain"/>; this file only names the lock a record is
/// claimed under, records the granted token on the record itself as a fencing
/// high-water mark, and refuses a write that presents a token the record has
/// already moved past.
/// </para>
/// </summary>
internal sealed partial class RepoContextStore
{
    /// <summary>
    /// Claims the record at <paramref name="key"/> for <paramref name="owner"/>,
    /// acquiring the underlying distributed lock and stamping the granted fencing
    /// token onto the record.
    /// </summary>
    /// <param name="key">The full repository-context key of the memory record to claim.</param>
    /// <param name="owner">The claiming agent identity. Must be non-empty.</param>
    /// <param name="leaseSeconds">The lease length to request in seconds, or <see langword="null"/> to defer to the configured default. The lock clamps it to the configured maximum.</param>
    /// <param name="maxWaitSeconds">How long to wait in the lock's FIFO queue, or <see langword="null"/> to fail immediately under contention.</param>
    /// <param name="cancellationToken">Cancels the claim.</param>
    /// <returns>The claim outcome. Losing a race is reported, not thrown.</returns>
    /// <exception cref="McpException">The key is malformed or does not address a memory record, the owner is empty, or a duration is not positive.</exception>
    public async Task<RepoContextClaimResult> ClaimAsync(
        string key,
        string owner,
        long? leaseSeconds,
        long? maxWaitSeconds,
        CancellationToken cancellationToken)
    {
        RequireClaimableKey(key);
        RequireNonEmpty(owner, "owner");
        var lease = ResolveDuration(leaseSeconds, "leaseSeconds");
        var maxWait = ResolveDuration(maxWaitSeconds, "maxWaitSeconds");
        var lockName = RepoContextClaimNames.LockName(key);

        var tree = Tree(RepoContextTrees.Memory);
        if (await ReadMemoryAsync(tree, key, cancellationToken).ConfigureAwait(false) is null)
        {
            return NotGranted(key, lockName, "missing");
        }

        var padlock = _grainFactory.GetGrain<ILatticeLockGrain>(lockName);
        LockLease grant;
        if (maxWait > TimeSpan.Zero)
        {
            try
            {
                grant = await padlock.AcquireAsync(new LockAcquireRequest(lease, maxWait)).ConfigureAwait(false);
            }
            catch (TimeoutException)
            {
                return NotGranted(key, lockName, "timeout");
            }
        }
        else
        {
            if (await padlock.TryAcquireAsync(lease).ConfigureAwait(false) is not { } tried)
            {
                return NotGranted(key, lockName, "contended");
            }

            grant = tried;
        }

        var token = grant.Token.FencingToken;
        var stamped = await MutateMemoryAsync(
            tree,
            key,
            record => RepoContextClaimFence.StampClaim(record, token, owner, _replicaId),
            cancellationToken).ConfigureAwait(false);

        if (!stamped)
        {
            // The record was removed between the existence probe and the grant. Hand
            // the lock straight back so the next waiter is not left queued behind a
            // claim on a record that no longer exists.
            await padlock.ReleaseAsync(grant.Token).ConfigureAwait(false);
            return NotGranted(key, lockName, "missing");
        }

        return new RepoContextClaimResult
        {
            Key = key,
            LockName = lockName,
            Granted = true,
            FencingToken = token,
            Owner = owner,
            Region = _replicaId,
            LeaseExpiresAtUtc = ToIso(grant.ExpiresAt),
            LeaseSeconds = grant.LeaseDuration.TotalSeconds,
        };
    }

    /// <summary>
    /// Extends the lease on an existing claim without changing its fencing token.
    /// A renew presenting a superseded token is reported as not granted, which is
    /// the signal the holder was fenced out and must stop writing.
    /// </summary>
    /// <param name="key">The full repository-context key of the claimed memory record.</param>
    /// <param name="fencingToken">The token from the original grant.</param>
    /// <param name="leaseSeconds">The lease length to request in seconds, or <see langword="null"/> to defer to the configured default.</param>
    /// <param name="cancellationToken">Cancels the renew.</param>
    /// <returns>The renew outcome.</returns>
    /// <exception cref="McpException">The key is malformed or does not address a memory record, or a duration is not positive.</exception>
    public async Task<RepoContextClaimResult> RenewClaimAsync(
        string key,
        long fencingToken,
        long? leaseSeconds,
        CancellationToken cancellationToken)
    {
        RequireClaimableKey(key);
        var lease = ResolveDuration(leaseSeconds, "leaseSeconds");
        var lockName = RepoContextClaimNames.LockName(key);
        var padlock = _grainFactory.GetGrain<ILatticeLockGrain>(lockName);

        LockLease renewed;
        try
        {
            renewed = await padlock.RenewAsync(new LockToken(fencingToken), lease).ConfigureAwait(false);
        }
        catch (LatticeLockConflictException)
        {
            return NotGranted(key, lockName, "superseded");
        }

        var state = await ReadClaimStateAsync(key, cancellationToken).ConfigureAwait(false);
        return new RepoContextClaimResult
        {
            Key = key,
            LockName = lockName,
            Granted = true,
            FencingToken = renewed.Token.FencingToken,
            Owner = state.Owner,
            Region = state.Region ?? _replicaId,
            LeaseExpiresAtUtc = ToIso(renewed.ExpiresAt),
            LeaseSeconds = renewed.LeaseDuration.TotalSeconds,
        };
    }

    /// <summary>
    /// Releases a claim, handing the lock to the next FIFO waiter and marking the
    /// record's claim as no longer live so unfenced writes are admitted again. The
    /// fencing high-water mark is never lowered, so the released token stays refused
    /// once another claim has moved past it.
    /// </summary>
    /// <param name="key">The full repository-context key of the claimed memory record.</param>
    /// <param name="fencingToken">The token from the grant being released.</param>
    /// <param name="cancellationToken">Cancels the release.</param>
    /// <returns>The release outcome. A stale release is reported, not thrown.</returns>
    /// <exception cref="McpException">The key is malformed or does not address a memory record.</exception>
    public async Task<RepoContextReleaseClaimResult> ReleaseClaimAsync(
        string key,
        long fencingToken,
        CancellationToken cancellationToken)
    {
        RequireClaimableKey(key);
        var lockName = RepoContextClaimNames.LockName(key);
        var padlock = _grainFactory.GetGrain<ILatticeLockGrain>(lockName);

        // Idempotent by contract: a release presenting a token that no longer holds
        // the lock is a silent no-op on the lock, so it is safe to issue before the
        // record is consulted and cannot disturb a current holder.
        await padlock.ReleaseAsync(new LockToken(fencingToken)).ConfigureAwait(false);

        var tree = Tree(RepoContextTrees.Memory);
        var existing = await ReadMemoryAsync(tree, key, cancellationToken).ConfigureAwait(false);
        if (existing is null)
        {
            return NotReleased(key, lockName, fencingToken, "missing");
        }

        var state = RepoContextClaimFence.Read(existing);
        if (state.FencingToken is { } fence && fencingToken < fence)
        {
            return NotReleased(key, lockName, fencingToken, "stale");
        }

        await MutateMemoryAsync(
            tree,
            key,
            record => RepoContextClaimFence.StampRelease(record, fencingToken),
            cancellationToken).ConfigureAwait(false);

        return new RepoContextReleaseClaimResult
        {
            Key = key,
            LockName = lockName,
            Released = true,
            FencingToken = fencingToken,
        };
    }

    /// <summary>
    /// Reports the claim recorded on a record alongside the live, advisory status of
    /// the lock that grants it. Read-only.
    /// </summary>
    /// <param name="key">The full repository-context key of the memory record.</param>
    /// <param name="cancellationToken">Cancels the read.</param>
    /// <returns>The claim status.</returns>
    /// <exception cref="McpException">The key is malformed or does not address a memory record.</exception>
    public async Task<RepoContextClaimStatusResult> ClaimStatusAsync(string key, CancellationToken cancellationToken)
    {
        RequireClaimableKey(key);
        var lockName = RepoContextClaimNames.LockName(key);

        var existing = await ReadMemoryAsync(
            Tree(RepoContextTrees.Memory), key, cancellationToken).ConfigureAwait(false);
        var state = existing is null
            ? default
            : RepoContextClaimFence.Read(existing);

        var status = await _grainFactory.GetGrain<ILatticeLockGrain>(lockName).GetStatusAsync().ConfigureAwait(false);

        return new RepoContextClaimStatusResult
        {
            Key = key,
            LockName = lockName,
            Exists = existing is not null,
            Claimed = state.IsClaimLive,
            IsHeld = status.IsHeld,
            FencingToken = state.FencingToken,
            ReleasedFencingToken = state.ReleasedFencingToken,
            Owner = state.Owner,
            Region = state.Region,
            LockFencingToken = status.CurrentFencingToken,
            LeaseExpiresAtUtc = status.LeaseExpiresAt is { } expiry ? ToIso(expiry) : null,
            QueueDepth = status.QueueDepth,
        };
    }

    /// <summary>
    /// The fencing gate every memory write passes through. Throws when the record
    /// carries a claim the caller is not entitled to write under; returns silently
    /// when the record is unclaimed, which is what keeps every pre-existing caller
    /// working unchanged.
    /// </summary>
    /// <param name="key">The key being written.</param>
    /// <param name="existing">The stored record, or <see langword="null"/> when the write creates it.</param>
    /// <param name="fencingToken">The token the caller presented, or <see langword="null"/>.</param>
    /// <exception cref="RepoContextClaimConflictException">The write is refused by the fencing check.</exception>
    private void EnforceFence(string key, MemoryRecord? existing, long? fencingToken)
    {
        if (existing is null)
        {
            // Nothing stored yet, so nothing has been claimed: a create is admitted
            // whether or not it carries a token.
            return;
        }

        var verdict = RepoContextClaimFence.Evaluate(existing, fencingToken, _replicaId);
        if (verdict == RepoContextFenceVerdict.Accepted)
        {
            return;
        }

        var state = RepoContextClaimFence.Read(existing);
        throw new RepoContextClaimConflictException(
            RepoContextClaimFence.Explain(verdict, key, state, fencingToken, _replicaId),
            key,
            verdict,
            fencingToken,
            state.FencingToken,
            state.Owner,
            state.Region);
    }

    /// <summary>
    /// Rejects a fencing token presented against a record family that cannot carry a
    /// claim, rather than silently ignoring it and letting the caller believe its
    /// write was fenced.
    /// </summary>
    /// <param name="key">The key being written.</param>
    /// <param name="kind">The record family the key addresses.</param>
    /// <param name="fencingToken">The token the caller presented, or <see langword="null"/>.</param>
    /// <exception cref="McpException">A token was presented against a non-memory record.</exception>
    private static void RejectFenceOnNonMemory(string key, RepoContextRecordKind kind, long? fencingToken)
    {
        if (fencingToken is not null && kind != RepoContextRecordKind.Memory)
        {
            throw new McpException(
                $"A fencing token was presented for '{key}', but claims are supported on memory records only, "
                + $"not on a {kind} record. Omit 'fencingToken'.");
        }
    }

    /// <summary>
    /// Reads the folded memory record at <paramref name="key"/>, or
    /// <see langword="null"/> when it has no live value.
    /// </summary>
    private async Task<MemoryRecord?> ReadMemoryAsync(ILattice tree, string key, CancellationToken cancellationToken)
        => RepoContextMemoryCodec.Fold(
            await tree.GetAsync(key, cancellationToken).ConfigureAwait(false), _serializer);

    /// <summary>Reads just the claim state at <paramref name="key"/>.</summary>
    private async Task<RepoContextClaimState> ReadClaimStateAsync(string key, CancellationToken cancellationToken)
    {
        var existing = await ReadMemoryAsync(
            Tree(RepoContextTrees.Memory), key, cancellationToken).ConfigureAwait(false);
        return existing is null ? default : RepoContextClaimFence.Read(existing);
    }

    /// <summary>
    /// Applies <paramref name="mutate"/> to the stored memory record and writes it
    /// back through the multi-value-register accessor, preserving whatever remaining
    /// time-to-live the entry carried. Returns <see langword="false"/> when the
    /// record no longer exists.
    /// </summary>
    private async Task<bool> MutateMemoryAsync(
        ILattice tree, string key, Action<MemoryRecord> mutate, CancellationToken cancellationToken)
    {
        var versioned = await tree.GetWithVersionAsync(key, cancellationToken).ConfigureAwait(false);
        if (RepoContextMemoryCodec.Fold(versioned.Value, _serializer) is not { } record)
        {
            return false;
        }

        mutate(record);
        var bytes = _serializer.SerializeToArray(record);
        var accessor = RepoContextMemoryCodec.Accessor(tree, key);
        if (RemainingTtl(versioned.ExpiresAtTicks) is { } window)
        {
            await accessor.SetAsync(_replicaId, bytes, window, cancellationToken).ConfigureAwait(false);
        }
        else
        {
            await accessor.SetAsync(_replicaId, bytes, cancellationToken).ConfigureAwait(false);
        }

        return true;
    }

    /// <summary>
    /// Parses <paramref name="key"/> and rejects any family that cannot carry a
    /// claim. Fencing is enforced on the memory record itself, so only a memory
    /// record can be claimed: offering a lock over a record whose writes nobody
    /// checks would be decoration.
    /// </summary>
    private static RepoContextKey RequireClaimableKey(string key)
    {
        var parsed = ParseKey(key);
        if (parsed.Kind != RepoContextRecordKind.Memory)
        {
            throw new McpException(
                $"The key '{key}' addresses a {parsed.Kind} record. Claims are supported on memory records only "
                + "('repo/{repoId}/mem/{topic}/{id}'), because the fencing check is enforced on the memory "
                + "record's own write path.");
        }

        return parsed;
    }

    /// <summary>
    /// Converts an optional second count to a duration, rejecting a non-positive
    /// value. <see langword="null"/> maps to <see cref="TimeSpan.Zero"/>, which the
    /// lock reads as "use the configured default".
    /// </summary>
    private static TimeSpan ResolveDuration(long? seconds, string parameterName)
    {
        if (seconds is not { } value)
        {
            return TimeSpan.Zero;
        }

        if (value <= 0L)
        {
            throw new McpException($"The '{parameterName}' parameter must be a positive number of seconds.");
        }

        return TimeSpan.FromSeconds(value);
    }

    private static RepoContextClaimResult NotGranted(string key, string lockName, string reason)
        => new()
        {
            Key = key,
            LockName = lockName,
            Granted = false,
            Reason = reason,
        };

    private static RepoContextReleaseClaimResult NotReleased(
        string key, string lockName, long fencingToken, string reason)
        => new()
        {
            Key = key,
            LockName = lockName,
            Released = false,
            FencingToken = fencingToken,
            Reason = reason,
        };

    private static string ToIso(DateTimeOffset instant) => instant.UtcDateTime.ToString("O");
}
