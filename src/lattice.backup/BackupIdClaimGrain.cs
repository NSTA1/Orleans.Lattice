namespace Orleans.Lattice.Backup;

/// <summary>The default <see cref="IBackupIdClaimGrain"/>.</summary>
internal sealed class BackupIdClaimGrain(
    IGrainContext context,
    ILatticeBackupCatalogStore catalog,
    ILatticeBackupSink sink,
    [PersistentState("backup-id-claim", LatticeOptions.StorageProviderName)]
    IPersistentState<BackupIdClaimState> state)
    : IGrainBase, IBackupIdClaimGrain
{
    // A publish's claim lapses if it never reports back, so a capture that fails
    // between its claim and its release cannot hold the id for good.
    internal static readonly TimeSpan PendingClaimLifetime = TimeSpan.FromMinutes(15);

    /// <inheritdoc />
    public IGrainContext GrainContext => context;

    // One entry per claim granted and not yet released: a scope can hold several
    // at once (two captures of the same content), and each release retires one.
    // Persisted (issue #5632): a claimant may wait on its scope's hold for minutes
    // while a pass or a delete retires the id, and the claim must survive this
    // grain's activation being lost meanwhile, or another scope could take the
    // ownerless id and have it overwritten - #5627 again.
    private List<BackupIdPendingClaim> Claims => state.State.Claims;

    /// <inheritdoc />
    public async Task<bool> TryClaimAsync(string scopeKey)
    {
        ArgumentException.ThrowIfNullOrEmpty(scopeKey);
        var backupId = context.GrainId.Key.ToString()!;
        var now = DateTimeOffset.UtcNow;
        if (Claims.RemoveAll(claim => claim.Until <= now) > 0)
        {
            await PersistAsync();
        }

        if (Claims.Exists(claim => !string.Equals(claim.ScopeKey, scopeKey, StringComparison.Ordinal)))
        {
            return false;
        }

        var owner = await BackupIdOwnership.OwnerScopeKeyAsync(catalog, sink, backupId);
        if (owner is not null && !string.Equals(owner, scopeKey, StringComparison.Ordinal))
        {
            return false;
        }

        // Recorded for the owning scope too (issue #5627): its publish may wait on
        // the scope's hold while a retention pass or a delete retires the id, and
        // the id must stay this scope's until that publish completes, or another
        // scope could take the ownerless id and have it overwritten. Granted only
        // once it is stored: a claim whose write fails is taken back, and the
        // capture fails and is retried.
        var claim = new BackupIdPendingClaim { ScopeKey = scopeKey, Until = now + PendingClaimLifetime };
        Claims.Add(claim);
        try
        {
            await PersistAsync();
        }
        catch
        {
            Claims.Remove(claim);
            throw;
        }

        return true;
    }

    /// <inheritdoc />
    public async Task ReleaseAsync(string scopeKey)
    {
        var index = Claims.FindIndex(claim => string.Equals(claim.ScopeKey, scopeKey, StringComparison.Ordinal));
        if (index >= 0)
        {
            // A release whose write fails leaves the claim in storage, so a later
            // activation may bring it back; it then refuses other scopes until it
            // lapses, which errs on the safe side.
            Claims.RemoveAt(index);
            await PersistAsync();
        }
    }

    // The last claim released clears the record, so a bare id leaves nothing in
    // storage once no publish of it is in flight.
    private Task PersistAsync() => Claims.Count == 0 && !RedProof.ClaimNeverCleared ? state.ClearStateAsync() : state.WriteStateAsync(); // RED-PROOF
}
