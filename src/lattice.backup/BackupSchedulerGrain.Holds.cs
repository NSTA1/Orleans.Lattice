using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Backup;

/// <summary>
/// The per-id holds that serialise every publish to, and every delete from, the
/// scope's catalog and sink (issue #5418). A backup id is a content address, so a
/// capture of identical content republishes an existing id; without these holds a
/// retention pass that had doomed that id would delete the manifest the republish
/// had just rewritten and registered.
/// </summary>
/// <remarks>
/// <para>
/// A publish holds its id for the length of its publish call; a pass holds every
/// id it dooms as retiring from the doom decision - before it removes any row - to
/// the moment it has deleted or restored that id; a catalog scrub holds its id as
/// retiring too. An operator delete holds its id by a lease from the in-grain step
/// that removes its row until the caller, having deleted the artifacts and the sink
/// manifest outside the grain, ends it (issue #5628); a set member written ahead
/// of its set's stamp is held by a lease until the stamp registers it (issue
/// #5629). A lease runs out if its holder is lost. A held id is never doomed, and a
/// publish or delete of a held id waits. The holds are activation-local: they live
/// and die with the scope's grain, like the pass guard.
/// </para>
/// <para>
/// A call waits at most <see cref="LatticeBackupOptions.PublishHoldWait"/>, well
/// inside the grain response timeout, and then returns busy having written nothing;
/// <see cref="SchedulerBackupPublicationGate"/> retries it across calls, so a pass
/// deleting a large artifact from a slow sink delays a capture but never fails it.
/// </para>
/// </remarks>
internal sealed partial class BackupSchedulerGrain
{
    private readonly HashSet<string> _publishing = new(StringComparer.Ordinal);
    private readonly HashSet<string> _retiring = new(StringComparer.Ordinal);

    // Set members a publish has written to the sink and whose row waits for the
    // set's membership stamp (issue #5449): a pass never dooms one. Each is a
    // lease, so a member whose capture died before its stamp is held only until
    // the lease runs out (issue #5629).
    private readonly Dictionary<string, DateTimeOffset> _pendingMembers = new(StringComparer.Ordinal);

    // Operator deletes in progress (issue #5628): the id stays reserved from the
    // short in-grain step that removes its row until the caller has deleted its
    // artifacts and sink manifest outside the grain and released it. A lease, so a
    // caller lost part-way cannot hold the id for good.
    private readonly Dictionary<string, DateTimeOffset> _deleting = new(StringComparer.Ordinal);

    // Ids published while a pass reads its listing; null outside a listing.
    private HashSet<string>? _publishedDuringListing;
    private TaskCompletionSource? _holdsChanged;
    private BackupPublisher? _publisher;

    private BackupPublisher Publisher => _publisher ??= publisher ?? new BackupPublisher(catalog, sink);

    private TimeSpan HoldWait => backupOptions?.Value.PublishHoldWait ?? TimeSpan.FromSeconds(10);

    private TimeSpan PendingStampLease => backupOptions?.Value.PendingStampLease ?? TimeSpan.FromMinutes(30);

    private TimeSpan DeleteLease => backupOptions?.Value.DeleteLease ?? TimeSpan.FromMinutes(30);

    /// <inheritdoc />
    public async Task<BackupPublishResult> PublishAsync(BackupPublishRequest request)
    {
        ArgumentNullException.ThrowIfNull(request);
        var manifest = request.Manifest;
        var bareId = manifest.Id;
        var claimedBare = false;
        try
        {
            if (request.Mode == BackupPublishMode.Full)
            {
                (var id, claimedBare) = await Publisher.ResolveFullIdAsync(manifest, ScopeKey);
                manifest = manifest with { Id = id };
            }

            if (!await TryHoldAsync(manifest.Id, publishing: true))
            {
                return BackupPublishResult.Busy;
            }

            var published = false;
            try
            {
                var result = await Publisher.PublishAsync(manifest, request.Mode, request.Register);
                published = result.Outcome == BackupPublishOutcome.Published;
                if (published && request.Mode == BackupPublishMode.Full && !request.Register)
                {
                    _pendingMembers[manifest.Id] = DateTimeOffset.UtcNow + PendingStampLease;
                }
                else if (request.Mode == BackupPublishMode.Register)
                {
                    _pendingMembers.Remove(manifest.Id);
                }

                return result;
            }
            finally
            {
                ReleaseHold(manifest.Id, publishing: true, published);
            }
        }
        finally
        {
            if (claimedBare)
            {
                await Publisher.ReleaseClaimAsync(bareId, ScopeKey);
            }
        }
    }

    /// <inheritdoc />
    public async Task<BackupDeleteStart> BeginDeleteAsync(string backupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        if (!await TryHoldAsync(backupId, publishing: false, joinDelete: true))
        {
            return new BackupDeleteStart(BackupRetireOutcome.Busy, null);
        }

        try
        {
            var manifest = await Publisher.BeginDeleteAsync(backupId);
            if (manifest is null)
            {
                return new BackupDeleteStart(BackupRetireOutcome.Unchanged, null);
            }

            // The hold becomes a lease that outlives this call: the caller deletes
            // the artifacts and the sink manifest outside the grain, then ends it.
            _deleting[backupId] = DateTimeOffset.UtcNow + DeleteLease;
            return new BackupDeleteStart(BackupRetireOutcome.Removed, manifest);
        }
        finally
        {
            ReleaseHold(backupId, publishing: false, published: false);
        }
    }

    /// <inheritdoc />
    public Task EndDeleteAsync(string backupId)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        if (_deleting.Remove(backupId))
        {
            SignalHoldsChanged();
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public async Task<BackupRetireOutcome> RetireAsync(string backupId, BackupRetireKind kind)
    {
        ArgumentException.ThrowIfNullOrEmpty(backupId);
        if (kind != BackupRetireKind.OrphanRow)
        {
            throw new ArgumentOutOfRangeException(
                nameof(kind), kind, "An operator delete runs through BeginDeleteAsync and EndDeleteAsync.");
        }

        if (!await TryHoldAsync(backupId, publishing: false))
        {
            return BackupRetireOutcome.Busy;
        }

        try
        {
            return await Publisher.RetireAsync(backupId, kind);
        }
        finally
        {
            ReleaseHold(backupId, publishing: false, published: false);
        }
    }

    private bool IsHeld(string backupId) =>
        _publishing.Contains(backupId)
        || _retiring.Contains(backupId)
        || HasLiveLease(_pendingMembers, backupId)
        || HasLiveLease(_deleting, backupId)
        || (_publishedDuringListing?.Contains(backupId) ?? false);

    private static bool HasLiveLease(Dictionary<string, DateTimeOffset> leases, string backupId)
    {
        if (!leases.TryGetValue(backupId, out var until))
        {
            return false;
        }

        if (until > DateTimeOffset.UtcNow)
        {
            return true;
        }

        leases.Remove(backupId);
        return false;
    }

    private void ReserveRetiring(List<BackupManifest> doomed)
    {
        foreach (var manifest in doomed)
        {
            _retiring.Add(manifest.Id);
        }
    }

    private void ReleaseRetiring(List<BackupManifest> doomed)
    {
        foreach (var manifest in doomed)
        {
            _retiring.Remove(manifest.Id);
        }

        SignalHoldsChanged();
    }

    private void ReleaseRetiring(string backupId)
    {
        _retiring.Remove(backupId);
        SignalHoldsChanged();
    }

    private async Task<bool> TryHoldAsync(string backupId, bool publishing, bool joinDelete = false)
    {
        var wait = HoldWait;
        var started = DateTimeOffset.UtcNow;

        // A delete joins a delete already in progress on the id - both run the same
        // idempotent steps - so a retry is never blocked by its own earlier attempt.
        while (_publishing.Contains(backupId)
            || _retiring.Contains(backupId)
            || (!joinDelete && HasLiveLease(_deleting, backupId)))
        {
            var remaining = wait - (DateTimeOffset.UtcNow - started);
            if (remaining <= TimeSpan.Zero)
            {
                return false;
            }

            _holdsChanged ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            try
            {
                await _holdsChanged.Task.WaitAsync(remaining);
            }
            catch (TimeoutException)
            {
                // Re-checked by the loop condition, then reported busy.
            }
        }

        (publishing ? _publishing : _retiring).Add(backupId);
        return true;
    }

    private void ReleaseHold(string backupId, bool publishing, bool published)
    {
        (publishing ? _publishing : _retiring).Remove(backupId);
        if (published)
        {
            _publishedDuringListing?.Add(backupId);
        }

        SignalHoldsChanged();
    }

    private void SignalHoldsChanged()
    {
        var signal = _holdsChanged;
        _holdsChanged = null;
        signal?.TrySetResult();
    }
}
