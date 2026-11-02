namespace Orleans.Lattice.Backup;

/// <summary>
/// An <see cref="IBackupPublicationGate"/> with no serialisation: it runs each
/// step of <see cref="BackupPublisher"/> directly. Used where no grain runtime is
/// available - a capture or maintenance service built outside a silo, as unit
/// tests do. A host with the backup add-on registered uses
/// <see cref="SchedulerBackupPublicationGate"/>.
/// </summary>
/// <param name="publisher">The publication core.</param>
internal sealed class DirectBackupPublicationGate(BackupPublisher publisher) : IBackupPublicationGate
{
    /// <inheritdoc />
    public async Task<BackupManifest> PublishFullAsync(BackupManifest manifest, bool register, CancellationToken cancellationToken)
    {
        var scopeKey = BackupScopeKey.For(manifest.Scope);
        var (id, claimedBare) = await publisher.ResolveFullIdAsync(manifest, scopeKey).ConfigureAwait(false);
        try
        {
            var result = await publisher.PublishAsync(manifest with { Id = id }, BackupPublishMode.Full, register)
                .ConfigureAwait(false);
            return result.Manifest!;
        }
        finally
        {
            if (claimedBare)
            {
                await publisher.ReleaseClaimAsync(manifest.Id, scopeKey).ConfigureAwait(false);
            }
        }
    }

    /// <inheritdoc />
    public async Task<BackupManifest?> PublishIncrementAsync(BackupManifest manifest, CancellationToken cancellationToken) =>
        (await publisher.PublishAsync(manifest, BackupPublishMode.Increment, register: true).ConfigureAwait(false)).Manifest;

    /// <inheritdoc />
    public async Task<BackupManifest> RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken) =>
        (await publisher.PublishAsync(manifest, BackupPublishMode.Register, register: true).ConfigureAwait(false)).Manifest!;

    /// <inheritdoc />
    public async Task<bool> RegisterRebuiltAsync(BackupManifest manifest, CancellationToken cancellationToken) =>
        (await publisher.PublishAsync(manifest, BackupPublishMode.Rebuild, register: true).ConfigureAwait(false)).Outcome
            == BackupPublishOutcome.Published;

    /// <inheritdoc />
    public async Task<bool> RetireAsync(BackupManifest row, BackupRetireKind kind, CancellationToken cancellationToken) =>
        await publisher.RetireAsync(row.Id, kind).ConfigureAwait(false) == BackupRetireOutcome.Removed;
}
