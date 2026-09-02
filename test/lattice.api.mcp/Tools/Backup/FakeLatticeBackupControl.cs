using System.Runtime.CompilerServices;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// A stateful in-memory fake of the internal <see cref="ILatticeBackupControl"/>
/// facade for the backup MCP tool tests. It models the handful of operations the
/// tools adapt (capture, incremental capture, list, describe, inventory, scope
/// status, artifact export, restore, revert, delete) and throws
/// <see cref="NotSupportedException"/> for the facade members the tools never
/// call. Its <see cref="Authorized"/> flag emulates the facade's own fail-closed
/// access gate: when cleared, every operation throws
/// <see cref="LatticeAuthorizationDeniedException"/>, exactly as an unauthorized
/// caller would be denied - so the MCP layer adds no authorization path of its
/// own.
/// </summary>
internal sealed class FakeLatticeBackupControl : ILatticeBackupControl
{
    private readonly Dictionary<string, BackupManifest> _backups = new(StringComparer.Ordinal);
    private readonly Dictionary<(string BackupId, string ArtifactId), byte[]> _artifacts = new();
    private int _nextId;

    /// <summary>When <c>false</c>, every operation is denied fail-closed.</summary>
    public bool Authorized { get; set; } = true;

    /// <summary>The scope status the fake returns; <c>null</c> models an unknown scope.</summary>
    public BackupScopeStatus? ScopeStatus { get; set; }

    /// <summary>The inventory the fake returns.</summary>
    public BackupInventoryReport Inventory { get; set; } = new(0, 0, 0, 0, null, null, 0, 0, 0);

    /// <summary>The most recent restore result passed to <see cref="RevertRestoreAsync"/>.</summary>
    public LatticeRestoreResult? LastReverted { get; private set; }

    /// <summary>
    /// Invoked inside every gated operation, so a test can observe the ambient
    /// state (the stamped credential in particular) exactly as the real facade
    /// would see it during the call.
    /// </summary>
    public Action? OnOperation { get; set; }

    /// <summary>Seeds an artifact's bytes so <see cref="ExportArtifactAsync"/> can stream them.</summary>
    public void SeedArtifact(string backupId, string artifactId, byte[] bytes)
        => _artifacts[(backupId, artifactId)] = bytes;

    private void Gate()
    {
        OnOperation?.Invoke();
        if (!Authorized)
        {
            throw new LatticeAuthorizationDeniedException("The caller is not authorized for the backup scope.");
        }
    }

    public Task<LatticeBackupCaptureResult> CreateBackupAsync(
        LatticeBackupCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        Gate();
        var id = $"bk-{_nextId++}";
        var manifest = BackupToolTestData.Manifest(id, request.Name, request.Scope);
        _backups[id] = manifest;
        return Task.FromResult(new LatticeBackupCaptureResult(id, manifest));
    }

    public Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(
        LatticeBackupIncrementalCaptureRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        Gate();
        var id = $"bk-{_nextId++}";
        var manifest = BackupToolTestData.Manifest(
            id, request.Name, request.Scope, BackupKind.Incremental, request.BaseBackupId);
        _backups[id] = manifest;
        return Task.FromResult(new LatticeBackupCaptureResult(id, manifest));
    }

    public Task<BackupCatalogPage> ListBackupsAsync(
        BackupCatalogRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        Gate();
        var entries = _backups.Values.OrderBy(m => m.Id, StringComparer.Ordinal).ToArray();
        return Task.FromResult(new BackupCatalogPage { Entries = entries, NextPageToken = null });
    }

    public Task<BackupChainDescription?> DescribeBackupAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        Gate();
        if (!_backups.TryGetValue(backupId, out var manifest))
        {
            return Task.FromResult<BackupChainDescription?>(null);
        }

        var chain = manifest.BaseBackupId is null
            ? new[] { manifest.Id }
            : new[] { manifest.BaseBackupId, manifest.Id };
        return Task.FromResult<BackupChainDescription?>(new BackupChainDescription(manifest, chain));
    }

    public Task<BackupInventoryReport> GetInventoryAsync(CancellationToken cancellationToken = default)
    {
        Gate();
        return Task.FromResult(Inventory);
    }

    public Task<BackupScopeStatus?> GetScopeStatusAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(scope);
        Gate();
        return Task.FromResult(ScopeStatus);
    }

    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(
        string backupId,
        string artifactId,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Gate();
        if (!_artifacts.TryGetValue((backupId, artifactId), out var bytes))
        {
            throw new KeyNotFoundException($"No artifact {artifactId} for backup {backupId}.");
        }

        const int chunkSize = 16;
        for (var offset = 0; offset < bytes.Length; offset += chunkSize)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var length = Math.Min(chunkSize, bytes.Length - offset);
            yield return new ReadOnlyMemory<byte>(bytes, offset, length);
            await Task.Yield();
        }
    }

    public Task<LatticeRestoreResult> RestoreBackupAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        Gate();
        return Task.FromResult(new LatticeRestoreResult(
            request.BackupId,
            request.TargetTreeId ?? "captured-tree",
            request.Mode,
            request.OperationId ?? "op-1",
            new[] { request.BackupId },
            entriesApplied: 3,
            shadowPhysicalTreeId: request.Mode == LatticeRestoreMode.ShadowCutover ? "phys-new" : null,
            previousPhysicalTreeId: request.Mode == LatticeRestoreMode.ShadowCutover ? "phys-old" : null));
    }

    public Task RevertRestoreAsync(
        LatticeRestoreResult restore,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(restore);
        Gate();
        LastReverted = restore;
        return Task.CompletedTask;
    }

    public Task<bool> DeleteBackupAsync(
        string backupId,
        CancellationToken cancellationToken = default)
    {
        Gate();
        return Task.FromResult(_backups.Remove(backupId));
    }

    public Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(
        LatticeBackupSetCaptureRequest request,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task ScheduleBackupAsync(
        LatticeBackupScheduleRequest request,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task CancelScheduleAsync(
        BackupScopeSelector scope,
        bool incremental,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public IAsyncEnumerable<BackupManifest> StreamBackupsAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<BackupCatalogRebuildReport> RebuildCatalogFromSinkAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<BackupCatalogScrubReport> ScrubCatalogAgainstSinkAsync(
        bool pruneOrphans = false,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<LatticeRestoreResult> ColdRestoreAsync(
        LatticeRestoreRequest request,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(
        BackupScopeSelector scope,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<BackupHealthReport> CheckBackupHealthAsync(
        string backupId,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task<BackupHealthReport?> GetBackupHealthAsync(
        string backupId,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();

    public Task ConfigureBackupHealthAsync(
        string backupId,
        BackupHealthConfig config,
        CancellationToken cancellationToken = default)
        => throw new NotSupportedException();
}
