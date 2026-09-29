using System.Runtime.CompilerServices;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Backups;

/// <summary>
/// A scripted <see cref="ILatticeBackupControl"/>: an in-memory catalogue with
/// every verb overridable by a delegate (so a test can hold a call open on a
/// <see cref="TaskCompletionSource"/> or make it throw), and a record of every
/// call made.
/// </summary>
internal sealed class FakeBackupControl : ILatticeBackupControl
{
    /// <summary>The catalogue, in capture order.</summary>
    public List<BackupManifest> Catalogue { get; } = [];

    /// <summary>The latest stored health report per backup id.</summary>
    public Dictionary<string, BackupHealthReport> HealthReports { get; } = new(StringComparer.Ordinal);

    /// <summary>The schedule status per tree id.</summary>
    public Dictionary<string, BackupScopeStatus> ScopeStatuses { get; } = new(StringComparer.Ordinal);

    /// <summary>The artifact bytes per artifact id.</summary>
    public Dictionary<string, byte[]> Artifacts { get; } = new(StringComparer.Ordinal);

    /// <summary>Every call, by verb name, with its argument.</summary>
    public List<(string Verb, object? Argument)> Calls { get; } = [];

    /// <summary>The capability probe; everything allowed by default.</summary>
    public Func<BackupScopeSelector, Task<BackupScopeCapabilities>> Probe { get; set; } =
        scope => Task.FromResult(AllowAll(scope));

    /// <summary>Whether health monitoring applies; false by default.</summary>
    public Func<Task<bool>> HealthAvailable { get; set; } = () => Task.FromResult(false);

    /// <summary>The inventory; not served by default, as over the gRPC binding.</summary>
    public Func<Task<BackupInventoryReport>> Inventory { get; set; } =
        () => Task.FromException<BackupInventoryReport>(new NotSupportedException("not served"));

    /// <summary>The list verb; filters the in-memory catalogue by default.</summary>
    public Func<BackupCatalogRequest, Task<BackupCatalogPage>>? List { get; set; }

    /// <summary>The describe verb; reads the in-memory catalogue by default.</summary>
    public Func<string, Task<BackupChainDescription?>>? Describe { get; set; }

    /// <summary>The full-capture verb.</summary>
    public Func<LatticeBackupCaptureRequest, Task<LatticeBackupCaptureResult>>? Capture { get; set; }

    /// <summary>The incremental-capture verb.</summary>
    public Func<LatticeBackupIncrementalCaptureRequest, Task<LatticeBackupCaptureResult>>? CaptureIncremental { get; set; }

    /// <summary>The set-capture verb.</summary>
    public Func<LatticeBackupSetCaptureRequest, Task<LatticeBackupSetCaptureResult>>? CaptureSet { get; set; }

    /// <summary>The restore verb.</summary>
    public Func<LatticeRestoreRequest, Task<LatticeRestoreResult>>? Restore { get; set; }

    /// <summary>The cold-restore verb; not served by default.</summary>
    public Func<LatticeRestoreRequest, Task<LatticeRestoreResult>> ColdRestore { get; set; } =
        _ => Task.FromException<LatticeRestoreResult>(new NotSupportedException("not served"));

    /// <summary>The delete verb; removes from the in-memory catalogue by default.</summary>
    public Func<string, Task<bool>>? Delete { get; set; }

    /// <summary>The revert verb.</summary>
    public Func<LatticeRestoreResult, Task> Revert { get; set; } = _ => Task.CompletedTask;

    /// <summary>The schedule verb.</summary>
    public Func<LatticeBackupScheduleRequest, Task> Schedule { get; set; } = _ => Task.CompletedTask;

    /// <summary>The cancel-schedule verb.</summary>
    public Func<BackupScopeSelector, bool, Task> CancelSchedule { get; set; } = (_, _) => Task.CompletedTask;

    /// <summary>The scope-status verb; reads <see cref="ScopeStatuses"/> by default.</summary>
    public Func<BackupScopeSelector, Task<BackupScopeStatus?>>? ScopeStatus { get; set; }

    /// <summary>The catalogue rebuild; not served by default.</summary>
    public Func<Task<BackupCatalogRebuildReport>> Rebuild { get; set; } =
        () => Task.FromException<BackupCatalogRebuildReport>(new NotSupportedException("not served"));

    /// <summary>The catalogue scrub; not served by default.</summary>
    public Func<bool, Task<BackupCatalogScrubReport>> Scrub { get; set; } =
        _ => Task.FromException<BackupCatalogScrubReport>(new NotSupportedException("not served"));

    /// <summary>The health check; reports healthy by default.</summary>
    public Func<string, Task<BackupHealthReport>>? Check { get; set; }

    /// <summary>The health read; reads <see cref="HealthReports"/> by default.</summary>
    public Func<string, Task<BackupHealthReport?>>? GetHealth { get; set; }

    /// <summary>The health configuration verb.</summary>
    public Func<string, BackupHealthConfig, Task> Configure { get; set; } = (_, _) => Task.CompletedTask;

    /// <summary>A fault every artifact export throws when set.</summary>
    public Exception? ExportFault { get; set; }

    /// <summary>A capability set allowing every operation over <paramref name="scope"/>.</summary>
    /// <param name="scope">The scope.</param>
    public static BackupScopeCapabilities AllowAll(BackupScopeSelector scope) => new()
    {
        Scope = scope,
        CanList = true,
        CanCapture = true,
        CanCaptureIncremental = true,
        CanRestore = true,
        CanDelete = true,
    };

    /// <summary>A capability set allowing nothing.</summary>
    /// <param name="scope">The scope.</param>
    public static BackupScopeCapabilities DenyAll(BackupScopeSelector scope) => new() { Scope = scope };

    /// <summary>A manifest for tests.</summary>
    /// <param name="id">The backup id.</param>
    /// <param name="name">The backup name.</param>
    /// <param name="tree">The tree id.</param>
    /// <param name="createdAt">When it was captured.</param>
    /// <param name="baseId">The base backup id, making it incremental.</param>
    /// <param name="artifacts">The artifact ids.</param>
    public static BackupManifest Manifest(
        string id,
        string name = "nightly",
        string tree = "orders",
        DateTimeOffset? createdAt = null,
        string? baseId = null,
        params string[] artifacts) =>
        new(
            id,
            name,
            createdAt ?? new DateTimeOffset(2026, 9, 28, 14, 2, 11, TimeSpan.Zero),
            baseId is null ? BackupKind.Full : BackupKind.Incremental,
            BackupScopeSelector.WholeTree(tree),
            new BackupConsistencyCut(10, 20),
            new BackupTopologySnapshot(1, 1, ["root"]),
            "digest",
            [],
            [.. artifacts.Select(artifact => new BackupContentDescriptor(artifact, "hash-" + artifact, 2048, 2, BackupScopeSelector.WholeTree(tree)))],
            [],
            baseId);

    /// <summary>A restore result for tests, carrying physical ids no page may show.</summary>
    /// <param name="request">The request.</param>
    public static LatticeRestoreResult RestoreResult(LatticeRestoreRequest request) =>
        new(
            request.BackupId,
            request.TargetTreeId ?? "orders",
            request.Mode,
            "op-1",
            [request.BackupId],
            42,
            shadowPhysicalTreeId: request.Mode == LatticeRestoreMode.ShadowCutover ? "orders-physical-shadow-7f3a" : null,
            previousPhysicalTreeId: request.Mode == LatticeRestoreMode.ShadowCutover ? "orders-physical-previous-1c2d" : null);

    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CreateBackupAsync(LatticeBackupCaptureRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(CreateBackupAsync), request));
        return Capture?.Invoke(request) ?? Task.FromResult(Captured(request.Name, request.Scope, null));
    }

    /// <inheritdoc />
    public Task<LatticeBackupCaptureResult> CreateIncrementalBackupAsync(LatticeBackupIncrementalCaptureRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(CreateIncrementalBackupAsync), request));
        return CaptureIncremental?.Invoke(request) ?? Task.FromResult(Captured(request.Name, request.Scope, request.BaseBackupId));
    }

    /// <inheritdoc />
    public Task<LatticeBackupSetCaptureResult> CreateBackupSetAsync(LatticeBackupSetCaptureRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(CreateBackupSetAsync), request));
        if (CaptureSet is { } set)
        {
            return set(request);
        }

        var members = request.Scopes.Select(scope => Captured(request.Name + "-" + scope.TreeId, scope, null)).ToArray();
        return Task.FromResult(new LatticeBackupSetCaptureResult(
            new BackupSetManifest("set-1", request.Name, DateTimeOffset.UnixEpoch, request.CrossTreeConsistent, null, [.. members.Select(member => member.BackupId)]),
            members));
    }

    /// <inheritdoc />
    public Task ScheduleBackupAsync(LatticeBackupScheduleRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ScheduleBackupAsync), request));
        return Schedule(request);
    }

    /// <inheritdoc />
    public Task CancelScheduleAsync(BackupScopeSelector scope, bool incremental, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(CancelScheduleAsync), (scope, incremental)));
        return CancelSchedule(scope, incremental);
    }

    /// <inheritdoc />
    public Task<BackupCatalogPage> ListBackupsAsync(BackupCatalogRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ListBackupsAsync), request));
        if (List is { } list)
        {
            return list(request);
        }

        IEnumerable<BackupManifest> rows = request.OrderByCreatedDescending
            ? Catalogue.OrderByDescending(manifest => manifest.CreatedAtUtc)
            : Catalogue.OrderBy(manifest => manifest.Id, StringComparer.Ordinal);
        rows = rows.Where(manifest =>
            (request.Kind is null || manifest.Kind == request.Kind)
            && (request.NamePrefix is null || manifest.Name.StartsWith(request.NamePrefix, StringComparison.Ordinal))
            && (request.TreeId is null || manifest.Scope.TreeId == request.TreeId));
        var skip = request.PageToken is { } token ? int.Parse(token, System.Globalization.CultureInfo.InvariantCulture) : 0;
        var size = request.PageSize <= 0 ? 100 : request.PageSize;
        var all = rows.ToList();
        var page = all.Skip(skip).Take(size).ToList();
        return Task.FromResult(new BackupCatalogPage
        {
            Entries = page,
            NextPageToken = skip + size < all.Count ? (skip + size).ToString(System.Globalization.CultureInfo.InvariantCulture) : null,
        });
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<BackupManifest> StreamBackupsAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(StreamBackupsAsync), null));
        foreach (var manifest in Catalogue.OrderBy(manifest => manifest.Id, StringComparer.Ordinal).ToArray())
        {
            await Task.Yield();
            cancellationToken.ThrowIfCancellationRequested();
            yield return manifest;
        }
    }

    /// <inheritdoc />
    public Task<BackupChainDescription?> DescribeBackupAsync(string backupId, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(DescribeBackupAsync), backupId));
        if (Describe is { } describe)
        {
            return describe(backupId);
        }

        var manifest = Catalogue.Find(candidate => candidate.Id == backupId);
        if (manifest is null)
        {
            return Task.FromResult<BackupChainDescription?>(null);
        }

        var chain = new List<string>();
        for (var link = manifest; link is not null; link = link.BaseBackupId is { } baseId ? Catalogue.Find(candidate => candidate.Id == baseId) : null)
        {
            chain.Insert(0, link.Id);
        }

        return Task.FromResult<BackupChainDescription?>(new BackupChainDescription(manifest, chain));
    }

    /// <inheritdoc />
    public Task<bool> DeleteBackupAsync(string backupId, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(DeleteBackupAsync), backupId));
        return Delete?.Invoke(backupId) ?? Task.FromResult(Catalogue.RemoveAll(manifest => manifest.Id == backupId) > 0);
    }

    /// <inheritdoc />
    public Task<LatticeRestoreResult> RestoreBackupAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(RestoreBackupAsync), request));
        return Restore?.Invoke(request) ?? Task.FromResult(RestoreResult(request));
    }

    /// <inheritdoc />
    public Task RevertRestoreAsync(LatticeRestoreResult restore, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(RevertRestoreAsync), restore));
        return Revert(restore);
    }

    /// <inheritdoc />
    public async IAsyncEnumerable<ReadOnlyMemory<byte>> ExportArtifactAsync(string backupId, string artifactId, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ExportArtifactAsync), (backupId, artifactId)));
        await Task.Yield();
        if (ExportFault is { } fault)
        {
            throw fault;
        }

        if (!Artifacts.TryGetValue(artifactId, out var bytes))
        {
            throw new KeyNotFoundException(artifactId);
        }

        for (var offset = 0; offset < bytes.Length; offset += 4)
        {
            yield return bytes.AsMemory(offset, Math.Min(4, bytes.Length - offset));
        }
    }

    /// <inheritdoc />
    public Task<BackupInventoryReport> GetInventoryAsync(CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(GetInventoryAsync), null));
        return Inventory();
    }

    /// <inheritdoc />
    public Task<BackupCatalogRebuildReport> RebuildCatalogFromSinkAsync(CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(RebuildCatalogFromSinkAsync), null));
        return Rebuild();
    }

    /// <inheritdoc />
    public Task<BackupCatalogScrubReport> ScrubCatalogAgainstSinkAsync(bool pruneOrphans = false, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ScrubCatalogAgainstSinkAsync), pruneOrphans));
        return Scrub(pruneOrphans);
    }

    /// <inheritdoc />
    public Task<LatticeRestoreResult> ColdRestoreAsync(LatticeRestoreRequest request, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ColdRestoreAsync), request));
        return ColdRestore(request);
    }

    /// <inheritdoc />
    public Task<BackupScopeStatus?> GetScopeStatusAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(GetScopeStatusAsync), scope));
        return ScopeStatus?.Invoke(scope)
            ?? Task.FromResult(ScopeStatuses.TryGetValue(scope.TreeId, out var status) ? status : null);
    }

    /// <inheritdoc />
    public Task<BackupScopeCapabilities> ProbeCapabilitiesAsync(BackupScopeSelector scope, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ProbeCapabilitiesAsync), scope));
        return Probe(scope);
    }

    /// <inheritdoc />
    public Task<bool> IsHealthMonitoringAvailableAsync(CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(IsHealthMonitoringAvailableAsync), null));
        return HealthAvailable();
    }

    /// <inheritdoc />
    public Task<BackupHealthReport> CheckBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(CheckBackupHealthAsync), backupId));
        return Check?.Invoke(backupId)
            ?? Task.FromResult(new BackupHealthReport(backupId, BackupHealthStatus.Healthy, true, [], [], DateTimeOffset.UnixEpoch, "All present."));
    }

    /// <inheritdoc />
    public Task<BackupHealthReport?> GetBackupHealthAsync(string backupId, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(GetBackupHealthAsync), backupId));
        return GetHealth?.Invoke(backupId)
            ?? Task.FromResult(HealthReports.TryGetValue(backupId, out var report) ? report : null);
    }

    /// <inheritdoc />
    public Task ConfigureBackupHealthAsync(string backupId, BackupHealthConfig config, CancellationToken cancellationToken = default)
    {
        Calls.Add((nameof(ConfigureBackupHealthAsync), (backupId, config)));
        return Configure(backupId, config);
    }

    /// <summary>The number of calls to <paramref name="verb"/>.</summary>
    /// <param name="verb">The verb's method name.</param>
    public int CountOf(string verb) => Calls.Count(call => call.Verb == verb);

    /// <summary>The argument of the last call to <paramref name="verb"/>.</summary>
    /// <typeparam name="T">The argument type.</typeparam>
    /// <param name="verb">The verb's method name.</param>
    public T LastOf<T>(string verb) => (T)Calls.Last(call => call.Verb == verb).Argument!;

    private LatticeBackupCaptureResult Captured(string name, BackupScopeSelector scope, string? baseId)
    {
        var id = "captured" + (Catalogue.Count + 1).ToString(System.Globalization.CultureInfo.InvariantCulture);
        var manifest = Manifest(id, name, scope.TreeId, baseId: baseId, artifacts: "a1");
        Catalogue.Add(manifest);
        return new LatticeBackupCaptureResult(id, manifest);
    }
}
