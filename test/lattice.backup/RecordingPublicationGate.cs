using System.Collections.Concurrent;

namespace Orleans.Lattice.Backup.Tests;

/// <summary>
/// An <see cref="IBackupPublicationGate"/> that forwards to another and records
/// each call, so a test can pin that a publish or delete path goes through the
/// per-id hold (issue #5418) rather than writing the catalog or sink directly.
/// </summary>
/// <param name="inner">The gate to forward to.</param>
internal sealed class RecordingPublicationGate(IBackupPublicationGate inner) : IBackupPublicationGate
{
    /// <summary>The calls recorded since the last <see cref="Reset"/>, as "Method:backupId[:detail]".</summary>
    public static ConcurrentQueue<string> Calls { get; } = new();

    /// <summary>Clears the recorded calls.</summary>
    public static void Reset() => Calls.Clear();

    /// <summary>Whether a call of <paramref name="method"/> for <paramref name="backupId"/> was recorded.</summary>
    /// <param name="method">The gate method's name.</param>
    /// <param name="backupId">The backup id.</param>
    /// <param name="detail">An optional detail the call must also carry.</param>
    public static bool Saw(string method, string backupId, string? detail = null) =>
        Calls.Contains(detail is null ? $"{method}:{backupId}" : $"{method}:{backupId}:{detail}");

    /// <inheritdoc />
    public async Task<BackupManifest> PublishFullAsync(BackupManifest manifest, bool register, CancellationToken cancellationToken)
    {
        var published = await inner.PublishFullAsync(manifest, register, cancellationToken);
        Calls.Enqueue($"{nameof(PublishFullAsync)}:{published.Id}");
        return published;
    }

    /// <inheritdoc />
    public async Task<BackupManifest?> PublishIncrementAsync(BackupManifest manifest, CancellationToken cancellationToken)
    {
        var published = await inner.PublishIncrementAsync(manifest, cancellationToken);
        Calls.Enqueue($"{nameof(PublishIncrementAsync)}:{manifest.Id}:{(published is null ? "abandoned" : "published")}");
        return published;
    }

    /// <inheritdoc />
    public async Task<BackupManifest> RegisterAsync(BackupManifest manifest, CancellationToken cancellationToken)
    {
        var published = await inner.RegisterAsync(manifest, cancellationToken);
        Calls.Enqueue($"{nameof(RegisterAsync)}:{manifest.Id}:{(manifest.SetId is null ? "unstamped" : "stamped")}");
        return published;
    }

    /// <inheritdoc />
    public async Task<bool> RegisterRebuiltAsync(BackupManifest manifest, CancellationToken cancellationToken)
    {
        var listed = await inner.RegisterRebuiltAsync(manifest, cancellationToken);
        Calls.Enqueue($"{nameof(RegisterRebuiltAsync)}:{manifest.Id}:{(listed ? "listed" : "skipped")}");
        return listed;
    }

    /// <inheritdoc />
    public async Task<bool> RetireAsync(BackupManifest row, BackupRetireKind kind, CancellationToken cancellationToken)
    {
        var removed = await inner.RetireAsync(row, kind, cancellationToken);
        Calls.Enqueue($"{nameof(RetireAsync)}:{row.Id}:{kind}");
        return removed;
    }
}
