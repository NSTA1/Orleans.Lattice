using NSubstitute;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.Backup;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// Builds a stub <see cref="IBackupsDomain"/> for the Backups plugin's render
/// tests: a catalogue reader that answers from in-memory manifests, and a fixed
/// tree list. Every answer is supplied up front, so a render never touches a
/// clock, a network, or a background task.
/// </summary>
internal static class StubBackupsDomain
{
    /// <summary>
    /// Builds a domain over <paramref name="entries"/>.
    /// </summary>
    /// <param name="entries">The manifests the single catalogue page returns.</param>
    /// <param name="trees">The trees the capture picker offers.</param>
    /// <param name="healthAvailable">Whether the server reports health monitoring available.</param>
    /// <param name="status">The status the catalogue page reports.</param>
    /// <param name="message">The message a non-success page carries.</param>
    /// <param name="health">The stored health report keyed by backup id, if any.</param>
    public static IBackupsDomain Create(
        IReadOnlyList<BackupManifest>? entries = null,
        IReadOnlyList<BackupTreeOption>? trees = null,
        bool healthAvailable = false,
        BackupOperationStatus status = BackupOperationStatus.Succeeded,
        string? message = null,
        IReadOnlyDictionary<string, BackupHealthReport>? health = null)
    {
        var reader = Substitute.For<IBackupCatalogReader>();

        reader.LoadPageAsync(
                Arg.Any<int>(),
                Arg.Any<string?>(),
                Arg.Any<BackupCatalogFilter?>(),
                Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new BackupListView
            {
                Status = status,
                Entries = entries ?? Array.Empty<BackupManifest>(),
                Message = message,
            }));

        reader.LoadSummaryAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(new BackupCatalogSummary
            {
                Status = BackupOperationStatus.Succeeded,
                Kinds = new[] { BackupKind.Full, BackupKind.Incremental },
                Scopes = new[] { "orders" },
            }));

        reader.LoadFullBackupsAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult<IReadOnlyList<BackupManifest>>(Array.Empty<BackupManifest>()));

        reader.IsHealthMonitoringAvailableAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(healthAvailable));

        reader.GetHealthAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var id = call.ArgAt<string>(0);
                BackupHealthReport? report = null;
                health?.TryGetValue(id, out report);
                return Task.FromResult(report);
            });

        var domain = Substitute.For<IBackupsDomain>();
        domain.Catalog.Returns(reader);
        domain.LoadTreesAsync(Arg.Any<CancellationToken>())
            .Returns(_ => Task.FromResult(trees ?? Array.Empty<BackupTreeOption>()));

        return domain;
    }
}
