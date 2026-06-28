using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Lattice;

namespace MultiSiteManufacturing.Host.Lattice;

/// <summary>
/// Enables a durable per-key change-history view, with a value-retaining
/// retention mode, on the two CRDT trees the sample showcases - the operator
/// last-writer-wins register (<see cref="PartCrdtStore.OperatorTreeId"/>) and the
/// process-label OR-Set (<see cref="PartCrdtStore.LabelsTreeId"/>).
/// <para>
/// Without an enabled history view a tree retains no durable change history: the
/// timeline is served only from the bounded retained write-ahead-log window and
/// is lost once that window is garbage-collected. Enabling the view (and setting
/// a value-retaining retention mode) makes the Explorer History tab show a
/// durable, retention-bounded timeline - successive last-writer-wins values plus
/// diffs for the operator register, and element-level member changes for the
/// label OR-Set - that survives source WAL garbage collection.
/// </para>
/// <para>
/// Registered as a hosted service ahead of <see cref="Inventory.InventorySeeder"/>
/// on the seeding silo so the views exist before the seeder writes the showcase
/// revisions, ensuring every seeded mutation is tailed into the durable history.
/// </para>
/// </summary>
public sealed class HistoryShowcaseActivator(
    IGrainFactory grains,
    ILatticeViewFactory viewFactory,
    IServiceProvider services,
    ILogger<HistoryShowcaseActivator> logger) : IHostedService
{
    /// <summary>Durable history-view name for the operator last-writer-wins tree.</summary>
    public const string OperatorHistoryView = PartCrdtStore.OperatorTreeId + "-history";

    /// <summary>Durable history-view name for the process-label OR-Set tree.</summary>
    public const string LabelsHistoryView = PartCrdtStore.LabelsTreeId + "-history";

    /// <summary>
    /// Value-retaining retention so the History tab can render successive
    /// last-writer-wins values plus diffs. CRDT revisions are always stored
    /// delta-only regardless of mode, so this only shapes the operator register's
    /// byte values; the label OR-Set keeps its compact author deltas either way.
    /// </summary>
    private const HistoryRetentionMode Retention = HistoryRetentionMode.FullValue;

    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken) => EnableAsync(cancellationToken);

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    /// <summary>
    /// Idempotently enables the durable history view plus value-retaining
    /// retention on both showcase CRDT trees. Safe to call on every silo start:
    /// setting the retention mode overwrites the same registry value and the view
    /// is created only when it does not already exist. Public so a test can invoke
    /// it directly against a deployed cluster.
    /// </summary>
    public async Task EnableAsync(CancellationToken cancellationToken)
    {
        await EnableTreeAsync(PartCrdtStore.OperatorTreeId, OperatorHistoryView, cancellationToken);
        await EnableTreeAsync(PartCrdtStore.LabelsTreeId, LabelsHistoryView, cancellationToken);
    }

    private async Task EnableTreeAsync(string treeId, string viewName, CancellationToken cancellationToken)
    {
        var source = grains.GetGrain<ILattice>(treeId);
        await source.SetHistoryRetentionAsync(Retention, window: null, cancellationToken);

        var existing = await viewFactory.GetAsync(viewName, cancellationToken);
        if (existing is not null)
        {
            return;
        }

        viewFactory.Create(source, viewName, LatticeHistoryView.Definition(viewName, services));
        logger.LogInformation(
            "Enabled durable change-history view {ViewName} over {TreeId} with {Retention} retention.",
            viewName,
            treeId,
            Retention);
    }
}
