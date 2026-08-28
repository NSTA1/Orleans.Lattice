using System.Collections.Immutable;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Default <see cref="ILatticeAdmin"/> implementation. A single activation
/// per cluster keyed by <see cref="LatticeConstants.AdminGrainKey"/>. Reduces
/// cluster-wide administrative queries across every registered tree reported
/// by the tree registry.
/// </summary>
internal sealed partial class LatticeAdminGrain(
    IGrainContext context,
    IGrainFactory grainFactory,
    ILogger<LatticeAdminGrain> logger,
    LatticeOptionsResolver? optionsResolver = null,
    IWalStorageProviderCatalog? walProviderCatalog = null,
    IWalRecordEncoder? walRecordEncoder = null,
    IOptionsMonitor<LatticeOptions>? optionsMonitor = null) : ILatticeAdmin, IGrainBase
{
    IGrainContext IGrainBase.GrainContext => context;

    /// <summary>
    /// How many trees a cluster-wide storage-usage fan-out samples at once.
    /// Read from the default (unnamed) options: the roll-up spans every tree,
    /// so a per-tree override has no meaningful scope here. Falls back to the
    /// documented default when no options monitor is registered.
    /// </summary>
    private int MaxConcurrentUsageTrees
        => optionsMonitor?.Get(Options.DefaultName).MaxConcurrentStorageUsageTrees
           ?? LatticeOptions.DefaultMaxConcurrentStorageUsageTrees;

    /// <summary>
    /// Wall-clock budget the roll-up may spend sampling trees before it stops
    /// dispatching and returns a flagged partial. Cluster-wide, for the same
    /// reason as <see cref="MaxConcurrentUsageTrees"/>.
    /// </summary>
    private TimeSpan UsageRollupBudget
        => optionsMonitor?.Get(Options.DefaultName).StorageUsageRollupBudget
           ?? LatticeOptions.DefaultStorageUsageRollupBudget;

    /// <inheritdoc />
    public Task<ClusterStorageUsageReport> GetTotalStorageUsageAsync(CancellationToken cancellationToken = default)
        => BuildRollupAsync(forceRefresh: false, cancellationToken);

    /// <inheritdoc />
    public Task<ClusterStorageUsageReport> RefreshStorageUsageAsync(CancellationToken cancellationToken = default)
        => BuildRollupAsync(forceRefresh: true, cancellationToken);

    /// <inheritdoc />
    public async Task PollWalUsageAsync(CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var treeIds = await registry.GetAllTreeIdsAsync();
        cancellationToken.ThrowIfCancellationRequested();

        if (treeIds.Count == 0) return;

        // Bounded so a large cluster's poll tick cannot dispatch one call per
        // tree in a single burst that races the Orleans response deadline.
        await BoundedFanOut.RunAsync(
            treeIds.Count,
            MaxConcurrentUsageTrees,
            slot => PollWalUsageForTreeAsync(treeIds[slot], cancellationToken),
            cancellationToken);
    }

    private async Task PollWalUsageForTreeAsync(string treeId, CancellationToken cancellationToken)
    {
        try
        {
            var wal = grainFactory.GetGrain<ILatticeWalUsage>(treeId);
            await wal.GetWalUsageAsync(cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Caller-driven cancellation aborts the poll promptly instead of
            // being absorbed once per remaining tree.
            throw;
        }
        catch (Exception ex)
        {
            // A failing tree must not abort the cluster-wide WAL poll;
            // the next tick retries. The metrics sink's staleness horizon
            // covers a few missed polls before a series expires.
            logger.LogDebug(ex, "WAL-usage poll failed for tree {TreeId}", treeId);
        }
    }

    private async Task<ClusterStorageUsageReport> BuildRollupAsync(bool forceRefresh, CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var registry = grainFactory.GetGrain<ILatticeRegistry>(LatticeConstants.RegistryTreeId);
        var treeIds = await registry.GetAllTreeIdsAsync();
        cancellationToken.ThrowIfCancellationRequested();

        // Bounded: each tree sampled here fans out again to its own shard roots
        // and WAL partitions, so an unbounded outer level multiplies against the
        // inner one into a burst that fails on the response deadline rather than
        // merely taking longer. BoundedFanOut writes results by slot index, so
        // the registry's sort order survives the bound.
        //
        // Bounding the burst is necessary but not sufficient: a deep refresh
        // re-walks every shard of every tree, so a large enough catalogue cannot
        // be sampled inside one response deadline however gently it is
        // dispatched. The budget caps the total, so a roll-up that cannot finish
        // returns the trees it did sample and flags the rest, instead of failing
        // the whole call and telling the caller nothing.
        var budget = UsageRollupBudget;
        using var budgetSource = budget > TimeSpan.Zero
            ? CancellationTokenSource.CreateLinkedTokenSource(cancellationToken)
            : null;
        budgetSource?.CancelAfter(budget);
        // The caller's own token still gates dispatch, so a caller-driven cancel
        // aborts promptly; the budget token is handed to the per-tree call, where
        // its expiry degrades that tree to "did not answer" rather than faulting
        // the roll-up.
        var budgetToken = budgetSource?.Token ?? cancellationToken;

        var reports = await BoundedFanOut.RunAsync(
            treeIds.Count,
            MaxConcurrentUsageTrees,
            slot => GetTreeUsageAsync(treeIds[slot], forceRefresh, budgetToken, cancellationToken),
            cancellationToken);


        cancellationToken.ThrowIfCancellationRequested();

        // Tree ids come back sorted from the registry; preserve that order.
        var sorted = reports.ToImmutableArray();


        long walRetained = 0;
        long snapshot = 0;
        long leafState = 0;
        long total = 0;
        var partial = false;
        foreach (var report in sorted)
        {
            walRetained += report.WalRetainedBytes;
            snapshot += report.SnapshotBytes;
            leafState += report.LeafStateBytes;
            total += report.TotalBytes;
            partial |= report.Partial;
        }

        return new ClusterStorageUsageReport
        {
            TreeCount = sorted.Length,
            WalRetainedBytes = walRetained,
            SnapshotBytes = snapshot,
            LeafStateBytes = leafState,
            TotalBytes = total,
            Partial = partial,
            Trees = sorted,
            SampledAt = DateTimeOffset.UtcNow,
        };
    }

    /// <summary>
    /// Samples one tree's usage for the roll-up.
    /// <para>
    /// Two cancellation tokens, deliberately: <paramref name="callerToken"/> is
    /// the caller's own, and its cancellation must abort the whole roll-up
    /// rather than be absorbed once per tree into a cluster report of partial
    /// zeroes that would look like a real - but wildly understated - answer.
    /// <paramref name="budgetToken"/> additionally expires when the roll-up's
    /// wall-clock budget runs out; that is not a failure of the caller's request
    /// but a deliberate truncation, so it degrades this tree to "did not answer"
    /// and lets the roll-up return a flagged partial.
    /// </para>
    /// </summary>
    private async Task<TreeStorageUsageReport> GetTreeUsageAsync(
        string treeId, bool forceRefresh, CancellationToken budgetToken, CancellationToken callerToken)
    {
        try
        {
            var usage = grainFactory.GetGrain<ILatticeStorageUsage>(treeId);
            return await usage.GetReportAsync(forceRefresh, budgetToken);
        }
        catch (OperationCanceledException) when (callerToken.IsCancellationRequested)
        {
            // Caller-driven cancellation must abort the roll-up rather than be
            // absorbed once per tree into a cluster report of partial zeroes,
            // which would look like a real - but wildly understated - answer.
            throw;
        }
        catch (OperationCanceledException)
        {
            // Budget expiry, not caller cancellation. Report the tree as
            // not-answered so the roll-up can still return; every tree left
            // after the budget lapses short-circuits here without dialing its
            // grain, so the truncated report is returned promptly.
            return NotAnswered(treeId);
        }
        catch (Exception ex)
        {
            logger.LogWarning(ex, "Cluster storage-usage roll-up failed for tree {TreeId}", treeId);
            // A failing tree contributes a partial zero rather than aborting
            // the whole cluster roll-up.
            return NotAnswered(treeId);
        }
    }

    private static TreeStorageUsageReport NotAnswered(string treeId) => new()
    {
        TreeId = treeId,
        Partial = true,
        SampledAt = DateTimeOffset.UtcNow,
    };
}
