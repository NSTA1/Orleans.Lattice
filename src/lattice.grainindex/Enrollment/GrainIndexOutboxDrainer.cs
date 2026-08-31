using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex.Enrollment;

/// <summary>
/// Applies the index writes that were recorded in the outbox but never
/// confirmed, until the index agrees with the state the grains have already
/// committed.
/// </summary>
/// <remarks>
/// <para>
/// This is what turns a failed index write from silent drift into a bounded
/// delay. An entry lands in the outbox before its batch is attempted and is
/// cleared only once the batch has committed, so anything still there describes
/// work the system owes: a batch the tree rejected, a batch a stopped silo never
/// got to, or - in
/// <see cref="GrainIndexProjectionMode.Eventual"/> mode - a batch that was
/// deliberately deferred. Because the entry carries the whole batch, converging
/// it is a tree-to-tree operation: no grain is activated, so an index outage is
/// repaired without waking the grains it affected.
/// </para>
/// <para>
/// Retries reuse the entry's original idempotency key, so a batch that actually
/// committed before the writer learned of it re-attaches to the original saga
/// instead of running twice.
/// </para>
/// <para>
/// A pass is driven explicitly through <see cref="DrainAsync"/> rather than by
/// an internal timer, so the background schedule is one caller among several -
/// and a test can converge the outbox at an exact moment instead of waiting for
/// one.
/// </para>
/// </remarks>
internal sealed class GrainIndexOutboxDrainer
{
    private readonly IGrainIndexEnrollmentStore _store;
    private readonly IGrainFactory _grainFactory;
    private readonly IOptionsMonitor<GrainIndexOptions> _options;
    private readonly ILogger<GrainIndexOutboxDrainer> _logger;
    private readonly ConcurrentDictionary<string, ILattice> _trees = new(StringComparer.Ordinal);

    /// <summary>Initialises the drain.</summary>
    /// <param name="store">The registry-backed outbox. Must not be <c>null</c>.</param>
    /// <param name="grainFactory">Resolves each index's backing tree. Must not be <c>null</c>.</param>
    /// <param name="options">The per-index options monitor. Must not be <c>null</c>.</param>
    /// <param name="logger">Reports entries that could not be applied. Must not be <c>null</c>.</param>
    /// <exception cref="ArgumentNullException">Any argument is <c>null</c>.</exception>
    public GrainIndexOutboxDrainer(
        IGrainIndexEnrollmentStore store,
        IGrainFactory grainFactory,
        IOptionsMonitor<GrainIndexOptions> options,
        ILogger<GrainIndexOutboxDrainer> logger)
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(grainFactory);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(logger);

        _store = store;
        _grainFactory = grainFactory;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// Applies up to <paramref name="maxItems"/> outstanding index writes.
    /// </summary>
    /// <param name="maxItems">
    /// The most entries to apply in this pass. Values below 1 are treated as 1,
    /// so a pass always makes progress.
    /// </param>
    /// <param name="cancellationToken">Cancels the pass between entries.</param>
    /// <returns>What the pass did.</returns>
    public async Task<GrainIndexOutboxDrainResult> DrainAsync(
        int maxItems,
        CancellationToken cancellationToken)
    {
        var budget = Math.Max(1, maxItems);
        var scanned = 0;
        var applied = 0;
        var failed = 0;
        var skipped = 0;

        await foreach (var pending in _store.ScanPendingAsync(cancellationToken).ConfigureAwait(true))
        {
            cancellationToken.ThrowIfCancellationRequested();
            scanned++;

            var tree = ResolveTree(pending.IndexName);
            if (tree is null)
            {
                skipped++;
            }
            else if (await TryApplyAsync(tree, pending, cancellationToken).ConfigureAwait(true))
            {
                applied++;
            }
            else
            {
                failed++;
            }

            if (scanned >= budget)
                break;
        }

        return new GrainIndexOutboxDrainResult(scanned, applied, failed, skipped);
    }

    private async Task<bool> TryApplyAsync(
        ILattice tree,
        GrainIndexPendingProjection pending,
        CancellationToken cancellationToken)
    {
        try
        {
            await GrainIndexPlanApplier
                .ApplyAsync(tree, pending.Plan, pending.OperationId, cancellationToken)
                .ConfigureAwait(true);

            await _store
                .CompleteAsync(
                    pending.IndexName,
                    pending.GrainKey,
                    pending.Plan.Projection,
                    cancellationToken)
                .ConfigureAwait(true);

            return true;
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // One entry that will not apply must not stall the ones behind it,
            // so the failure is reported and the entry left for the next pass.
            _logger.LogWarning(
                ex,
                "Grain index '{IndexName}' could not apply the pending projection for grain '{GrainKey}'; it stays in the outbox and will be retried.",
                pending.IndexName,
                pending.GrainKey);

            return false;
        }
    }

    /// <summary>
    /// The tree backing an index, or <c>null</c> when this silo does not declare
    /// that index and therefore has no tree name for it.
    /// </summary>
    private ILattice? ResolveTree(string indexName)
    {
        if (_trees.TryGetValue(indexName, out var cached))
            return cached;

        var treeName = _options.Get(indexName).TreeName;
        if (string.IsNullOrEmpty(treeName))
        {
            // An entry written by a silo that declares an index this one does
            // not. Leaving it alone is the only safe move: this silo cannot know
            // which tree it belongs to, and discarding it would lose the write.
            _logger.LogDebug(
                "Grain index '{IndexName}' is not declared on this silo, so its pending projections are left for a silo that declares it.",
                indexName);
            return null;
        }

        return _trees.GetOrAdd(indexName, _grainFactory.GetGrain<ILattice>(treeName));
    }
}
