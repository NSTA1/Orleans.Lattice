namespace VehicleFleetSimulator.AzureThroughput.Engine;

/// <summary>
/// Selects which <c>ILattice</c> operation the benchmark silo dispatches
/// per producer batch. Used by <see cref="BenchWorkloadDispatcher"/> in
/// <c>TcpIngestService.FlushAsync</c>. The default <see cref="SetMany"/>
/// preserves the harness's legacy behaviour (one <c>SetManyAsync</c> per
/// producer batch); the other four modes exist so a single rung can
/// produce headline numbers for every public <c>ILattice</c> op against
/// the c2-iii operating point. See throughput-capture-plan.md.
/// </summary>
public enum BenchWorkloadMode
{
    SetMany,
    SetManyAtomic,
    SetPoint,
    GetPoint,
    GetMany,

    // Fixed-shape atomic-write modes added so a single rung can produce the
    // single-tree vs multi-tree (cross-tree) atomic-write comparison the
    // published single-silo perf doc wants at matched batch sizes. Unlike
    // SetManyAtomic (whose saga slice follows BENCH_ATOMIC_BATCH_SIZE), these
    // pin their batch shapes: SetManyAtomic2 slices the producer batch into
    // 2-key single-tree sagas; CrossTreeAtomic2 / CrossTreeAtomic64 commit
    // all-or-nothing across two trees ({treeId} and {treeId}-b) with 2 keys
    // (1 per tree) and 64 keys (32 per tree) per saga respectively, via
    // IGrainFactory.BeginAtomicWrite(...).CommitAsync().
    SetManyAtomic2,
    CrossTreeAtomic2,
    CrossTreeAtomic64,

    // set-point-mv: the exact same write path as SetPoint (one SetAsync per
    // key against the target tree), but the silo additionally attaches an
    // asynchronous materialised view derived from that tree (a key-preserving
    // passthrough view registered via AddLatticeViews). It exists only as the
    // A/B partner of set-point: the primary tree's foreground write path is
    // untouched, so comparing the two cohorts shows whether maintaining a
    // materialised view perturbs the source tree's point-write throughput and
    // latency. It should not - the view maintainer tails the WAL off the hot
    // path, so the asynchronous derivation must not appear on the writer's
    // critical path.
    SetPointMv,
}
