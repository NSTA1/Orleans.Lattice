namespace MultiSiteManufacturing.Host.Federation;

/// <summary>Canned chaos presets exposed in the UI fly-out.</summary>
public enum ChaosPreset
{
    /// <summary>Resets every site to <see cref="SiteConfig.Nominal"/>.</summary>
    ClearAll,

    /// <summary>Pauses Stuttgart CMM Lab + Toulouse NDT Lab and adds a 4s delay.</summary>
    TransoceanicBackhaulOutage,

    /// <summary>Adds an 8s delay at Nagoya Heat Treatment.</summary>
    CustomsHold,

    /// <summary>Pauses Cincinnati MRB entirely.</summary>
    MrbWeekend,

    /// <summary>
    /// Applies a 10% transient-failure rate and 50–250 ms jitter to the
    /// <c>lattice</c> backend only. Drives baseline-vs-lattice divergence
    /// without touching site chaos.
    /// </summary>
    LatticeStorageFlakes,

    /// <summary>
    /// Opens a 300 ms reorder window on the <c>baseline</c> backend
    /// only - incoming writes are buffered and flushed in shuffled
    /// order. Combined with the <b>Race</b> row action, this is the
    /// canonical way to force baseline into an arrival-order fold that
    /// disagrees with lattice's HLC-order fold, highlighting the
    /// affected row in the inventory grid.
    /// </summary>
    BaselineReorderStorm,

    /// <summary>
    /// Simulates an inter-silo network partition between the two silos
    /// inside a single Orleans cluster. When active, each silo accepts
    /// only writes for parts whose serial hashes to its own "half" of
    /// the cluster - silo A keeps the even-hash parts, silo B keeps the
    /// odd-hash parts - and drops the rest on the floor at router
    /// ingress. The split is simulation-only: the shared lattice tree
    /// still reflects every accepted write, so on heal (ClearAll or a
    /// direct ConfigurePartitionAsync call) both silos converge
    /// immediately. This is <i>not</i> a true Orleans transport-level
    /// partition; it exists to let a demo show "two browser tabs write
    /// different subsets during a split, both sets are visible after
    /// heal". Scope: intra-cluster only - has no effect on cross-cluster
    /// HTTP replication (use <see cref="ReplicationDisconnect"/> for
    /// that).
    /// </summary>
    ClusterSplit,

    /// <summary>
    /// Pauses cross-cluster replication in both directions. Implemented
    /// as an <see cref="Orleans.Lattice.Replication.IReplicationTransport"/>
    /// decorator that wraps the gRPC push transport: outbound ships
    /// become no-ops and inbound applies return "unavailable" so the
    /// peer backs off. The local WAL keeps growing while disconnected;
    /// on <see cref="ClearAll"/> the flag clears and replication
    /// resumes from the current cursor, catching the peer up with the
    /// accumulated backlog. This is the app-level equivalent of
    /// <c>docker network disconnect msmfg_wan</c> - it lets the
    /// operator demonstrate cross-cluster divergence and
    /// convergence-on-heal from the browser without touching the
    /// compose CLI.
    /// </summary>
    ReplicationDisconnect,
}
