namespace Orleans.Lattice.BPlusTree.State;

/// <summary>
/// Persistent cursor state for <see cref="Grains.TagIndexReconcileGrain"/>.
/// Tracks the progress of an in-flight digest-gated sweep so it resumes after a
/// silo restart, and carries the per-tree digest baseline across sweeps so a
/// clean index incurs only digest-probe cost.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.TagIndexReconcileState)]
internal sealed class TagIndexReconcileState
{
    /// <summary>Whether a sweep is currently in progress.</summary>
    [Id(0)] public bool InProgress { get; set; }

    /// <summary>The current phase of the in-flight sweep.</summary>
    [Id(1)] public TagIndexReconcilePhase Phase { get; set; }

    /// <summary>
    /// Snapshot of the covered-tree set taken when the sweep began. The probe
    /// phase walks this list; capturing it up front keeps the sweep stable even
    /// if the covered set changes mid-sweep.
    /// </summary>
    [Id(2)] public List<string> CoveredTrees { get; set; } = [];

    /// <summary>Cursor into <see cref="CoveredTrees"/> during the probe phase.</summary>
    [Id(3)] public int NextProbeIndex { get; set; }

    /// <summary>
    /// Trees the probe phase found divergent (or whose digest was unavailable),
    /// queued for the repair phase.
    /// </summary>
    [Id(4)] public List<string> DirtyTrees { get; set; } = [];

    /// <summary>Cursor into <see cref="DirtyTrees"/> during the repair phase.</summary>
    [Id(5)] public int NextRepairIndex { get; set; }

    /// <summary>
    /// Probe-time fingerprint for each dirty tree, committed into
    /// <see cref="Baselines"/> once that tree's repair completes.
    /// </summary>
    [Id(6)] public Dictionary<string, byte[]> PendingBaselines { get; set; } = new(StringComparer.Ordinal);

    /// <summary>
    /// Per-tree digest fingerprint from the last successful reconcile. A tree
    /// whose current fingerprint equals its baseline is clean and skipped.
    /// Persisted across sweeps; this is the gate that makes clean cycles cheap.
    /// </summary>
    [Id(7)] public Dictionary<string, byte[]> Baselines { get; set; } = new(StringComparer.Ordinal);

    /// <summary>Whether the in-flight sweep is an audit-only (probe, never repair) pass.</summary>
    [Id(8)] public bool ProbeOnlySweep { get; set; }

    /// <summary>Number of covered trees probed in the in-flight sweep.</summary>
    [Id(9)] public int TreesProbed { get; set; }

    /// <summary>Number of trees the in-flight sweep found divergent.</summary>
    [Id(10)] public int TreesMismatched { get; set; }

    /// <summary>Live keys scanned across all repaired trees in the in-flight sweep.</summary>
    [Id(11)] public int KeysScanned { get; set; }

    /// <summary>Membership rows scanned across all repaired trees in the in-flight sweep.</summary>
    [Id(12)] public int MembershipRowsScanned { get; set; }

    /// <summary>Orphan membership rows removed across all repaired trees in the in-flight sweep.</summary>
    [Id(13)] public int OrphanRowsRemoved { get; set; }
}
