namespace Orleans.Lattice.Schema;

/// <summary>
/// Silo-wide options for schema enforcement, populated through
/// <c>AddLatticeSchemaEnforcement(...)</c>. Per-tree behaviour (the rules and the
/// per-tree strict flag) lives in each tree's <see cref="LatticeSchemaPolicy"/>;
/// these options are the global switches that must be known before any policy is
/// loaded.
/// </summary>
public sealed class LatticeSchemaEnforcementOptions
{
    /// <summary>
    /// Globally enables strict-mode ingest for system-origin writes that reach the
    /// enforcement interceptor. When <c>false</c> (the default), the interceptor
    /// skips those system-origin writes, so they pay zero overhead. When
    /// <c>true</c>, intercepted system-origin writes are inspected and a
    /// non-compliant item is dead-lettered for any tree whose policy also sets
    /// <see cref="LatticeSchemaPolicy.StrictIngest"/>. Replicated typed-CRDT
    /// deltas and replicated atomic-batch entries reach the interceptor; a plain
    /// last-writer-wins replication apply, a backup restore and a tree merge bypass
    /// write interception and are not made schema-checked by this switch.
    /// </summary>
    public bool StrictIngest { get; set; }

    /// <summary>
    /// Enables the merge-result observer, which validates the merged value of a
    /// completed per-key merge against the tree's policy. A violation never blocks
    /// or changes the merge: it is counted on the
    /// <c>orleans.lattice.leaf.merge_observer_events</c> counter (tagged by tree)
    /// and logged as a warning carrying the validation reason. Off by default.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Observed merge paths.</b> Only the foreground leaf set path (a
    /// last-writer-wins set) and the typed-CRDT apply path consult the observer.
    /// Merges that arrive through <c>MergeManyAsync</c> - a plain last-writer-wins
    /// replication apply, a tree <c>MergeAsync</c>, a merging restore, and snapshot
    /// and resize copies - are not observed.
    /// </para>
    /// <para>
    /// <b>Cost.</b> When on, every leaf set and CRDT apply on every tree in the
    /// silo consults the observer, and batched sets take the per-key path rather
    /// than the batched commit.
    /// </para>
    /// <para>
    /// <b>Sampling.</b> The flag is read once, at registration, from the delegate
    /// passed to the first <c>AddLatticeSchemaEnforcement</c> call. Setting it in a
    /// later <c>AddLatticeSchemaEnforcement</c> or
    /// <c>ConfigureLatticeSchemaEnforcement</c> delegate has no effect.
    /// </para>
    /// </remarks>
    public bool ValidateCrdtMergeResults { get; set; }

    /// <summary>
    /// The maximum number of leading value bytes copied into a
    /// <see cref="LatticeSchemaDeadLetterEntry.ValuePreview"/>. Bounds the storage
    /// cost of retaining a diverted item. Values below 1 are clamped to 1. Defaults to 4096.
    /// </summary>
    public int DeadLetterPreviewMaxBytes { get; set; } = 4096;

    /// <summary>
    /// How often each silo checks whether a schema policy change was committed
    /// elsewhere in the cluster, which bounds how long a silo can keep enforcing
    /// the previous policy. The silo that served <see cref="ILatticeSchemaAdmin"/>
    /// applies a change before the call returns; every other silo stops serving the
    /// previous policy within this interval of the change being published, which
    /// the silo that committed it does immediately. Each check is one small grain
    /// call per silo and no store read; a shorter interval tightens the bound at the
    /// cost of more frequent checks. Must be positive. Defaults to one second.
    /// </summary>
    public TimeSpan PropagationInterval { get; set; } = TimeSpan.FromSeconds(1);
}
