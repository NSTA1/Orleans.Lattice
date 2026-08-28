namespace Orleans.Lattice;

/// <summary>
/// A remove-wins (disable-wins) flag CRDT - the inverse of the enable-wins
/// <see cref="OrFlag"/>. Presence requires at least one
/// <see cref="Enable(string, long)"/> dot, but any disable dot that an
/// enable has not observed continues to suppress the flag, so a concurrent
/// <see cref="Disable(string, long)"/> dominates a concurrent enable.
/// State-level <see cref="Merge(RwFlag, RwFlag)"/> is the pointwise union of
/// every replica's enable, disable, and tombstone dots, making the CRDT
/// commutative, associative, and idempotent under arbitrary delivery order.
/// <para>
/// The flag tracks presence ("enabled") rather than a set of element values,
/// so it carries no element payload. It is the remove-wins counterpart to
/// <see cref="OrFlag"/> for composite-key membership rows - e.g. a tag/key
/// secondary index where a revoke must never silently resurrect against a
/// concurrent re-add (revocation lists, opt-out / blocklist membership,
/// suppression) - giving OR-Set-grade convergence with a remove-wins
/// tie-break instead of carrying a singleton remove-wins set's element bytes.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.RwFlag)]
public sealed class RwFlag : ICrdt<RwFlag>
{
    // Below this many tombstone dots a linear scan beats allocating and
    // populating a HashSet for the membership checks. A flag carries one
    // dot per concurrent enable/disable, overwhelmingly 1-2 in practice,
    // so the linear path is the common case; the set is only built once a
    // flag genuinely accumulates many concurrent dots. Mirrors
    // OrSet.DotLinearScanThreshold.
    private const int DotLinearScanThreshold = 4;

    /// <summary>
    /// Enable dots. The flag can only be enabled when at least one enable
    /// dot is present; enable dots are grow-only and are never cancelled
    /// (the disable side gates presence).
    /// </summary>
    [Id(0)]
    public List<OrSetDot> Enables { get; set; }

    /// <summary>
    /// Disable (remove) dots. A disable dot suppresses the flag until an
    /// enable observes it and cancels it via <see cref="Tombstones"/>.
    /// </summary>
    [Id(1)]
    public List<OrSetDot> Disables { get; set; }

    /// <summary>
    /// Observed-enable tombstones: disable dots that an
    /// <see cref="Enable(string, long)"/> has observed and cancelled. A dot
    /// in this list cancels the matching dot in <see cref="Disables"/> on
    /// merge.
    /// </summary>
    [Id(2)]
    public List<OrSetDot> Tombstones { get; set; }

    /// <summary>Creates an empty remove-wins flag.</summary>
    public RwFlag()
    {
        Enables = [];
        Disables = [];
        Tombstones = [];
    }

    // Direct-assign constructor for the clone fast path: takes ownership of
    // already-built backing stores so the clone allocates no discarded
    // empty-collection shells from field initializers that an object
    // initializer would immediately overwrite. Mirrors OrSet.
    private RwFlag(List<OrSetDot> enables, List<OrSetDot> disables, List<OrSetDot> tombstones)
    {
        Enables = enables;
        Disables = disables;
        Tombstones = tombstones;
    }

    /// <summary>
    /// Returns <c>true</c> when at least one enable dot is present and no
    /// disable dot survives (every disable dot has been cancelled by an
    /// observed enable). A single unobserved or concurrent disable keeps the
    /// flag off - this is the remove-wins tie-break.
    /// </summary>
    public bool IsEnabled => Enables.Count > 0 && LiveDisableCount() == 0;

    /// <inheritdoc />
    /// <remarks>
    /// A <see cref="RwFlag"/> is bottom when it is not enabled - i.e. it
    /// carries no live presence. Disable dots and tombstones may still be
    /// present and are preserved for causal-history purposes, but a
    /// containing composite (e.g. <see cref="OrMap{TKey, TValue}"/>) treats
    /// the slot as absent.
    /// </remarks>
    public bool IsBottom => !IsEnabled;

    /// <summary>
    /// Disables (removes) the flag with a fresh causal dot. The disable
    /// dominates any concurrent enable that has not observed it: it survives
    /// a later merge and keeps the flag off until an enable observes and
    /// cancels this dot.
    /// </summary>
    /// <param name="replicaId">The replica authoring the disable. Must be non-empty.</param>
    /// <param name="counter">The replica-local monotonic counter for the dot.</param>
    public void Disable(string replicaId, long counter)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        Disables.Add(new OrSetDot { ReplicaId = replicaId, Counter = counter });
    }

    /// <summary>
    /// Enables the flag with a fresh causal dot and tombstones every disable
    /// dot currently observed. Concurrent disables on other replicas (with
    /// dots not in the local <see cref="Disables"/> at the time of the
    /// enable) survive a later merge because their dots are not tombstoned
    /// here, so they continue to suppress the flag - remove wins. Returns
    /// <c>true</c> when at least one new disable dot was tombstoned.
    /// </summary>
    /// <param name="replicaId">The replica authoring the enable. Must be non-empty.</param>
    /// <param name="counter">The replica-local monotonic counter for the enable dot.</param>
    public bool Enable(string replicaId, long counter)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        Enables.Add(new OrSetDot { ReplicaId = replicaId, Counter = counter });
        if (Disables.Count == 0) return false;
        var anyAdded = false;
        if (Tombstones.Count <= DotLinearScanThreshold || Disables.Count <= DotLinearScanThreshold)
        {
            // Tiny tombstone list (the common 0-1-dot case), or few disable
            // dots to tombstone: a linear Contains against the growing list
            // beats allocating a HashSet. A flag is typically disabled once or
            // twice between enables, so the disable side is the small one even
            // when the tombstone history is long.
            foreach (var dot in Disables)
            {
                if (!Tombstones.Contains(dot)) { Tombstones.Add(dot); anyAdded = true; }
            }
            return anyAdded;
        }
        var tombSet = OrSetDotSet.Build(Tombstones);
        foreach (var dot in Disables)
        {
            if (tombSet.Add(dot))
            {
                Tombstones.Add(dot);
                anyAdded = true;
            }
        }
        return anyAdded;
    }

    /// <summary>
    /// Lattice merge: pointwise union of <see cref="Enables"/>,
    /// <see cref="Disables"/>, and <see cref="Tombstones"/>. Commutative,
    /// associative, idempotent.
    /// </summary>
    public static RwFlag Merge(RwFlag left, RwFlag right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <summary>
    /// In-place lattice merge: applies the union of <paramref name="other"/>'s
    /// enable, disable, and tombstone dots into this flag. Equivalent to
    /// <see cref="Merge(RwFlag, RwFlag)"/> followed by replacing the
    /// receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(RwFlag other)
    {
        ArgumentNullException.ThrowIfNull(other);
        UnionInto(Enables, other.Enables);
        UnionInto(Disables, other.Disables);
        UnionInto(Tombstones, other.Tombstones);
    }

    /// <summary>Creates a deep copy of this flag.</summary>
    public RwFlag Clone() => new([.. Enables], [.. Disables], [.. Tombstones]);

    /// <summary>
    /// Folds a <see cref="RwFlagDelta"/> into this flag: every dot in
    /// <see cref="RwFlagDelta.Enables"/> is unioned into <see cref="Enables"/>,
    /// every dot in <see cref="RwFlagDelta.Disables"/> is unioned into
    /// <see cref="Disables"/>, and every dot in
    /// <see cref="RwFlagDelta.Tombstones"/> is unioned into
    /// <see cref="Tombstones"/>. The merge is commutative, associative, and
    /// idempotent against arrival order and duplicate delivery - applying the
    /// same delta twice yields the same state because the per-dot sets are
    /// unions.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Empty
    /// collections are valid; <c>null</c> collections are treated as empty.
    /// </param>
    public void MergeDelta(RwFlagDelta delta)
    {
        UnionDots(Enables, delta.Enables);
        UnionDots(Disables, delta.Disables);
        UnionDots(Tombstones, delta.Tombstones);
    }

    private int LiveDisableCount()
    {
        if (Disables.Count == 0) return 0;
        if (Tombstones.Count == 0) return Disables.Count;
        if (Tombstones.Count <= DotLinearScanThreshold || Disables.Count <= DotLinearScanThreshold)
        {
            // Tiny tombstone list, or few disable dots: linear scan beats
            // hashing. Disables is the small side on any flag that has been
            // toggled repeatedly, so this keeps IsSet allocation-free there.
            var liveLinear = 0;
            foreach (var dot in Disables)
            {
                if (!Tombstones.Contains(dot)) liveLinear++;
            }
            return liveLinear;
        }
        var tombSet = OrSetDotSet.Build(Tombstones);
        var live = 0;
        foreach (var dot in Disables)
        {
            if (!tombSet.Contains(dot)) live++;
        }
        return live;
    }

    private static void UnionInto(List<OrSetDot> target, List<OrSetDot> source)
    {
        if (source.Count == 0) return;
        if (target.Count <= DotLinearScanThreshold && source.Count <= DotLinearScanThreshold)
        {
            // Tiny dot lists (the common 1-2-concurrent-dot case): a linear
            // Contains is cheaper than allocating a HashSet.
            foreach (var dot in source)
            {
                if (!target.Contains(dot)) target.Add(dot);
            }
            return;
        }
        var seen = OrSetDotSet.Build(target, source.Count);
        foreach (var dot in source)
        {
            if (seen.Add(dot)) target.Add(dot);
        }
    }

    private static void UnionDots(List<OrSetDot> target, IReadOnlyList<OrSetDot>? source)
    {
        if (source is not { Count: > 0 }) return;
        if (target.Count <= DotLinearScanThreshold && source.Count <= DotLinearScanThreshold)
        {
            for (var i = 0; i < source.Count; i++)
            {
                var dot = source[i];
                if (!target.Contains(dot)) target.Add(dot);
            }
            return;
        }
        var seen = OrSetDotSet.Build(target, source.Count);
        for (var i = 0; i < source.Count; i++)
        {
            var dot = source[i];
            if (seen.Add(dot)) target.Add(dot);
        }
    }
}
