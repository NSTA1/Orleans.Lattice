namespace Orleans.Lattice;

/// <summary>
/// An observed-remove (enable-wins) flag CRDT. Each call to
/// <see cref="Enable(string, long)"/> tags the flag with a unique
/// <see cref="OrSetDot"/>; <see cref="Disable"/> drops only the dots
/// currently observed as enabled. State-level <see cref="Merge(OrFlag, OrFlag)"/>
/// is the union of every replica's enable dots minus the union of every
/// replica's observed-remove dots, making the CRDT commutative,
/// associative, and idempotent under arbitrary delivery order.
/// <para>
/// The flag is the single-element specialisation of <see cref="OrSet"/>:
/// it tracks presence ("enabled") rather than a set of element values, so
/// it carries no element payload. It is the minimal observed-remove
/// primitive for composite-key membership rows - e.g. a tag/key secondary
/// index where the meaningful bit is whether a <c>(tag, member)</c> row is
/// present - giving OR-Set-grade convergence under concurrent active-active
/// enable / disable without storing a singleton set's element bytes.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.OrFlag)]
public sealed class OrFlag : ICrdt<OrFlag>
{
    // Below this many tombstone dots a linear scan beats allocating and
    // populating a HashSet for the membership checks. A flag carries one
    // dot per concurrent enable/disable, overwhelmingly 1-2 in practice,
    // so the linear path is the common case; the set is only built once a
    // flag genuinely accumulates many concurrent dots. Mirrors
    // OrSet.DotLinearScanThreshold.
    private const int DotLinearScanThreshold = 4;

    /// <summary>
    /// Live enable dots. The flag is enabled if and only if at least one
    /// of these dots is not present in <see cref="Tombstones"/>.
    /// </summary>
    [Id(0)]
    public List<OrSetDot> Enables { get; set; } = [];

    /// <summary>
    /// Observed-remove (disable) dots. A dot in this list cancels the
    /// matching dot in <see cref="Enables"/> on merge.
    /// </summary>
    [Id(1)]
    public List<OrSetDot> Tombstones { get; set; } = [];

    /// <summary>Returns <c>true</c> when at least one enable dot is not tombstoned.</summary>
    public bool IsEnabled => LiveEnableCount() > 0;

    /// <inheritdoc />
    /// <remarks>
    /// An <see cref="OrFlag"/> is bottom when it is not enabled - i.e. no
    /// enable dot survives the tombstone set. Tombstones may still be
    /// present and are preserved for causal-history purposes, but a
    /// containing composite (e.g. <see cref="OrMap{TKey, TValue}"/>)
    /// treats the slot as absent.
    /// </remarks>
    public bool IsBottom => !IsEnabled;

    /// <summary>Enables the flag with a fresh causal dot.</summary>
    /// <param name="replicaId">The replica authoring the enable. Must be non-empty.</param>
    /// <param name="counter">The replica-local monotonic counter for the dot.</param>
    public void Enable(string replicaId, long counter)
    {
        ArgumentException.ThrowIfNullOrEmpty(replicaId);
        Enables.Add(new OrSetDot { ReplicaId = replicaId, Counter = counter });
    }

    /// <summary>
    /// Disables the flag by tombstoning every enable dot currently
    /// observed. Concurrent enables on other replicas (with dots not in
    /// the local <see cref="Enables"/> at the time of the disable) survive
    /// a later merge because their dots are not tombstoned here. Returns
    /// <c>true</c> when at least one new dot was tombstoned.
    /// </summary>
    public bool Disable()
    {
        if (Enables.Count == 0) return false;
        var anyAdded = false;
        if (Tombstones.Count <= DotLinearScanThreshold)
        {
            // Tiny tombstone list (the common 0-1-dot case): a linear
            // Contains against the growing list beats allocating a HashSet.
            foreach (var dot in Enables)
            {
                if (!Tombstones.Contains(dot)) { Tombstones.Add(dot); anyAdded = true; }
            }
            return anyAdded;
        }
        var tombSet = new HashSet<OrSetDot>(Tombstones);
        foreach (var dot in Enables)
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
    /// Lattice merge: pointwise union of <see cref="Enables"/> and
    /// <see cref="Tombstones"/>. Commutative, associative, idempotent.
    /// </summary>
    public static OrFlag Merge(OrFlag left, OrFlag right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var result = left.Clone();
        result.MergeFrom(right);
        return result;
    }

    /// <summary>
    /// In-place lattice merge: applies the union of <paramref name="other"/>'s
    /// enable and tombstone dots into this flag. Equivalent to
    /// <see cref="Merge(OrFlag, OrFlag)"/> followed by replacing the
    /// receiver, but avoids the intermediate clone.
    /// </summary>
    public void MergeFrom(OrFlag other)
    {
        ArgumentNullException.ThrowIfNull(other);
        UnionInto(Enables, other.Enables);
        UnionInto(Tombstones, other.Tombstones);
    }

    /// <summary>Creates a deep copy of this flag.</summary>
    public OrFlag Clone() => new()
    {
        Enables = [.. Enables],
        Tombstones = [.. Tombstones],
    };

    /// <summary>
    /// Folds an <see cref="OrFlagDelta"/> into this flag: every dot in
    /// <see cref="OrFlagDelta.Enables"/> is unioned into <see cref="Enables"/>,
    /// every dot in <see cref="OrFlagDelta.Disables"/> is unioned into
    /// <see cref="Tombstones"/>. The merge is commutative, associative,
    /// and idempotent against arrival order and duplicate delivery -
    /// applying the same delta twice yields the same state because the
    /// per-dot sets are unions.
    /// </summary>
    /// <param name="delta">
    /// The typed CRDT delta authored by the producing call site. Empty
    /// collections are valid; <c>null</c> collections are treated as empty.
    /// </param>
    public void MergeDelta(OrFlagDelta delta)
    {
        UnionDots(Enables, delta.Enables);
        UnionDots(Tombstones, delta.Disables);
    }

    private int LiveEnableCount()
    {
        if (Enables.Count == 0) return 0;
        if (Tombstones.Count == 0) return Enables.Count;
        if (Tombstones.Count <= DotLinearScanThreshold)
        {
            // Tiny tombstone list: linear scan beats hashing.
            var liveLinear = 0;
            foreach (var dot in Enables)
            {
                if (!Tombstones.Contains(dot)) liveLinear++;
            }
            return liveLinear;
        }
        var tombSet = new HashSet<OrSetDot>(Tombstones);
        var live = 0;
        foreach (var dot in Enables)
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
        var seen = new HashSet<OrSetDot>(target);
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
        var seen = new HashSet<OrSetDot>(target);
        for (var i = 0; i < source.Count; i++)
        {
            var dot = source[i];
            if (seen.Add(dot)) target.Add(dot);
        }
    }
}
