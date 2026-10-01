namespace Orleans.Lattice.Schema;

/// <summary>
/// The serializable, per-tree schema-enforcement policy: the ordered set of
/// <see cref="LatticeSchemaRule"/>s an incoming value must satisfy, plus the
/// per-tree strict-ingest flag. A value is valid for the policy only when it
/// satisfies <b>every</b> rule; an empty rule set accepts every value.
/// </summary>
/// <remarks>
/// The policy is persisted in the companion-owned reserved <c>sys-schema-policy</c>
/// tree, keyed by the governed tree id, and resolved / cached per tree by the
/// enforcement provider. A tree with no persisted policy pays a single cached
/// lookup that short-circuits to "no enforcement".
/// </remarks>
[GenerateSerializer]
[Alias(SchemaTypeAliases.LatticeSchemaPolicy)]
[Immutable]
public sealed class LatticeSchemaPolicy
{
    /// <summary>
    /// Initializes a new <see cref="LatticeSchemaPolicy"/>.
    /// </summary>
    /// <param name="rules">The rules a value must satisfy. Must not be <c>null</c>; may be empty.</param>
    /// <param name="strictIngest">
    /// When <c>true</c>, intercepted system-origin writes are re-validated against
    /// the rules and a non-compliant item is dead-lettered rather than applied.
    /// Replicated typed-CRDT deltas and replicated atomic-batch entries are
    /// intercepted; a plain last-writer-wins replication apply, a backup restore and
    /// a tree merge bypass write interception and are not governed by this flag.
    /// When <c>false</c>
    /// (the default), intercepted system-origin writes are trusted.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="rules"/> is <c>null</c>.</exception>
    public LatticeSchemaPolicy(IReadOnlyList<LatticeSchemaRule> rules, bool strictIngest = false)
    {
        ArgumentNullException.ThrowIfNull(rules);
        // Defensively copy the caller-owned collection so a mutation of the
        // original list after construction cannot change this [Immutable] policy.
        Rules = [.. rules];
        StrictIngest = strictIngest;
    }

    /// <summary>The rules a value must satisfy, applied conjunctively.</summary>
    [Id(0)]
    public IReadOnlyList<LatticeSchemaRule> Rules { get; }

    /// <summary>
    /// Whether intercepted system-origin writes are re-validated (strict mode). A
    /// non-compliant intercepted item is dead-lettered rather than applied, so the
    /// intercepted system-origin write never blocks.
    /// </summary>
    [Id(1)]
    public bool StrictIngest { get; }
}
