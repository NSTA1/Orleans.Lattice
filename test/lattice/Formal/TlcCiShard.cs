namespace Orleans.Lattice.Tests.Formal;

/// <summary>
/// Assigns each TLC mutation case a test category that names its CI shard, so
/// <c>.github/workflows/test-shards.json</c> can split the mutation gates
/// across legs with a <c>TestCategory</c> filter.
/// <para>
/// WHY A CATEGORY AND NOT A NAME PREFIX. A test case's FullName carries its
/// arguments in parentheses, for example
/// <c>Each_property_fires_under_its_mutation_and_not_on_the_base(Replication, X)</c>.
/// The NUnit adapter's filter parser rejects a <c>FullyQualifiedName~</c>
/// value with an unbalanced parenthesis, so a filter cannot select on a
/// module's name inside the argument list. A category needs no such matching.
/// </para>
/// <para>
/// BALANCE, NOT CORRECTNESS. These groups only spread CPU-bound JVM runs over
/// runners; a case with no group runs in the <c>formal-tlc</c> shard, which
/// claims the rest of <see cref="TlcModelCheckTests"/>. A new module therefore
/// runs whether or not anybody adds it here. AtomicCommit* variants are split
/// into six groups using their manifest state counts as weights, so a newly
/// declared variant is routed automatically and the work remains balanced.
/// </para>
/// <para>
/// SHARE A CONTROL WHERE YOU CAN. <see cref="TlcModelCheckTests"/> runs each
/// distinct control arm once per test process, so mutants that share a
/// control (the same module, target property and options) are cheaper in one
/// shard than split across two. Every group here is therefore whole modules,
/// or a split along a target property: every Replication EventualConvergence
/// mutant shares one control and so lives in <see cref="Convergence"/>.
/// </para>
/// </summary>
internal static class TlcCiShard
{
    /// <summary>The Replication EventualConvergence* (temporal) mutants, which share one control arm.</summary>
    public const string Convergence = "TlcShardConvergence";

    /// <summary>The other Replication mutants, plus the mutants of every Wal* module.</summary>
    public const string ReplicationWal = "TlcShardReplicationWal";

    /// <summary>The mutants of every ShardOwnership* module.</summary>
    public const string ShardOwnership = "TlcShardShardOwnership";

    /// <summary>
    /// The mutants of the replication companion modules (every Replication*
    /// module but the main one: ReplicationReBootstrap, ReplicationLowWatermark,
    /// ReplicationCausalDelivery), and their variant configurations.
    /// </summary>
    public const string ReBootstrap = "TlcShardReBootstrap";

    /// <summary>
    /// The mutants of every AtomicCommit* module. Its variant configurations
    /// are split into <see cref="AtomicVariantShards"/>: a variant has no control arm, so
    /// splitting them off leaves every shared control with its mutants.
    /// </summary>
    public const string Atomic = "TlcShardAtomic";

    /// <summary>The first state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants1 = "TlcShardAtomicVariants1";

    /// <summary>The second state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants2 = "TlcShardAtomicVariants2";

    /// <summary>The third state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants3 = "TlcShardAtomicVariants3";

    /// <summary>The fourth state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants4 = "TlcShardAtomicVariants4";

    /// <summary>The fifth state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants5 = "TlcShardAtomicVariants5";

    /// <summary>The sixth state-balanced group of AtomicCommit* variant configurations.</summary>
    public const string AtomicVariants6 = "TlcShardAtomicVariants6";

    /// <summary>The six categories used to partition AtomicCommit* variants.</summary>
    public static IReadOnlyList<string> AtomicVariantShards { get; } =
        [AtomicVariants1, AtomicVariants2, AtomicVariants3, AtomicVariants4, AtomicVariants5, AtomicVariants6];

    /// <summary>The mutants of every Backup* module.</summary>
    public const string Backup = "TlcShardBackup";

    /// <summary>The BPlusCascade base-model TLC run.</summary>
    public const string BPlusCascadeBase = "TlcShardBPlusCascadeBase";

    /// <summary>The BPlusCascade mutants outside the parent-accounting, recovery, separator-range, and height groups.</summary>
    public const string BPlusCascadeMutations1 = "TlcShardBPlusCascadeMutations1";

    /// <summary>The BPlusCascade parent-accounting, recovery, separator-range, and height mutants.</summary>
    public const string BPlusCascadeMutations2 = "TlcShardBPlusCascadeMutations2";

    /// <summary>Every category <see cref="Of"/> and <see cref="OfVariant"/> can return.</summary>
    public static IReadOnlyList<string> All { get; } =
        [Convergence, ReplicationWal, ShardOwnership, ReBootstrap, Atomic, .. AtomicVariantShards, Backup,
         BPlusCascadeBase, BPlusCascadeMutations1, BPlusCascadeMutations2];

    private static readonly Lazy<IReadOnlyDictionary<(string Module, string Variant), string>> AtomicVariantCategories =
        new(CreateAtomicVariantCategories, LazyThreadSafetyMode.ExecutionAndPublication);

    /// <summary>
    /// The shard category of <paramref name="mutation"/> of
    /// <paramref name="module"/>, or <see langword="null"/> to leave it in the
    /// <c>formal-tlc</c> shard.
    /// </summary>
    public static string? Of(SpecModule module, SpecMutation mutation)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentNullException.ThrowIfNull(mutation);

        var name = module.Name;
        if (string.Equals(name, "Replication", StringComparison.Ordinal))
        {
            return mutation.Name.StartsWith("EventualConvergence", StringComparison.Ordinal)
                ? Convergence
                : ReplicationWal;
        }

        if (IsReplicationCompanion(name))
        {
            return ReBootstrap;
        }

        if (name.StartsWith("Wal", StringComparison.Ordinal))
        {
            return ReplicationWal;
        }

        if (name.StartsWith("ShardOwnership", StringComparison.Ordinal))
        {
            return ShardOwnership;
        }

        if (name.StartsWith("Backup", StringComparison.Ordinal))
        {
            return Backup;
        }

        if (string.Equals(name, "BPlusCascade", StringComparison.Ordinal))
        {
            return mutation.Target is "ParentChildAccounting" or "RecoverablePending" or "SeparatorRanges" or "ReachMaxHeight"
                ? BPlusCascadeMutations2
                : BPlusCascadeMutations1;
        }

        return name.StartsWith("AtomicCommit", StringComparison.Ordinal) ? Atomic : null;
    }

    /// <summary>The shard category of the base-model case, or <see langword="null"/> to leave it in the catch-all.</summary>
    public static string? OfModule(SpecModule module)
    {
        ArgumentNullException.ThrowIfNull(module);
        return string.Equals(module.Name, "BPlusCascade", StringComparison.Ordinal)
            ? BPlusCascadeBase
            : null;
    }

    /// <summary>
    /// The shard category of variant configuration <paramref name="variant"/>
    /// of <paramref name="module"/>, or <see langword="null"/> to leave it in
    /// the <c>formal-tlc</c> shard.
    /// </summary>
    public static string? OfVariant(SpecModule module, string variant)
    {
        ArgumentNullException.ThrowIfNull(module);
        ArgumentNullException.ThrowIfNull(variant);

        if (IsReplicationCompanion(module.Name))
        {
            return ReBootstrap;
        }

        if (!module.Name.StartsWith("AtomicCommit", StringComparison.Ordinal))
        {
            return null;
        }

        return AtomicVariantCategories.Value.TryGetValue((module.Name, variant), out var category)
            ? category
            : AtomicVariants1;
    }

    private static IReadOnlyDictionary<(string Module, string Variant), string> CreateAtomicVariantCategories()
    {
        var assignments = new Dictionary<(string Module, string Variant), string>();
        var loads = new long[AtomicVariantShards.Count];
        var variants = SpecModuleCatalogue.Repository()
            .Where(module => module.Name.StartsWith("AtomicCommit", StringComparison.Ordinal))
            .SelectMany(module => module.Manifest.Variants.Select(variant =>
                (Module: module.Name, Variant: variant.Key, States: variant.Value)))
            .OrderByDescending(variant => variant.States)
            .ThenBy(variant => variant.Module, StringComparer.Ordinal)
            .ThenBy(variant => variant.Variant, StringComparer.Ordinal);

        foreach (var variant in variants)
        {
            var shard = Array.IndexOf(loads, loads.Min());
            assignments.Add((variant.Module, variant.Variant), AtomicVariantShards[shard]);
            loads[shard] += variant.States;
        }

        return assignments;
    }

    private static bool IsReplicationCompanion(string name) =>
        name.StartsWith("Replication", StringComparison.Ordinal)
        && !string.Equals(name, "Replication", StringComparison.Ordinal);

    /// <summary>
    /// Adds <see cref="Of"/>'s category to a mutation case whose arguments are
    /// <c>(SpecModule, SpecMutation)</c>, and <see cref="OfVariant"/>'s to a
    /// variant case whose arguments are <c>(SpecModule, string)</c>.
    /// </summary>
    public static TestCaseData Tag(TestCaseData data)
    {
        ArgumentNullException.ThrowIfNull(data);

        var category = data.Arguments switch
        {
            [SpecModule module] => OfModule(module),
            [SpecModule module, SpecMutation mutation, ..] => Of(module, mutation),
            [SpecModule module, string variant] => OfVariant(module, variant),
            _ => null,
        };

        if (category is not null)
        {
            data.SetCategory(category);
        }

        return data;
    }
}
