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
/// runs whether or not anybody adds it here. Each group was sized from the
/// per-case durations of a measured run to about 30-60 test-minutes, which the
/// harness runs about four cases at a time, so 8-16 minutes of wall-clock on a
/// 4-core runner. Re-measure from a run's <c>formal-tlc*</c> TRX files when a
/// shard drifts past 20.
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

    /// <summary>The ReplicationReBootstrap mutants.</summary>
    public const string ReBootstrap = "TlcShardReBootstrap";

    /// <summary>The mutants and variant configurations of every AtomicCommit* module.</summary>
    public const string Atomic = "TlcShardAtomic";

    /// <summary>The mutants of every Backup* module.</summary>
    public const string Backup = "TlcShardBackup";

    /// <summary>Every category <see cref="Of"/> and <see cref="OfVariant"/> can return.</summary>
    public static IReadOnlyList<string> All { get; } = [Convergence, ReplicationWal, ShardOwnership, ReBootstrap, Atomic, Backup];

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

        if (string.Equals(name, "ReplicationReBootstrap", StringComparison.Ordinal))
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

        return name.StartsWith("AtomicCommit", StringComparison.Ordinal) ? Atomic : null;
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

        return module.Name.StartsWith("AtomicCommit", StringComparison.Ordinal) ? Atomic : null;
    }

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
