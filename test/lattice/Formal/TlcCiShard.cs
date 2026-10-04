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
/// runs whether or not anybody adds it here. Each group was sized from measured
/// per-case durations to about 4-5 minutes of wall-clock on a 4-core runner.
/// </para>
/// <para>
/// SHARE A CONTROL WHERE YOU CAN. <see cref="TlcModelCheckTests"/> runs each
/// distinct control arm once per test process, so mutants that share a
/// control (the same module, target property and options) are cheaper in one
/// shard than split across two: every Replication EventualConvergence mutant
/// shares one control and so lives in <see cref="Convergence"/>.
/// </para>
/// </summary>
internal static class TlcCiShard
{
    /// <summary>The Replication EventualConvergence* (temporal) mutants, which share one control arm.</summary>
    public const string Convergence = "TlcShardConvergence";

    /// <summary>The other Replication mutants, plus the WalDurability and WalMove mutants.</summary>
    public const string ReplicationWal = "TlcShardReplicationWal";

    /// <summary>The ShardOwnership and ShardOwnershipRetention mutants.</summary>
    public const string ShardOwnership = "TlcShardShardOwnership";

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

        if (name.StartsWith("Wal", StringComparison.Ordinal))
        {
            return ReplicationWal;
        }

        return name.StartsWith("ShardOwnership", StringComparison.Ordinal) ? ShardOwnership : null;
    }

    /// <summary>
    /// Adds <see cref="Of"/>'s category to a mutation case whose arguments are
    /// <c>(SpecModule, SpecMutation)</c>.
    /// </summary>
    public static TestCaseData Tag(TestCaseData data)
    {
        ArgumentNullException.ThrowIfNull(data);

        if (data.Arguments is [SpecModule module, SpecMutation mutation, ..] && Of(module, mutation) is { } category)
        {
            data.SetCategory(category);
        }

        return data;
    }
}
