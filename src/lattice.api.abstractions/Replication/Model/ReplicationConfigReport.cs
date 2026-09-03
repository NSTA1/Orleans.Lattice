using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// An operator-facing report of the effective replicated-tree set: one
/// <see cref="ReplicationTreeConfigEntry"/> per enrolled tree the caller is
/// authorized to see. Returned by
/// <see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/>.
/// <para>
/// The report reconciles <b>both</b> enrollment sources a replication-enabled
/// host resolves against - runtime enables authored into the config tree and the
/// static deployment-time replicated-tree map that acts as a fallback floor - so
/// a purely statically configured estate is reported as replicating rather than
/// as empty. Each entry's
/// <see cref="ReplicationTreeConfigEntry.Source"/> names which one is in force.
/// </para>
/// <para>
/// The report is <b>permission-scoped</b>: it includes only trees the caller
/// holds the replication authority over (fail-closed discovery), so it never
/// reveals the existence of a tree the caller may not manage.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ApiReplicationTypeAliases.ReplicationConfigReport)]
[Immutable]
public sealed record ReplicationConfigReport
{
    /// <summary>Initializes a new <see cref="ReplicationConfigReport"/>.</summary>
    /// <param name="trees">
    /// The per-tree config entries the caller is authorized to see. Must not be
    /// <c>null</c>; an empty list means no enrolled tree is visible to the
    /// caller.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="trees"/> is <c>null</c>.</exception>
    public ReplicationConfigReport(IReadOnlyList<ReplicationTreeConfigEntry> trees)
    {
        ArgumentNullException.ThrowIfNull(trees);
        Trees = trees;
    }

    /// <summary>
    /// The per-tree replication config entries visible to the caller, in the
    /// order the facade produced them. Empty when the caller is authorized to
    /// see no enrolled tree.
    /// </summary>
    [Id(0)] public IReadOnlyList<ReplicationTreeConfigEntry> Trees { get; init; }

    /// <summary>An empty report - no enrolled tree is visible to the caller.</summary>
    public static ReplicationConfigReport Empty { get; } =
        new(ImmutableArray<ReplicationTreeConfigEntry>.Empty);
}
