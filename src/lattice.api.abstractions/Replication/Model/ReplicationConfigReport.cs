using System.Collections.Immutable;

namespace Orleans.Lattice.Api.Replication;

/// <summary>
/// An operator-facing report of the runtime replicated-tree set: one
/// <see cref="ReplicationTreeConfigEntry"/> per configured tree the caller is
/// authorized to see. Returned by
/// <see cref="ILatticeReplicationControl.GetReplicationConfigAsync"/>.
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
    /// <c>null</c>; an empty list means no configured tree is visible to the
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
    /// see no configured tree.
    /// </summary>
    [Id(0)] public IReadOnlyList<ReplicationTreeConfigEntry> Trees { get; init; }

    /// <summary>An empty report - no configured tree is visible to the caller.</summary>
    public static ReplicationConfigReport Empty { get; } =
        new(ImmutableArray<ReplicationTreeConfigEntry>.Empty);
}
