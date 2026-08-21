namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The read-only status of a single materialised view, returned by the view status
/// read and the view rebuild verb. Reports the view's source tree, its apply lag
/// (committed-but-unapplied source WAL entries), and the id of the view tree
/// currently serving reads. A pure projection with no side effects.
/// <para>
/// A materialised view tails its source tree's WAL and projects each mutation into a
/// backing <c>view-{name}</c> tree; a rebuild atomically shadow-swaps a freshly built
/// generation in, so the active view tree id changes across a rebuild. The lag is the
/// number of committed source entries the view has not yet applied - zero means the
/// view has caught up to the source head as of this read.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeViewStatus)]
[Immutable]
public sealed record TreeViewStatus
{
    /// <summary>The logical view name this status reports.</summary>
    [Id(0)] public required string ViewName { get; init; }

    /// <summary>The source tree id the view is derived from and authorized against.</summary>
    [Id(1)] public required string SourceTreeId { get; init; }

    /// <summary>Whether the view is an aggregation (grouped reduce) view.</summary>
    [Id(2)] public bool IsAggregation { get; init; }

    /// <summary>
    /// The view's apply lag: the number of committed-but-unapplied source WAL entries
    /// summed across every source partition. Zero means the view has caught up to the
    /// source head as of this read.
    /// </summary>
    [Id(3)] public long ApplyLag { get; init; }

    /// <summary>
    /// The grain id of the view tree currently serving reads: the generation-addressed
    /// id for the durable active generation. Changes across a rebuild's shadow-swap.
    /// </summary>
    [Id(4)] public string ActiveTreeId { get; init; } = string.Empty;

    /// <summary>
    /// The host-registered runtime projection provider key, or <see langword="null"/>
    /// when unavailable for a startup-only or legacy registration.
    /// </summary>
    [Id(5)] public string? ProviderKey { get; init; }

    /// <summary>
    /// The projection version derived by the server, or <see langword="null"/> when
    /// unavailable.
    /// </summary>
    [Id(6)] public string? ProjectionVersion { get; init; }
}
