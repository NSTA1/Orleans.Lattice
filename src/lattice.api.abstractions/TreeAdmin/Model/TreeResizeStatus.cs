namespace Orleans.Lattice.Api.TreeAdmin;

/// <summary>
/// The read-only status of a tree's online resize, returned by the resize trigger
/// and undo verbs and the standalone status read. Reports whether a resize is
/// currently in flight and the tree's current B+ node capacity
/// (<see cref="CurrentMaxLeafKeys"/> / <see cref="CurrentMaxInternalChildren"/>) as
/// observed from its registry configuration, default-seeded from the cluster
/// defaults when the tree carries no explicit sizing. A pure projection with no
/// side effects.
/// <para>
/// Resize is online and self-completing (it snapshots the tree into a
/// destination physical tree with the new sizing, shadow-forwards live writes, and
/// atomically swaps the alias, then clears itself), so this status intentionally
/// surfaces the observable idle/in-flight signal and the effective node capacity
/// rather than the coordinator's internal phase machine.
/// </para>
/// </summary>
[GenerateSerializer]
[Alias(ApiTreeAdminTypeAliases.TreeResizeStatus)]
[Immutable]
public sealed record TreeResizeStatus
{
    /// <summary>The tree id whose resize status this reports.</summary>
    [Id(0)] public required string TreeId { get; init; }

    /// <summary>
    /// <see langword="true"/> when an online resize is currently in flight for the
    /// tree; <see langword="false"/> when the coordinator is idle (either no resize
    /// has ever been initiated, or the last one has run to completion).
    /// </summary>
    [Id(1)] public bool InProgress { get; init; }

    /// <summary>
    /// The tree's current maximum number of keys per leaf node, as observed from its
    /// registry configuration (default-seeded from the cluster default when the tree
    /// carries no explicit sizing).
    /// </summary>
    [Id(2)] public int CurrentMaxLeafKeys { get; init; }

    /// <summary>
    /// The tree's current maximum number of children per internal node, as observed
    /// from its registry configuration (default-seeded from the cluster default when
    /// the tree carries no explicit sizing).
    /// </summary>
    [Id(3)] public int CurrentMaxInternalChildren { get; init; }

    /// <summary>
    /// The target maximum keys per leaf node requested by the resize trigger that
    /// produced this status, or <see langword="null"/> for a standalone status read
    /// or an undo (the coordinator's in-flight target is not publicly surfaced).
    /// </summary>
    [Id(4)] public int? RequestedMaxLeafKeys { get; init; }

    /// <summary>
    /// The target maximum children per internal node requested by the resize trigger
    /// that produced this status, or <see langword="null"/> for a standalone status
    /// read or an undo.
    /// </summary>
    [Id(5)] public int? RequestedMaxInternalChildren { get; init; }

    /// <summary>
    /// <see langword="true"/> while an undo of the tree's resize has been accepted
    /// and is still unwinding. Undo is accept-then-poll: the undo verb returns once
    /// the intent is persisted (waiting only a bounded time for the unwind), so this
    /// flag is how a caller tells an accepted, still-unwinding undo apart from a
    /// resize that is simply running. Read it before <see cref="InProgress"/>:
    /// this <see langword="true"/> means an undo was requested and is unwinding
    /// (whatever <see cref="InProgress"/> says - an undo of an already completed
    /// resize leaves it <see langword="false"/>); otherwise
    /// <see cref="InProgress"/> <see langword="true"/> means a resize is running and
    /// <see langword="false"/> means there is none in flight.
    /// </summary>
    [Id(6)] public bool UndoRequested { get; init; }
}
