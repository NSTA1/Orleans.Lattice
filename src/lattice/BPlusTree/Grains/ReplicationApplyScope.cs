namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Marks the current asynchronous flow as a replication apply (issue #4593).
/// While it is marked, every routing resolution on <see cref="LatticeGrain"/>
/// checks that the resolved physical copy's receive fence is open, so a
/// replicated write can never land on a restored copy a coordinated restore
/// still holds closed.
/// <para>
/// The mark lives in an <see cref="AsyncLocal{T}"/> rather than on the grain:
/// <see cref="LatticeGrain"/> is a stateless worker whose calls interleave, so an
/// instance field would leak the mark into an unrelated call. Call
/// <see cref="Enter"/> only from inside an <see langword="async"/> method: the
/// method's own execution context carries the mark, and it is discarded when the
/// method returns, so it never leaks into the caller's flow.
/// </para>
/// </summary>
internal static class ReplicationApplyScope
{
    private static readonly AsyncLocal<bool> Active = new();

    private static readonly AsyncLocal<bool> BypassBootstrapShadow = new();

    /// <summary>Whether the current flow is a replication apply.</summary>
    internal static bool IsActive => Active.Value;

    /// <summary>
    /// Whether the current replication apply targets the original (old)
    /// physical tree even while a bootstrap hold routes applies to its held
    /// shadow copy: a prepare's mirror (issue #4791), or any committed apply
    /// (issue #5694, <see cref="EnterCommitted"/>).
    /// </summary>
    internal static bool IsBootstrapShadowBypassed => BypassBootstrapShadow.Value;

    /// <summary>Marks the current flow as a replication apply.</summary>
    internal static void Enter() => Active.Value = true;

    /// <summary>
    /// Marks the current replication apply as targeting the original physical
    /// tree rather than a held bootstrap shadow copy. Same lifetime rule as
    /// <see cref="Enter"/>: call it only from inside an <see langword="async"/>
    /// method, whose return discards the mark.
    /// </summary>
    internal static void EnterBootstrapShadowBypass() => BypassBootstrapShadow.Value = true;

    /// <summary>
    /// Marks the current flow as a committed (non-prepared) replication apply,
    /// which targets the original physical tree during a bootstrap hold (issue
    /// #5694). The original's resize mirror forwards the write to the held copy,
    /// as it does a local write, so the write reaches both trees: an abort that
    /// discards the copy keeps it, and a cutover publishes it. Routed to the
    /// held copy alone, it was lost with an aborted copy, because the sender's
    /// cursor and the receiver's live dedupe cache already count it delivered.
    /// A bootstrap drain's own rows still reach the copy, because the drain's
    /// route scope (<see cref="LatticeBootstrapShadowRouteContext"/>) takes
    /// precedence. Same lifetime rule as <see cref="Enter"/>.
    /// </summary>
    internal static void EnterCommitted()
    {
        Active.Value = true;
        BypassBootstrapShadow.Value = true;
    }
}
