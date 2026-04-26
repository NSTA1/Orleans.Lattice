namespace Orleans.Lattice.Replication;

/// <summary>
/// Centralised Orleans serialization alias constants for every type
/// that participates in the replication wire format. Each alias is a
/// short, fixed string that provides a stable wire-format identity
/// independent of CLR type names. Replication aliases use the
/// <c>olr.</c> prefix to avoid collision with core <c>Orleans.Lattice</c>
/// aliases (which use <c>ol.</c>).
/// </summary>
internal static class ReplicationTypeAliases
{
    /// <summary>Alias for <see cref="ReplogEntry"/>.</summary>
    internal const string ReplogEntry = "olr.re";

    /// <summary>Alias for <see cref="ReplogOp"/>.</summary>
    internal const string ReplogOp = "olr.ro";

    /// <summary>Alias for <see cref="Replication.ReplicationMode"/>.</summary>
    internal const string ReplicationMode = "olr.rm";

    // Per-shard write-ahead-log types

    /// <summary>Alias for the per-shard WAL grain interface.</summary>
    internal const string IReplogShardGrain = "olr.gw";

    /// <summary>Alias for the per-shard WAL persistent state class.</summary>
    internal const string ReplogShardState = "olr.ws";

    /// <summary>Alias for a single sequenced entry returned from a WAL read.</summary>
    internal const string ReplogShardEntry = "olr.we";

    /// <summary>Alias for a paged WAL read result.</summary>
    internal const string ReplogShardPage = "olr.wp";

    // Per-origin high-water-mark types

    /// <summary>Alias for the per-origin HWM grain interface.</summary>
    internal const string IReplicationHighWaterMarkGrain = "olr.gh";

    /// <summary>Alias for the per-origin HWM persistent state class.</summary>
    internal const string ReplicationHighWaterMarkState = "olr.hs";

    // Inbound apply pipeline

    /// <summary>Alias for the apply-result return value.</summary>
    internal const string ApplyResult = "olr.ar";

    // Typed CRDT deltas (commit-time wire payloads for replicable primitives)

    /// <summary>Alias for <see cref="LwwRegisterDelta"/>.</summary>
    internal const string LwwRegisterDelta = "olr.ld";

    /// <summary>Alias for <see cref="OrSetDelta"/>.</summary>
    internal const string OrSetDelta = "olr.od";

    /// <summary>Alias for <see cref="OrSetDot"/>.</summary>
    internal const string OrSetDot = "olr.dt";

    /// <summary>Alias for <see cref="PnCounterDelta"/>.</summary>
    internal const string PnCounterDelta = "olr.pd";

    /// <summary>Alias for <see cref="VersionVectorDelta"/>.</summary>
    internal const string VersionVectorDelta = "olr.vd";

    // WAL storage abstraction and transport-side resume token

    /// <summary>Alias for <see cref="WalEntry"/>.</summary>
    internal const string WalEntry = "olr.w2";

    /// <summary>Alias for <see cref="WalResumeToken"/>.</summary>
    internal const string WalResumeToken = "olr.wt";
}
