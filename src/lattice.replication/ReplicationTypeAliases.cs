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
}
