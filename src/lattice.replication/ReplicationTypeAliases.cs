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
}
