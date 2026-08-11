using Orleans.Lattice.Api.Replication;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Pure projections from the replication control facade's domain models onto the
/// compact MCP structured-content DTOs the replication tools return, plus the
/// merge-mode name parsing an enable tool needs. Kept side-effect free and
/// allocation-lean so a tool invocation maps a facade result without any I/O of
/// its own.
/// </summary>
internal static class ReplicationToolMappings
{
    /// <summary>Projects a single per-tree config entry onto its compact MCP DTO.</summary>
    /// <param name="entry">The config entry to project. Must not be <c>null</c>.</param>
    /// <returns>The compact per-tree config projection.</returns>
    public static McpReplicationTreeConfig ToMcp(ReplicationTreeConfigEntry entry)
    {
        ArgumentNullException.ThrowIfNull(entry);
        return new McpReplicationTreeConfig
        {
            TreeId = entry.TreeId,
            Enabled = entry.Enabled,
            Mode = entry.Mode?.ToString(),
            Ambiguous = entry.Ambiguous,
        };
    }

    /// <summary>Projects the permission-scoped config report onto its MCP DTO.</summary>
    /// <param name="report">The config report. Must not be <c>null</c>.</param>
    /// <returns>The MCP config DTO.</returns>
    public static McpReplicationConfig ToMcp(ReplicationConfigReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        var trees = new McpReplicationTreeConfig[report.Trees.Count];
        for (var i = 0; i < report.Trees.Count; i++)
        {
            trees[i] = ToMcp(report.Trees[i]);
        }

        return new McpReplicationConfig { Trees = trees };
    }

    /// <summary>Projects an enable result onto its MCP DTO.</summary>
    /// <param name="result">The enable result. Must not be <c>null</c>.</param>
    /// <returns>The MCP enable-result DTO.</returns>
    public static McpReplicationEnableResult ToMcp(ReplicationEnableResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new McpReplicationEnableResult
        {
            TreeId = result.TreeId,
            Mode = result.Mode.ToString(),
            AlreadyEnabled = result.AlreadyEnabled,
            BootstrapRequested = result.BootstrapRequested,
        };
    }

    /// <summary>Projects a disable result onto its MCP DTO.</summary>
    /// <param name="result">The disable result. Must not be <c>null</c>.</param>
    /// <returns>The MCP disable-result DTO.</returns>
    public static McpReplicationDisableResult ToMcp(ReplicationDisableResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new McpReplicationDisableResult
        {
            TreeId = result.TreeId,
            AlreadyDisabled = result.AlreadyDisabled,
        };
    }

    /// <summary>
    /// Parses a merge-mode name (for example <c>OrSet</c>, case-insensitive) into
    /// its <see cref="LatticeMergeMode"/>. The mode is required when enabling a
    /// tree - it is fixed at enable time and matches the tree's CRDT semantics -
    /// so a <c>null</c>, empty, or unrecognised value is rejected rather than
    /// defaulted.
    /// </summary>
    /// <param name="mode">The merge-mode name.</param>
    /// <returns>The parsed merge mode.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="mode"/> is <c>null</c>, empty, or not a recognised merge
    /// mode.
    /// </exception>
    public static LatticeMergeMode ToMergeMode(string? mode)
    {
        if (string.IsNullOrEmpty(mode))
        {
            throw new ArgumentException(
                "A merge mode is required to enable replication for a tree. Expected one of: "
                + "LwwRegister, OrSet, PnCounter, VersionVector, MvRegister, OrMap, Sequence, OrFlag, RwFlag, RwSet.",
                nameof(mode));
        }

        if (Enum.TryParse<LatticeMergeMode>(mode, ignoreCase: true, out var parsed)
            && Enum.IsDefined(parsed))
        {
            return parsed;
        }

        throw new ArgumentException(
            $"Unrecognised merge mode '{mode}'. Expected one of: LwwRegister, OrSet, PnCounter, "
            + "VersionVector, MvRegister, OrMap, Sequence, OrFlag, RwFlag, RwSet.",
            nameof(mode));
    }
}
