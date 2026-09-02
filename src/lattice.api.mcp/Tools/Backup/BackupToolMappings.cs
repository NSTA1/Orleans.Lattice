using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Backup;

namespace Orleans.Lattice.Api.Mcp;

/// <summary>
/// Pure projections from the backup control facade's domain models onto the
/// compact MCP structured-content DTOs the backup tools return. Kept side-effect
/// free and allocation-lean so a tool invocation maps a facade result without any
/// I/O of its own.
/// </summary>
internal static class BackupToolMappings
{
    /// <summary>Projects a <see cref="BackupManifest"/> onto its compact MCP DTO.</summary>
    /// <param name="manifest">The manifest to project. Must not be <c>null</c>.</param>
    /// <returns>The compact manifest projection.</returns>
    public static McpBackupManifest ToMcp(BackupManifest manifest)
    {
        ArgumentNullException.ThrowIfNull(manifest);
        return new McpBackupManifest
        {
            Id = manifest.Id,
            Name = manifest.Name,
            CreatedAtUtc = manifest.CreatedAtUtc,
            Kind = manifest.Kind.ToString(),
            TreeId = manifest.Scope.TreeId,
            ScopeKind = manifest.Scope.Kind.ToString(),
            KeyOrPrefix = manifest.Scope.KeyOrPrefix,
            BaseBackupId = manifest.BaseBackupId,
            ArtifactCount = manifest.ContentDescriptors.Count,
            SetId = manifest.SetId,
            SetName = manifest.SetName,
            CapturingClusterId = manifest.CapturingClusterId,
        };
    }

    /// <summary>Projects a capture result onto its MCP DTO.</summary>
    /// <param name="result">The capture result. Must not be <c>null</c>.</param>
    /// <returns>The MCP capture-result DTO.</returns>
    public static McpBackupCaptureResult ToMcp(LatticeBackupCaptureResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new McpBackupCaptureResult
        {
            BackupId = result.BackupId,
            Manifest = ToMcp(result.Manifest),
        };
    }

    /// <summary>Projects one page of the backup catalog onto its MCP DTO.</summary>
    /// <param name="page">The catalog page. Must not be <c>null</c>.</param>
    /// <returns>The MCP catalog-page DTO.</returns>
    public static McpBackupCatalogPage ToMcp(BackupCatalogPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        var entries = new McpBackupManifest[page.Entries.Count];
        for (var i = 0; i < page.Entries.Count; i++)
        {
            entries[i] = ToMcp(page.Entries[i]);
        }

        return new McpBackupCatalogPage
        {
            Entries = entries,
            NextPageToken = page.NextPageToken,
        };
    }

    /// <summary>
    /// Projects a chain description (or its absence) onto its MCP DTO. A
    /// <see langword="null"/> <paramref name="description"/> maps to a
    /// not-found result.
    /// </summary>
    /// <param name="description">The chain description, or <c>null</c> when the backup was not found.</param>
    /// <returns>The MCP chain DTO.</returns>
    public static McpBackupChain ToMcp(BackupChainDescription? description)
    {
        if (description is null)
        {
            return new McpBackupChain { Found = false };
        }

        return new McpBackupChain
        {
            Found = true,
            Manifest = ToMcp(description.Manifest),
            ChainBackupIds = description.ChainBackupIds,
            Artifacts = ToMcpArtifacts(description.Manifest),
        };
    }

    /// <summary>
    /// Projects a manifest's content descriptors onto their compact MCP artifact
    /// DTOs, exposing the artifact ids that drive <c>export_artifact</c>.
    /// </summary>
    /// <param name="manifest">The manifest whose artifacts to project.</param>
    /// <returns>The per-artifact projections, in manifest order.</returns>
    private static IReadOnlyList<McpBackupArtifact> ToMcpArtifacts(BackupManifest manifest)
    {
        var descriptors = manifest.ContentDescriptors;
        if (descriptors.Count == 0)
        {
            return Array.Empty<McpBackupArtifact>();
        }

        var artifacts = new McpBackupArtifact[descriptors.Count];
        for (var i = 0; i < descriptors.Count; i++)
        {
            var descriptor = descriptors[i];
            artifacts[i] = new McpBackupArtifact
            {
                ArtifactId = descriptor.ArtifactId,
                ContentHash = descriptor.ContentHash,
                ByteLength = descriptor.ByteLength,
                ChunkCount = descriptor.ChunkCount,
            };
        }

        return artifacts;
    }

    /// <summary>Projects an inventory report onto its MCP DTO.</summary>
    /// <param name="report">The inventory report. Must not be <c>null</c>.</param>
    /// <returns>The MCP inventory DTO.</returns>
    public static McpBackupInventory ToMcp(BackupInventoryReport report)
    {
        ArgumentNullException.ThrowIfNull(report);
        return new McpBackupInventory
        {
            TotalBackupCount = report.TotalBackupCount,
            TotalCatalogBytes = report.TotalCatalogBytes,
            FullBackupCount = report.FullBackupCount,
            IncrementalBackupCount = report.IncrementalBackupCount,
            OldestBackupUtc = report.OldestBackupUtc,
            NewestBackupUtc = report.NewestBackupUtc,
            CaptureFailureCount = report.CaptureFailureCount,
            RestoreFailureCount = report.RestoreFailureCount,
            BytesReclaimed = report.BytesReclaimed,
        };
    }

    /// <summary>
    /// Projects a scope status (or its absence) onto its MCP DTO. A
    /// <see langword="null"/> <paramref name="status"/> maps to a not-found
    /// result.
    /// </summary>
    /// <param name="status">The scope status, or <c>null</c> when the scope is unknown.</param>
    /// <returns>The MCP scope-status DTO.</returns>
    public static McpBackupScopeStatus ToMcp(BackupScopeStatus? status)
    {
        if (status is null)
        {
            return new McpBackupScopeStatus { Found = false };
        }

        return new McpBackupScopeStatus
        {
            Found = true,
            TreeId = status.Scope.TreeId,
            ScopeKind = status.Scope.Kind.ToString(),
            KeyOrPrefix = status.Scope.KeyOrPrefix,
            FullScheduleRegistered = status.FullScheduleRegistered,
            IncrementalScheduleRegistered = status.IncrementalScheduleRegistered,
            LastFullRunUtc = status.LastFullRunUtc,
            LastFullSuccessUtc = status.LastFullSuccessUtc,
            LastIncrementalRunUtc = status.LastIncrementalRunUtc,
            LastIncrementalSuccessUtc = status.LastIncrementalSuccessUtc,
            LastRunOutcome = status.LastRunOutcome.ToString(),
            ChainDepth = status.ChainDepth,
        };
    }

    /// <summary>Projects a restore result onto its MCP DTO.</summary>
    /// <param name="result">The restore result. Must not be <c>null</c>.</param>
    /// <returns>The MCP restore-result DTO.</returns>
    public static McpRestoreResult ToMcp(LatticeRestoreResult result)
    {
        ArgumentNullException.ThrowIfNull(result);
        return new McpRestoreResult
        {
            BackupId = result.BackupId,
            TargetTreeId = result.TargetTreeId,
            Mode = result.Mode.ToString(),
            OperationId = result.OperationId,
            ManifestChain = result.ManifestChain,
            EntriesApplied = result.EntriesApplied,
            ShadowPhysicalTreeId = result.ShadowPhysicalTreeId,
            PreviousPhysicalTreeId = result.PreviousPhysicalTreeId,
        };
    }

    /// <summary>
    /// Rebuilds a <see cref="BackupScopeSelector"/> from the three wire fields an
    /// MCP tool accepts: the tree id, the scope kind name, and the optional key
    /// or prefix.
    /// </summary>
    /// <param name="treeId">The tree id. Must not be <c>null</c> or empty.</param>
    /// <param name="scopeKind">
    /// The scope kind name (<c>WholeTree</c>, <c>Prefix</c>, or <c>Key</c>,
    /// case-insensitive). Defaults to <c>WholeTree</c> when <c>null</c> or empty.
    /// </param>
    /// <param name="keyOrPrefix">The key or prefix for a non-whole-tree scope.</param>
    /// <returns>The reconstructed scope selector.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="scopeKind"/> is not a recognised kind, or the arguments
    /// are inconsistent with the kind.
    /// </exception>
    public static BackupScopeSelector ToScope(string treeId, string? scopeKind, string? keyOrPrefix)
    {
        var kind = ParseScopeKind(scopeKind);
        return kind switch
        {
            BackupScopeKind.WholeTree => BackupScopeSelector.WholeTree(treeId),
            BackupScopeKind.Prefix => BackupScopeSelector.Prefix(treeId, keyOrPrefix ?? string.Empty),
            BackupScopeKind.Key => BackupScopeSelector.Key(treeId, keyOrPrefix ?? string.Empty),
            _ => BackupScopeSelector.WholeTree(treeId),
        };
    }

    private static BackupScopeKind ParseScopeKind(string? scopeKind)
    {
        if (string.IsNullOrEmpty(scopeKind))
        {
            return BackupScopeKind.WholeTree;
        }

        if (Enum.TryParse<BackupScopeKind>(scopeKind, ignoreCase: true, out var kind)
            && Enum.IsDefined(kind))
        {
            return kind;
        }

        throw new ArgumentException(
            $"Unrecognised backup scope kind '{scopeKind}'. Expected WholeTree, Prefix, or Key.",
            nameof(scopeKind));
    }

    /// <summary>
    /// Parses a restore mode name (<c>InPlace</c> or <c>ShadowCutover</c>,
    /// case-insensitive) into its enum, defaulting to
    /// <see cref="LatticeRestoreMode.InPlace"/> when <c>null</c> or empty.
    /// </summary>
    /// <param name="mode">The restore mode name.</param>
    /// <returns>The parsed restore mode.</returns>
    /// <exception cref="ArgumentException"><paramref name="mode"/> is not a recognised mode.</exception>
    public static LatticeRestoreMode ToRestoreMode(string? mode)
    {
        if (string.IsNullOrEmpty(mode))
        {
            return LatticeRestoreMode.InPlace;
        }

        if (Enum.TryParse<LatticeRestoreMode>(mode, ignoreCase: true, out var parsed)
            && Enum.IsDefined(parsed))
        {
            return parsed;
        }

        throw new ArgumentException(
            $"Unrecognised restore mode '{mode}'. Expected InPlace or ShadowCutover.",
            nameof(mode));
    }
}
