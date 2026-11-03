namespace Orleans.Lattice.Backup;

/// <summary>One claim on a bare backup id, held by a publish of one scope.</summary>
[GenerateSerializer]
[Alias(BackupTypeAliases.BackupIdPendingClaim)]
internal sealed class BackupIdPendingClaim
{
    /// <summary>The claiming scope's key.</summary>
    [Id(0)]
    public string ScopeKey { get; set; } = string.Empty;

    /// <summary>When the claim lapses if its publish never releases it.</summary>
    [Id(1)]
    public DateTimeOffset Until { get; set; }
}
