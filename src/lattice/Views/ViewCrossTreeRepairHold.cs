namespace Orleans.Lattice.Views;

/// <summary>
/// Durable ownership of a submitted joint slice until its repair and checkpoint
/// retirement complete. The source log and view generation cannot rebind while
/// this record exists.
/// </summary>
[GenerateSerializer]
[Alias(TypeAliases.ViewCrossTreeRepairHold)]
internal sealed class ViewCrossTreeRepairHold
{
    [Id(0)] public Guid TransactionId { get; set; }
    [Id(1)] public string WalTreeId { get; set; } = string.Empty;
    [Id(2)] public required ViewCrossTreeReadiness Original { get; set; }
    [Id(3)] public bool RepairApplied { get; set; }
    [Id(4)] public Dictionary<int, long> RetireThrough { get; set; } = new();
    [Id(5)] public bool OriginalApplied { get; set; }
    [Id(6)] public Dictionary<string, HybridLogicalClock> RepairSourceVersions { get; set; } = new(StringComparer.Ordinal);
    [Id(7)] public string ProjectionVersion { get; set; } = string.Empty;
    [Id(8)] public string SourceTreeId { get; set; } = string.Empty;
}
