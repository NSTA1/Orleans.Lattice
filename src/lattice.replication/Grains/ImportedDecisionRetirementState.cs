namespace Orleans.Lattice.Replication.Grains;

/// <summary>Persisted state of <see cref="IImportedDecisionRetirementGrain"/>.</summary>
[GenerateSerializer]
[Alias(ReplicationTypeAliases.ImportedDecisionRetirementState)]
internal sealed class ImportedDecisionRetirementState
{
    /// <summary>Per source cluster, the imported decision rows still retained.</summary>
    [Id(0)]
    public Dictionary<string, ImportedDecisionSet> Sources { get; set; } = new(StringComparer.Ordinal);
}
