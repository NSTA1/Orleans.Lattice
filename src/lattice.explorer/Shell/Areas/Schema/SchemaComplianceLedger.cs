using System.Collections.Concurrent;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Shell.Areas.Schema;

/// <summary>
/// The compliance scans run in this circuit, by tree: the directory's compliance
/// summary. A scan reads every value of a tree, so the directory never starts one
/// on its own; it shows the last result it has, or none.
/// </summary>
internal sealed class SchemaComplianceLedger
{
    private readonly ConcurrentDictionary<string, SchemaComplianceResult> _results = new(StringComparer.Ordinal);

    /// <summary>Raised after a result is recorded, with the tree id.</summary>
    public event Action<string>? Recorded;

    /// <summary>Records the result of a scan of <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="report">The scan report.</param>
    /// <param name="scannedAt">When the scan finished.</param>
    public void Record(string treeId, LatticeSchemaComplianceReport report, DateTimeOffset scannedAt)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        _results[treeId] = new SchemaComplianceResult(report, scannedAt);
        Recorded?.Invoke(treeId);
    }

    /// <summary>The last scan of <paramref name="treeId"/> in this circuit, or <see langword="null"/>.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <returns>The result.</returns>
    public SchemaComplianceResult? Find(string treeId) =>
        _results.TryGetValue(treeId, out var result) ? result : null;

    /// <summary>Forgets the result for <paramref name="treeId"/>, such as after its policy changed.</summary>
    /// <param name="treeId">The logical tree id.</param>
    public void Forget(string treeId) => _results.TryRemove(treeId, out _);
}
