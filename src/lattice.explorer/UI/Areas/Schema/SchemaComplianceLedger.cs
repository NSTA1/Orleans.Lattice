using System.Collections.Concurrent;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.UI.Areas.Schema;

/// <summary>
/// The compliance scans run in this circuit, by tree: the directory's compliance
/// summary. A scan reads every value of a tree, so the directory never starts one
/// on its own; it shows the last result it has, or none.
/// </summary>
/// <remarks>
/// The results belong to the caller who ran the scans (the sign-in, the endpoint
/// and the asserted tenant): when any of them changes every result is forgotten,
/// so one caller's scan is never shown to another.
/// </remarks>
/// <param name="caller">The circuit's caller, or <see langword="null"/> for a host with no sessions.</param>
internal sealed class SchemaComplianceLedger(ShellCaller? caller = null)
{
    private readonly ShellCaller _caller = caller ?? new ShellCaller();
    private readonly ConcurrentDictionary<string, SchemaComplianceResult> _results = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();
    private ShellCallerKey _resultsFor;

    /// <summary>Raised after a result is recorded, with the tree id.</summary>
    public event Action<string>? Recorded;

    /// <summary>Records the result of a scan of <paramref name="treeId"/>.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <param name="report">The scan report.</param>
    /// <param name="scannedAt">When the scan finished.</param>
    public void Record(string treeId, LatticeSchemaComplianceReport report, DateTimeOffset scannedAt)
    {
        ArgumentException.ThrowIfNullOrEmpty(treeId);
        ForgetIfTheCallerChanged();
        _results[treeId] = new SchemaComplianceResult(report, scannedAt);
        Recorded?.Invoke(treeId);
    }

    /// <summary>The last scan of <paramref name="treeId"/> in this circuit, or <see langword="null"/>.</summary>
    /// <param name="treeId">The logical tree id.</param>
    /// <returns>The result.</returns>
    public SchemaComplianceResult? Find(string treeId)
    {
        ForgetIfTheCallerChanged();
        return _results.TryGetValue(treeId, out var result) ? result : null;
    }

    /// <summary>Forgets the result for <paramref name="treeId"/>, such as after its policy changed.</summary>
    /// <param name="treeId">The logical tree id.</param>
    public void Forget(string treeId) => _results.TryRemove(treeId, out _);

    private void ForgetIfTheCallerChanged()
    {
        var current = _caller.Current;
        lock (_gate)
        {
            if (_resultsFor != current)
            {
                _results.Clear();
                _resultsFor = current;
            }
        }
    }
}
