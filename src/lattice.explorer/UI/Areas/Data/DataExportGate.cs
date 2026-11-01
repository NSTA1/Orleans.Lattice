using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Decides, fail-closed, whether the caller may export a tree, so the Export action is
/// drawn only for a caller who can read some of its keys. It asks the Data API once per
/// tree per caller for a one-entry range read with gate coverage: a whole or narrowed
/// range-read grant shows the action; a denial, a tree the caller cannot observe, no Data
/// API, or any fault hides it. The download is still authorized by the facade, per key.
/// </summary>
/// <remarks>
/// Only trees are exported: a view or a shared prefix is not offered. The answers belong
/// to the caller they were asked for (the sign-in, the endpoint and the asserted tenant),
/// and are forgotten when any of them changes.
/// </remarks>
internal sealed class DataExportGate
{
    private readonly IDataExportSources? _sources;
    private readonly ShellCaller _caller;
    private readonly Dictionary<string, Task<bool>> _answersByTree = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();
    private ShellCallerKey _answersFor;

    /// <summary>Creates the gate.</summary>
    /// <param name="services">The circuit's services.</param>
    public DataExportGate(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _sources = services.GetService(typeof(IDataExportSources)) as IDataExportSources;
        _caller = ShellCaller.Of(services);
    }

    /// <summary>Whether the caller may export <paramref name="tree"/>.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    public Task<bool> CanExportAsync(DataTreeEntry tree, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        if (tree.Kind != DataTreeKind.Tree || _sources is null)
        {
            return Task.FromResult(false);
        }

        var caller = _caller.Current;
        Task<bool> verdict;
        lock (_gate)
        {
            if (_answersFor != caller)
            {
                _answersByTree.Clear();
                _answersFor = caller;
            }

            if (!_answersByTree.TryGetValue(tree.LogicalId, out verdict!))
            {
                verdict = ProbeAsync(_sources, tree.LogicalId);
                _answersByTree.Add(tree.LogicalId, verdict);
            }
        }

        return verdict.WaitAsync(cancellationToken);
    }

    private static async Task<bool> ProbeAsync(IDataExportSources sources, string treeId)
    {
        try
        {
            using var source = sources.OpenForCircuit();
            var page = await source.ReadAsync(
                new DataRangeRequest { TreeId = treeId, PageSize = 1, IncludeGateCoverage = true },
                CancellationToken.None).ConfigureAwait(false);
            return page.GateCoverage is LatticeRangeReadGateCoverage.Unrestricted or LatticeRangeReadGateCoverage.Filtered;
        }
        catch (Exception)
        {
            // Fail closed: a probe that cannot answer grants nothing.
            return false;
        }
    }
}
