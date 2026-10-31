using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Decides, fail-closed, which import modes the caller may use on a tree (issue #5365): an
/// upsert needs the write right the Data API's capability probe reports, and a seed the
/// bulk-load grant the tree-administration probe reports. No facade, a view or a shared
/// prefix, a refusal or a fault all read as no, so an Import control the caller cannot use
/// is never drawn. The facades still authorize every chunk and batch the import sends.
/// </summary>
/// <remarks>
/// The answers belong to the caller they were asked for (the sign-in, the endpoint and the
/// asserted tenant), and are forgotten when any of them changes.
/// </remarks>
internal sealed class DataImportGate
{
    private readonly IServiceProvider _services;
    private readonly ShellCaller _caller;
    private readonly Dictionary<string, Task<DataImportRights>> _answers = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();
    private ShellCallerKey _answersFor;

    /// <summary>Creates the gate.</summary>
    /// <param name="services">The circuit's services, from which the facades are resolved lazily.</param>
    public DataImportGate(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
        _caller = ShellCaller.Of(services);
    }

    /// <summary>The import modes the caller may use on <paramref name="tree"/>.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    public Task<DataImportRights> ForTreeAsync(DataTreeEntry tree, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        if (tree.Kind != DataTreeKind.Tree || tree.IsShared)
        {
            return Task.FromResult(DataImportRights.None);
        }

        var caller = _caller.Current;
        Task<DataImportRights> verdict;
        lock (_gate)
        {
            if (_answersFor != caller)
            {
                _answers.Clear();
                _answersFor = caller;
            }

            if (!_answers.TryGetValue(tree.StateId, out verdict!))
            {
                verdict = ProbeAsync(tree);
                _answers.Add(tree.StateId, verdict);
            }
        }

        return verdict.WaitAsync(cancellationToken);
    }

    /// <summary>Forgets every answer, so the next ask probes afresh: a tree was just created or loaded.</summary>
    public void Forget()
    {
        lock (_gate)
        {
            _answers.Clear();
        }
    }

    private async Task<DataImportRights> ProbeAsync(DataTreeEntry tree)
    {
        var upsert = CanUpsertAsync(tree);
        var seed = CanSeedAsync(tree.StateId);
        return new DataImportRights(await upsert.ConfigureAwait(false), await seed.ConfigureAwait(false));
    }

    private async Task<bool> CanUpsertAsync(DataTreeEntry tree)
    {
        try
        {
            if (_services.GetService(typeof(DataWriteGate)) is not DataWriteGate writes)
            {
                return false;
            }

            return (await writes.ForTreeAsync(tree).ConfigureAwait(false)).CanWrite;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private async Task<bool> CanSeedAsync(string stateId)
    {
        try
        {
            var admin = _services.GetShellFacade<Orleans.Lattice.Api.TreeAdmin.ILatticeTreeAdmin>();
            var capabilities = await admin.ProbeCapabilitiesAsync(stateId).ConfigureAwait(false);
            return capabilities?.CanBulkLoad == true;
        }
        catch (Exception)
        {
            // Fail closed: no facade, a refusal or a fault grants nothing.
            return false;
        }
    }
}
