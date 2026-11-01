using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// Decides, fail-closed, whether the caller may create, edit or delete entries in
/// a tree, by asking the data facade's capability probe. No facade, a view or a
/// shared prefix, a refusal or a fault all read as <see cref="DataWriteRights.None"/>,
/// so a write the caller cannot perform is never drawn. The facade still
/// authorizes every real write; hiding a control is never the defence.
/// </summary>
/// <remarks>
/// The tree-wide answer is remembered per tree for the caller it was asked for (the
/// sign-in, the endpoint and the asserted tenant); when any of them changes every
/// answer is forgotten. A per-key answer is asked fresh each time, because a key
/// is opened once and its rule can be narrower than the tree's.
/// </remarks>
internal sealed class DataWriteGate
{
    private readonly IServiceProvider _services;
    private readonly ShellCaller _caller;
    private readonly Dictionary<string, Task<DataWriteRights>> _treeAnswers = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();
    private ShellCallerKey _answersFor;

    /// <summary>Creates the gate.</summary>
    /// <param name="services">The circuit's services, from which the facade is resolved lazily.</param>
    public DataWriteGate(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
        _caller = ShellCaller.Of(services);
    }

    /// <summary>The data facade, or <see langword="null"/> when the head serves none.</summary>
    public ILatticeDataApi? Data
    {
        get
        {
            try
            {
                return _services.GetShellFacade<ILatticeDataApi>();
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }
    }

    /// <summary>Whether <paramref name="tree"/> is a kind of thing that holds writable entries at all.</summary>
    /// <param name="tree">The tree.</param>
    public static bool IsWritableKind(DataTreeEntry tree)
    {
        ArgumentNullException.ThrowIfNull(tree);
        return tree.Kind == DataTreeKind.Tree;
    }

    /// <summary>What the caller may change anywhere in <paramref name="tree"/>.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    public Task<DataWriteRights> ForTreeAsync(DataTreeEntry tree, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        if (!IsWritableKind(tree))
        {
            return Task.FromResult(DataWriteRights.None);
        }

        var caller = _caller.Current;
        Task<DataWriteRights> verdict;
        lock (_gate)
        {
            if (_answersFor != caller)
            {
                _treeAnswers.Clear();
                _answersFor = caller;
            }

            if (!_treeAnswers.TryGetValue(tree.StateId, out verdict!))
            {
                verdict = ProbeAsync(tree.StateId, key: null);
                _treeAnswers.Add(tree.StateId, verdict);
            }
        }

        return verdict.WaitAsync(cancellationToken);
    }

    /// <summary>What the caller may change at <paramref name="key"/> of <paramref name="tree"/>.</summary>
    /// <param name="tree">The tree.</param>
    /// <param name="key">The key.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    public Task<DataWriteRights> ForKeyAsync(DataTreeEntry tree, string key, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(tree);
        ArgumentNullException.ThrowIfNull(key);
        return IsWritableKind(tree)
            ? ProbeAsync(tree.StateId, key).WaitAsync(cancellationToken)
            : Task.FromResult(DataWriteRights.None);
    }

    private async Task<DataWriteRights> ProbeAsync(string stateId, string? key)
    {
        if (Data is not { } data)
        {
            return DataWriteRights.None;
        }

        try
        {
            var capabilities = await data.ProbeCapabilitiesAsync(stateId, key).ConfigureAwait(false);
            return capabilities is null ? DataWriteRights.None : new DataWriteRights(capabilities.CanWrite, capabilities.CanDelete);
        }
        catch (Exception)
        {
            // Fail closed: a probe that cannot answer grants nothing.
            return DataWriteRights.None;
        }
    }
}
