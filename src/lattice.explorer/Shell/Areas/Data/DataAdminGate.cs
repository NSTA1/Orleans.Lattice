using Orleans.Lattice.Api.TreeAdmin;

namespace Orleans.Lattice.Explorer.Shell.Areas.Data;

/// <summary>
/// Decides, fail-closed, whether the caller may run a tree-administration action
/// (reconcile or rebuild a view, reconcile a tag index) over a tree. It asks the
/// tree-administration facade's capability probe once per tree per circuit; no
/// facade, a refusal, a fault or a missing grant all read as "no", so an action
/// the caller cannot perform is never drawn. The server still authorises every
/// real call.
/// </summary>
internal sealed class DataAdminGate
{
    private readonly IServiceProvider _services;
    private readonly Dictionary<string, Task<bool>> _verdicts = new(StringComparer.Ordinal);
    private readonly Lock _gate = new();

    /// <summary>Creates the gate.</summary>
    /// <param name="services">The circuit's services, from which the facade is resolved lazily.</param>
    public DataAdminGate(IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(services);
        _services = services;
    }

    /// <summary>The tree-administration facade, or <see langword="null"/> when the head serves none.</summary>
    public ILatticeTreeAdmin? Admin
    {
        get
        {
            try
            {
                return (ILatticeTreeAdmin?)_services.GetService(typeof(ILatticeTreeAdmin));
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }
    }

    /// <summary>Whether the caller holds administrative authority over the tree <paramref name="stateTreeId"/>.</summary>
    /// <param name="stateTreeId">The tree's state id. Never rendered.</param>
    /// <param name="cancellationToken">Stops waiting.</param>
    public Task<bool> CanAdministerAsync(string stateTreeId, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(stateTreeId);
        Task<bool> verdict;
        lock (_gate)
        {
            if (!_verdicts.TryGetValue(stateTreeId, out verdict!))
            {
                verdict = ProbeAsync(stateTreeId);
                _verdicts.Add(stateTreeId, verdict);
            }
        }

        return verdict.WaitAsync(cancellationToken);
    }

    private async Task<bool> ProbeAsync(string stateTreeId)
    {
        if (Admin is not { } admin)
        {
            return false;
        }

        try
        {
            var capabilities = await admin.ProbeCapabilitiesAsync(stateTreeId).ConfigureAwait(false);
            return capabilities?.CanAdministerTree == true;
        }
        catch (Exception)
        {
            // Fail closed: a probe that cannot answer grants nothing.
            return false;
        }
    }
}
