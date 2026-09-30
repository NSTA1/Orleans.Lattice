using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;

/// <summary>
/// The circuit's staged install flows, by tenant, source, slug and version. Keeping
/// them here rather than in the page is what makes the review page a resumable
/// status page: navigate away mid-install and the flow is still at its stage when
/// you come back.
/// </summary>
/// <remarks>
/// A flow holds what its caller read and chose, so the flows belong to one
/// sign-in at one endpoint: when that identity changes, every flow is forgotten
/// rather than resumed for the next caller. The flows are already filed by tenant,
/// so a tenant switch keeps them.
/// </remarks>
/// <param name="facades">The circuit's facades.</param>
internal sealed class AppInstallFlowStore(AppsFacades facades)
{
    private readonly Dictionary<AppInstallFlowKey, AppInstallFlow> _flows = [];
    private ShellCallerKey _owner;

    /// <summary>The flows in progress for the caller now, in no particular order.</summary>
    public IReadOnlyCollection<AppInstallFlow> Flows
    {
        get
        {
            ForgetIfTheCallerChanged();
            return _flows.Values;
        }
    }

    /// <summary>The flow for <paramref name="key"/>, creating it (not yet loaded) on first use.</summary>
    /// <param name="key">The flow's identity.</param>
    /// <param name="source">The source's summary, or <see langword="null"/> when it is not listed.</param>
    public AppInstallFlow GetOrCreate(AppInstallFlowKey key, AppSourceSummary? source)
    {
        ArgumentNullException.ThrowIfNull(key);
        ForgetIfTheCallerChanged();
        if (!_flows.TryGetValue(key, out var flow))
        {
            flow = new AppInstallFlow(facades, key, source);
            _flows[key] = flow;
        }

        return flow;
    }

    /// <summary>Forgets the flow for <paramref name="key"/>, so the next visit starts over.</summary>
    /// <param name="key">The flow's identity.</param>
    /// <returns>Whether a flow was forgotten.</returns>
    public bool Remove(AppInstallFlowKey key)
    {
        ForgetIfTheCallerChanged();
        return _flows.Remove(key);
    }

    private void ForgetIfTheCallerChanged()
    {
        var owner = facades.Caller.Identity;
        if (owner != _owner)
        {
            _flows.Clear();
            _owner = owner;
        }
    }
}
