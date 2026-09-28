using Orleans.Lattice.Api.Apps;

namespace Orleans.Lattice.Explorer.Shell.Areas.Apps.Catalogue;

/// <summary>
/// The circuit's staged install flows, by tenant, source, slug and version. Keeping
/// them here rather than in the page is what makes the review page a resumable
/// status page: navigate away mid-install and the flow is still at its stage when
/// you come back.
/// </summary>
/// <param name="facades">The circuit's facades.</param>
internal sealed class AppInstallFlowStore(AppsFacades facades)
{
    private readonly Dictionary<AppInstallFlowKey, AppInstallFlow> _flows = [];

    /// <summary>The flows in progress, in no particular order.</summary>
    public IReadOnlyCollection<AppInstallFlow> Flows => _flows.Values;

    /// <summary>The flow for <paramref name="key"/>, creating it (not yet loaded) on first use.</summary>
    /// <param name="key">The flow's identity.</param>
    /// <param name="source">The source's summary, or <see langword="null"/> when it is not listed.</param>
    public AppInstallFlow GetOrCreate(AppInstallFlowKey key, AppSourceSummary? source)
    {
        ArgumentNullException.ThrowIfNull(key);
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
    public bool Remove(AppInstallFlowKey key) => _flows.Remove(key);
}
