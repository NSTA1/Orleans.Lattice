using Orleans.Runtime;

namespace Orleans.Lattice;

/// <summary>
/// Routes one receiver-bootstrap apply for a logical tree to its held physical
/// copy without changing the tree identity carried by the replication record.
/// </summary>
internal static class LatticeBootstrapShadowRouteContext
{
    public static Scope BeginScope(string logicalTreeId, string physicalTreeId)
    {
        ArgumentException.ThrowIfNullOrEmpty(logicalTreeId);
        ArgumentException.ThrowIfNullOrEmpty(physicalTreeId);

        var previousLogical = RequestContext.Get(LatticeEventConstants.BootstrapShadowLogicalTreeRequestContextKey) as string;
        var previousPhysical = RequestContext.Get(LatticeEventConstants.BootstrapShadowPhysicalTreeRequestContextKey) as string;
        RequestContext.Set(LatticeEventConstants.BootstrapShadowLogicalTreeRequestContextKey, logicalTreeId);
        RequestContext.Set(LatticeEventConstants.BootstrapShadowPhysicalTreeRequestContextKey, physicalTreeId);
        return new Scope(previousLogical, previousPhysical, active: true);
    }

    public static bool TryGetPhysicalTreeId(string logicalTreeId, out string physicalTreeId)
    {
        var scopedLogical = RequestContext.Get(LatticeEventConstants.BootstrapShadowLogicalTreeRequestContextKey) as string;
        var scopedPhysical = RequestContext.Get(LatticeEventConstants.BootstrapShadowPhysicalTreeRequestContextKey) as string;
        if (string.Equals(scopedLogical, logicalTreeId, StringComparison.Ordinal)
            && !string.IsNullOrEmpty(scopedPhysical))
        {
            physicalTreeId = scopedPhysical;
            return true;
        }

        physicalTreeId = "";
        return false;
    }

    private static void Restore(string? logicalTreeId, string? physicalTreeId)
    {
        if (logicalTreeId is null)
            RequestContext.Remove(LatticeEventConstants.BootstrapShadowLogicalTreeRequestContextKey);
        else
            RequestContext.Set(LatticeEventConstants.BootstrapShadowLogicalTreeRequestContextKey, logicalTreeId);

        if (physicalTreeId is null)
            RequestContext.Remove(LatticeEventConstants.BootstrapShadowPhysicalTreeRequestContextKey);
        else
            RequestContext.Set(LatticeEventConstants.BootstrapShadowPhysicalTreeRequestContextKey, physicalTreeId);
    }

    public struct Scope : IDisposable
    {
        private readonly string? _previousLogical;
        private readonly string? _previousPhysical;
        private readonly bool _active;
        private bool _disposed;

        internal Scope(string? previousLogical, string? previousPhysical, bool active)
        {
            _previousLogical = previousLogical;
            _previousPhysical = previousPhysical;
            _active = active;
            _disposed = false;
        }

        public void Dispose()
        {
            if (!_active || _disposed) return;
            _disposed = true;
            Restore(_previousLogical, _previousPhysical);
        }
    }
}
