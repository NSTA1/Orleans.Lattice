using Orleans.Runtime;

namespace Orleans.Lattice;

internal static class LatticeBootstrapShadowCutoverContext
{
    public static bool IsActive => RequestContext.Get(LatticeEventConstants.BootstrapShadowCutoverRequestContextKey) is true;

    public static Scope BeginScope()
    {
        var previous = RequestContext.Get(LatticeEventConstants.BootstrapShadowCutoverRequestContextKey);
        RequestContext.Set(LatticeEventConstants.BootstrapShadowCutoverRequestContextKey, true);
        return new Scope(previous, active: true);
    }

    private static void Restore(object? previous)
    {
        if (previous is null)
            RequestContext.Remove(LatticeEventConstants.BootstrapShadowCutoverRequestContextKey);
        else
            RequestContext.Set(LatticeEventConstants.BootstrapShadowCutoverRequestContextKey, previous);
    }

    public struct Scope : IDisposable
    {
        private readonly object? _previous;
        private readonly bool _active;
        private bool _disposed;

        internal Scope(object? previous, bool active)
        {
            _previous = previous;
            _active = active;
            _disposed = false;
        }

        public void Dispose()
        {
            if (!_active || _disposed) return;
            _disposed = true;
            Restore(_previous);
        }
    }
}
