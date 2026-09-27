using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.Apps.Tests;

/// <summary>A settable <see cref="IAppRegistryProjection"/>.</summary>
internal sealed class FakeAppRegistryProjection : IAppRegistryProjection
{
    public FakeAppRegistryProjection(CompiledAppRegistrySnapshot snapshot) => Current = snapshot;

    public CompiledAppRegistrySnapshot Current { get; set; }

    public long CurrentEpoch => Current.Epoch;

    public int WarmCalls { get; private set; }

    public Exception? WarmFault { get; set; }

    public Task EnsureWarmAsync(CancellationToken cancellationToken = default)
    {
        WarmCalls++;
        return WarmFault is null ? Task.CompletedTask : Task.FromException(WarmFault);
    }
}
