using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// A settable <see cref="IAppRegistryProjection"/>. Snapshots are compiled through the real
/// snapshot factory, so the app tool surface reads the same shape it reads in production.
/// </summary>
internal sealed class RepoContextAppRegistryProjection : IAppRegistryProjection
{
    public CompiledAppRegistrySnapshot Current { get; set; } = CompiledAppRegistrySnapshot.Empty;

    public long CurrentEpoch => Current.Epoch;

    public Task EnsureWarmAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public void Publish(long epoch, params AppRegistryRecord[] records)
        => Current = CompiledAppRegistrySnapshot.Compile(records, epoch);
}
