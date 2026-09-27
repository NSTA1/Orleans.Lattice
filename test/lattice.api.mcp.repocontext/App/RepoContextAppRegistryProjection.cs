using System.Reflection;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.App;

/// <summary>
/// A settable <see cref="IAppRegistryProjection"/>. Snapshots are compiled through the real
/// (internal) snapshot factory, reached by reflection because the apps package exposes its
/// internals only to its own test projects.
/// </summary>
internal sealed class RepoContextAppRegistryProjection : IAppRegistryProjection
{
    private static readonly MethodInfo CompileMethod =
        typeof(CompiledAppRegistrySnapshot).GetMethod("Compile", BindingFlags.NonPublic | BindingFlags.Static)
        ?? throw new InvalidOperationException("CompiledAppRegistrySnapshot.Compile was not found.");

    public CompiledAppRegistrySnapshot Current { get; set; } = CompiledAppRegistrySnapshot.Empty;

    public long CurrentEpoch => Current.Epoch;

    public Task EnsureWarmAsync(CancellationToken cancellationToken = default) => Task.CompletedTask;

    public void Publish(long epoch, params AppRegistryRecord[] records)
        => Current = (CompiledAppRegistrySnapshot)CompileMethod.Invoke(null, [records, epoch])!;
}
