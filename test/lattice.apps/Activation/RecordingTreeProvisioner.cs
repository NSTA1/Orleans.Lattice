using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Auth;

namespace Orleans.Lattice.Apps.Tests;

/// <summary>Records tree provisioning calls and tracks which trees exist and are soft-deleted.</summary>
internal sealed class RecordingTreeProvisioner : IAppTreeProvisioner
{
    public Dictionary<string, AppTreeDeclaration> Created { get; } = new(StringComparer.Ordinal);

    public HashSet<string> SoftDeleted { get; } = new(StringComparer.Ordinal);

    public List<string> Ensured { get; } = new();

    public Func<string, Exception?>? FailEnsure { get; set; }

    public Func<string, Exception?>? FailDelete { get; set; }

    public Task EnsureAsync(string treeId, AppTreeDeclaration declaration, CancellationToken cancellationToken)
    {
        if (FailEnsure?.Invoke(treeId) is { } failure)
            throw failure;
        Ensured.Add(treeId);
        Created.TryAdd(treeId, declaration);
        SoftDeleted.Remove(treeId);
        return Task.CompletedTask;
    }

    public Task SoftDeleteAsync(string treeId, CancellationToken cancellationToken)
    {
        if (FailDelete?.Invoke(treeId) is { } failure)
            throw failure;
        if (Created.ContainsKey(treeId))
            SoftDeleted.Add(treeId);
        return Task.CompletedTask;
    }
}
