using Orleans.Lattice.Schema;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Schema;

internal sealed partial class FakeSchemaControl
{
    /// <summary>The durable catalog scripted for this circuit.</summary>
    public LatticeSchemaRuntimeRegistration Registry { get; set; } = new();

    /// <summary>The last registration submitted.</summary>
    public LatticeSchemaRuntimeRegistration? LastRegistration { get; private set; }

    /// <inheritdoc />
    public Task<LatticeSchemaRuntimeRegistration> GetRegistryAsync(string treeId, CancellationToken cancellationToken = default)
    {
        Record("GetRegistry", treeId);
        return Task.FromResult(Registry);
    }

    /// <inheritdoc />
    public Task RegisterSchemaAsync(string treeId, LatticeSchemaRuntimeRegistration registration, CancellationToken cancellationToken = default)
    {
        Record("RegisterSchema", treeId);
        LastRegistration = registration;
        Registry = registration;
        return Task.CompletedTask;
    }
}
