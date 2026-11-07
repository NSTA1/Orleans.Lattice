using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Orleans.Hosting;
using Orleans.Lattice.BPlusTree.Grains;
using Orleans.TestingHost;

namespace Orleans.Lattice.Tests.BPlusTree;

/// <summary>
/// Test-only (#5833): runs a test cluster's leaves in eager-claim mode, the production
/// path while the pre-arm capability is not yet advertised by every silo (mid-roll).
/// The silo's own manifest gate decides <see cref="IWalAppendFenceGate.IsOpen"/>; only
/// the pre-arm class is held closed. Lives in the test assembly, so no production
/// configuration can select it.
/// </summary>
internal sealed class EagerAppendFenceSiloConfigurator : ISiloConfigurator
{
    /// <inheritdoc />
    public void Configure(ISiloBuilder siloBuilder) =>
        siloBuilder.Services.Replace(ServiceDescriptor.Singleton<IWalAppendFenceGate>(
            sp => new EagerGate(ActivatorUtilities.CreateInstance<ClusterManifestWalAppendFenceGate>(sp))));

    private sealed class EagerGate(IWalAppendFenceGate inner) : IWalAppendFenceGate
    {
        public bool IsOpen => inner.IsOpen;

        public bool IsPreArmOpen => false;
    }
}
