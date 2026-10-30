namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Keeps the registry's privileged routing and system-tree access behind the
/// same internal-origin boundary as the physical grains it controls.
/// </summary>
internal sealed partial class LatticeRegistryGrain : IIncomingGrainCallFilter
{
    Task IIncomingGrainCallFilter.Invoke(IIncomingGrainCallContext call)
    {
        if (context is not null)
        {
            LatticeInternalOriginContext.EnsureInternalGrainOrigin(
                context.ActivationServices, LatticeConstants.RegistryTreeId, LatticeOperation.Admin);
        }

        return call.Invoke();
    }
}
