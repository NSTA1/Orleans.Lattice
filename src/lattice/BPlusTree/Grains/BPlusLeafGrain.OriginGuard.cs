using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Defense-in-depth internal-origin assertion for the physical leaf grain
/// (issue #1103). All access-gate enforcement lives on the <c>ILattice</c>
/// facade; the leaf grains it ultimately drives enforce no policy of their own,
/// so a direct external grain call to a leaf key would otherwise bypass the gate.
/// </summary>
internal sealed partial class BPlusLeafGrain
{
    private bool? _internalOriginEnforced;

    /// <summary>
    /// Refuses a direct external grain call to this internal leaf grain that would
    /// bypass the facade's access gate. A no-op unless the authorization layer's
    /// capability-stripping filter is registered (signalled by the
    /// <see cref="LatticeInternalOriginEnforcementMarker"/> sentinel); a no-auth
    /// cluster, or one with a custom gate but no filter, pays nothing. When active,
    /// every legitimate caller (the shard root, replication-apply, structural
    /// maintenance, and the atomic-write saga) is silo-sourced and carries the
    /// re-derived internal-origin marker, so only a direct external client call is
    /// rejected.
    /// </summary>
    private void EnsureInternalOrigin(LatticeOperation operation)
    {
        _internalOriginEnforced ??=
            context.ActivationServices.GetService<LatticeInternalOriginEnforcementMarker>() is not null;
        if (_internalOriginEnforced is true)
        {
            LatticeInternalOriginContext.EnsureInternalGrainOrigin(
                state.State.TreeId ?? string.Empty, operation);
        }
    }
}
