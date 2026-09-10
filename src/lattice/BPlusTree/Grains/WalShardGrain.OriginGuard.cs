using Microsoft.Extensions.DependencyInjection;

namespace Orleans.Lattice.BPlusTree.Grains;

/// <summary>
/// Defense-in-depth internal-origin assertion for the physical write-ahead-log
/// shard grain. All access-gate enforcement lives on the <c>ILattice</c> facade
/// and on the change-feed visibility filter; the WAL shard grain enforces no
/// policy of its own, so a direct external grain call to a WAL shard key would
/// otherwise bypass the gate entirely.
/// <para>
/// This matters more here than on any other physical grain: a WAL shard's grain
/// key is <c>{physicalTreeId}/{partition}</c> - derivable from a tree name alone -
/// and a single <c>ReadAsync</c> returns the raw commit log, meaning every key,
/// every value, and every mutation in commit order for that tree. The append
/// entry points are the mirror-image risk: an unguarded <c>AppendAsync</c> lets an
/// external caller inject arbitrary records into the log that downstream shippers
/// and view maintainers replay as though they were committed writes.
/// </para>
/// </summary>
internal sealed partial class WalShardGrain
{
    private bool? _internalOriginEnforced;

    /// <summary>
    /// Refuses a direct external grain call to this internal WAL shard grain that
    /// would bypass the facade's access gate. A no-op unless the authorization
    /// layer's capability-stripping filter is registered (signalled by the
    /// <see cref="LatticeInternalOriginEnforcementMarker"/> sentinel); a no-auth
    /// cluster, or one with a custom gate but no filter, pays nothing. When active,
    /// every legitimate caller - the foreground commit-log writer, the replication
    /// shipper, the view maintainers, the WAL usage / introspection grains, and the
    /// administrative move coordinator - is silo-sourced (or arrives through this
    /// cluster's own in-silo hosted client), so it carries the re-derived
    /// internal-origin marker and only a direct external client call is rejected.
    /// </summary>
    /// <param name="operation">The operation being attempted, for the thrown exception.</param>
    private void EnsureInternalOrigin(LatticeOperation operation)
    {
        _internalOriginEnforced ??=
            context.ActivationServices.GetService<LatticeInternalOriginEnforcementMarker>() is not null;
        if (_internalOriginEnforced is true)
        {
            LatticeInternalOriginContext.EnsureInternalGrainOrigin(_treeId, operation);
        }
    }
}
