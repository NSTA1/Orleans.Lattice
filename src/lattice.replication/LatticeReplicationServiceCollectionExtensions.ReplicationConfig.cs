using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;
using Orleans.Hosting;
using Orleans.Lattice.Backup;
using Orleans.Lattice.BPlusTree.Grains;

namespace Orleans.Lattice.Replication;

public static partial class LatticeReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Statically enrols the self-referential
    /// <see cref="LatticeSystemTreeNames.ReplicationConfig"/> tree into
    /// replication under its fixed <see cref="LatticeMergeMode.OrMap"/> mode,
    /// registers the OR-Map shape for its
    /// <see cref="LatticeReplicationConfigEntry"/> value, and swaps the static
    /// merge-mode and membership seams for their dynamic, snapshot-backed
    /// counterparts. Invoked by
    /// <see cref="AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions}, bool)"/>
    /// when its <c>enableRuntimeConfig</c> argument is set, so a runtime
    /// replication-configuration change authored on any cluster converges across
    /// every enrolled peer over the existing engine.
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the one static anchor of the runtime replication-configuration
    /// design: because the config tree is the source of every other tree's
    /// replication membership, it cannot configure its own replication and so is
    /// enrolled uniformly on every cluster here, exactly as
    /// <see cref="ReplicateLatticeSystemTrees(ISiloBuilder, bool)"/> seeds the
    /// membership and auth system trees. The existing static
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map is untouched
    /// and continues to act as seed/fallback for user trees.
    /// </para>
    /// <para>
    /// The reserved config-tree entry is merged via a <c>PostConfigureAll</c> so
    /// it wins regardless of the order in which the host configures its own
    /// replicated-trees map, and the <see cref="LatticeMergeMode.OrMap"/> mode is
    /// forced even if a host had declared the id under a different mode.
    /// </para>
    /// <para>
    /// <b>Idempotency.</b> Applying the anchor more than once is a no-op after
    /// the first application - the enrolment and the OR-Map shape registration
    /// are applied exactly once, so a host that requests <c>enableRuntimeConfig</c>
    /// on more than one call is safe.
    /// </para>
    /// </remarks>
    /// <param name="builder">The silo builder.</param>
    private static void ApplyReplicationConfigAnchor(ISiloBuilder builder)
    {
        // Idempotency: a marker singleton records that the anchor has already
        // been applied so a second call adds neither a duplicate PostConfigure
        // nor a duplicate OR-Map shape registration (the latter would fault the
        // shape-startup drain when it tried to register a distinct descriptor
        // instance for the same (tree, mode) slot).
        if (builder.Services.Any(d => d.ServiceType == typeof(ReplicationConfigAnchorMarker)))
        {
            return;
        }

        builder.Services.AddSingleton<ReplicationConfigAnchorMarker>();

        var reserved = LatticeSystemTreeNames.BuildReplicationConfigEnrolmentMap();
        builder.Services.PostConfigureAll<LatticeReplicationOptions>(options =>
        {
            var merged = new Dictionary<string, LatticeMergeMode>(StringComparer.Ordinal);
            if (options.ReplicatedTrees is not null)
            {
                foreach (var kv in options.ReplicatedTrees)
                {
                    merged[kv.Key] = kv.Value;
                }
            }

            foreach (var kv in reserved)
            {
                merged[kv.Key] = kv.Value;
            }

            options.ReplicatedTrees = merged;
        });

        // Register the composite OR-Map shape so the producer-side accessor and
        // receiver-side applier can (de)serialise and merge the per-tree config
        // records carried by the config tree.
        builder.AddOrMapShape<string, LatticeReplicationConfigEntry>(LatticeSystemTreeNames.ReplicationConfig);

        WireDynamicSeams(builder);
    }

    /// <summary>
    /// Installs the compiled-snapshot maintainer and swaps the options-backed
    /// <see cref="ILatticeMergeModeResolver"/> and
    /// <see cref="IReplicatedTreeMembership"/> seams for their dynamic,
    /// snapshot-backed counterparts, keeping the static
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> map as
    /// seed/fallback. Only the default options-backed registrations installed by
    /// <see cref="AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions})"/>
    /// are replaced; a host-supplied custom resolver or membership is left
    /// untouched.
    /// </summary>
    private static void WireDynamicSeams(ISiloBuilder builder)
    {
        // The snapshot store + compiled-snapshot maintainer. The maintainer is a
        // per-silo singleton registered once as the concrete type and once as an
        // IMutationObserver routed at that same instance, so a sys-replication-config
        // write refreshes the exact snapshot the dynamic seams read.
        builder.Services.TryAddSingleton<ILatticeReplicationConfigStore, LatticeReplicationConfigStore>();
        builder.Services.TryAddSingleton<CompiledReplicationConfigSnapshotMaintainer>();
        builder.Services.AddSingleton<IMutationObserver>(
            sp => sp.GetRequiredService<CompiledReplicationConfigSnapshotMaintainer>());

        // The runtime enable/disable authoring seam (and its tree-content probe)
        // the API facade depends on to author per-tree enablement into the config
        // OR-Map. Registered here because it composes the config store, the
        // precondition validator, the replication context, and the bootstrap
        // admin seam that AddLatticeReplication already installed.
        builder.Services.TryAddSingleton<ILatticeTreeContentProbe, GrainFactoryTreeContentProbe>();
        builder.Services.TryAddSingleton<ILatticeReplicationConfigAuthority, LatticeReplicationConfigAuthority>();

        // Swap the options-only merge-mode resolver for the snapshot-backed one
        // (snapshot first, fail-closed on ambiguity, static options as fallback).
        // Only replace the default ConfiguredLatticeMergeModeResolver so a
        // host-supplied custom resolver is respected.
        for (var i = builder.Services.Count - 1; i >= 0; i--)
        {
            var d = builder.Services[i];
            if (d.ServiceType == typeof(ILatticeMergeModeResolver)
                && d.ImplementationType == typeof(ConfiguredLatticeMergeModeResolver))
            {
                builder.Services[i] = ServiceDescriptor.Singleton<ILatticeMergeModeResolver>(
                    sp => new SnapshotLatticeMergeModeResolver(
                        sp.GetRequiredService<CompiledReplicationConfigSnapshotMaintainer>(),
                        sp.GetRequiredService<ConfiguredLatticeMergeModeResolver>()));
                break;
            }
        }

        // Swap the options-only replicated-tree membership for the snapshot-backed
        // union (snapshot-enabled OR statically declared). Only replace the
        // default OptionsReplicatedTreeMembership so a host-supplied custom
        // membership is respected.
        for (var i = builder.Services.Count - 1; i >= 0; i--)
        {
            var d = builder.Services[i];
            if (d.ServiceType == typeof(IReplicatedTreeMembership)
                && d.ImplementationType == typeof(OptionsReplicatedTreeMembership))
            {
                builder.Services[i] = ServiceDescriptor.Singleton<IReplicatedTreeMembership>(
                    sp => new SnapshotReplicatedTreeMembership(
                        sp.GetRequiredService<CompiledReplicationConfigSnapshotMaintainer>(),
                        sp.GetRequiredService<IOptionsMonitor<LatticeReplicationOptions>>()));
                break;
            }
        }
    }

    /// <summary>
    /// Internal marker singleton used by
    /// <see cref="ApplyReplicationConfigAnchor(ISiloBuilder)"/> to make the
    /// static config-tree anchor idempotent.
    /// </summary>
    internal sealed class ReplicationConfigAnchorMarker
    {
    }
}
