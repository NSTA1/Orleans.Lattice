using System.Linq;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;

namespace Orleans.Lattice.Replication;

public static partial class LatticeReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Statically enrols the self-referential
    /// <see cref="LatticeSystemTreeNames.ReplicationConfig"/> tree into
    /// replication under its fixed <see cref="LatticeMergeMode.OrMap"/> mode and
    /// registers the OR-Map shape for its
    /// <see cref="LatticeReplicationConfigEntry"/> value, so a runtime
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
    /// <b>Guardrail.</b> Enrolling the config tree requires
    /// <c>Orleans.Lattice.Replication</c> to be registered first (via
    /// <see cref="AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions})"/>);
    /// calling this before that add-on fails fast with an actionable message
    /// rather than silently declaring a tree that never ships.
    /// </para>
    /// <para>
    /// <b>Idempotency.</b> Calling this more than once is a no-op after the
    /// first call - the enrolment and the OR-Map shape registration are applied
    /// exactly once, so it is safe for a composed host and a library add-on to
    /// both request it.
    /// </para>
    /// </remarks>
    /// <param name="builder">The silo builder. Must not be <see langword="null"/>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <see langword="null"/>.</exception>
    /// <exception cref="InvalidOperationException"><see cref="AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions})"/> was not called first.</exception>
    public static ISiloBuilder ReplicateLatticeReplicationConfig(this ISiloBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Guardrail: the receiver-side applier registered by AddLatticeReplication
        // is the concrete replication-only marker. Its absence means the
        // pipeline is not wired, so enrolling the config tree would declare a
        // tree that nothing ships or applies. Fail fast, mirroring the other
        // system-tree add-ons.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ReplicationApplier)))
        {
            throw new InvalidOperationException(
                "ReplicateLatticeReplicationConfig() requires Orleans.Lattice.Replication to be registered first. "
                + "Call siloBuilder.AddLatticeReplication(...) before enrolling the sys-replication-config tree for replication.");
        }

        // Idempotency: a marker singleton records that the anchor has already
        // been applied so a second call adds neither a duplicate PostConfigure
        // nor a duplicate OR-Map shape registration (the latter would fault the
        // shape-startup drain when it tried to register a distinct descriptor
        // instance for the same (tree, mode) slot).
        if (builder.Services.Any(d => d.ServiceType == typeof(ReplicationConfigAnchorMarker)))
        {
            return builder;
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

        return builder;
    }

    /// <summary>
    /// Internal marker singleton used by
    /// <see cref="ReplicateLatticeReplicationConfig(ISiloBuilder)"/> to make the
    /// static config-tree anchor idempotent.
    /// </summary>
    internal sealed class ReplicationConfigAnchorMarker
    {
    }
}
