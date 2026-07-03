using Microsoft.Extensions.DependencyInjection;
using Orleans.Hosting;

namespace Orleans.Lattice.Replication;

public static partial class LatticeReplicationServiceCollectionExtensions
{
    /// <summary>
    /// Enrols the reserved <c>Orleans.Lattice.Membership</c> and
    /// <c>Orleans.Lattice.Auth</c> system trees into replication so a
    /// multi-cluster deployment converges on a single identity and authorization
    /// surface across sites. The membership users/groups/edges trees and the
    /// authorization policy tree are enrolled last-writer-wins; the append-only
    /// audit tree is enrolled as an observed-remove set only when
    /// <paramref name="includeAudit"/> is <c>true</c> (see
    /// <see cref="LatticeSystemTreeNames"/> for why audit is off by default).
    /// </summary>
    /// <remarks>
    /// <para>
    /// This is the first-class, explicit, gated way to opt these trees in - it
    /// merges the reserved ids from <see cref="LatticeSystemTreeNames"/> into
    /// <see cref="LatticeReplicationOptions.ReplicatedTrees"/> via a
    /// <c>PostConfigure</c> so it wins regardless of the order in which the host
    /// configures its own replicated-trees map. The reserved entries are
    /// authoritative: the correct merge mode is forced even if a host had
    /// declared one of these ids under a different mode.
    /// </para>
    /// <para>
    /// <b>Divergence-window semantics.</b> Enrolment gives <i>eventual</i>
    /// cross-cluster consistency: a policy or membership edit made on one site
    /// becomes visible on another only after the change ships and the receiver's
    /// compiled-snapshot maintainer rebuilds off the change feed. During that
    /// window a revoke authored on site A is not yet enforced on site B, so a
    /// user may still perform an operation on B that A's newer policy forbids.
    /// This is the LWW convergence contract; a deployment that must close the
    /// window for specific data trees layers the auth package's strict-epoch
    /// fence on top (off by default, opt-in per tree).
    /// </para>
    /// <para>
    /// <b>Guardrail.</b> Enrolling these trees requires
    /// <c>Orleans.Lattice.Replication</c> to be registered first - the receiver
    /// apply seam and the merge-mode resolver that make replication actually run
    /// come from <see cref="AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions})"/>.
    /// Calling this before that add-on fails fast with a clear message rather than
    /// silently declaring trees that never ship.
    /// </para>
    /// </remarks>
    /// <param name="builder">The silo builder. Must not be <c>null</c>.</param>
    /// <param name="includeAudit">Whether to also enrol the append-only audit tree. Defaults to <c>false</c>.</param>
    /// <returns>The same <paramref name="builder"/> for chaining.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> is <c>null</c>.</exception>
    /// <exception cref="InvalidOperationException"><see cref="AddLatticeReplication(ISiloBuilder, Action{LatticeReplicationOptions})"/> was not called first.</exception>
    public static ISiloBuilder ReplicateLatticeSystemTrees(
        this ISiloBuilder builder,
        bool includeAudit = false)
    {
        ArgumentNullException.ThrowIfNull(builder);

        // Guardrail: AddLatticeReplication registers the receiver-side applier
        // (ReplicationApplier) as the concrete replication-only marker. Its
        // absence means the replication pipeline is not wired, so enrolling the
        // system trees would declare replicated trees that nothing ships or
        // applies. Fail fast at registration with an actionable message,
        // mirroring how the other add-ons guard their ordering.
        if (!builder.Services.Any(d => d.ServiceType == typeof(ReplicationApplier)))
        {
            throw new InvalidOperationException(
                "ReplicateLatticeSystemTrees() requires Orleans.Lattice.Replication to be registered first. "
                + "Call siloBuilder.AddLatticeReplication(...) before enrolling the reserved membership/auth "
                + "system trees for replication.");
        }

        var reserved = LatticeSystemTreeNames.BuildEnrolmentMap(includeAudit);

        // PostConfigureAll so the reserved entries are merged after every
        // Configure/ConfigureAll action on LatticeReplicationOptions has run,
        // including a host that sets ReplicatedTrees after this call. The
        // reserved ids overwrite so their correct merge mode always wins; any
        // other host-declared tree is preserved. Applied to every named
        // (per-tree) options instance because the commit-time observer and the
        // merge-mode resolver read options via Get(treeId).
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

        return builder;
    }
}
