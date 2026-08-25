namespace Orleans.Lattice.Replication;

/// <summary>
/// The decision the <see cref="IReplicationTenantIsolationGate"/> returns for an
/// inbound replicated write, classifying whether the write may land in the tenant
/// namespace derived from its tree id, or must be refused because it targets a
/// non-existent tenant or a tenant that is not resident in this serving region.
/// </summary>
/// <remarks>
/// The gate derives the owning tenant from the entry's tree id alone (never from
/// a wire-supplied tenant field), so the decision cannot be influenced by a peer.
/// Platform-owned system / definition trees and bare legacy (default-tenant
/// adoption) trees always classify as <see cref="Admit"/> so definitions converge
/// everywhere and pre-tenancy trees keep replicating unchanged; only a well-formed
/// <c>t/{tenantId}/{name}</c> tree naming a real tenant is subject to the
/// existence and residency checks.
/// </remarks>
public enum ReplicationTenantIsolationDecision
{
    /// <summary>
    /// The write may be applied: the tree is platform-owned, adopted by the
    /// reserved default tenant, or names a real tenant that exists and is resident
    /// in this serving region.
    /// </summary>
    Admit = 0,

    /// <summary>
    /// The write is refused because its tree id names a tenant that does not exist
    /// in the tenant registry. A replicated write must never create or smuggle into
    /// a foreign or non-existent tenant, so the entry is dead-lettered rather than
    /// applied (and no tenant is auto-created from an inbound write).
    /// </summary>
    RejectUnknownTenant = 1,

    /// <summary>
    /// The write is refused because its tenant, while it exists, is not resident /
    /// online in the region serving this receiver, as reported by the residency
    /// resolver. The entry is dead-lettered rather than applied.
    /// </summary>
    RejectOutOfRegion = 2,
}
