using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Deterministic builders and a substitutable <see cref="ITenantRegistry"/> for
/// the tenant-policy engine tests. Records are stamped with hand-built clocks so
/// admin and grant membership is exact and never timing-dependent, and the fake
/// registry serves a mutable in-memory list so a change-feed refresh can be
/// driven synchronously.
/// </summary>
internal static class TenantPolicyTestData
{
    /// <summary>
    /// Builds a tenant record with the given status, admin subjects, and
    /// tenant-grantee grants, stamped with a monotonically increasing clock so
    /// every add wins deterministically.
    /// </summary>
    internal static TenantRecord Record(
        string tenantId,
        TenantStatus status = TenantStatus.Active,
        IEnumerable<string>? admins = null,
        IEnumerable<CrossTenantGrant>? grants = null)
    {
        var id = TenantId.Parse(tenantId);
        var tick = 1L;
        var record = TenantRecord.Create(
            id,
            status,
            TenantQuotas.Unbounded,
            TenantPlacement.Shared,
            Clock(tick++),
            "test");

        if (admins is not null)
        {
            foreach (var admin in admins)
            {
                record.AddAdminSubject(admin, Clock(tick++), "test");
            }
        }

        if (grants is not null)
        {
            foreach (var grant in grants)
            {
                record.AddGrant(grant, Clock(tick++), "test");
            }
        }

        return record;
    }

    /// <summary>Builds a tenant-grantee cross-tenant grant.</summary>
    internal static CrossTenantGrant TenantGrant(
        string granteeTenantId,
        string scope,
        TenantGrantOperations operations) =>
        CrossTenantGrant.Create(granteeTenantId, TenantGranteeKind.Tenant, scope, operations);

    /// <summary>
    /// A substitutable <see cref="ITenantRegistry"/> serving a mutable list of
    /// records from <see cref="ListAsync"/>. Only <see cref="ListAsync"/> is
    /// exercised by the snapshot maintainer; the remaining members are inert.
    /// </summary>
    internal sealed class FakeTenantRegistry : ITenantRegistry
    {
        /// <summary>The mutable backing list the maintainer scans on each rebuild.</summary>
        public List<TenantRecord> Records { get; } = [];

        public Task<TenantRecord?> GetAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult<TenantRecord?>(Records.Find(r => r.Id.Equals(tenant)));

        public Task<bool> ExistsAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(Records.Exists(r => r.Id.Equals(tenant)));

#pragma warning disable CS1998 // async enumerator with no await is intentional for a synchronous fake
        public async IAsyncEnumerable<TenantRecord> ListAsync(
            [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            // Snapshot to tolerate a test editing the list between rebuilds.
            foreach (var record in Records.ToArray())
            {
                yield return record;
            }
        }
#pragma warning restore CS1998

        public Task<TenantRecord> PutAsync(TenantRecord record, CancellationToken cancellationToken = default)
        {
            Records.Add(record);
            return Task.FromResult(record);
        }

        public Task<bool> DeleteAsync(TenantId tenant, CancellationToken cancellationToken = default) =>
            Task.FromResult(Records.RemoveAll(r => r.Id.Equals(tenant)) > 0);
    }
}
