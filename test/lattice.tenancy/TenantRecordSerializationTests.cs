using static Orleans.Lattice.Tenancy.Tests.TestClocks;

namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Persistence round-trip coverage for <see cref="TenantRecord"/> through the
/// exact <see cref="OrleansLatticeSerializer{T}"/> the registry uses. A fully
/// populated record - non-default status, bounded quotas with a burst, a
/// non-shared placement, a tenant-admin subject, and a cross-tenant grant - must
/// recover every field with full fidelity, because the durable
/// <c>sys-tenant-registry</c> state is stored and read back through this
/// serializer. The contrasting default-JSON test documents why the Orleans
/// serializer is required: the JSON path silently drops the record's private-init
/// id and its internal CRDT registers.
/// </summary>
public sealed class TenantRecordSerializationTests
{
    private static TenantRecord Populated()
    {
        var record = TenantRecord.Create(
            TenantId.Parse("acme"),
            TenantStatus.Suspended,
            new TenantQuotas
            {
                MaxBytes = 1 << 20,
                MaxKeys = 500,
                MaxMemoryBytes = 1L << 30,
                MaxTreeCount = 8,
                MaxOpsPerSecond = 1000,
                BurstPercent = 25,
            },
            new TenantPlacement
            {
                WalProviderName = "wal-eu",
                PlacementFilter = "filter-eu",
                DedicatedWal = true,
            },
            Clock(10),
            "writer-1");

        record.AddAdminSubject("admin-1", Clock(20), "writer-1");
        record.AddGrant(
            CrossTenantGrant.Create("other-tenant", TenantGranteeKind.Tenant, "tree-shared", TenantGrantOperations.ReadWrite),
            Clock(30),
            "writer-1");
        return record;
    }

    [Test]
    public void Orleans_serializer_round_trips_a_populated_record_with_full_fidelity()
    {
        var record = Populated();
        var serializer = TestSerializers.TenantRecords;

        var recovered = serializer.Deserialize(serializer.Serialize(record));

        var grant = record.Grants[0];
        Assert.Multiple(() =>
        {
            Assert.That(recovered.Id, Is.EqualTo(TenantId.Parse("acme")), "id");
            Assert.That(recovered.Id.Value, Is.EqualTo("acme"), "id text");
            Assert.That(recovered.Status, Is.EqualTo(TenantStatus.Suspended), "status");

            Assert.That(recovered.Quotas.MaxBytes, Is.EqualTo(1 << 20), "quota MaxBytes");
            Assert.That(recovered.Quotas.MaxKeys, Is.EqualTo(500), "quota MaxKeys");
            Assert.That(recovered.Quotas.MaxMemoryBytes, Is.EqualTo(1L << 30), "quota MaxMemoryBytes");
            Assert.That(recovered.Quotas.MaxTreeCount, Is.EqualTo(8), "quota MaxTreeCount");
            Assert.That(recovered.Quotas.MaxOpsPerSecond, Is.EqualTo(1000), "quota MaxOpsPerSecond");
            Assert.That(recovered.Quotas.BurstPercent, Is.EqualTo(25), "quota BurstPercent");

            Assert.That(recovered.Placement.WalProviderName, Is.EqualTo("wal-eu"), "placement WAL provider");
            Assert.That(recovered.Placement.PlacementFilter, Is.EqualTo("filter-eu"), "placement filter");
            Assert.That(recovered.Placement.DedicatedWal, Is.True, "placement dedicated WAL");

            Assert.That(recovered.HasAdminSubject("admin-1"), Is.True, "admin subject");

            Assert.That(recovered.TryGetGrant(grant.GrantId, out var recoveredGrant), Is.True, "grant present");
            Assert.That(recoveredGrant.Grantee, Is.EqualTo("other-tenant"), "grant grantee");
            Assert.That(recoveredGrant.GranteeKind, Is.EqualTo(TenantGranteeKind.Tenant), "grant grantee kind");
            Assert.That(recoveredGrant.Scope, Is.EqualTo("tree-shared"), "grant scope");
            Assert.That(recoveredGrant.Operations, Is.EqualTo(TenantGrantOperations.ReadWrite), "grant operations");
        });
    }

    [Test]
    public void Default_json_serializer_loses_the_record_which_is_why_the_registry_uses_orleans()
    {
        var record = Populated();
        var json = JsonLatticeSerializer<TenantRecord>.Default;

        var hollow = json.Deserialize(json.Serialize(record));

        // System.Text.Json cannot set the private-init Id or the internal CRDT
        // registers, so the record comes back hollow. This is the exact defect the
        // OrleansLatticeSerializer fixes; the registry must never use this path.
        Assert.Multiple(() =>
        {
            Assert.That(hollow.Id.Value, Is.Null, "the JSON path drops the private-init id");
            Assert.That(hollow.HasAdminSubject("admin-1"), Is.False, "the JSON path drops internal subject state");
            Assert.That(hollow.Grants, Is.Empty, "the JSON path drops internal grant state");
        });
    }
}
