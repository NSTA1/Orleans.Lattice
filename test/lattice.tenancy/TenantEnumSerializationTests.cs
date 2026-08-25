namespace Orleans.Lattice.Tenancy.Tests;

/// <summary>
/// Round-trip coverage for the three public tenancy enums through the Orleans
/// binary serializer. They are serialized into the durable
/// <see cref="TenantRecord"/> / <see cref="CrossTenantGrant"/> stored in the
/// <c>sys-tenant-registry</c> tree, so each carries <c>[GenerateSerializer]</c>
/// and a stable <c>[Alias]</c>; these tests prove the decorated format
/// round-trips every declared value.
/// </summary>
[TestFixture]
public sealed class TenantEnumSerializationTests
{
    [TestCase(TenantStatus.Active)]
    [TestCase(TenantStatus.Suspended)]
    public void TenantStatus_round_trips(TenantStatus value)
    {
        var serializer = TestSerializers.For<TenantStatus>();

        Assert.That(serializer.Deserialize(serializer.Serialize(value)), Is.EqualTo(value));
    }

    [TestCase(TenantGranteeKind.Tenant)]
    [TestCase(TenantGranteeKind.Subject)]
    public void TenantGranteeKind_round_trips(TenantGranteeKind value)
    {
        var serializer = TestSerializers.For<TenantGranteeKind>();

        Assert.That(serializer.Deserialize(serializer.Serialize(value)), Is.EqualTo(value));
    }

    [TestCase(TenantGrantOperations.None)]
    [TestCase(TenantGrantOperations.Read)]
    [TestCase(TenantGrantOperations.ReadWrite)]
    public void TenantGrantOperations_round_trips(TenantGrantOperations value)
    {
        var serializer = TestSerializers.For<TenantGrantOperations>();

        Assert.That(serializer.Deserialize(serializer.Serialize(value)), Is.EqualTo(value));
    }
}
