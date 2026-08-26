using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Unit coverage for the public wire model of the tenant quota-authoring surface:
/// the <see cref="TenantQuotasDescriptor"/> value type (its per-dimension ceilings,
/// the <see cref="TenantQuotasDescriptor.Unbounded"/> sentinel, and the
/// <see cref="TenantQuotasDescriptor.IsUnbounded"/> predicate) and the
/// <see cref="TenantQuotasUpdateResult"/> record that a transport binding exchanges.
/// These are pure value types with no timing or ordering behaviour.
/// </summary>
[TestFixture]
public sealed class TenantQuotasModelTests
{
    [Test]
    public void Descriptor_round_trips_every_dimension()
    {
        var descriptor = new TenantQuotasDescriptor
        {
            MaxBytes = 1_000,
            MaxKeys = 2_000,
            MaxMemoryBytes = 3_000,
            MaxTreeCount = 4,
            MaxOpsPerSecond = 5_000,
            BurstPercent = 25,
        };

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.MaxBytes, Is.EqualTo(1_000));
            Assert.That(descriptor.MaxKeys, Is.EqualTo(2_000));
            Assert.That(descriptor.MaxMemoryBytes, Is.EqualTo(3_000));
            Assert.That(descriptor.MaxTreeCount, Is.EqualTo(4));
            Assert.That(descriptor.MaxOpsPerSecond, Is.EqualTo(5_000));
            Assert.That(descriptor.BurstPercent, Is.EqualTo(25));
            Assert.That(descriptor.IsUnbounded, Is.False);
        });
    }

    [Test]
    public void Unbounded_sentinel_leaves_every_dimension_null_and_is_unbounded()
    {
        var unbounded = TenantQuotasDescriptor.Unbounded;

        Assert.Multiple(() =>
        {
            Assert.That(unbounded.MaxBytes, Is.Null);
            Assert.That(unbounded.MaxKeys, Is.Null);
            Assert.That(unbounded.MaxMemoryBytes, Is.Null);
            Assert.That(unbounded.MaxTreeCount, Is.Null);
            Assert.That(unbounded.MaxOpsPerSecond, Is.Null);
            Assert.That(unbounded.BurstPercent, Is.EqualTo(0));
            Assert.That(unbounded.IsUnbounded, Is.True);
        });
    }

    [Test]
    public void Default_descriptor_is_unbounded()
    {
        Assert.That(default(TenantQuotasDescriptor).IsUnbounded, Is.True);
    }

    [Test]
    public void IsUnbounded_is_false_when_any_single_dimension_is_bounded()
    {
        Assert.Multiple(() =>
        {
            Assert.That((TenantQuotasDescriptor.Unbounded with { MaxBytes = 1 }).IsUnbounded, Is.False);
            Assert.That((TenantQuotasDescriptor.Unbounded with { MaxKeys = 1 }).IsUnbounded, Is.False);
            Assert.That((TenantQuotasDescriptor.Unbounded with { MaxMemoryBytes = 1 }).IsUnbounded, Is.False);
            Assert.That((TenantQuotasDescriptor.Unbounded with { MaxTreeCount = 1 }).IsUnbounded, Is.False);
            Assert.That((TenantQuotasDescriptor.Unbounded with { MaxOpsPerSecond = 1 }).IsUnbounded, Is.False);
        });
    }

    [Test]
    public void BurstPercent_alone_does_not_make_a_descriptor_bounded()
    {
        Assert.That((TenantQuotasDescriptor.Unbounded with { BurstPercent = 50 }).IsUnbounded, Is.True);
    }

    [Test]
    public void UpdateResult_round_trips_its_members()
    {
        var quotas = new TenantQuotasDescriptor { MaxBytes = 42, BurstPercent = 10 };
        var result = new TenantQuotasUpdateResult { TenantId = "acme", Quotas = quotas };

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Quotas, Is.EqualTo(quotas));
        });
    }
}
