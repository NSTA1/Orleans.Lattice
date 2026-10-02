using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Serialization;

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

    // A descriptor with the six pre-epic members set, serialized by the type as it stood at
    // 83b748e4a, before epic #4154 appended the delegated access caps. Never regenerate.
    private const string PreEpicPayload = "IOgAQh8Bgj4Bwl0BEQFCnAFl4A==";

    private static readonly TenantQuotasDescriptor PreEpicSample = new()
    {
        MaxBytes = 1_000, MaxKeys = 2_000, MaxMemoryBytes = 3_000, MaxTreeCount = 4, MaxOpsPerSecond = 5_000, BurstPercent = 25,
    };

    [Test]
    public void Descriptor_with_the_delegated_access_caps_set_round_trips_through_the_serializer()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var descriptor = PreEpicSample with
        {
            MaxGroups = 50, MaxMembershipEdges = 600, MaxMemberSubjects = 70, MaxTenantRules = 80,
        };

        var read = serializer.Deserialize<TenantQuotasDescriptor>(serializer.SerializeToArray(descriptor));

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(descriptor));
            Assert.That(read.MaxGroups, Is.EqualTo(50));
            Assert.That(read.MaxMembershipEdges, Is.EqualTo(600));
            Assert.That(read.MaxMemberSubjects, Is.EqualTo(70));
            Assert.That(read.MaxTenantRules, Is.EqualTo(80));
        });
    }

    [Test]
    public void Pre_epic_payload_reads_every_original_dimension_and_leaves_the_delegated_access_caps_null()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();

        var read = serializer.Deserialize<TenantQuotasDescriptor>(Convert.FromBase64String(PreEpicPayload));

        Assert.Multiple(() =>
        {
            Assert.That(read, Is.EqualTo(PreEpicSample));
            Assert.That(read.MaxGroups, Is.Null);
            Assert.That(read.MaxMembershipEdges, Is.Null);
            Assert.That(read.MaxMemberSubjects, Is.Null);
            Assert.That(read.MaxTenantRules, Is.Null);
        });
    }

    [Test]
    public void Pre_epic_payload_rewrites_every_original_field_unchanged()
    {
        using var services = new ServiceCollection().AddSerializer().BuildServiceProvider();
        var serializer = services.GetRequiredService<Serializer>();
        var payload = Convert.FromBase64String(PreEpicPayload);

        var rewritten = serializer.SerializeToArray(serializer.Deserialize<TenantQuotasDescriptor>(payload));

        // Any appended member is written before the end-of-object marker, which a pre-epic reader skips.
        Assert.Multiple(() =>
        {
            Assert.That(rewritten[..(payload.Length - 1)], Is.EqualTo(payload[..^1]));
            Assert.That(rewritten[^1], Is.EqualTo(payload[^1]));
        });
    }

    [Test]
    public void The_delegated_access_caps_do_not_affect_IsUnbounded()
    {
        var capsOnly = TenantQuotasDescriptor.Unbounded with
        {
            MaxGroups = 1, MaxMembershipEdges = 1, MaxMemberSubjects = 1, MaxTenantRules = 1,
        };
        var boundedWithoutCaps = TenantQuotasDescriptor.Unbounded with { MaxBytes = 1 };

        Assert.Multiple(() =>
        {
            Assert.That(capsOnly.IsUnbounded, Is.True, "the access caps are not data-plane dimensions");
            Assert.That((TenantQuotasDescriptor.Unbounded with { MaxGroups = 1 }).IsUnbounded, Is.True);
            Assert.That((TenantQuotasDescriptor.Unbounded with { MaxMembershipEdges = 1 }).IsUnbounded, Is.True);
            Assert.That((TenantQuotasDescriptor.Unbounded with { MaxMemberSubjects = 1 }).IsUnbounded, Is.True);
            Assert.That((TenantQuotasDescriptor.Unbounded with { MaxTenantRules = 1 }).IsUnbounded, Is.True);
            Assert.That(boundedWithoutCaps.IsUnbounded, Is.False);
        });
    }

    [Test]
    public void The_unbounded_sentinel_leaves_the_delegated_access_caps_at_their_defaults()
    {
        var unbounded = TenantQuotasDescriptor.Unbounded;

        Assert.Multiple(() =>
        {
            Assert.That(unbounded.MaxGroups, Is.Null);
            Assert.That(unbounded.MaxMembershipEdges, Is.Null);
            Assert.That(unbounded.MaxMemberSubjects, Is.Null);
            Assert.That(unbounded.MaxTenantRules, Is.Null);
        });
    }
}
