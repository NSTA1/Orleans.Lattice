using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TenantSelfAwarenessToolMappings"/>, the pure
/// projections from the read-only tenant self-awareness facade's domain results
/// onto the compact MCP structured-content DTOs. Proves each projection copies
/// every field, stringifies the lifecycle status enums, preserves list order, and
/// rejects a null argument. All deterministic - no I/O.
/// </summary>
[TestFixture]
public sealed class TenantSelfAwarenessToolMappingsTests
{
    [Test]
    public void ToMcp_descriptor_copies_every_field()
    {
        var descriptor = new TenantDescriptor
        {
            TenantId = "acme",
            Status = TenantLifecycleStatus.Suspended,
            IsDefault = false,
        };

        var mapped = TenantSelfAwarenessToolMappings.ToMcp(descriptor);

        Assert.Multiple(() =>
        {
            Assert.That(mapped.TenantId, Is.EqualTo("acme"));
            Assert.That(mapped.Status, Is.EqualTo(nameof(TenantLifecycleStatus.Suspended)));
            Assert.That(mapped.IsDefault, Is.False);
        });
    }

    [Test]
    public void ToMcp_list_preserves_order_and_projects_each_row()
    {
        IReadOnlyList<TenantDescriptor> tenants =
        [
            new TenantDescriptor { TenantId = "alpha", Status = TenantLifecycleStatus.Active, IsDefault = false },
            new TenantDescriptor { TenantId = "beta", Status = TenantLifecycleStatus.Suspended, IsDefault = false },
        ];

        var mapped = TenantSelfAwarenessToolMappings.ToMcp(tenants);

        Assert.Multiple(() =>
        {
            Assert.That(mapped.Tenants.Select(t => t.TenantId), Is.EqualTo(new[] { "alpha", "beta" }));
            Assert.That(mapped.Tenants[0].Status, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
            Assert.That(mapped.Tenants[1].Status, Is.EqualTo(nameof(TenantLifecycleStatus.Suspended)));
        });
    }

    [Test]
    public void ToMcp_empty_list_yields_empty_result()
    {
        var mapped = TenantSelfAwarenessToolMappings.ToMcp(Array.Empty<TenantDescriptor>());

        Assert.That(mapped.Tenants, Is.Empty);
    }

    [Test]
    public void ToMcp_report_copies_status_and_region_rows()
    {
        var report = new TenantStatusReport
        {
            TenantId = "acme",
            Status = TenantLifecycleStatus.Active,
            IsDefault = false,
            Regions =
            [
                new TenantRegionStatusDescriptor
                {
                    RegionId = "region-a",
                    Status = TenantRegionLifecycleStatus.Online,
                    IsAllowed = true,
                },
                new TenantRegionStatusDescriptor
                {
                    RegionId = "region-b",
                    Status = TenantRegionLifecycleStatus.Draining,
                    IsAllowed = false,
                },
            ],
        };

        var mapped = TenantSelfAwarenessToolMappings.ToMcp(report);

        Assert.Multiple(() =>
        {
            Assert.That(mapped.TenantId, Is.EqualTo("acme"));
            Assert.That(mapped.Status, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
            Assert.That(mapped.IsDefault, Is.False);
            Assert.That(mapped.Regions.Select(r => r.RegionId), Is.EqualTo(new[] { "region-a", "region-b" }));
            Assert.That(mapped.Regions[0].Status, Is.EqualTo(nameof(TenantRegionLifecycleStatus.Online)));
            Assert.That(mapped.Regions[0].IsAllowed, Is.True);
            Assert.That(mapped.Regions[1].Status, Is.EqualTo(nameof(TenantRegionLifecycleStatus.Draining)));
            Assert.That(mapped.Regions[1].IsAllowed, Is.False);
        });
    }

    [Test]
    public void ToMcp_rejects_null_arguments()
    {
        Assert.Multiple(() =>
        {
            Assert.That(() => TenantSelfAwarenessToolMappings.ToMcp((TenantDescriptor)null!), Throws.ArgumentNullException);
            Assert.That(() => TenantSelfAwarenessToolMappings.ToMcp((IReadOnlyList<TenantDescriptor>)null!), Throws.ArgumentNullException);
            Assert.That(() => TenantSelfAwarenessToolMappings.ToMcp((TenantStatusReport)null!), Throws.ArgumentNullException);
        });
    }
}
