using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TenantAdminToolMappings"/>, the pure projections from
/// the tenant-admin control facade's domain results onto the compact MCP
/// structured-content DTOs. Proves each projection copies every field and
/// stringifies the lifecycle status via its enum name, and that a null domain
/// result is rejected. All deterministic - no cluster, no I/O.
/// </summary>
[TestFixture]
public sealed class TenantAdminToolMappingsTests
{
    [Test]
    public void ToMcp_creation_result_copies_id_and_stringifies_status()
    {
        var result = TenantAdminToolMappings.ToMcp(
            new TenantCreationResult { TenantId = "acme", Status = TenantLifecycleStatus.Active });

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Status, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
        });
    }

    [Test]
    public void ToMcp_status_change_result_copies_every_field()
    {
        var result = TenantAdminToolMappings.ToMcp(new TenantStatusChangeResult
        {
            TenantId = "acme",
            PreviousStatus = TenantLifecycleStatus.Active,
            NewStatus = TenantLifecycleStatus.Suspended,
            Changed = true,
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.PreviousStatus, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
            Assert.That(result.NewStatus, Is.EqualTo(nameof(TenantLifecycleStatus.Suspended)));
            Assert.That(result.Changed, Is.True);
        });
    }

    [Test]
    public void ToMcp_deletion_result_copies_id_and_cascaded_tree_count()
    {
        var result = TenantAdminToolMappings.ToMcp(
            new TenantDeletionResult { TenantId = "acme", CascadedTreeCount = 5 });

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.CascadedTreeCount, Is.EqualTo(5));
        });
    }

    [Test]
    public void ToMcp_rejects_a_null_creation_result()
    {
        Assert.That(() => TenantAdminToolMappings.ToMcp((TenantCreationResult)null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ToMcp_rejects_a_null_status_change_result()
    {
        Assert.That(() => TenantAdminToolMappings.ToMcp((TenantStatusChangeResult)null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ToMcp_quotas_update_result_copies_every_field()
    {
        var result = TenantAdminToolMappings.ToMcp(new TenantQuotasUpdateResult
        {
            TenantId = "acme",
            Quotas = new TenantQuotasDescriptor
            {
                MaxBytes = 1_000,
                MaxKeys = 2_000,
                MaxMemoryBytes = 3_000,
                MaxTreeCount = 4,
                MaxOpsPerSecond = 5_000,
                BurstPercent = 25,
            },
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.MaxBytes, Is.EqualTo(1_000));
            Assert.That(result.MaxKeys, Is.EqualTo(2_000));
            Assert.That(result.MaxMemoryBytes, Is.EqualTo(3_000));
            Assert.That(result.MaxTreeCount, Is.EqualTo(4));
            Assert.That(result.MaxOpsPerSecond, Is.EqualTo(5_000));
            Assert.That(result.BurstPercent, Is.EqualTo(25));
            Assert.That(result.IsUnbounded, Is.False);
        });
    }

    [Test]
    public void ToMcp_quotas_update_result_flags_unbounded_when_every_dimension_is_lifted()
    {
        var result = TenantAdminToolMappings.ToMcp(new TenantQuotasUpdateResult
        {
            TenantId = "acme",
            Quotas = TenantQuotasDescriptor.Unbounded,
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.IsUnbounded, Is.True);
            Assert.That(result.MaxBytes, Is.Null);
            Assert.That(result.MaxKeys, Is.Null);
            Assert.That(result.MaxMemoryBytes, Is.Null);
            Assert.That(result.MaxTreeCount, Is.Null);
            Assert.That(result.MaxOpsPerSecond, Is.Null);
        });
    }

    [Test]
    public void ToMcp_rejects_a_null_deletion_result()
    {
        Assert.That(() => TenantAdminToolMappings.ToMcp((TenantDeletionResult)null!), Throws.ArgumentNullException);
    }

    [Test]
    public void ToMcp_rejects_a_null_quotas_update_result()
    {
        Assert.That(() => TenantAdminToolMappings.ToMcp((TenantQuotasUpdateResult)null!), Throws.ArgumentNullException);
    }

    // ----- region residency -----

    [Test]
    public void ToMcp_region_authorization_result_copies_the_allowed_set()
    {
        var result = TenantAdminToolMappings.ToMcp(new TenantRegionAuthorizationResult
        {
            TenantId = "acme",
            AllowedRegions = ["eu-west", "ap-south"],
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.AllowedRegions, Is.EqualTo(new[] { "eu-west", "ap-south" }));
        });
    }

    [Test]
    public void ToMcp_region_authorization_result_carries_an_empty_allowed_set_through()
    {
        var result = TenantAdminToolMappings.ToMcp(new TenantRegionAuthorizationResult
        {
            TenantId = "acme",
            AllowedRegions = [],
        });

        Assert.That(result.AllowedRegions, Is.Empty,
            "A full revocation must be reported as an empty set, not as an absent one.");
    }

    [Test]
    public void ToMcp_residency_change_result_copies_every_field_and_stringifies_row_status()
    {
        var result = TenantAdminToolMappings.ToMcp(new TenantResidencyChangeResult
        {
            TenantId = "acme",
            AddedRegions = ["ap-south"],
            RemovedRegions = ["eu-west"],
            Regions =
            [
                new TenantRegionStatusDescriptor
                {
                    RegionId = "ap-south",
                    Status = TenantRegionLifecycleStatus.Backfilling,
                    IsAllowed = true,
                },
                new TenantRegionStatusDescriptor
                {
                    RegionId = "eu-west",
                    Status = TenantRegionLifecycleStatus.Draining,
                    IsAllowed = true,
                },
            ],
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.AddedRegions, Is.EqualTo(new[] { "ap-south" }));
            Assert.That(result.RemovedRegions, Is.EqualTo(new[] { "eu-west" }));
            Assert.That(result.Regions, Has.Count.EqualTo(2));
            Assert.That(result.Regions[0].Status, Is.EqualTo(nameof(TenantRegionLifecycleStatus.Backfilling)));
            Assert.That(result.Regions[1].Status, Is.EqualTo(nameof(TenantRegionLifecycleStatus.Draining)));
        });
    }

    [Test]
    public void ToMcp_region_status_report_copies_every_row()
    {
        var result = TenantAdminToolMappings.ToMcp(new TenantRegionStatusReport
        {
            TenantId = "acme",
            Regions =
            [
                new TenantRegionStatusDescriptor
                {
                    RegionId = "eu-west",
                    Status = TenantRegionLifecycleStatus.Online,
                    IsAllowed = true,
                },
                new TenantRegionStatusDescriptor
                {
                    RegionId = "ap-south",
                    Status = TenantRegionLifecycleStatus.None,
                    IsAllowed = true,
                },
            ],
        });

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Regions.Select(r => r.RegionId), Is.EqualTo(new[] { "eu-west", "ap-south" }));
            Assert.That(result.Regions[0].Status, Is.EqualTo(nameof(TenantRegionLifecycleStatus.Online)));
            Assert.That(result.Regions[1].Status, Is.EqualTo(nameof(TenantRegionLifecycleStatus.None)),
                "An allowed-but-not-yet-resident region reports None, not an absent status.");
        });
    }

    [Test]
    public void ToMcp_region_status_report_carries_an_empty_row_set_through()
    {
        var result = TenantAdminToolMappings.ToMcp(new TenantRegionStatusReport
        {
            TenantId = "acme",
            Regions = [],
        });

        Assert.That(result.Regions, Is.Empty);
    }

    [Test]
    public void ToMcp_rejects_a_null_region_result()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                () => TenantAdminToolMappings.ToMcp((TenantRegionAuthorizationResult)null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => TenantAdminToolMappings.ToMcp((TenantResidencyChangeResult)null!),
                Throws.ArgumentNullException);
            Assert.That(
                () => TenantAdminToolMappings.ToMcp((TenantRegionStatusReport)null!),
                Throws.ArgumentNullException);
        });
    }
}
