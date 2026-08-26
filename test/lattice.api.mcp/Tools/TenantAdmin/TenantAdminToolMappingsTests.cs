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
}
