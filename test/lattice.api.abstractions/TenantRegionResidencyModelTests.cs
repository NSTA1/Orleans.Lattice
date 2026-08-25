using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Abstractions.Tests;

/// <summary>
/// Unit coverage for the public wire model of the T20 per-tenant region-residency
/// control surface: the two failed-precondition exceptions (their message, carried
/// ids, and the required <see cref="Exception"/> base so the same-silo deep-copy
/// contract holds) and the four data-transfer records (their required-init members
/// round-trip the values a transport binding exchanges). These are pure value types
/// with no timing or ordering behaviour.
/// </summary>
[TestFixture]
public sealed class TenantRegionResidencyModelTests
{
    [Test]
    public void TenantRegionNotAllowedException_carries_the_tenant_and_region_ids()
    {
        var exception = new TenantRegionNotAllowedException("acme", "us-east");

        Assert.Multiple(() =>
        {
            Assert.That(exception.TenantId, Is.EqualTo("acme"));
            Assert.That(exception.RegionId, Is.EqualTo("us-east"));
            Assert.That(exception.Message, Does.Contain("acme").And.Contain("us-east"));
            Assert.That(exception, Is.InstanceOf<Exception>());
            Assert.That(exception.GetType().BaseType, Is.EqualTo(typeof(Exception)),
                "a serializable-convention exception must derive directly from System.Exception");
        });
    }

    [Test]
    public void TenantLastRegionException_carries_the_tenant_id()
    {
        var exception = new TenantLastRegionException("acme");

        Assert.Multiple(() =>
        {
            Assert.That(exception.TenantId, Is.EqualTo("acme"));
            Assert.That(exception.Message, Does.Contain("acme"));
            Assert.That(exception, Is.InstanceOf<Exception>());
            Assert.That(exception.GetType().BaseType, Is.EqualTo(typeof(Exception)),
                "a serializable-convention exception must derive directly from System.Exception");
        });
    }

    [Test]
    public void TenantRegionStatusDescriptor_round_trips_its_members()
    {
        var descriptor = new TenantRegionStatusDescriptor
        {
            RegionId = "us-east",
            Status = TenantRegionLifecycleStatus.Online,
            IsAllowed = true,
        };

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.RegionId, Is.EqualTo("us-east"));
            Assert.That(descriptor.Status, Is.EqualTo(TenantRegionLifecycleStatus.Online));
            Assert.That(descriptor.IsAllowed, Is.True);
        });
    }

    [Test]
    public void TenantRegionStatusDescriptor_status_defaults_to_none()
    {
        var descriptor = new TenantRegionStatusDescriptor { RegionId = "us-east" };

        Assert.Multiple(() =>
        {
            Assert.That(descriptor.Status, Is.EqualTo(TenantRegionLifecycleStatus.None));
            Assert.That(descriptor.IsAllowed, Is.False);
        });
    }

    [Test]
    public void TenantRegionStatusReport_round_trips_its_members()
    {
        var rows = new[]
        {
            new TenantRegionStatusDescriptor { RegionId = "a", Status = TenantRegionLifecycleStatus.Online, IsAllowed = true },
        };
        var report = new TenantRegionStatusReport { TenantId = "acme", Regions = rows };

        Assert.Multiple(() =>
        {
            Assert.That(report.TenantId, Is.EqualTo("acme"));
            Assert.That(report.Regions, Is.EqualTo(rows));
        });
    }

    [Test]
    public void TenantRegionAuthorizationResult_round_trips_its_members()
    {
        var allowed = new[] { "a", "b" };
        var result = new TenantRegionAuthorizationResult { TenantId = "acme", AllowedRegions = allowed };

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.AllowedRegions, Is.EqualTo(allowed));
        });
    }

    [Test]
    public void TenantResidencyChangeResult_round_trips_its_members()
    {
        var added = new[] { "a" };
        var removed = new[] { "b" };
        var regions = new[]
        {
            new TenantRegionStatusDescriptor { RegionId = "a", Status = TenantRegionLifecycleStatus.Provisioning, IsAllowed = true },
        };
        var result = new TenantResidencyChangeResult
        {
            TenantId = "acme",
            AddedRegions = added,
            RemovedRegions = removed,
            Regions = regions,
        };

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.AddedRegions, Is.EqualTo(added));
            Assert.That(result.RemovedRegions, Is.EqualTo(removed));
            Assert.That(result.Regions, Is.EqualTo(regions));
        });
    }
}
