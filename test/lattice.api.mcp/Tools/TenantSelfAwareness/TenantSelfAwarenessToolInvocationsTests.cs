using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="TenantSelfAwarenessToolInvocations"/>, the pure adapter
/// layer between the read-only tenant self-awareness MCP tools and the
/// <see cref="ILatticeTenantSelfService"/> facade. Proves each of the three
/// operations delegates to the facade and shapes the compact MCP DTO (stringifying
/// the lifecycle status), that a null facade is rejected, and that a fail-closed
/// not-found the facade raises surfaces unchanged (the MCP layer adds no
/// authorization path of its own). All deterministic against a substituted facade -
/// no cluster, no ordering-by-timing.
/// </summary>
[TestFixture]
public sealed class TenantSelfAwarenessToolInvocationsTests
{
    [Test]
    public async Task GetCurrent_delegates_to_the_facade_and_shapes_the_result()
    {
        var service = Substitute.For<ILatticeTenantSelfService>();
        service.GetCurrentTenantAsync(Arg.Any<CancellationToken>())
            .Returns(new TenantDescriptor
            {
                TenantId = "acme",
                Status = TenantLifecycleStatus.Active,
                IsDefault = false,
            });

        var result = await TenantSelfAwarenessToolInvocations.GetCurrentTenantAsync(service, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Status, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
            Assert.That(result.IsDefault, Is.False);
        });
        await service.Received(1).GetCurrentTenantAsync(Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task List_delegates_to_the_facade_and_shapes_each_row()
    {
        var service = Substitute.For<ILatticeTenantSelfService>();
        service.ListAccessibleTenantsAsync(Arg.Any<CancellationToken>())
            .Returns(new[]
            {
                new TenantDescriptor { TenantId = "alpha", Status = TenantLifecycleStatus.Active, IsDefault = false },
                new TenantDescriptor { TenantId = "beta", Status = TenantLifecycleStatus.Suspended, IsDefault = false },
            });

        var result = await TenantSelfAwarenessToolInvocations.ListAccessibleTenantsAsync(service, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.Tenants.Select(t => t.TenantId), Is.EqualTo(new[] { "alpha", "beta" }));
            Assert.That(result.Tenants[1].Status, Is.EqualTo(nameof(TenantLifecycleStatus.Suspended)));
        });
    }

    [Test]
    public async Task List_of_no_accessible_tenants_yields_empty_result()
    {
        var service = Substitute.For<ILatticeTenantSelfService>();
        service.ListAccessibleTenantsAsync(Arg.Any<CancellationToken>())
            .Returns(Array.Empty<TenantDescriptor>());

        var result = await TenantSelfAwarenessToolInvocations.ListAccessibleTenantsAsync(service, CancellationToken.None);

        Assert.That(result.Tenants, Is.Empty);
    }

    [Test]
    public async Task Get_delegates_to_the_facade_and_shapes_the_report_with_regions()
    {
        var service = Substitute.For<ILatticeTenantSelfService>();
        service.GetTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantStatusReport
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
                ],
            });

        var result = await TenantSelfAwarenessToolInvocations.GetTenantAsync(service, "acme", CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Status, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
            Assert.That(result.IsDefault, Is.False);
            Assert.That(result.Regions, Has.Count.EqualTo(1));
            Assert.That(result.Regions[0].RegionId, Is.EqualTo("region-a"));
            Assert.That(result.Regions[0].Status, Is.EqualTo(nameof(TenantRegionLifecycleStatus.Online)));
            Assert.That(result.Regions[0].IsAllowed, Is.True);
        });
    }

    [Test]
    public void Get_fail_closed_not_found_surfaces_unchanged()
    {
        var service = Substitute.For<ILatticeTenantSelfService>();
        service.GetTenantAsync("secret", Arg.Any<CancellationToken>())
            .Returns<Task<TenantStatusReport>>(_ => throw new TenantNotFoundException("secret"));

        Assert.That(
            async () => await TenantSelfAwarenessToolInvocations.GetTenantAsync(service, "secret", CancellationToken.None),
            Throws.TypeOf<TenantNotFoundException>());
    }

    [Test]
    public void Null_service_is_rejected_on_every_invocation()
    {
        Assert.Multiple(() =>
        {
            Assert.That(
                async () => await TenantSelfAwarenessToolInvocations.GetCurrentTenantAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await TenantSelfAwarenessToolInvocations.ListAccessibleTenantsAsync(null!, CancellationToken.None),
                Throws.ArgumentNullException);
            Assert.That(
                async () => await TenantSelfAwarenessToolInvocations.GetTenantAsync(null!, "acme", CancellationToken.None),
                Throws.ArgumentNullException);
        });
    }
}
