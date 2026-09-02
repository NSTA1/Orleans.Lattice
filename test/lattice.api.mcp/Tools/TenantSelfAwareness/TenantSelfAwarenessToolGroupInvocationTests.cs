using Microsoft.Extensions.DependencyInjection;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests that drive every <see cref="TenantSelfAwarenessToolGroup"/> tool's
/// own invocation delegate through <see cref="McpToolInvocation"/>: the body that
/// stamps the caller credential, resolves
/// <see cref="ILatticeTenantSelfService"/> from the request service provider, and
/// forwards the bound arguments to <c>TenantSelfAwarenessToolInvocations</c>. The
/// sibling <see cref="TenantSelfAwarenessToolGroupTests"/> covers only the
/// advertised metadata, which never reaches these bodies.
/// </summary>
/// <remarks>
/// All deterministic against a substituted facade - no cluster, no transport.
/// </remarks>
[TestFixture]
public sealed class TenantSelfAwarenessToolGroupInvocationTests
{
    private ILatticeTenantSelfService _service = null!;

    [SetUp]
    public void SetUp() => _service = Substitute.For<ILatticeTenantSelfService>();

    private ServiceProvider Services()
        => new ServiceCollection().AddSingleton(_service).BuildServiceProvider();

    private McpServerTool Tool(string name)
        => new TenantSelfAwarenessToolGroup([_service]).Tools.Single(t => t.ProtocolTool.Name == name);

    private async Task<T> CallAsync<T>(string name, params (string Name, object? Value)[] args)
    {
        await using var services = Services();
        var result = await McpToolInvocation.CallAsync(
            Tool(name), services, McpToolInvocation.Args(args));
        return result.Structured<T>();
    }

    [Test]
    public async Task Current_tool_delegate_reports_the_ambient_tenant()
    {
        _service.GetCurrentTenantAsync(Arg.Any<CancellationToken>())
            .Returns(new TenantDescriptor
            {
                TenantId = "acme",
                Status = TenantLifecycleStatus.Active,
                IsDefault = false,
            });

        var result = await CallAsync<McpTenantDescriptor>("lattice_tenant_current");

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Status, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
            Assert.That(result.IsDefault, Is.False);
        });
    }

    [Test]
    public async Task List_tool_delegate_projects_every_accessible_tenant()
    {
        _service.ListAccessibleTenantsAsync(Arg.Any<CancellationToken>())
            .Returns(new[]
            {
                new TenantDescriptor { TenantId = "alpha", Status = TenantLifecycleStatus.Active, IsDefault = false },
                new TenantDescriptor { TenantId = "beta", Status = TenantLifecycleStatus.Suspended, IsDefault = false },
            });

        var result = await CallAsync<McpTenantListResult>("lattice_tenant_list");

        Assert.Multiple(() =>
        {
            Assert.That(result.Tenants.Select(t => t.TenantId), Is.EqualTo(new[] { "alpha", "beta" }));
            Assert.That(result.Tenants[1].Status, Is.EqualTo(nameof(TenantLifecycleStatus.Suspended)));
        });
    }

    [Test]
    public async Task List_tool_delegate_returns_an_empty_list_for_a_caller_who_sees_nothing()
    {
        _service.ListAccessibleTenantsAsync(Arg.Any<CancellationToken>())
            .Returns(Array.Empty<TenantDescriptor>());

        var result = await CallAsync<McpTenantListResult>("lattice_tenant_list");

        Assert.That(result.Tenants, Is.Empty,
            "A caller who can see no tenant gets an empty list, never another caller's tenant.");
    }

    [Test]
    public async Task Get_tool_delegate_forwards_the_tenant_id_and_projects_regions_and_quotas()
    {
        _service.GetTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantStatusReport
            {
                TenantId = "acme",
                Status = TenantLifecycleStatus.Active,
                IsDefault = false,
                Regions =
                [
                    new TenantRegionStatusDescriptor
                    {
                        RegionId = "eu-west",
                        Status = TenantRegionLifecycleStatus.Online,
                        IsAllowed = true,
                    },
                ],
                Quotas = new TenantQuotasDescriptor { MaxBytes = 1_024, BurstPercent = 10 },
            });

        var result = await CallAsync<McpTenantStatusResult>("lattice_tenant_get", ("tenantId", "acme"));

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Regions.Single().RegionId, Is.EqualTo("eu-west"));
            Assert.That(result.Regions.Single().Status, Is.EqualTo(nameof(TenantRegionLifecycleStatus.Online)));
            Assert.That(result.Quotas.MaxBytes, Is.EqualTo(1_024));
            Assert.That(result.Quotas.BurstPercent, Is.EqualTo(10));
        });
        await _service.Received(1).GetTenantAsync("acme", Arg.Any<CancellationToken>());
    }

    [Test]
    public void Get_tool_delegate_surfaces_the_facades_fail_closed_not_found()
    {
        _service.GetTenantAsync("hidden", Arg.Any<CancellationToken>())
            .Returns<Task<TenantStatusReport>>(_ => throw new KeyNotFoundException("no such tenant"));

        Assert.That(
            async () => await CallAsync<McpTenantStatusResult>("lattice_tenant_get", ("tenantId", "hidden")),
            Throws.InstanceOf<KeyNotFoundException>(),
            "A tenant outside the caller's authority is indistinguishable from an absent one, and the "
            + "facade's fail-closed not-found must surface unchanged.");
    }
}
