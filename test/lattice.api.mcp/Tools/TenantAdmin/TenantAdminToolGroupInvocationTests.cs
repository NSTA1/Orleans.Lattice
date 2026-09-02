using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using ModelContextProtocol.Server;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests that drive every <see cref="TenantAdminToolGroup"/> tool's own
/// invocation delegate through <see cref="McpToolInvocation"/>: the body that
/// stamps the caller credential, resolves <see cref="ILatticeTenantAdmin"/> or
/// <see cref="ILatticeTenantRegionAdmin"/> from the request service provider, and
/// forwards the bound arguments to <c>TenantAdminToolInvocations</c>. The sibling
/// <see cref="TenantAdminToolGroupTests"/> covers only the advertised metadata,
/// which never reaches these bodies.
/// </summary>
/// <remarks>
/// The delegates decide which facade each tool binds to - the lifecycle tools
/// resolve <see cref="ILatticeTenantAdmin"/>, the three region tools resolve
/// <see cref="ILatticeTenantRegionAdmin"/> - and <c>set_quotas</c> additionally
/// assembles a <see cref="TenantQuotasDescriptor"/> from six separate bound
/// arguments. Neither is observable from tool metadata, so each test asserts on
/// what actually reached the facade. All deterministic - substituted facades, no
/// cluster, no transport.
/// </remarks>
[TestFixture]
public sealed class TenantAdminToolGroupInvocationTests
{
    private ILatticeTenantAdmin _admin = null!;
    private ILatticeTenantRegionAdmin _regionAdmin = null!;

    [SetUp]
    public void SetUp()
    {
        _admin = Substitute.For<ILatticeTenantAdmin>();
        _regionAdmin = Substitute.For<ILatticeTenantRegionAdmin>();
    }

    private ServiceProvider Services()
        => new ServiceCollection()
            .AddSingleton(_admin)
            .AddSingleton(_regionAdmin)
            .BuildServiceProvider();

    private static McpServerTool Tool(string name)
        => new TenantAdminToolGroup(
                Options.Create(new LatticeApiMcpOptions { EnableTenantAdminControlTools = true }))
            .Tools.Single(t => t.ProtocolTool.Name == name);

    private async Task<T> CallAsync<T>(string name, params (string Name, object? Value)[] args)
    {
        await using var services = Services();
        var result = await McpToolInvocation.CallAsync(
            Tool(name), services, McpToolInvocation.Args(args));
        return result.Structured<T>();
    }

    private static TenantRegionStatusDescriptor Region(
        string regionId, TenantRegionLifecycleStatus status, bool isAllowed = true)
        => new() { RegionId = regionId, Status = status, IsAllowed = isAllowed };

    // ---- lifecycle tools (ILatticeTenantAdmin) -----------------------------

    [Test]
    public async Task Create_tool_delegate_forwards_the_requested_admin_subjects()
    {
        string[] subjects = ["ops@example.com", "sre@example.com"];
        _admin.CreateTenantAsync("acme", Arg.Any<IReadOnlyCollection<string>?>(), Arg.Any<CancellationToken>())
            .Returns(new TenantCreationResult
            {
                TenantId = "acme",
                Status = TenantLifecycleStatus.Active,
                AdminSubjects = subjects,
            });

        var result = await CallAsync<McpTenantCreateResult>(
            "lattice_tenant_create", ("tenantId", "acme"), ("adminSubjects", subjects));

        Assert.That(result.AdminSubjects, Is.EqualTo(subjects));
        await _admin.Received(1).CreateTenantAsync(
            "acme",
            Arg.Is<IReadOnlyCollection<string>?>(s => s != null && s.SequenceEqual(subjects)),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Create_tool_delegate_passes_a_null_subject_list_when_the_argument_is_omitted()
    {
        _admin.CreateTenantAsync("acme", Arg.Any<IReadOnlyCollection<string>?>(), Arg.Any<CancellationToken>())
            .Returns(new TenantCreationResult
            {
                TenantId = "acme",
                Status = TenantLifecycleStatus.Active,
                AdminSubjects = ["caller"],
            });

        var result = await CallAsync<McpTenantCreateResult>("lattice_tenant_create", ("tenantId", "acme"));

        Assert.That(result.TenantId, Is.EqualTo("acme"));
        await _admin.Received(1).CreateTenantAsync("acme", null, Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Suspend_tool_delegate_forwards_the_tenant_id()
    {
        _admin.SuspendTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantStatusChangeResult
            {
                TenantId = "acme",
                PreviousStatus = TenantLifecycleStatus.Active,
                NewStatus = TenantLifecycleStatus.Suspended,
                Changed = true,
            });

        var result = await CallAsync<McpTenantStatusChangeResult>(
            "lattice_tenant_suspend", ("tenantId", "acme"));

        Assert.Multiple(() =>
        {
            Assert.That(result.NewStatus, Is.EqualTo(nameof(TenantLifecycleStatus.Suspended)));
            Assert.That(result.Changed, Is.True);
        });
    }

    [Test]
    public async Task Resume_tool_delegate_forwards_the_tenant_id()
    {
        _admin.ResumeTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantStatusChangeResult
            {
                TenantId = "acme",
                PreviousStatus = TenantLifecycleStatus.Suspended,
                NewStatus = TenantLifecycleStatus.Active,
                Changed = true,
            });

        var result = await CallAsync<McpTenantStatusChangeResult>(
            "lattice_tenant_resume", ("tenantId", "acme"));

        Assert.That(result.NewStatus, Is.EqualTo(nameof(TenantLifecycleStatus.Active)));
    }

    [Test]
    public async Task Delete_tool_delegate_reports_the_cascaded_tree_count()
    {
        _admin.DeleteTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantDeletionResult { TenantId = "acme", CascadedTreeCount = 4 });

        var result = await CallAsync<McpTenantDeleteResult>(
            "lattice_tenant_delete", ("tenantId", "acme"));

        Assert.That(result.CascadedTreeCount, Is.EqualTo(4));
    }

    [Test]
    public async Task Set_quotas_tool_delegate_assembles_the_descriptor_from_every_bound_dimension()
    {
        TenantQuotasDescriptor? captured = null;
        _admin.SetTenantQuotasAsync("acme", Arg.Any<TenantQuotasDescriptor>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var quotas = call.ArgAt<TenantQuotasDescriptor>(1);
                captured = quotas;
                return Task.FromResult(new TenantQuotasUpdateResult
                {
                    TenantId = "acme",
                    Quotas = quotas,
                });
            });

        var result = await CallAsync<McpTenantSetQuotasResult>(
            "lattice_tenant_set_quotas",
            ("tenantId", "acme"),
            ("maxBytes", 1_000L),
            ("maxKeys", 2_000L),
            ("maxMemoryBytes", 3_000L),
            ("maxTreeCount", 4L),
            ("maxOpsPerSecond", 5_000L),
            ("burstPercent", 25));

        Assert.That(captured, Is.Not.Null, "The delegate must build a quotas descriptor and pass it to the facade.");
        var observed = captured!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(observed.MaxBytes, Is.EqualTo(1_000L));
            Assert.That(observed.MaxKeys, Is.EqualTo(2_000L));
            Assert.That(observed.MaxMemoryBytes, Is.EqualTo(3_000L));
            Assert.That(observed.MaxTreeCount, Is.EqualTo(4L));
            Assert.That(observed.MaxOpsPerSecond, Is.EqualTo(5_000L));
            Assert.That(observed.BurstPercent, Is.EqualTo(25));
            Assert.That(result.BurstPercent, Is.EqualTo(25));
        });
    }

    [Test]
    public async Task Set_quotas_tool_delegate_leaves_every_omitted_dimension_unbounded()
    {
        TenantQuotasDescriptor? captured = null;
        _admin.SetTenantQuotasAsync("acme", Arg.Any<TenantQuotasDescriptor>(), Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var quotas = call.ArgAt<TenantQuotasDescriptor>(1);
                captured = quotas;
                return Task.FromResult(new TenantQuotasUpdateResult { TenantId = "acme", Quotas = quotas });
            });

        var result = await CallAsync<McpTenantSetQuotasResult>(
            "lattice_tenant_set_quotas", ("tenantId", "acme"));

        Assert.That(captured, Is.Not.Null);
        var observed = captured!.Value;
        Assert.Multiple(() =>
        {
            Assert.That(observed.MaxBytes, Is.Null, "An omitted ceiling means unbounded on that dimension.");
            Assert.That(observed.MaxKeys, Is.Null);
            Assert.That(observed.MaxMemoryBytes, Is.Null);
            Assert.That(observed.MaxTreeCount, Is.Null);
            Assert.That(observed.MaxOpsPerSecond, Is.Null);
            Assert.That(observed.BurstPercent, Is.Zero);
            Assert.That(result.IsUnbounded, Is.True);
        });
    }

    // ---- region tools (ILatticeTenantRegionAdmin) --------------------------

    [Test]
    public async Task Authorize_regions_tool_delegate_binds_the_region_admin_facade()
    {
        string[] regions = ["eu-west", "us-east"];
        _regionAdmin.AuthorizeAllowedRegionsAsync(
                "acme", Arg.Any<IReadOnlyCollection<string>>(), Arg.Any<CancellationToken>())
            .Returns(new TenantRegionAuthorizationResult { TenantId = "acme", AllowedRegions = regions });

        var result = await CallAsync<McpTenantRegionAuthorizationResult>(
            "lattice_tenant_authorize_regions",
            ("tenantId", "acme"),
            ("allowedRegions", regions));

        Assert.That(result.AllowedRegions, Is.EqualTo(regions));
        await _regionAdmin.Received(1).AuthorizeAllowedRegionsAsync(
            "acme",
            Arg.Is<IReadOnlyCollection<string>>(r => r.SequenceEqual(regions)),
            Arg.Any<CancellationToken>());
        await _admin.DidNotReceiveWithAnyArgs().CreateTenantAsync(default!, default, default);
    }

    [Test]
    public async Task Authorize_regions_tool_delegate_substitutes_an_empty_set_for_a_null_list()
    {
        _regionAdmin.AuthorizeAllowedRegionsAsync(
                "acme", Arg.Any<IReadOnlyCollection<string>>(), Arg.Any<CancellationToken>())
            .Returns(new TenantRegionAuthorizationResult { TenantId = "acme", AllowedRegions = [] });

        var result = await CallAsync<McpTenantRegionAuthorizationResult>(
            "lattice_tenant_authorize_regions",
            ("tenantId", "acme"),
            ("allowedRegions", null));

        Assert.That(result.AllowedRegions, Is.Empty);
        await _regionAdmin.Received(1).AuthorizeAllowedRegionsAsync(
            "acme",
            Arg.Is<IReadOnlyCollection<string>>(r => r.Count == 0),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Set_residency_tool_delegate_reports_the_added_and_removed_regions()
    {
        _regionAdmin.SetResidencyAsync("acme", Arg.Any<IReadOnlyCollection<string>>(), Arg.Any<CancellationToken>())
            .Returns(new TenantResidencyChangeResult
            {
                TenantId = "acme",
                AddedRegions = ["us-east"],
                RemovedRegions = ["eu-west"],
                Regions =
                [
                    Region("eu-west", TenantRegionLifecycleStatus.Draining),
                    Region("us-east", TenantRegionLifecycleStatus.Provisioning),
                ],
            });

        var result = await CallAsync<McpTenantResidencyChangeResult>(
            "lattice_tenant_set_residency",
            ("tenantId", "acme"),
            ("residencyRegions", new[] { "us-east" }));

        Assert.Multiple(() =>
        {
            Assert.That(result.AddedRegions, Is.EqualTo(new[] { "us-east" }));
            Assert.That(result.RemovedRegions, Is.EqualTo(new[] { "eu-west" }));
            Assert.That(
                result.Regions.Select(r => r.Status),
                Is.EqualTo(new[]
                {
                    nameof(TenantRegionLifecycleStatus.Draining),
                    nameof(TenantRegionLifecycleStatus.Provisioning),
                }),
                "A newly added region reports Provisioning, not Online - the transition is asynchronous.");
        });
    }

    [Test]
    public async Task Set_residency_tool_delegate_substitutes_an_empty_set_for_a_null_list()
    {
        _regionAdmin.SetResidencyAsync("acme", Arg.Any<IReadOnlyCollection<string>>(), Arg.Any<CancellationToken>())
            .Returns(new TenantResidencyChangeResult
            {
                TenantId = "acme",
                AddedRegions = [],
                RemovedRegions = [],
                Regions = [],
            });

        await CallAsync<McpTenantResidencyChangeResult>(
            "lattice_tenant_set_residency",
            ("tenantId", "acme"),
            ("residencyRegions", null));

        await _regionAdmin.Received(1).SetResidencyAsync(
            "acme",
            Arg.Is<IReadOnlyCollection<string>>(r => r.Count == 0),
            Arg.Any<CancellationToken>());
    }

    [Test]
    public async Task Region_status_tool_delegate_projects_every_row()
    {
        _regionAdmin.GetTenantRegionStatusAsync("acme", Arg.Any<CancellationToken>())
            .Returns(new TenantRegionStatusReport
            {
                TenantId = "acme",
                Regions =
                [
                    Region("eu-west", TenantRegionLifecycleStatus.Online),
                    Region("us-east", TenantRegionLifecycleStatus.None, isAllowed: true),
                ],
            });

        var result = await CallAsync<McpTenantRegionStatusResult>(
            "lattice_tenant_region_status", ("tenantId", "acme"));

        Assert.Multiple(() =>
        {
            Assert.That(result.TenantId, Is.EqualTo("acme"));
            Assert.That(result.Regions.Select(r => r.RegionId), Is.EqualTo(new[] { "eu-west", "us-east" }));
            Assert.That(result.Regions[0].Status, Is.EqualTo(nameof(TenantRegionLifecycleStatus.Online)));
            Assert.That(result.Regions[1].IsAllowed, Is.True,
                "An allowed region the tenant has not moved into yet is still reported.");
        });
    }

    // ---- the facade's own fail-closed gate ---------------------------------

    [Test]
    public void Delegate_surfaces_the_facades_fail_closed_denial()
    {
        _admin.SuspendTenantAsync("acme", Arg.Any<CancellationToken>())
            .Returns<Task<TenantStatusChangeResult>>(
                _ => throw new LatticeAuthorizationDeniedException("denied"));

        Assert.That(
            async () => await CallAsync<McpTenantStatusChangeResult>(
                "lattice_tenant_suspend", ("tenantId", "acme")),
            Throws.InstanceOf<LatticeAuthorizationDeniedException>(),
            "The MCP layer adds no authorization path: the facade's denial must surface unchanged.");
    }
}
