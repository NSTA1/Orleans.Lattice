using System.Collections.Immutable;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Tests.Connection;
using Orleans.Lattice.Explorer.UI.Areas.Apps.Catalogue;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.Catalogue;

/// <summary>
/// The Apps area's memoised probe is keyed on the tenant the circuit asserts: a
/// tenant switch re-probes, and an answer read under one tenant is never served
/// under another.
/// </summary>
[TestFixture]
public sealed class AppsAccessTenantTests
{
    [Test]
    public async Task A_tenant_switch_re_probes_and_never_serves_the_previous_tenants_apps()
    {
        var (access, provider, workspace) = Create("acme");

        var acme = await access.GetAsync();
        provider.Set("globex");
        var staleRead = access.Current;
        var globex = await access.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(acme.MyApps.Select(app => app.Slug), Is.EqualTo(new[] { "acme-board" }));
            Assert.That(staleRead, Is.Null, "acme's snapshot is not the current one under globex");
            Assert.That(globex.MyApps.Select(app => app.Slug), Is.EqualTo(new[] { "globex-board" }));
            Assert.That(access.Current, Is.SameAs(globex));
            Assert.That(workspace.ReceivedCalls().Count(), Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Returning_to_a_tenant_reads_it_again_rather_than_trusting_an_old_memo()
    {
        var (access, provider, workspace) = Create("acme");

        await access.GetAsync();
        provider.Set("globex");
        await access.GetAsync();
        provider.Set("acme");
        var again = await access.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(again.MyApps.Select(app => app.Slug), Is.EqualTo(new[] { "acme-board" }));
            Assert.That(workspace.ReceivedCalls().Count(), Is.EqualTo(3));
        });
    }

    [Test]
    public async Task An_unchanged_tenant_keeps_one_probe()
    {
        var (access, _, workspace) = Create("acme");

        var first = await access.GetAsync();
        var second = await access.GetAsync();

        Assert.Multiple(() =>
        {
            Assert.That(second, Is.SameAs(first));
            Assert.That(workspace.ReceivedCalls().Count(), Is.EqualTo(1));
        });
    }

    [Test]
    public async Task Invalidate_re_probes_under_the_current_tenant()
    {
        var (access, provider, _) = Create("acme");
        await access.GetAsync();

        provider.Set("globex");
        access.Invalidate();

        Assert.That((await access.GetAsync()).MyApps.Select(app => app.Slug), Is.EqualTo(new[] { "globex-board" }));
    }

    [Test]
    public void A_lifecycle_change_settles_only_for_the_caller_that_made_it()
    {
        // #4414: the settling record was filed under the slug alone, so an install
        // under acme made the same slug's reads settle under globex too.
        var time = new ManualTimeProvider();
        var (access, provider, _) = Create("acme", time);

        access.Invalidate("crm");
        var underAcme = access.ChangedRecently("crm");
        provider.Set("globex");
        var underGlobex = access.ChangedRecently("crm");
        provider.Set("acme");
        var backUnderAcme = access.ChangedRecently("crm");
        time.Advance(AppsAccess.SettlingWindow + TimeSpan.FromSeconds(1));

        Assert.Multiple(() =>
        {
            Assert.That(underAcme, Is.True);
            Assert.That(underGlobex, Is.False, "globex never changed crm");
            Assert.That(backUnderAcme, Is.True, "acme's own change still settles for acme");
            Assert.That(access.ChangedRecently("crm"), Is.False, "the window has passed");
            Assert.That(access.ChangedRecently("board"), Is.False);
        });
    }

    [Test]
    public void Without_tenancy_the_facades_assert_no_tenant()
    {
        using var services = new ServiceCollection().BuildServiceProvider();

        Assert.That(new AppsFacades(services).AssertedTenant, Is.Null);
    }

    private static (AppsAccess Access, FakeActiveTenantProvider Provider, ILatticeAppWorkspace Workspace) Create(string tenant, TimeProvider? time = null)
    {
        var provider = new FakeActiveTenantProvider(tenant);
        var workspace = Substitute.For<ILatticeAppWorkspace>();

        // The cluster scopes the caller's apps to the asserted tenant.
        workspace.ListMyAppsAsync(Arg.Any<CancellationToken>()).Returns(_ => Task.FromResult<ImmutableArray<WorkspaceAppSummary>>(
            [new WorkspaceAppSummary { Slug = provider.AssertedTenant + "-board", Version = "1.0.0" }]));

        var services = new ServiceCollection()
            .AddSingleton<ILatticeActiveTenantProvider>(provider)
            .AddKeyedSingleton(ShellFacades.Key, workspace)
            .BuildServiceProvider();
        return (new AppsAccess(new AppsFacades(services), time: time), provider, workspace);
    }
}
