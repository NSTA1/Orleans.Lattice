using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Core.Tenancy;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Data;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Suggestions;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>
/// A tenant created, suspended, resumed or deleted here is reflected in the
/// remembered tenant suggestions at once rather than once they go stale, and a
/// deleted tenant's soft-deleted trees leave the remembered tree lists.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancySuggestionFreshnessTests : TenancyTestContext
{
    [Test]
    public void A_created_tenant_is_suggested_at_once()
    {
        UseTenancyAs(isOperator: true);
        ListTenantsLive();
        var cut = RenderAt<TenancyDirectoryPage>("tenancy?new=true");
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-tenancy-form"), Has.Count.EqualTo(1)));
        Assert.That(Suggested(), Does.Not.Contain("globex"));

        TenancyForms.Type(cut, "Tenant id", "globex");
        cut.Find("form.lt-tenancy-form").Submit();

        cut.WaitUntil(() => Assert.That(Cluster.Tenants.ContainsKey("globex"), Is.True));
        Assert.That(Suggested(), Does.Contain("globex"));
    }

    [Test]
    public void A_deleted_tenant_is_no_longer_suggested_and_its_trees_leave_the_remembered_lists()
    {
        UseTenancyAs(isOperator: true);
        Cluster.WithTenant("globex");
        ListTenantsLive();
        var directoryChanges = 0;
        Services.GetRequiredService<DataDirectory>().Changed += () => directoryChanges++;
        var cut = RenderAt<TenancyTenantPage>("tenancy/globex");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Delete tenant"));
        Assert.That(Suggested(), Does.Contain("globex"));

        TenancyForms.Button(cut, "Delete tenant").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("form.lt-confirm"), Is.Not.Empty));
        TenancyForms.Type(cut, "Tenant name", "globex");
        cut.Find("form.lt-confirm").Submit();

        cut.WaitUntil(() => Assert.That(Cluster.Tenants.ContainsKey("globex"), Is.False));
        Assert.Multiple(() =>
        {
            Assert.That(Suggested(), Does.Not.Contain("globex"));
            Assert.That(directoryChanges, Is.GreaterThan(0), "the soft-deleted trees leave the remembered tree lists");
        });
    }

    [Test]
    public void Suspending_and_resuming_a_tenant_forgets_the_remembered_suggestions()
    {
        UseTenancyAs(isOperator: true);
        var source = Services.GetRequiredService<IExplorerAccessibleTenantSource>();
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Suspend tenant"));
        _ = Suggested();
        var before = source.ReceivedCalls().Count();

        TenancyForms.Button(cut, "Suspend tenant").Click();
        cut.WaitUntil(() => TenancyForms.Button(cut, "Suspend"));
        TenancyForms.Button(cut, "Suspend").Click();
        cut.WaitUntil(() => Assert.That(Cluster.Tenants["acme"].Status, Is.EqualTo(TenantLifecycleStatus.Suspended)));
        _ = Suggested();
        var afterSuspend = source.ReceivedCalls().Count();
        Assert.That(afterSuspend, Is.GreaterThan(before), "the suggestions are re-read after a suspend");

        TenancyForms.Button(cut, "Resume tenant").Click();
        cut.WaitUntil(() => Assert.That(Cluster.Tenants["acme"].Status, Is.EqualTo(TenantLifecycleStatus.Active)));
        _ = Suggested();
        Assert.That(source.ReceivedCalls().Count(), Is.GreaterThan(afterSuspend), "and again after a resume");
    }

    private void ListTenantsLive()
    {
        var source = Services.GetRequiredService<IExplorerAccessibleTenantSource>();
        source.GetAccessibleTenantsAsync(Arg.Any<CancellationToken>()).Returns(_ =>
            new ValueTask<IReadOnlyList<ExplorerTenantId>>(
                [.. Cluster.Tenants.Keys.Where(tenant => tenant != TenantId.DefaultId).Select(tenant => new ExplorerTenantId(tenant))]));
    }

    private IReadOnlyList<string> Suggested()
    {
        var set = Services.GetRequiredService<ExplorerSuggestions>().Tenants.SuggestAsync(string.Empty, 50, default).AsTask().GetAwaiter().GetResult();
        return [.. set.Items.Select(item => item.Value)];
    }
}
