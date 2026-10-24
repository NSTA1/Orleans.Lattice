using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>Lifecycle replies and admission redirects belong to the page that started them.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyLifecycleOwnershipTests : TenancyTestContext
{
    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Status_reply_after_navigation_does_not_change_the_current_tenant(bool resume, bool fault)
    {
        Cluster.WithTenant("globex");
        Cluster.Tenants["acme"].Status = resume ? TenantLifecycleStatus.Suspended : TenantLifecycleStatus.Active;
        var method = resume ? nameof(Cluster.ResumeTenantAsync) : nameof(Cluster.SuspendTenantAsync);
        var previous = Cluster.Hold(method);
        UseTenancyAs();
        await Catalog.GetTenantsAsync(CancellationToken.None);
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => TenancyForms.Button(cut, resume ? "Resume tenant" : "Suspend tenant"));
        if (!resume)
        {
            TenancyForms.Button(cut, "Suspend tenant").Click();
        }
        TenancyForms.Button(cut, resume ? "Resume tenant" : "Suspend").Click();
        cut.WaitUntil(() => Assert.That(Cluster.Calls, Does.Contain(method)));

        Navigation.NavigateTo("tenancy/globex");
        cut.Render();
        cut.WaitUntil(() => Assert.That(State(cut), Is.EqualTo("Active")));
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult();
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => State(cut) != "Active" || Services.GetToasts().Any(),
            TimeSpan.FromSeconds(1)), Is.False, "acme's mutation must not change globex or publish a toast under its name");
        Assert.That(TenancyForms.Button(cut, "Suspend tenant").HasAttribute("disabled"), Is.False,
            "the old request must not leave the new tenant busy");
        if (!fault)
        {
            var tenants = await Catalog.GetTenantsAsync(CancellationToken.None);
            Assert.That(tenants.Single(tenant => tenant.TenantId == "acme").Status,
                Is.EqualTo(resume ? TenantLifecycleStatus.Active : TenantLifecycleStatus.Suspended),
                "accepted mutations still invalidate the shared catalogue after navigation");
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Delete_reply_after_navigation_does_not_redirect_or_notify_for_the_new_tenant(bool fault)
    {
        Cluster.WithTenant("globex");
        var previous = Cluster.Hold(nameof(Cluster.DeleteTenantAsync));
        UseTenancyAs();
        await Catalog.GetTenantsAsync(CancellationToken.None);
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Delete tenant"));
        TenancyForms.Button(cut, "Delete tenant").Click();
        TenancyForms.Type(cut, "Tenant name", "acme");
        cut.Find("form.lt-confirm").Submit();
        cut.WaitUntil(() => Assert.That(Cluster.Calls, Does.Contain(nameof(Cluster.DeleteTenantAsync))));

        Navigation.NavigateTo("tenancy/globex");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find("h1").TextContent, Is.EqualTo("globex")));
        var current = Navigation.Uri;
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult();
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => Navigation.Uri != current || Services.GetToasts().Any(),
            TimeSpan.FromSeconds(1)), Is.False, "deleting acme must not redirect away from globex or claim globex was deleted");
        Assert.That(Cluster.Tenants.ContainsKey("globex"), Is.True);
        if (!fault)
        {
            cut.WaitUntil(() => Assert.That(Cluster.Tenants.ContainsKey("acme"), Is.False,
                "leaving the page does not undo an accepted deletion"));
            var tenants = await Catalog.GetTenantsAsync(CancellationToken.None);
            Assert.That(tenants.Select(tenant => tenant.TenantId), Does.Not.Contain("acme"),
                "the deleted tenant must also disappear from the shared catalogue");
        }
    }

    [TestCase(false)]
    [TestCase(true)]
    public void Lifecycle_confirmation_is_closed_when_the_tenant_changes(bool delete)
    {
        Cluster.WithTenant("globex");
        UseTenancyAs();
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Delete tenant"));
        TenancyForms.Button(cut, delete ? "Delete tenant" : "Suspend tenant").Click();
        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Is.Not.Empty));

        Navigation.NavigateTo("tenancy/globex");
        cut.Render();

        cut.WaitUntil(() => Assert.That(cut.FindAll("[role=alertdialog]"), Is.Empty,
            "confirmation of a destructive action on acme must not turn into confirmation for globex"));
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Regions_admission_after_disposal_does_not_redirect_the_next_page(bool fault)
    {
        var previous = new TaskCompletionSource<TenantDescriptor>(TaskCreationOptions.RunContinuationsAsynchronously);
        var self = Substitute.For<ILatticeTenantSelfService>();
        self.GetCurrentTenantAsync(Arg.Any<CancellationToken>()).Returns(previous.Task);
        Services.AddKeyedSingleton(ShellFacades.Key, self);
        UseTenancyAs(isOperator: false);
        RenderAt<TenancyRegionsPage>("tenancy/acme/regions");
        Assert.That(self.ReceivedCalls(), Is.Not.Empty);

        await DisposeComponentsAsync();
        Navigation.NavigateTo("tenancy");
        var current = Navigation.Uri;
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult(new TenantDescriptor { TenantId = "acme" });
        }

        Assert.That(await TestPoll.TryUntilAsync(() => Navigation.Uri != current, TimeSpan.FromSeconds(1)),
            Is.False, "a disposed Regions page must not redirect the current page");
        var call = self.ReceivedCalls().Single(call => call.GetMethodInfo().Name == nameof(self.GetCurrentTenantAsync));
        Assert.That(((CancellationToken)call.GetArguments()[0]!).IsCancellationRequested, Is.True);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Regions_superseded_admission_does_not_replace_the_current_tenants_section(bool fault)
    {
        var previous = new TaskCompletionSource<TenantDescriptor>(TaskCreationOptions.RunContinuationsAsynchronously);
        var self = Substitute.For<ILatticeTenantSelfService>();
        self.GetCurrentTenantAsync(Arg.Any<CancellationToken>())
            .Returns(previous.Task, Task.FromResult(new TenantDescriptor { TenantId = "acme" }));
        Services.AddKeyedSingleton(ShellFacades.Key, self);
        Cluster.WithTenant("globex");
        UseTenancyAs();
        var cut = RenderAt<TenancyRegionsPage>("tenancy/acme/regions");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Is.Not.Empty));

        Navigation.NavigateTo("tenancy/globex/regions");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-tenancy-section"), Is.Not.Empty));
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult(new TenantDescriptor { TenantId = "acme" });
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("This tenant could not be read", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False);
        Assert.That(cut.FindComponent<TenancyRegions>().Instance.TenantId, Is.EqualTo("globex"));
    }

    [TestCase(false, false)]
    [TestCase(false, true)]
    [TestCase(true, false)]
    [TestCase(true, true)]
    public async Task Lifecycle_reply_after_disposal_does_not_notify_or_navigate(bool delete, bool fault)
    {
        var method = delete ? nameof(Cluster.DeleteTenantAsync) : nameof(Cluster.SuspendTenantAsync);
        var previous = Cluster.Hold(method);
        UseTenancyAs();
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Delete tenant"));
        TenancyForms.Button(cut, delete ? "Delete tenant" : "Suspend tenant").Click();
        if (delete)
        {
            TenancyForms.Type(cut, "Tenant name", "acme");
            cut.Find("form.lt-confirm").Submit();
        }
        else
        {
            TenancyForms.Button(cut, "Suspend").Click();
        }
        cut.WaitUntil(() => Assert.That(Cluster.Calls, Does.Contain(method)));

        await DisposeComponentsAsync();
        Navigation.NavigateTo("tenancy");
        var current = Navigation.Uri;
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult();
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => Navigation.Uri != current || Services.GetToasts().Any(), TimeSpan.FromSeconds(1)), Is.False);
    }

    [Test]
    public async Task Previous_status_completion_does_not_clear_the_current_mutations_busy_state()
    {
        Cluster.WithTenant("globex", TenantLifecycleStatus.Suspended);
        var previous = Cluster.Hold(nameof(Cluster.SuspendTenantAsync));
        var current = Cluster.Hold(nameof(Cluster.ResumeTenantAsync));
        UseTenancyAs();
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => TenancyForms.Button(cut, "Suspend tenant"));
        TenancyForms.Button(cut, "Suspend tenant").Click();
        TenancyForms.Button(cut, "Suspend").Click();
        cut.WaitUntil(() => Assert.That(Cluster.Calls, Does.Contain(nameof(Cluster.SuspendTenantAsync))));

        Navigation.NavigateTo("tenancy/globex");
        cut.Render();
        cut.WaitUntil(() => Assert.That(TenancyForms.Button(cut, "Resume tenant").HasAttribute("disabled"), Is.False));
        TenancyForms.Button(cut, "Resume tenant").Click();
        cut.WaitUntil(() => Assert.That(Cluster.Calls, Does.Contain(nameof(Cluster.ResumeTenantAsync))));
        previous.SetResult();

        Assert.That(await TestPoll.TryUntilAsync(
            () => !TenancyForms.Button(cut, "Resume tenant").HasAttribute("disabled"),
            TimeSpan.FromSeconds(1)), Is.False);
        current.SetResult();
        cut.WaitUntil(() =>
        {
            Assert.That(State(cut), Is.EqualTo("Active"));
            Assert.That(Services.GetToasts().Single().Message, Is.EqualTo("Tenant globex is active."));
        });
    }

    private static string State(IRenderedComponent<TenancyTenantPage> cut) =>
        cut.FindAll(".lt-dl__row").Single(row => row.QuerySelector("dt")!.TextContent == "State")
            .QuerySelector("dd")!.TextContent.Trim();
}
