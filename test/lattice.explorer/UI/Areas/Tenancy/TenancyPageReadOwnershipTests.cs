using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>Admission and workspace reads belong to the address that started them.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancyPageReadOwnershipTests : TenancyTestContext
{
    [TestCase(typeof(TenancyQuotaPage), "quota")]
    [TestCase(typeof(TenancyGrantsPage), "sharing")]
    public async Task Admission_superseded_fault_does_not_hide_the_current_section(Type page, string section)
    {
        var previous = new TaskCompletionSource<TenantDescriptor>(TaskCreationOptions.RunContinuationsAsynchronously);
        var self = Substitute.For<ILatticeTenantSelfService>();
        self.GetCurrentTenantAsync(Arg.Any<CancellationToken>())
            .Returns(previous.Task, Task.FromResult(new TenantDescriptor { TenantId = "acme" }));
        Services.AddKeyedSingleton(ShellFacades.Key, self);
        Cluster.WithTenant("globex");
        UseTenancyAs();
        Navigation.NavigateTo($"tenancy/acme/{section}");
        var cut = Render<DynamicComponent>(parameters => parameters.Add(component => component.Type, page));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));

        Navigation.NavigateTo($"tenancy/globex/{section}");
        if (page == typeof(TenancyQuotaPage))
        {
            cut.FindComponent<TenancyQuotaPage>().Render();
        }
        else
        {
            cut.FindComponent<TenancyGrantsPage>().Render();
        }
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-tenancy-section"), Is.Not.Empty));
        previous.SetException(new TimeoutException());

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("This tenant could not be read", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False, "the previous admission's failure must not replace globex's section");
    }

    [TestCase(typeof(TenancyQuotaPage), "quota")]
    [TestCase(typeof(TenancyGrantsPage), "sharing")]
    public async Task Admission_disposal_does_not_redirect_the_next_page(Type page, string section)
    {
        var previous = new TaskCompletionSource<TenantDescriptor>(TaskCreationOptions.RunContinuationsAsynchronously);
        var self = Substitute.For<ILatticeTenantSelfService>();
        self.GetCurrentTenantAsync(Arg.Any<CancellationToken>()).Returns(previous.Task);
        Services.AddKeyedSingleton(ShellFacades.Key, self);
        UseTenancyAs(isOperator: false);
        Navigation.NavigateTo($"tenancy/acme/{section}");
        var cut = Render<DynamicComponent>(parameters => parameters.Add(component => component.Type, page));
        cut.WaitUntil(() => Assert.That(self.ReceivedCalls(), Is.Not.Empty));

        await DisposeComponentsAsync();
        Navigation.NavigateTo("tenancy");
        var next = Navigation.Uri;
        previous.SetResult(new TenantDescriptor { TenantId = "acme" });

        Assert.That(await TestPoll.TryUntilAsync(() => Navigation.Uri != next, TimeSpan.FromSeconds(1)),
            Is.False, "a page that was left must not redirect the page now being shown");
        Assert.That(ReadToken(self).IsCancellationRequested, Is.True);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Workspace_superseded_status_or_fault_does_not_replace_the_current_tenant(bool fault)
    {
        var previous = new TaskCompletionSource<TenantStatusReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var self = WorkspaceSelf();
        self.GetTenantAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        self.GetTenantAsync("globex", Arg.Any<CancellationToken>()).Returns(Status("globex", TenantLifecycleStatus.Active));
        Cluster.WithTenant("globex");
        UseTenancyAs();
        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");
        cut.WaitUntil(() => Assert.That(self.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(self.GetTenantAsync)), Is.True));

        Navigation.NavigateTo("t/globex/tenancy");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-pill__text").Select(pill => pill.TextContent), Does.Contain("Active")));
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult(Status("acme", TenantLifecycleStatus.Suspended));
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("Suspended", StringComparison.Ordinal)
                || cut.Markup.Contains("This tenant could not be read", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False, "acme's status or failure does not belong under globex");
    }

    [Test]
    public async Task Workspace_superseded_overview_does_not_start_reads_for_the_new_section()
    {
        var previous = new TaskCompletionSource<TenantStatusReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var self = WorkspaceSelf();
        self.GetTenantAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        var quota = Substitute.For<ILatticeTenantQuotaUsage>();
        quota.GetQuotaUsageAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(new TenantQuotaUsageReport { TenantId = "acme" });
        Services.AddKeyedSingleton(ShellFacades.Key, quota);
        UseTenancyAs();
        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");
        cut.WaitUntil(() => Assert.That(self.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(self.GetTenantAsync)), Is.True));

        Navigation.NavigateTo("t/acme/tenancy/members");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Find(".lt-tenancy-section__title").TextContent, Is.EqualTo("Admin subjects")));
        previous.SetResult(Status("acme", TenantLifecycleStatus.Active));

        Assert.That(await TestPoll.TryUntilAsync(() => quota.ReceivedCalls().Any(), TimeSpan.FromSeconds(1)),
            Is.False, "an abandoned overview must not start its follow-on quota read");
    }

    [Test]
    public async Task Workspace_disposal_cancels_the_status_read_and_prevents_follow_on_reads()
    {
        var previous = new TaskCompletionSource<TenantStatusReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var self = WorkspaceSelf();
        self.GetTenantAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        UseTenancyAs();
        var cut = RenderAt<MyTenantPage>("t/acme/tenancy");
        cut.WaitUntil(() => Assert.That(self.ReceivedCalls().Any(call => call.GetMethodInfo().Name == nameof(self.GetTenantAsync)), Is.True));

        await DisposeComponentsAsync();
        previous.SetResult(Status("acme", TenantLifecycleStatus.Active));

        Assert.That(await TestPoll.TryUntilAsync(
            () => Cluster.Calls.Contains(nameof(Cluster.GetQuotaUsageAsync)), TimeSpan.FromSeconds(1)), Is.False);
        var call = self.ReceivedCalls().Single(call => call.GetMethodInfo().Name == nameof(self.GetTenantAsync));
        Assert.That(((CancellationToken)call.GetArguments()[1]!).IsCancellationRequested, Is.True);
    }

    private ILatticeTenantSelfService WorkspaceSelf()
    {
        var self = Substitute.For<ILatticeTenantSelfService>();
        self.GetCurrentTenantAsync(Arg.Any<CancellationToken>()).Returns(_ => Cluster.GetCurrentTenantAsync());
        self.ListAccessibleTenantsAsync(Arg.Any<CancellationToken>()).Returns(_ => Cluster.ListAccessibleTenantsAsync());
        Services.AddKeyedSingleton(ShellFacades.Key, self);
        return self;
    }

    private static CancellationToken ReadToken(ILatticeTenantSelfService self) =>
        (CancellationToken)self.ReceivedCalls().Single(call => call.GetMethodInfo().Name == nameof(self.GetCurrentTenantAsync)).GetArguments()[0]!;

    private static TenantStatusReport Status(string tenant, TenantLifecycleStatus status) =>
        new() { TenantId = tenant, Status = status, Regions = [] };
}
