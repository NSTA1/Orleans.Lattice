using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Api.TenantAdmin;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Areas.Tenancy;
using Orleans.Lattice.Explorer.UI.Transport;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Tenancy;

/// <summary>Late tenant reads never replace the tenant currently being administered.</summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class TenancySupersededReadTests : TenancyTestContext
{
    [TestCase(false)]
    [TestCase(true)]
    public async Task Overview_superseded_read_keeps_the_current_tenants_state(bool fault)
    {
        var previous = new TaskCompletionSource<TenantStatusReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var self = Substitute.For<ILatticeTenantSelfService>();
        self.GetCurrentTenantAsync(Arg.Any<CancellationToken>()).Returns(_ => Cluster.GetCurrentTenantAsync());
        self.GetTenantAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        self.GetTenantAsync("globex", Arg.Any<CancellationToken>()).Returns(Status("globex", TenantLifecycleStatus.Active));
        Services.AddKeyedSingleton(ShellFacades.Key, self);
        RefuseRegionFallback();
        Cluster.WithTenant("globex");
        UseTenancyAs();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));

        Navigation.NavigateTo("tenancy/globex");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-pill__text").Select(pill => pill.TextContent), Does.Contain("Active")));

        if (fault)
        {
            previous.SetException(new TenantNotFoundException("acme"));
        }
        else
        {
            previous.SetResult(Status("acme", TenantLifecycleStatus.Suspended));
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => notFound != 0 || cut.Markup.Contains("Suspended", StringComparison.Ordinal), TimeSpan.FromSeconds(1)),
            Is.False, "an old answer cannot change globex's status or mark its page not-found");
    }

    [Test]
    public async Task Overview_left_page_does_not_mark_the_next_page_not_found()
    {
        var previous = new TaskCompletionSource<TenantStatusReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var self = Substitute.For<ILatticeTenantSelfService>();
        self.GetCurrentTenantAsync(Arg.Any<CancellationToken>()).Returns(_ => Cluster.GetCurrentTenantAsync());
        self.GetTenantAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        Services.AddKeyedSingleton(ShellFacades.Key, self);
        RefuseRegionFallback();
        UseTenancyAs();
        var notFound = 0;
        Navigation.OnNotFound += (_, _) => notFound++;
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));

        await DisposeComponentsAsync();
        Navigation.NavigateTo("tenancy");
        previous.SetException(new TenantNotFoundException("acme"));

        Assert.That(await TestPoll.TryUntilAsync(() => notFound != 0, TimeSpan.FromSeconds(1)), Is.False);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Members_superseded_read_keeps_the_current_tenants_subjects(bool fault)
    {
        var previous = new TaskCompletionSource<TenantAdminSubjectReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var access = Substitute.For<ILatticeTenantAccessAdmin>();
        access.ListAdminSubjectsAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        access.ListAdminSubjectsAsync("globex", Arg.Any<CancellationToken>()).Returns(Subjects("globex", "current-admin"));
        Services.AddKeyedSingleton(ShellFacades.Key, access);
        var cut = RenderSection<TenancyMembers>(parameters => parameters.Add(component => component.TenantId, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));

        cut.Render(parameters => parameters.Add(component => component.TenantId, "globex"));
        cut.WaitUntil(() => Assert.That(cut.Find("tbody th").TextContent.Trim(), Is.EqualTo("current-admin")));
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult(Subjects("acme", "stale-admin"));
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("stale-admin", StringComparison.Ordinal) || cut.FindAll(".lt-empty").Count != 0,
            TimeSpan.FromSeconds(1)), Is.False, "neither an old list nor its fault belongs to globex");
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Sharing_superseded_read_keeps_the_current_tenants_grants(bool fault)
    {
        var previous = new TaskCompletionSource<TenantGrantReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var grants = Substitute.For<ILatticeTenantGrantAdmin>();
        grants.ListGrantsAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        grants.ListGrantsAsync("globex", Arg.Any<CancellationToken>()).Returns(GrantReport("globex", "current-orders"));
        Services.AddKeyedSingleton(ShellFacades.Key, grants);
        var cut = RenderSection<TenancyGrants>(parameters => parameters.Add(component => component.TenantId, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));

        cut.Render(parameters => parameters.Add(component => component.TenantId, "globex"));
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("current-orders")));
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult(GrantReport("acme", "stale-orders"));
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("stale-orders", StringComparison.Ordinal) || cut.Markup.Contains("Grants could not be read", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False, "an old grant must not appear with actions under a new tenant's heading");
    }

    [TestCase("GetAccessModelAsync")]
    [TestCase("ListGroupsAsync")]
    public async Task Members_late_directory_or_classification_keeps_the_current_tenants_entries(string method)
    {
        TenantFacades.AsTenantAdmin();
        Cluster.Tenants["acme"].Admins.Clear();
        Cluster.Tenants["acme"].Admins.Add("stale-admin");
        Cluster.WithTenant("globex", admins: ["t/globex/current"]);
        var previous = Directory.Hold(method);
        var cut = RenderSection<TenancyMembers>(parameters => parameters.Add(component => component.TenantId, "acme"));
        cut.WaitUntil(() => Assert.That(Directory.Calls, Does.Contain(method)));

        Directory.Hold(method).SetResult();
        cut.Render(parameters => parameters.Add(component => component.TenantId, "globex"));
        cut.WaitUntil(() => Assert.That(cut.Find("tbody th").TextContent.Trim(), Is.EqualTo("t/globex/current")));
        previous.SetResult();

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("stale-admin", StringComparison.Ordinal), TimeSpan.FromSeconds(1)),
            Is.False, "every await in the member load belongs to its captured tenant");
    }

    [Test]
    public async Task Sharing_disposal_cancels_its_outstanding_read()
    {
        var previous = new TaskCompletionSource<TenantGrantReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var grants = Substitute.For<ILatticeTenantGrantAdmin>();
        grants.ListGrantsAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        Services.AddKeyedSingleton(ShellFacades.Key, grants);
        var cut = RenderSection<TenancyGrants>(parameters => parameters
            .Add(component => component.TenantId, "acme")
            .Add(component => component.OpenOfferOnLoad, true));
        await DisposeComponentsAsync();
        previous.SetResult(GrantReport("acme", "stale-orders"));

        var token = grants.ReceivedCalls().Single(call => call.GetMethodInfo().Name == nameof(ILatticeTenantGrantAdmin.ListGrantsAsync))
            .GetArguments()[1];
        Assert.That(token, Is.TypeOf<CancellationToken>());
        Assert.That(((CancellationToken)token!).IsCancellationRequested, Is.True, "leaving cancels the outstanding read and its command");
    }

    private void RefuseRegionFallback()
    {
        var regions = Substitute.For<ILatticeTenantRegionAdmin>();
        regions.GetTenantRegionStatusAsync("acme", Arg.Any<CancellationToken>())
            .Returns(Task.FromException<TenantRegionStatusReport>(new TenantNotFoundException("acme")));
        Services.AddKeyedSingleton(ShellFacades.Key, regions);
    }

    private static TenantStatusReport Status(string tenant, TenantLifecycleStatus status) =>
        new() { TenantId = tenant, Status = status, Regions = [] };

    private static TenantAdminSubjectReport Subjects(string tenant, string subject) =>
        new() { TenantId = tenant, Subjects = [subject] };

    private static TenantGrantReport GrantReport(string tenant, string scope) =>
        new()
        {
            TenantId = tenant,
            Received = [],
            Issued = [new TenantGrantDescriptor
            {
                GrantId = tenant + "-" + scope,
                GranterTenantId = tenant,
                GranteeTenantId = "initech",
                Scope = scope,
                Operations = TenantGrantAccess.Read,
                State = TenantGrantLifecycleState.Active,
            }],
        };
}
