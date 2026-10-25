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
    public async Task Quota_superseded_read_keeps_the_current_tenants_limits(bool fault)
    {
        var previous = new TaskCompletionSource<TenantQuotaUsageReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var quota = Substitute.For<ILatticeTenantQuotaUsage>();
        quota.GetQuotaUsageAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        quota.GetQuotaUsageAsync("globex", Arg.Any<CancellationToken>()).Returns(new TenantQuotaUsageReport
        {
            TenantId = "globex",
            BurstPercent = 17,
        });
        Services.AddKeyedSingleton(ShellFacades.Key, quota);
        Cluster.WithTenant("globex");
        UseTenancyAs();
        var cut = RenderAt<TenancyQuotaPage>("tenancy/acme/quota");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));
        var previousToken = (CancellationToken)quota.ReceivedCalls()
            .Single(call => call.GetMethodInfo().Name == nameof(ILatticeTenantQuotaUsage.GetQuotaUsageAsync))
            .GetArguments()[1]!;
        var panel = cut.FindComponent<TenancyQuota>().Instance;

        Navigation.NavigateTo("tenancy/globex/quota");
        cut.Render();
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("17% above each ceiling")));
        Assert.That(cut.FindComponent<TenancyQuota>().Instance, Is.SameAs(panel), "synchronous admission retains the section");
        Assert.That(previousToken.IsCancellationRequested, Is.True, "changing tenants cancels the previous quota read");
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult(new TenantQuotaUsageReport { TenantId = "acme", BurstPercent = 23 });
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("23% above each ceiling", StringComparison.Ordinal)
                || cut.Markup.Contains("Quota could not be read", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False, "neither another tenant's quota nor its fault belongs under globex");
    }

    [Test]
    public async Task Quota_disposal_cancels_its_outstanding_read()
    {
        var previous = new TaskCompletionSource<TenantQuotaUsageReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var quota = Substitute.For<ILatticeTenantQuotaUsage>();
        quota.GetQuotaUsageAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        Services.AddKeyedSingleton(ShellFacades.Key, quota);
        RenderSection<TenancyQuota>(parameters => parameters.Add(component => component.TenantId, "acme"));

        await DisposeComponentsAsync();
        previous.SetResult(new TenantQuotaUsageReport { TenantId = "acme" });

        var token = quota.ReceivedCalls().Single(call => call.GetMethodInfo().Name == nameof(ILatticeTenantQuotaUsage.GetQuotaUsageAsync))
            .GetArguments()[1];
        Assert.That(token, Is.TypeOf<CancellationToken>());
        Assert.That(((CancellationToken)token!).IsCancellationRequested, Is.True);
    }

    [TestCase(false)]
    [TestCase(true)]
    public async Task Quota_previous_tenants_save_cannot_close_or_fault_the_current_editor(bool fault)
    {
        Cluster.WithTenant("globex");
        Cluster.Tenants["globex"].Quotas = new TenantQuotasDescriptor { MaxKeys = 200 };
        var previous = Cluster.Hold(nameof(FakeTenancyCluster.SetTenantQuotasAsync));
        var cut = RenderSection<TenancyQuota>(parameters => parameters
            .Add(component => component.TenantId, "acme")
            .Add(component => component.CanEdit, true));
        cut.WaitUntil(() => TenancyForms.Button(cut, "Edit quotas"));
        TenancyForms.Button(cut, "Edit quotas").Click();
        TenancyForms.Type(cut, "Keys", "100");
        cut.Find("form.lt-tenancy-form").Submit();
        cut.WaitUntil(() => Assert.That(Cluster.Calls, Does.Contain(nameof(FakeTenancyCluster.SetTenantQuotasAsync))));

        cut.Render(parameters => parameters.Add(component => component.TenantId, "globex"));
        cut.WaitUntil(() =>
        {
            Assert.That(cut.FindAll("form.lt-tenancy-form"), Is.Empty);
            Assert.That(cut.FindAll("tbody tr")[1].Children[2].TextContent.Trim(), Is.EqualTo("200"));
        });
        TenancyForms.Button(cut, "Edit quotas").Click();
        cut.WaitUntil(() => Assert.That(TenancyForms.Field(cut, "Keys").GetAttribute("value"), Is.EqualTo("200")));
        if (fault)
        {
            previous.SetException(new TimeoutException());
        }
        else
        {
            previous.SetResult();
        }

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.FindAll("form.lt-tenancy-form").Count == 0
                || cut.FindAll(".lt-tenancy-form__error").Count != 0
                || Services.GetToasts().Any(),
            TimeSpan.FromSeconds(1)), Is.False, "an old save must not alter the new tenant's editor or publish its toast");
    }

    [Test]
    public async Task Regions_superseded_refresh_fault_keeps_the_newer_reading()
    {
        var previous = new TaskCompletionSource<TenantRegionStatusReport>(TaskCreationOptions.RunContinuationsAsynchronously);
        var regions = Substitute.For<ILatticeTenantRegionAdmin>();
        regions.GetTenantRegionStatusAsync("acme", Arg.Any<CancellationToken>()).Returns(previous.Task);
        Services.AddKeyedSingleton(ShellFacades.Key, regions);
        var cut = RenderSection<TenancyRegions>(parameters => parameters.Add(component => component.TenantId, "acme"));
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));
        regions.GetTenantRegionStatusAsync("acme", Arg.Any<CancellationToken>()).Returns(new TenantRegionStatusReport
        {
            TenantId = "acme",
            Regions = [new TenantRegionStatusDescriptor
            {
                RegionId = "current-region",
                IsAllowed = true,
                Status = TenantRegionLifecycleStatus.Online,
            }],
        });

        await TenancyForms.Button(cut, "Refresh").ClickAsync(new());
        cut.WaitUntil(() => Assert.That(cut.Markup, Does.Contain("current-region")));
        previous.SetException(new TimeoutException());

        Assert.That(await TestPoll.TryUntilAsync(
            () => cut.Markup.Contains("Regions could not be read", StringComparison.Ordinal),
            TimeSpan.FromSeconds(1)), Is.False, "a superseded refresh fault cannot hide the successful reading");
    }

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
        Navigation.OnNotFound += (_, _) => Interlocked.Increment(ref notFound);
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
            () => Volatile.Read(ref notFound) != 0 || cut.Markup.Contains("Suspended", StringComparison.Ordinal), TimeSpan.FromSeconds(1)),
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
        Navigation.OnNotFound += (_, _) => Interlocked.Increment(ref notFound);
        var cut = RenderAt<TenancyTenantPage>("tenancy/acme");
        cut.WaitUntil(() => Assert.That(cut.FindAll(".lt-skeleton"), Has.Count.EqualTo(1)));

        await DisposeComponentsAsync();
        Navigation.NavigateTo("tenancy");
        previous.SetException(new TenantNotFoundException("acme"));

        Assert.That(await TestPoll.TryUntilAsync(() => Volatile.Read(ref notFound) != 0, TimeSpan.FromSeconds(1)), Is.False);
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
