using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// A page is only ever handed an address one of its own routes answers. The
/// layout cascades its location to every page it renders, and a navigation
/// updates that cascade before the page being left is torn down. A page that
/// answered a foreign address - "my tenant" reading <c>/access</c> as a tenant
/// that does not exist - used to declare the new address not found, so the
/// router rendered the not-found page at an address the spine showed as visible.
/// </summary>
/// <remarks>
/// The verdicts the layout waits on complete after the navigation, under the
/// test's control, so every decision is seen both while a verdict is unsettled
/// and once it settles.
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutPageHandoffTests : ShellLayoutTestContext
{
    private readonly FakeArea _tenancy = new("tenancy", "Tenancy", 1);
    private readonly FakeArea _access = new("access", "Access", 2) { IsTenantScoped = false };
    private readonly FakeArea _data = new("data", "Data", 3);
    private readonly List<string> _notFound = [];
    private readonly HandoffLog _log = new();

    [SetUp]
    public void Arrange()
    {
        Services.AddSingleton(_log);
        UseTenancy("default");
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(new ValueTask<bool>(true));
        AddArea(_tenancy);
        AddArea(_access);
        AddArea(_data);
        Navigation.OnNotFound += (_, _) => _notFound.Add(Navigation.Uri);
    }

    [Test]
    public void Leaving_my_tenant_for_a_cluster_wide_area_never_hands_it_that_address()
    {
        var cut = OpenMyTenant();

        NavigateAndRenderPage<AccessProbePage>(cut, "access");

        cut.WaitUntil(() => Assert.That(cut.FindAll("#access-page"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(HandedAddresses, Is.EqualTo(new[] { "my-tenant /t/default/tenancy", "access /access" }));
            Assert.That(_notFound, Is.Empty, "no page declared the address not found");
            Assert.That(cut.Find("main h1").TextContent, Is.Not.EqualTo("Nothing lives at this address"));
        });
    }

    [Test]
    public void An_access_verdict_that_settles_after_the_navigation_is_waited_for_and_never_read_as_hidden()
    {
        var cut = OpenMyTenant();
        var verdict = new TaskCompletionSource<AreaAvailability>(TaskCreationOptions.RunContinuationsAsynchronously);
        _access.Availability = _ => new ValueTask<AreaAvailability>(verdict.Task);

        NavigateAndRenderPage<AccessProbePage>(cut, "access");

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("main .lt-skeleton"), Is.Not.Empty, "the page is withheld while its verdict is unsettled");
            Assert.That(cut.FindAll("#access-page"), Is.Empty);
            Assert.That(cut.Markup, Does.Not.Contain("Nothing lives at this address"));
            Assert.That(HandedAddresses, Is.EqualTo(new[] { "my-tenant /t/default/tenancy" }));
            Assert.That(_notFound, Is.Empty);
        });

        cut.InvokeAsync(() => verdict.SetResult(AreaAvailability.Visible));

        cut.WaitUntil(() => Assert.That(cut.FindAll("#access-page"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "access"));
            Assert.That(HandedAddresses, Is.EqualTo(new[] { "my-tenant /t/default/tenancy", "access /access" }));
            Assert.That(_notFound, Is.Empty);
        });
    }

    [Test]
    public void An_operator_verdict_that_settles_after_the_navigation_keeps_the_tenant_rooted_address()
    {
        var cut = OpenMyTenant();
        var verdict = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        Switcher!.IsOperatorAsync(Arg.Any<CancellationToken>()).Returns(_ => new ValueTask<bool>(verdict.Task));

        NavigateAndRenderPage<DataProbePage>(cut, "t/default/data");

        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "t/default/data"), "an unsettled verdict does not change the canonical address");
            Assert.That(cut.FindAll("#data-page"), Is.Empty, "the page is not shown on an unsettled verdict");
            Assert.That(HandedAddresses, Is.EqualTo(new[] { "my-tenant /t/default/tenancy" }));
        });

        cut.InvokeAsync(() => verdict.SetResult(true));

        cut.WaitUntil(() => Assert.That(cut.FindAll("#data-page"), Has.Count.EqualTo(1)));
        Assert.Multiple(() =>
        {
            Assert.That(Navigation.Uri, Is.EqualTo(Navigation.BaseUri + "t/default/data"));
            Assert.That(HandedAddresses, Is.EqualTo(new[] { "my-tenant /t/default/tenancy", "data /t/default/data" }));
            Assert.That(_notFound, Is.Empty);
        });
    }

    private IRenderedComponent<ShellLayout> OpenMyTenant()
    {
        Navigation.NavigateTo("t/default/tenancy");
        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, PageOf<MyTenantProbePage>()));
        cut.WaitUntil(() => Assert.That(cut.FindAll("#my-tenant-page"), Has.Count.EqualTo(1)));
        return cut;
    }

    private void NavigateAndRenderPage<TPage>(IRenderedComponent<ShellLayout> cut, string relative)
        where TPage : IComponent
    {
        Navigation.NavigateTo(relative);
        cut.Render(parameters => parameters.Add(layout => layout.Body, PageOf<TPage>()));
    }

    private static RenderFragment PageOf<TPage>()
        where TPage : IComponent => builder =>
    {
        builder.OpenComponent<TPage>(0);
        builder.CloseComponent();
    };

    private List<string> HandedAddresses => _log.Entries;

    /// <summary>Every address a probe page was handed, as "{page} {address}", once per change.</summary>
    private sealed class HandoffLog
    {
        public List<string> Entries { get; } = [];
    }

    /// <summary>A probe page that records each new address it is handed.</summary>
    private abstract class ProbePage : ExplorerPage
    {
        private string? _last;

        [Inject]
        private HandoffLog Log { get; set; } = default!;

        protected abstract string Name { get; }

        protected override void OnParametersSet()
        {
            var address = Address.Format();
            if (!string.Equals(address, _last, StringComparison.Ordinal))
            {
                _last = address;
                Log.Entries.Add(Name + " " + address);
            }
        }
    }

    /// <summary>
    /// "My tenant", as the real page reads its address: an address with no tenant
    /// names no tenant, so it declares it not found.
    /// </summary>
    [Route("/t/{tenant}/tenancy")]
    [Route("/t/{tenant}/tenancy/{p1}")]
    private sealed class MyTenantProbePage : ProbePage
    {
        [Inject]
        private NavigationManager Navigation { get; set; } = default!;

        protected override string Name => "my-tenant";

        protected override void OnParametersSet()
        {
            base.OnParametersSet();
            if (Address.Tenant is null || !string.Equals(Address.Area, "tenancy", StringComparison.Ordinal))
            {
                Navigation.NotFound();
            }
        }

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "h1");
            builder.AddAttribute(1, "id", "my-tenant-page");
            builder.AddContent(2, Address.Tenant);
            builder.CloseElement();
        }
    }

    /// <summary>The Access area's root page.</summary>
    [Route("/access")]
    [Route("/t/{tenant}/access")]
    private sealed class AccessProbePage : ProbePage
    {
        protected override string Name => "access";

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "h1");
            builder.AddAttribute(1, "id", "access-page");
            builder.AddContent(2, "Access");
            builder.CloseElement();
        }
    }

    /// <summary>The Data area's root page.</summary>
    [Route("/data")]
    [Route("/t/{tenant}/data")]
    private sealed class DataProbePage : ProbePage
    {
        protected override string Name => "data";

        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "h1");
            builder.AddAttribute(1, "id", "data-page");
            builder.AddContent(2, "Data");
            builder.CloseElement();
        }
    }
}
