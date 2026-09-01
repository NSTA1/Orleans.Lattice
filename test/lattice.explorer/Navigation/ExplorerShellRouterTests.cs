using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Navigation;

/// <summary>
/// The shell router: adopting addresses, emitting navigations, and suppressing
/// the echo of its own navigation without any timing.
/// </summary>
[TestFixture]
public sealed class ExplorerShellRouterTests
{
    [Test]
    public void Current_BeforeAnyAddress_IsRoot()
    {
        var router = new ExplorerShellRouter();

        Assert.Multiple(() =>
        {
            Assert.That(router.Current, Is.SameAs(ExplorerRoute.Root));
            Assert.That(router.Status, Is.EqualTo(ExplorerRouteStatus.Bare));
        });
    }

    [Test]
    public void SetAddress_NewAddress_UpdatesCurrentAndAnnounces()
    {
        var router = new ExplorerShellRouter();
        ExplorerRoute? announced = null;
        router.RouteChanged += route => announced = route;

        var status = router.SetAddress("/explore/trees/orders/data");

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(ExplorerRouteStatus.Canonical));
            Assert.That(router.Current.Id, Is.EqualTo("orders"));
            Assert.That(announced, Is.SameAs(router.Current));
        });
    }

    [Test]
    public void SetAddress_SameRouteAgain_DoesNotAnnounce()
    {
        var router = new ExplorerShellRouter();
        router.SetAddress("/explore/trees/orders");

        var announcements = 0;
        router.RouteChanged += _ => announcements++;
        router.SetAddress("/explore/trees/orders");

        Assert.That(announcements, Is.Zero);
    }

    [Test]
    public void SetAddress_DifferentSpellingOfTheSameRoute_DoesNotAnnounceButReportsNormalized()
    {
        var router = new ExplorerShellRouter();
        router.SetAddress("/explore/trees/orders");

        var announcements = 0;
        router.RouteChanged += _ => announcements++;
        var status = router.SetAddress("/Explore/Trees/orders/");

        Assert.Multiple(() =>
        {
            Assert.That(announcements, Is.Zero);
            Assert.That(status, Is.EqualTo(ExplorerRouteStatus.Normalized));
            Assert.That(router.Status, Is.EqualTo(ExplorerRouteStatus.Normalized));
        });
    }

    [Test]
    public void SetAddress_MalformedAddress_ReportsMalformedAndStillLands()
    {
        var router = new ExplorerShellRouter();

        var status = router.SetAddress("/explore/trees/orders/data/extra");

        Assert.Multiple(() =>
        {
            Assert.That(status, Is.EqualTo(ExplorerRouteStatus.Malformed));
            Assert.That(router.Current.Surface, Is.EqualTo("data"));
        });
    }

    [Test]
    public void SetAddress_Null_IsBareAndDoesNotThrow()
    {
        var router = new ExplorerShellRouter();

        Assert.That(router.SetAddress(null), Is.EqualTo(ExplorerRouteStatus.Bare));
    }

    [Test]
    public void NavigateTo_RaisesTheNavigationRequestWithTheFormattedAddress()
    {
        var router = new ExplorerShellRouter();
        ExplorerNavigationRequest? request = null;
        router.NavigationRequested += r => request = r;

        router.NavigateTo(ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "orders"));

        Assert.Multiple(() =>
        {
            Assert.That(request?.Address, Is.EqualTo("/explore/trees/orders"));
            Assert.That(request?.Replace, Is.False);
        });
    }

    [Test]
    public void NavigateTo_Replace_PassesTheReplaceIntent()
    {
        var router = new ExplorerShellRouter();
        ExplorerNavigationRequest? request = null;
        router.NavigationRequested += r => request = r;

        router.NavigateTo(ExplorerRoute.Home, replace: true);

        Assert.That(request?.Replace, Is.True);
    }

    [Test]
    public void NavigateTo_UpdatesCurrentAndAnnounces()
    {
        var router = new ExplorerShellRouter();
        ExplorerRoute? announced = null;
        router.RouteChanged += route => announced = route;

        router.NavigateTo(ExplorerRoute.Home);

        Assert.Multiple(() =>
        {
            Assert.That(router.Current, Is.SameAs(ExplorerRoute.Home));
            Assert.That(announced, Is.SameAs(ExplorerRoute.Home));
            Assert.That(router.Status, Is.EqualTo(ExplorerRouteStatus.Canonical));
        });
    }

    [Test]
    public void NavigateTo_SameRoute_StillAsksForTheAddressButDoesNotAnnounce()
    {
        var router = new ExplorerShellRouter();
        router.NavigateTo(ExplorerRoute.Home);

        var announcements = 0;
        var requests = 0;
        router.RouteChanged += _ => announcements++;
        router.NavigationRequested += _ => requests++;
        router.NavigateTo(ExplorerRoute.Home);

        Assert.Multiple(() =>
        {
            Assert.That(announcements, Is.Zero);
            Assert.That(requests, Is.EqualTo(1));
        });
    }

    [Test]
    public void NavigateTo_ToRoot_ReportsBare()
    {
        var router = new ExplorerShellRouter();
        router.NavigateTo(ExplorerRoute.Home);

        router.NavigateTo(ExplorerRoute.Root);

        Assert.That(router.Status, Is.EqualTo(ExplorerRouteStatus.Bare));
    }

    [Test]
    public void NavigateTo_Null_Throws()
    {
        var router = new ExplorerShellRouter();

        Assert.That(() => router.NavigateTo(null!), Throws.ArgumentNullException);
    }

    [Test]
    public void NavigateTo_ThenTheAddressEchoesBack_AnnouncesExactlyOnce()
    {
        // The whole point of comparing routes by value: the browser reports the
        // navigation the router itself just made, and that must not read as a
        // second, independent navigation.
        var router = new ExplorerShellRouter();
        var announcements = 0;
        router.RouteChanged += _ => announcements++;
        router.NavigationRequested += request => router.SetAddress(request.Address);

        router.NavigateTo(ExplorerRoute.Home.WithSelection(ExplorerRouteSegments.Trees, "t/acme/orders"));

        Assert.Multiple(() =>
        {
            Assert.That(announcements, Is.EqualTo(1));
            Assert.That(router.Current.Id, Is.EqualTo("t/acme/orders"));
        });
    }

    [Test]
    public void Canonicalize_ReplacesTheAddressWithTheCanonicalSpelling()
    {
        var router = new ExplorerShellRouter();
        router.SetAddress("/Explore/Trees/");
        ExplorerNavigationRequest? request = null;
        router.NavigationRequested += r => request = r;

        router.Canonicalize();

        Assert.Multiple(() =>
        {
            Assert.That(request?.Address, Is.EqualTo("/explore/trees"));
            Assert.That(request?.Replace, Is.True);
            Assert.That(router.Status, Is.EqualTo(ExplorerRouteStatus.Canonical));
        });
    }

    [Test]
    public void Canonicalize_OnTheBareRoute_ReportsBare()
    {
        var router = new ExplorerShellRouter();

        router.Canonicalize();

        Assert.That(router.Status, Is.EqualTo(ExplorerRouteStatus.Bare));
    }

    [Test]
    public void Canonicalize_WithNoSubscriber_DoesNotThrow()
    {
        var router = new ExplorerShellRouter();

        Assert.That(router.Canonicalize, Throws.Nothing);
    }

    [Test]
    public void BackAndForward_AreJustAddresses()
    {
        // Browser history needs no special handling: it arrives as a location
        // change like any other, which is exactly why routing the whole shell
        // through the address makes Back and Forward work at all.
        var router = new ExplorerShellRouter();
        var seen = new List<string>();
        router.RouteChanged += route => seen.Add(ExplorerRoutePath.Format(route));

        router.SetAddress("/explore/trees/orders");
        router.SetAddress("/explore/trees/invoices");
        router.SetAddress("/explore/trees/orders");

        Assert.That(
            seen,
            Is.EqualTo(new[]
            {
                "/explore/trees/orders",
                "/explore/trees/invoices",
                "/explore/trees/orders",
            }));
    }
}
