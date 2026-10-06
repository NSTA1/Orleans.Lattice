using Orleans.Lattice.Explorer.UI.Areas.Apps.App;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.Tests.UI.Areas.Apps.App;

/// <summary>
/// The app pages' addresses: their own sections, the cross-area links (Data, Replication,
/// the catalogue's re-consent), the round trip between an in-frame path and the window
/// address, and the redirect from a legacy open address.
/// </summary>
[TestFixture]
public sealed class AppPageAddressesTests
{
    [Test]
    public void Section_and_cross_area_addresses_follow_the_grammar()
    {
        Assert.Multiple(() =>
        {
            Assert.That(AppPageAddresses.Page(null, "crm").Format(), Is.EqualTo("/apps/crm"));
            Assert.That(AppPageAddresses.Page("acme", "crm", AppPageTabs.Trees).Format(), Is.EqualTo("/t/acme/apps/crm/trees"));
            Assert.That(AppPageAddresses.DataTree(null, "crm", "orders").Format(), Is.EqualTo("/data/a/crm/orders"));
            Assert.That(AppPageAddresses.Replication("acme", "crm").Format(), Is.EqualTo("/t/acme/replication?app=crm"));
            Assert.That(AppPageAddresses.Reconsent(null, "in-image", "crm").Format(), Is.EqualTo("/apps/catalogue/in-image/crm"));
            Assert.That(AppPageAddresses.Reconsent(null, null, "crm").Format(), Is.EqualTo("/apps/catalogue?q=crm"));
            Assert.That(AppPageAddresses.Reconsent(null, string.Empty, "crm").Format(), Is.EqualTo("/apps/catalogue?q=crm"));
        });
    }

    [TestCase("/apps/crm/window", null)]
    [TestCase("/apps/crm/window/board", "/board")]
    [TestCase("/apps/crm/window/board/42?query=view%3Dall", "/board/42?view=all")]
    [TestCase("/apps/crm/window?query=tab%3D2", "/?tab=2")]
    [TestCase("/apps/crm/window/a%20b", "/a b")]
    public void A_window_address_names_its_in_frame_path(string address, string? framePath)
    {
        Assert.That(AppPageAddresses.FramePath(ExplorerAddress.Parse(address)), Is.EqualTo(framePath));
    }

    [TestCase("/board/42", "/apps/crm/window/board/42")]
    [TestCase("/board/42?view=all", "/apps/crm/window/board/42?query=view%3Dall")]
    [TestCase("/board/42#anchor", "/apps/crm/window/board/42")]
    [TestCase("//board///42/", "/apps/crm/window/board/42")]
    [TestCase("/", "/apps/crm/window")]
    [TestCase("/?", "/apps/crm/window")]
    [TestCase("/%41", "/apps/crm/window/%2541")]
    public void An_in_frame_path_becomes_a_window_address(string framePath, string address)
    {
        Assert.That(AppPageAddresses.FromFramePath(null, "crm", framePath)!.Format(), Is.EqualTo(address));
    }

    [TestCase("/apps/crm/open", "/apps/crm/window")]
    [TestCase("/apps/crm/open/board/42?query=view%3Dall", "/apps/crm/window/board/42?query=view%3Dall")]
    [TestCase("/apps/crm/open?path=%2Fa%2Fb%2Fc%2Fd%2Fe", "/apps/crm/window?path=%2Fa%2Fb%2Fc%2Fd%2Fe")]
    [TestCase("/t/acme/apps/crm/open/board", "/t/acme/apps/crm/window/board")]
    public void A_legacy_open_address_leads_to_the_same_in_app_path_in_the_window(string address, string window)
    {
        var target = AppPageAddresses.WindowForLegacyOpen(ExplorerAddress.Parse(address))!;

        Assert.Multiple(() =>
        {
            Assert.That(target.Tenant, Is.EqualTo(ExplorerAddress.Parse(window).Tenant));
            Assert.That(target.Path, Is.EqualTo(ExplorerAddress.Parse(window).Path));
            Assert.That(AppPageAddresses.FramePath(target), Is.EqualTo(AppPageAddresses.FramePath(ExplorerAddress.Parse(window))));
        });
    }

    [TestCase("/apps/crm/window")]
    [TestCase("/apps/crm/overview")]
    [TestCase("/apps/crm")]
    [TestCase("/apps")]
    [TestCase("/apps/catalogue/open")]
    [TestCase("/data/crm/open")]
    public void Only_a_legacy_open_address_is_redirected(string address)
    {
        Assert.That(AppPageAddresses.WindowForLegacyOpen(ExplorerAddress.Parse(address)), Is.Null);
    }

    [Test]
    public void The_legacy_redirect_needs_an_address()
    {
        Assert.That(() => AppPageAddresses.WindowForLegacyOpen(null!), Throws.ArgumentNullException);
    }
    [TestCase("/board/42?view=all")]
    [TestCase("/a b/c%20d")]
    [TestCase("/%41")]
    [TestCase("/a/b/c/d")]
    [TestCase("/a/b/c/d/e/f?x=1")]
    public void An_in_frame_path_survives_the_round_trip(string framePath)
    {
        var address = AppPageAddresses.FromFramePath("acme", "crm", framePath)!;

        Assert.Multiple(() =>
        {
            Assert.That(address.Tenant, Is.EqualTo("acme"));
            Assert.That(AppPageAddresses.FramePath(ExplorerAddress.Parse(address.Format())), Is.EqualTo(framePath));
        });
    }

    [Test]
    public void A_path_deeper_than_the_route_can_carry_goes_in_the_query_so_every_address_still_routes()
    {
        var shallow = AppPageAddresses.FromFramePath(null, "crm", "/a/b/c/d")!;
        var deep = AppPageAddresses.FromFramePath(null, "crm", "/a/b/c/d/e")!;

        Assert.Multiple(() =>
        {
            Assert.That(AppPageAddresses.MaxInAppSegments, Is.EqualTo(4));
            Assert.That(shallow.Path, Is.EqualTo(new[] { "crm", "window", "a", "b", "c", "d" }));
            Assert.That(deep.Path, Is.EqualTo(new[] { "crm", "window" }));
            Assert.That(deep.GetQuery(AppPageAddresses.InAppPath), Is.EqualTo("/a/b/c/d/e"));
            Assert.That(AppPageAddresses.FramePath(ExplorerAddress.Parse("/apps/crm/window?path=a%2Fb")), Is.EqualTo("/a/b"));
        });
    }

    [TestCase(null)]
    [TestCase("")]
    public void No_in_frame_path_is_no_address(string? framePath)
    {
        Assert.That(AppPageAddresses.FromFramePath(null, "crm", framePath), Is.Null);
    }

    [Test]
    public void A_path_the_grammar_cannot_carry_is_no_address()
    {
        Assert.That(AppPageAddresses.FromFramePath(null, "crm", "/bad\uD800"), Is.Null);
    }

    [Test]
    public void The_frame_path_needs_an_address()
    {
        Assert.That(() => AppPageAddresses.FramePath(null!), Throws.ArgumentNullException);
    }
}
