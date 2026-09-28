using Bunit;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Explorer.Shell.Areas.Access;
using Orleans.Lattice.Testing.Hygiene;

namespace Orleans.Lattice.Explorer.Tests.Shell.Areas.Access;

/// <summary>
/// The access-state banner and the area's navigation row.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class AccessChromeTests : AccessTestContext
{
    [Test]
    public void An_unknown_model_renders_nothing_rather_than_not_enforced()
    {
        var cut = Render<AccessPostureBanner>(parameters => parameters.Add(banner => banner.Model, null));

        Assert.That(cut.Markup.Trim(), Is.Empty);
    }

    [Test]
    public void The_banner_reads_the_posture_as_the_server_reported_it()
    {
        var cut = Render<AccessPostureBanner>(parameters => parameters.Add(banner => banner.Model, new AccessModelDescriptor
        {
            AuthenticationMode = AccessAuthenticationMode.Basic,
            RulesEnforced = true,
            AllTreesGrantsEnabled = true,
            AccessAdministrationDelegationEnabled = false,
            DirectoryAvailable = true,
            DirectoryProviderId = "entra",
            DirectoryExplanation = "Object id",
            LocalMembershipEffective = true,
        }));

        var facts = cut.FindAll(".lt-access-banner__item").ToDictionary(
            item => item.QuerySelector("dt")!.TextContent,
            item => item.QuerySelector("dd")!.TextContent);
        Assert.Multiple(() =>
        {
            Assert.That(facts, Is.EqualTo(new Dictionary<string, string>
            {
                ["Authentication"] = "Basic (username and password)",
                ["Rules"] = "Enforced",
                ["All-trees grants"] = "On",
                ["Access-admin delegation"] = "Off",
                ["Identity directory"] = "entra",
            }));
            Assert.That(cut.FindAll(".lt-access-notice"), Is.Empty);
        });
    }

    [Test]
    [TestCase("Anonymous", "Anonymous")]
    [TestCase("Claims", "Claims (token)")]
    [TestCase("Unknown", "Unknown")]
    public void Every_authentication_mode_is_named(string mode, string expected)
    {
        var cut = Render<AccessPostureBanner>(parameters => parameters.Add(banner => banner.Model, Admin.Model with { AuthenticationMode = Enum.Parse<AccessAuthenticationMode>(mode) }));

        Assert.That(cut.Find(".lt-access-banner__value").TextContent, Is.EqualTo(expected));
    }

    [Test]
    public void Unenforced_rules_and_token_only_membership_each_raise_a_notice()
    {
        var cut = Render<AccessPostureBanner>(parameters => parameters.Add(banner => banner.Model, Admin.Model with
        {
            RulesEnforced = false,
            LocalMembershipEffective = false,
        }));

        var notices = cut.FindAll(".lt-access-notice").Select(notice => notice.TextContent).ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(notices, Has.Length.EqualTo(2));
            Assert.That(notices[0], Does.Contain("not enforcing them"));
            Assert.That(notices[1], Does.Contain("only from identity tokens"));
            Assert.That(cut.FindAll(".lt-access-banner__value")[1].TextContent, Is.EqualTo("Recorded, not enforced"));
            Assert.That(cut.FindAll(".lt-access-banner__value")[4].TextContent, Is.EqualTo("None"));
        });
    }

    [Test]
    [TestCase("rules", "Rules")]
    [TestCase("groups", "Groups")]
    [TestCase("explain", "Explain")]
    public void The_navigation_marks_only_the_current_section(string current, string expected)
    {
        var cut = Render<AccessNav>(parameters => parameters.Add(nav => nav.Current, current));

        var marked = cut.FindAll(".lt-access-nav__link").Where(link => link.GetAttribute("aria-current") == "page").Select(link => link.TextContent);
        Assert.Multiple(() =>
        {
            Assert.That(marked, Is.EqualTo(new[] { expected }));
            Assert.That(cut.Find("nav").GetAttribute("aria-label"), Is.EqualTo("Access"));
            Assert.That(cut.FindAll(".lt-access-nav__link").Select(link => link.GetAttribute("href")), Is.EqualTo(new[] { "access/rules", "access/groups", "access/explain" }));
        });
    }

    [Test]
    public void The_navigation_links_the_areas_stylesheet_from_the_shells_asset_base()
    {
        var cut = Render<AccessNav>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("link[rel=stylesheet]").GetAttribute("href"), Is.EqualTo(AccessAssets.Stylesheet));
            Assert.That(AccessAssets.Stylesheet, Is.EqualTo("_content/Orleans.Lattice.Explorer.Shell/access/lattice-access.css"));
            Assert.That(File.Exists(Path.Combine(HygieneRepository.FindRepoRoot(), "src", "lattice.explorer", "Shell", "wwwroot", "access", "lattice-access.css")), Is.True);
        });
    }
}
