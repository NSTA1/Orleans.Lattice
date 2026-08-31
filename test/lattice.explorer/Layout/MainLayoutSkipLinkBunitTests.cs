using Bunit;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework.Internal;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Session;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// The shell's bypass: a same-page link to the main landmark, first in the tab
/// order, and visible while it has focus.
/// </summary>
/// <remarks>
/// <para>
/// Without it a keyboard caller meets the banner, the scope control, the
/// identity control and the whole area rail again on every surface change
/// before reaching the working surface. That is WCAG SC 2.4.1 Bypass Blocks, a
/// level A criterion.
/// </para>
/// <para>
/// bUnit's context locks its service collection after the first render, and
/// NUnit reuses one fixture instance across a fixture's tests by default, so a
/// fixture with more than one case must ask for an instance per case.
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class MainLayoutSkipLinkBunitTests : LatticeComponentTestContext
{
    [Test]
    public void The_skip_link_is_the_first_focusable_thing_in_the_shell()
    {
        ConfigureLayoutServices();

        var cut = Render<MainLayout>();

        var focusable = cut.FindAll("a[href], button, input, select, textarea, [tabindex]")
            .Where(element => element.GetAttribute("tabindex") != "-1")
            .ToArray();

        Assert.That(focusable, Is.Not.Empty, "a shell with no keyboard-reachable content cannot be measured");
        Assert.That(
            focusable[0].ClassList,
            Does.Contain("lx-shell-skip"),
            "the first stop used to be the sign-in control, so a caller met the whole chrome "
            + "before the surface they came for");
    }

    [Test]
    public void The_skip_link_is_a_same_page_link_to_the_main_landmark()
    {
        ConfigureLayoutServices();

        var cut = Render<MainLayout>();
        var link = cut.Find("a.lx-shell-skip");

        Assert.Multiple(() =>
        {
            Assert.That(
                link.GetAttribute("href"),
                Is.EqualTo("#" + ExplorerShellRegions.Main),
                "an anchor is what performs a bypass; a button that scrolls is not one");
            Assert.That(cut.Find("main").Id, Is.EqualTo(ExplorerShellRegions.Main));
        });
    }

    [Test]
    public void The_shell_exposes_a_banner_a_navigation_and_one_main_landmark()
    {
        ConfigureLayoutServices();

        var cut = Render<MainLayout>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("main"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("header.lx-shell-brand"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll("nav"), Has.Count.GreaterThanOrEqualTo(1));
        });
    }

    [Test]
    public void The_banner_offers_a_region_for_each_contributed_control()
    {
        // The two regions the epic's in-flight sibling features register into.
        // They are empty here because nothing has been contributed, which is the
        // point: the layout names a region and never a component.
        ConfigureLayoutServices();

        var cut = Render<MainLayout>();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lx-shell-brand-scope"), Has.Count.EqualTo(1));
            Assert.That(cut.FindAll(".lx-shell-brand-settings"), Has.Count.EqualTo(1));
            Assert.That(
                cut.Find(".lx-shell-brand-settings").TextContent.Trim(),
                Is.Empty,
                "and renders nothing until a feature registers against the placement");
        });
    }

    private void ConfigureLayoutServices()
    {
        ConfigureShellServices();

        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);

        var catalogReader = Substitute.For<ICatalogReader>();
        catalogReader
            .LoadAsync(Arg.Any<CatalogKind>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new CatalogPage()));

        var preferences = Substitute.For<IUiPreferenceStore>();
        preferences.IsLoaded.Returns(true);
        preferences.GetOrDefault("nav-kind", CatalogKind.Trees).Returns(CatalogKind.Trees);
        preferences.GetOrDefault<CatalogItem?>("nav-selected", null).Returns((CatalogItem?)null);

        var session = Substitute.For<IExplorerSession>();
        session.IsConfigured.Returns(true);

        var selection = Substitute.For<IExplorerSelection>();
        selection.Selected.Returns((CatalogItem?)null);

        Services.AddSingleton(connection);
        Services.AddSingleton(catalogReader);
        Services.AddSingleton(Substitute.For<IDeadLetterReader>());
        Services.AddSingleton(selection);
        Services.AddSingleton(session);
        Services.AddSingleton(preferences);
    }
}
