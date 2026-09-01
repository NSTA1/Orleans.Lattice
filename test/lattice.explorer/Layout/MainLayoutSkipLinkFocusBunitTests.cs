using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.Extensions.DependencyInjection;
using NSubstitute;
using NUnit.Framework.Internal;
using Orleans.Lattice.Explorer.Core.Catalog;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.Core.DeadLetter;
using Orleans.Lattice.Explorer.Core.Navigation;
using Orleans.Lattice.Explorer.Tests.Bunit;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Layout;

/// <summary>
/// <b>The skip link must perform the bypass, not merely advertise one.</b>
/// Activating it puts keyboard focus in the main landmark, so a caller reaches
/// the working surface instead of the top of the chrome (WCAG SC 2.4.1 Bypass
/// Blocks, level A).
/// </summary>
/// <remarks>
/// <para>
/// The <c>href="#..."</c> alone does not do it, and that is the trap this fixture
/// exists to hold shut. The framework intercepts a same-document anchor and
/// updates the address itself rather than letting the browser perform a fragment
/// navigation, and only a real fragment navigation moves the sequential focus
/// navigation starting point. The address therefore ends up right while focus
/// never moves - so the next Tab goes to whatever follows the link in document
/// order (the banner), and the caller is left exactly where the bypass was meant
/// to take them from.
/// </para>
/// <para>
/// A link that announces a bypass and does not perform one is a documented way of
/// failing 2.4.1 while appearing to satisfy it, which is precisely why the
/// structural half of this - "the skip link is the first tab stop" - is not
/// sufficient coverage on its own.
/// </para>
/// <para>
/// Focus is asserted through the framework's own focus interop, which is what
/// <see cref="ElementReferenceExtensions.FocusAsync"/> issues, and the element it
/// was issued against is matched to the rendered <c>main</c> - so this measures
/// "focus moved into the landmark", not merely "something was focused".
/// </para>
/// </remarks>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class MainLayoutSkipLinkFocusBunitTests : LatticeComponentTestContext
{
    [Test]
    public void Activating_the_skip_link_moves_focus_into_the_main_landmark()
    {
        var cut = RenderLayout();

        cut.Find(".lx-shell-skip").Click();

        // Two halves, and both are needed. The first says focus was actually
        // requested when the link was activated - without the handler the anchor
        // updates the address and nothing focuses at all. The second says the
        // element the shell holds a reference to, and therefore the one it
        // focuses, is the main landmark rather than some other captured element.
        JSInterop.VerifyFocusAsyncInvoke();

        var main = cut.Find("main");

        Assert.Multiple(() =>
        {
            Assert.That(
                main.GetAttribute("id"),
                Is.EqualTo(ExplorerShellRegions.Main),
                "the landmark the skip link targets is the one focus must land in");
            Assert.That(
                main.HasAttribute("blazor:elementreference"),
                Is.True,
                "the main landmark is not the element the shell captured, so the focus call above "
                + "moved the caller somewhere other than the working surface");
        });
    }

    [Test]
    public void The_skip_link_stays_a_real_link_to_the_main_landmark()
    {
        // The handler performs the bypass; the href is what makes this a link at
        // all - it gives the control its role and accessible name for free, and
        // it is what still works before script has started. Replacing the anchor
        // with a button would pass the focus case above and regress both.
        var cut = RenderLayout();
        var skip = cut.Find(".lx-shell-skip");

        Assert.Multiple(() =>
        {
            Assert.That(skip.TagName, Is.EqualTo("A").IgnoreCase);
            Assert.That(skip.GetAttribute("href"), Is.EqualTo("#" + ExplorerShellRegions.Main));
        });
    }

    [Test]
    public void The_skip_link_is_still_the_first_focusable_thing_in_the_shell()
    {
        // The bypass is only a bypass if it comes first; a working focus move
        // behind three other stops is not one.
        var cut = RenderLayout();

        var focusable = cut.FindAll("a[href], button, input, select, textarea, [tabindex]")
            .Where(element => element.GetAttribute("tabindex") != "-1")
            .ToArray();

        Assert.That(focusable, Is.Not.Empty);
        Assert.That(focusable[0].ClassList, Does.Contain("lx-shell-skip"));
    }

    private IRenderedComponent<MainLayout> RenderLayout()
    {
        ConfigureShellServices();

        var connection = Substitute.For<ILatticeStateConnection>();
        connection.Status.Returns(LatticeConnectionStatus.Disconnected);

        var catalogReader = Substitute.For<ICatalogReader>();
        catalogReader
            .LoadAsync(Arg.Any<CatalogKind>(), Arg.Any<string?>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
            .Returns(Task.FromResult(new CatalogPage()));

        var session = Substitute.For<IExplorerSession>();
        session.IsConfigured.Returns(true);

        var selection = Substitute.For<IExplorerSelection>();
        selection.Selected.Returns((CatalogItem?)null);

        Services.AddSingleton(connection);
        Services.AddSingleton(catalogReader);
        Services.AddSingleton(Substitute.For<IDeadLetterReader>());
        Services.AddSingleton(selection);
        Services.AddSingleton(session);

        return Render<MainLayout>();
    }
}
