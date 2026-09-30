using Bunit;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Rendering;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Design.Tokens;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.Tests.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Layout;

/// <summary>
/// The layout below and between the breakpoints: the spine as a rail at medium,
/// and below the small breakpoint the spine in a slide-in sheet and the header's
/// session slots and appearance folded into one overflow menu.
/// </summary>
[TestFixture]
[FixtureLifeCycle(LifeCycle.InstancePerTestCase)]
public sealed class ShellLayoutCompactTests : ShellLayoutTestContext
{
    [Test]
    public void Until_the_width_is_measured_the_expanded_frame_renders()
    {
        var cut = RenderLayout();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("#lt-shell-directory"), Has.Count.EqualTo(1));
            Assert.That(cut.Find("#lt-shell-directory").ClassList, Does.Not.Contain("lt-shell-directory--rail"));
            Assert.That(cut.FindAll(".lt-shell-header button").Select(button => button.TextContent.Trim()), Does.Not.Contain("Menu"));
        });
    }

    [Test]
    public async Task At_medium_the_spine_is_a_rail()
    {
        var cut = RenderLayout();

        await SetBandAsync(cut, 1);

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find("#lt-shell-directory").ClassList, Does.Contain("lt-shell-directory--rail"));
            Assert.That(cut.Find("#lt-shell-directory nav").ClassList, Does.Contain("lt-shell-directory__nav--rail"));
        });
    }

    [Test]
    public async Task Compact_moves_the_spine_into_a_sheet_opened_from_the_header()
    {
        AddArea(new FakeArea("data", "Data"));
        var cut = RenderLayout();

        await SetBandAsync(cut, 0);

        var toggle = cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Directory");
        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll("#lt-shell-directory"), Is.Empty, "the column is gone");
            Assert.That(cut.Find(".lt-shell").ClassList, Does.Contain("lt-shell--compact"));
            Assert.That(toggle.GetAttribute("aria-expanded"), Is.EqualTo("false"));
            Assert.That(cut.FindAll(".lt-dialog--sheet"), Is.Empty);
        });

        toggle.Click();

        var sheet = cut.Find(".lt-dialog--sheet");
        Assert.Multiple(() =>
        {
            Assert.That(sheet.ClassList, Does.Contain("lt-dialog--start"));
            Assert.That(sheet.QuerySelector("[data-lt-command='go.home']"), Is.Not.Null);
            Assert.That(sheet.QuerySelector("[data-lt-command='go.data']"), Is.Not.Null);
            Assert.That(cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Directory").GetAttribute("aria-expanded"), Is.EqualTo("true"));
        });
    }

    [Test]
    public async Task Following_a_stop_in_the_sheet_closes_it()
    {
        AddArea(new FakeArea("data", "Data"));
        var cut = RenderLayout();
        await SetBandAsync(cut, 0);
        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Directory").Click();

        cut.Find(".lt-dialog--sheet [data-lt-command='go.data']").Click();

        Assert.That(cut.FindAll(".lt-dialog--sheet"), Is.Empty);
    }

    [Test]
    public async Task Escape_closes_the_directory_sheet()
    {
        var cut = RenderLayout();
        await SetBandAsync(cut, 0);
        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Directory").Click();

        cut.Find(".lt-dialog--sheet").KeyDown(new KeyboardEventArgs { Key = "Escape" });

        Assert.That(cut.FindAll(".lt-dialog--sheet"), Is.Empty);
    }

    [Test]
    public async Task The_skip_link_to_the_directory_opens_the_sheet_when_compact()
    {
        var cut = RenderLayout();
        await SetBandAsync(cut, 0);

        cut.FindAll(".lt-shell-skip__link")[0].Click();

        Assert.That(cut.Find(".lt-dialog--sheet").ClassList, Does.Contain("lt-dialog--start"));
    }

    [Test]
    public async Task Compact_keeps_the_brand_whole_by_dropping_its_namespace()
    {
        // #3987: at 390px the full name was cut to "Orleans.Lattice Ex...".
        var cut = RenderLayout();
        Assert.That(cut.Find(".lt-shell-brand").TextContent.Trim(), Is.EqualTo("Orleans.Lattice Explorer"));

        await SetBandAsync(cut, 0);

        Assert.Multiple(() =>
        {
            Assert.That(cut.Find(".lt-shell-brand").TextContent.Trim(), Is.EqualTo("Lattice Explorer"));
            Assert.That(cut.FindAll(".lt-shell-brand__prefix"), Is.Empty);
        });
    }

    [Test]
    public async Task Compact_folds_the_session_slots_and_appearance_into_the_overflow_menu()
    {
        AddSlotProbes();
        var cut = RenderLayout();

        await SetBandAsync(cut, 0);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-shell-header [data-probe]"), Is.Empty, "the header keeps only the mark, the name and two buttons");
            Assert.That(cut.Find(".lt-shell-brand").TextContent, Does.Contain("Lattice Explorer"));
            Assert.That(cut.FindAll(".lt-shell-header .lt-shell-menu-host"), Is.Empty);
        });

        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Menu").Click();

        var menu = cut.Find(".lt-dialog--sheet.lt-dialog--end");
        Assert.Multiple(() =>
        {
            Assert.That(
                menu.QuerySelectorAll(".lt-shell-overflow [data-probe]").Select(element => element.GetAttribute("data-probe")),
                Is.EqualTo(new[] { "connection", "identity" }),
                "the session slots stack in the menu, connection first");
            Assert.That(menu.QuerySelector("[data-lt-command='appearance.theme.board']"), Is.Not.Null);
            Assert.That(cut.FindAll(".lt-shell > [data-probe='overlay']"), Has.Count.EqualTo(1), "the overlay slot never folds");
        });
    }

    [Test]
    public async Task Widening_again_closes_any_sheet_and_restores_the_header()
    {
        AddSlotProbes();
        var cut = RenderLayout();
        await SetBandAsync(cut, 0);
        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Menu").Click();

        await SetBandAsync(cut, 2);

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-dialog--sheet"), Is.Empty);
            Assert.That(cut.FindAll(".lt-shell-header [data-probe]"), Has.Count.EqualTo(2));
            Assert.That(cut.FindAll("#lt-shell-directory"), Has.Count.EqualTo(1));
        });
    }

    [Test]
    public async Task A_repeated_band_changes_nothing()
    {
        var cut = RenderLayout();
        await SetBandAsync(cut, 0);
        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Menu").Click();

        await SetBandAsync(cut, 0);

        Assert.That(cut.FindAll(".lt-dialog--sheet"), Has.Count.EqualTo(1), "the same band does not close an open sheet");
    }

    [Test]
    public async Task The_real_session_slots_render_inline_in_the_header_and_folded_in_the_menu()
    {
        // S2's own slot components, not probes: inline beside the appearance menu
        // while there is room, and in their folded, stacked form inside the menu
        // sheet below the small breakpoint.
        var cut = RenderLayout();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-shell-header [role='status'] .lt-pill"), Has.Count.EqualTo(1), "the connection indicator is in the header");
            Assert.That(cut.FindAll(".lt-shell-header section[aria-label='Connection']"), Is.Empty, "not folded while there is room");
        });

        await SetBandAsync(cut, 0);
        Assert.That(cut.FindAll(".lt-shell-header [role='status']"), Is.Empty, "the header folds");

        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Menu").Click();

        Assert.That(cut.FindAll(".lt-dialog--sheet .lt-shell-overflow section[aria-label='Connection']"), Has.Count.EqualTo(1),
            "the connection indicator renders folded in the menu");
    }

    [Test]
    public async Task Activating_anything_in_the_overflow_menu_closes_it()
    {
        AddSlotProbes();
        var cut = RenderLayout();
        await SetBandAsync(cut, 0);
        cut.FindAll(".lt-shell-header button").Single(button => button.TextContent.Trim() == "Menu").Click();

        cut.Find(".lt-dialog--sheet [data-lt-command='appearance.theme.board']").Click();

        Assert.Multiple(() =>
        {
            Assert.That(cut.FindAll(".lt-dialog--sheet"), Is.Empty, "the sheet never stacks under an overlay a slot opens");
            Assert.That(Applier.Applied.Last().Theme, Is.EqualTo(Orleans.Lattice.Explorer.UI.Layout.Appearance.ShellTheme.Board),
                "the control still acts before the sheet closes");
        });
    }

    [Test]
    public async Task The_measured_band_reaches_the_page()
    {
        var cut = Render<ShellLayout>(parameters => parameters.Add(layout => layout.Body, builder =>
        {
            builder.OpenComponent<BandProbe>(0);
            builder.CloseComponent();
        }));
        Assert.That(cut.Find("[data-band]").GetAttribute("data-band"), Is.EqualTo("Expanded"), "expanded until measured");

        await SetBandAsync(cut, 0);

        Assert.That(cut.Find("[data-band]").GetAttribute("data-band"), Is.EqualTo("Compact"));
    }

    /// <summary>A page that shows the band the layout cascades to it.</summary>
    public sealed class BandProbe : ComponentBase
    {
        [CascadingParameter(Name = LtBreakpointCascade.Name)]
        internal LtBreakpoint? Band { get; set; }

        /// <inheritdoc />
        protected override void BuildRenderTree(RenderTreeBuilder builder)
        {
            builder.OpenElement(0, "p");
            builder.AddAttribute(1, "data-band", Band?.ToString() ?? "none");
            builder.CloseElement();
        }
    }
}
