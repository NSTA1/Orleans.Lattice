using Orleans.Lattice.Explorer.Backup;
using Orleans.Lattice.Explorer.Backup.Components;
using Orleans.Lattice.Explorer.Core.Navigation;

namespace Orleans.Lattice.Explorer.Tests.Backup;

/// <summary>
/// The Backups area's open surface, carried on the shell's declared state
/// contract: remembered between visits, addressable in a link, and rendered by
/// the shared tab primitive rather than by markup this area owns.
/// </summary>
/// <remarks>
/// <para>
/// Before this the surface was written through an opaque plugin-namespace string
/// (<c>backups-subtab</c>), which the reset-view affordance could not enumerate,
/// the contract could not scope per cluster, and no link could name. All three
/// are what moving it onto the declared key buys, and each is asserted here.
/// </para>
/// <para>
/// Every render is driven by a stubbed domain answering synchronously over an
/// in-memory route model and an in-memory preference store, so nothing here
/// depends on timing, ordering, a wall clock, or garbage collection.
/// </para>
/// </remarks>
[TestFixture]
public sealed class BackupsSurfaceStateTests
{
    private static string Address(string surface) =>
        ExplorerRoutePath.Format(
            ExplorerRoute.Home.WithParameter(BackupsPluginKeys.SurfaceParameter, surface));

    private static string SelectionAddress(string surface) =>
        ExplorerRoutePath.Format(
            ExplorerRoute.Home
                .WithSelection(ExplorerRouteSegments.Trees, "orders")
                .WithSurface(surface));

    [Test]
    public async Task The_remembered_surface_is_the_one_that_opens()
    {
        var (html, state) = await BackupsRenderHarness.RenderPanelWithStateAsync(
            StubBackupsDomain.Create(),
            subTab: BackupsSubTab.New);

        Assert.Multiple(() =>
        {
            Assert.That(state.RememberedSurface, Is.EqualTo(BackupsSurfaces.New));
            Assert.That(html, Does.Contain("lx-backups-new"), "the capture form is the open surface");
        });
    }

    [Test]
    public async Task An_address_naming_a_surface_wins_over_the_remembered_one()
    {
        // A link somebody sent must show what they saw, not what the recipient
        // left open.
        var (html, _) = await BackupsRenderHarness.RenderPanelWithStateAsync(
            StubBackupsDomain.Create(),
            subTab: BackupsSubTab.New,
            address: Address(BackupsSurfaces.Existing));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-backups-body"), "the catalogue is the open surface");
            Assert.That(html, Does.Not.Contain("lx-backups-new"));
        });
    }

    [Test]
    public async Task A_surface_named_in_the_path_segment_of_a_selection_address_is_honoured_too()
    {
        // The grammar's own way to say it, which applies whenever the address
        // carries a catalogue selection. Both halves are read, because an
        // address can arrive from a bookmark taken in either shape.
        var (html, _) = await BackupsRenderHarness.RenderPanelWithStateAsync(
            StubBackupsDomain.Create(),
            subTab: BackupsSubTab.New,
            address: SelectionAddress(BackupsSurfaces.Existing));

        Assert.That(html, Does.Not.Contain("lx-backups-new"));
    }

    [Test]
    public async Task An_address_naming_a_surface_this_area_does_not_offer_falls_back_to_the_remembered_one()
    {
        // A surface segment belonging to some other area's address must not
        // leave this one showing nothing, and a value typed by hand must not
        // open something that is not there.
        var (html, _) = await BackupsRenderHarness.RenderPanelWithStateAsync(
            StubBackupsDomain.Create(),
            subTab: BackupsSubTab.New,
            address: Address("not-a-backups-surface"));

        Assert.That(html, Does.Contain("lx-backups-new"));
    }

    [Test]
    public async Task Opening_a_surface_remembers_it_and_puts_it_in_the_address()
    {
        var (_, state) = await BackupsRenderHarness.RenderPanelWithStateAsync(
            StubBackupsDomain.Create(),
            subTab: BackupsSubTab.New,
            afterFirstRender: panel => panel.SelectSurfaceAsync(BackupsSurfaces.Existing));

        Assert.Multiple(() =>
        {
            Assert.That(state.AddressedSurface, Is.EqualTo(BackupsSurfaces.Existing));
            Assert.That(state.RememberedSurface, Is.EqualTo(BackupsSurfaces.Existing));
        });
    }

    [Test]
    public async Task A_surface_slug_this_area_does_not_offer_opens_nothing()
    {
        var (html, state) = await BackupsRenderHarness.RenderPanelWithStateAsync(
            StubBackupsDomain.Create(),
            subTab: BackupsSubTab.New,
            afterFirstRender: panel => panel.SelectSurfaceAsync("not-a-backups-surface"));

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-backups-new"), "the open surface is unchanged");
            Assert.That(state.RememberedSurface, Is.EqualTo(BackupsSurfaces.New));
        });
    }

    [Test]
    public async Task The_surface_strip_is_the_subordinate_variant_bound_to_the_panel_it_controls()
    {
        // The presentation the shell reserves for a plugin's own surfaces, and
        // the relationship a tab must declare: a tab controlling nothing leaves
        // a screen-reader caller with nowhere to move into.
        var html = await BackupsRenderHarness.RenderPanelAsync(StubBackupsDomain.Create());

        Assert.Multiple(() =>
        {
            Assert.That(html, Does.Contain("lx-tabstrip-subordinate"));
            Assert.That(html, Does.Contain("id=\"" + BackupsSurfaces.PanelElementId + "\""));
            Assert.That(html, Does.Contain("aria-controls=\"" + BackupsSurfaces.PanelElementId + "\""));
            Assert.That(
                html,
                Does.Contain("lx-tabstrip-host"),
                "the strip is the shared primitive, not markup this area rolled itself");
        });
    }
}
