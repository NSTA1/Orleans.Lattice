using Microsoft.Extensions.DependencyInjection;
using Microsoft.Playwright;
using Orleans.Lattice.Schema;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Accessibility;

/// <summary>
/// Issue #5234: the schema Versions tab with its explanation, version map and the
/// list of what each migration hop changes is held to the same bar as every page:
/// the axe sweep in all eight appearances and no horizontal page scroll at 360 pixels.
/// </summary>
[TestFixture]
[Category("UI")]
public sealed class SchemaVersionsAccessibilityTests : UiTestBase
{
    private static readonly string VersionsPage = $"/schema/{ExplorerWorld.VersionsTree}?tab=versions";

    [Test]
    public async Task The_versions_tab_with_its_version_map_has_no_serious_violations_in_any_appearance()
    {
        var world = await UiHosts.WorldAsync();
        await VersionAsync(world);
        try
        {
            var page = await OpenAsync(world.Head, VersionsPage, WorldIdentities.Admin);
            await RegisterAsync(page);

            foreach (var appearance in ShellAppearanceChoice.All)
            {
                await Shell.SetAppearanceAsync(page, appearance);
                await Shell.WaitForMotionToSettleAsync(page);
                await AxeConformance.SweepAsync(page, $"the schema Versions tab on {VersionsPage} in {appearance}");
            }
        }
        finally
        {
            await Admin(world).ClearVersionConfigAsync(ExplorerWorld.VersionsTree);
        }
    }

    [Test]
    public async Task The_versions_tab_reflows_on_a_phone()
    {
        var world = await UiHosts.WorldAsync();
        await VersionAsync(world);
        try
        {
            var page = await OpenAsync(world.Head, VersionsPage, WorldIdentities.Admin, Shell.SmallWidth);
            await RegisterAsync(page);

            await Shell.AssertNoHorizontalPageScrollAsync(page, $"the schema Versions tab at {Shell.SmallWidth}px");
        }
        finally
        {
            await Admin(world).ClearVersionConfigAsync(ExplorerWorld.VersionsTree);
        }
    }

    private static ILatticeSchemaVersionAdmin Admin(ExplorerWorld world) => world.Head.Services.GetRequiredService<ILatticeSchemaVersionAdmin>();

    private static Task VersionAsync(ExplorerWorld world) =>
        Admin(world).SetVersionConfigAsync(ExplorerWorld.VersionsTree, new LatticeSchemaVersionConfig(1, 1, strictIngest: false));

    /// <summary>Registers the example definitions, which the catalog accepts again unchanged, so the map is shown.</summary>
    private static async Task RegisterAsync(IPage page)
    {
        var content = Shell.Content(page);
        await content.Locator("summary").Filter(new() { HasText = "Register versions and migration paths" }).ClickAsync();
        await content.GetByRole(AriaRole.Button, new() { Name = "Register definitions", Exact = true }).ClickAsync();
        await Expect(content.Locator("ol.lt-schema-hops .lt-schema-hops__step")).ToHaveTextAsync(["Renames qty to quantity."]);
        await Shell.WaitForMotionToSettleAsync(page);
    }
}
