using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Auth;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Journeys;

/// <summary>
/// Issue #5582 (epic #5401): a platform operator grants one tenant's telemetry, and
/// nothing else, from the Access rule editor. The Scope list offers "One tenant's
/// telemetry (tenant capability)"; choosing it asks for the tenant and leaves only
/// Telemetry tickable; the saved rule is stored at the tenant's capability scope and
/// its details name the tenant. Before this change Telemetry could only be ticked
/// with the cluster-wide scope, which exposes every tenant's and the platform's
/// telemetry.
/// </summary>
/// <remarks>
/// Each step keeps an ordered screenshot under <c>screenshots/journey-d5582-tenant-telemetry</c>
/// (or <c>EXPLORER_UI_SCREENSHOTS</c>), as the epic's pull requests show them.
/// </remarks>
[TestFixture]
[Category("UI")]
public sealed class TenantTelemetryRuleJourneyTests : UiTestBase
{
    private const string RuleId = "d5582-acme-telemetry";

    private int _shot;

    [Test]
    public async Task An_operator_grants_one_tenants_telemetry_and_nothing_else()
    {
        var world = await UiHosts.DelegatedWorldAsync();
        var capability = LatticeScope.TenantCapability(TenantId.Parse(ExplorerWorld.Acme)).TreeId;
        await world.RemoveRuleAsync(capability, RuleId);
        try
        {
            var page = await OpenAsync(world.Head, "/access/rules?new=true", WorldIdentities.Admin);
            var content = Shell.Content(page);
            var editor = page.GetByRole(AriaRole.Dialog, new() { Name = "New rule" }).Locator("form.lt-access-form");
            await editor.GetByRole(AriaRole.Textbox, new() { Name = "Rule id", Exact = true }).FillAsync(RuleId);
            await editor.GetByRole(AriaRole.Combobox, new() { Name = "Subject kind", Exact = true }).SelectOptionAsync("user");
            await PickAsync(page, editor.GetByRole(AriaRole.Combobox, new() { Name = "Subject", Exact = true }), WorldIdentities.Erin);

            // The Scope list offers one tenant's telemetry.
            var scope = editor.GetByRole(AriaRole.Combobox, new() { Name = "Scope", Exact = true });
            await Expect(scope.Locator("option[value=tenant-capability]")).ToHaveTextAsync("One tenant's telemetry (tenant capability)");
            await scope.SelectOptionAsync("tenant-capability");
            await Expect(editor.GetByRole(AriaRole.Combobox, new() { Name = "Tree", Exact = true })).ToHaveCountAsync(0);
            await ShotAsync(page, "scope-offers-one-tenants-telemetry");

            // It asks for the tenant, and leaves only Telemetry tickable.
            await PickAsync(page, editor.GetByRole(AriaRole.Combobox, new() { Name = "Tenant", Exact = true }), ExplorerWorld.Acme);
            await Expect(editor.GetByLabel("Telemetry", new() { Exact = true })).ToBeEnabledAsync();
            await Expect(editor.GetByLabel("Read", new() { Exact = true })).ToBeDisabledAsync();
            await Expect(editor.GetByLabel("Admin", new() { Exact = true })).ToBeDisabledAsync();
            await Expect(editor.GetByLabel("App install", new() { Exact = true })).ToBeDisabledAsync();
            await editor.GetByLabel("Telemetry", new() { Exact = true }).CheckAsync();
            await editor.GetByRole(AriaRole.Button, new() { Name = "Save rule" }).ScrollIntoViewIfNeededAsync();
            await ShotAsync(page, "only-telemetry-for-tenant-acme");

            // Saved at acme's capability scope.
            await editor.GetByRole(AriaRole.Button, new() { Name = "Save rule" }).ClickAsync();
            await Expect(Shell.ToastMessages(page).Filter(new() { HasText = $"Rule {RuleId} saved." })).ToHaveCountAsync(1);
            await ShotAsync(page, "rule-saved");

            // Its details name the tenant.
            await Shell.GotoAsync(page, world.Head, $"/access/rules/{RuleId}?tree={capability}");
            await Expect(page.GetByRole(AriaRole.Heading, new() { Name = RuleId, Level = 1 })).ToBeVisibleAsync();
            await Expect(content.Locator(".lt-dl")).ToContainTextAsync($"tenant {ExplorerWorld.Acme} capability (telemetry)");
            await Expect(content.Locator(".lt-dl")).ToContainTextAsync("Telemetry");
            await ShotAsync(page, "details-name-the-tenant");
        }
        finally
        {
            await world.RemoveRuleAsync(capability, RuleId);
        }
    }

    private static async Task PickAsync(IPage page, ILocator box, string value)
    {
        await box.FillAsync(value);
        var option = page.Locator(".lt-combobox__list [role=option]").Filter(new()
        {
            Has = page.Locator(".lt-combobox__value", new() { HasTextRegex = new Regex($"^\\s*{Regex.Escape(value)}\\s*$") }),
        });
        await option.First.ClickAsync();
        await Expect(box).ToHaveValueAsync(value);
    }

    private async Task ShotAsync(IPage page, string name)
    {
        // Under screenshots/, which the UI lane uploads on every run, so the pull
        // request's journey screenshots come from CI's run of the head commit.
        var directory = Environment.GetEnvironmentVariable("EXPLORER_UI_SCREENSHOTS") is { Length: > 0 } configured
            ? configured
            : Path.Combine(System.IO.Directory.GetCurrentDirectory(), "screenshots", "journey-d5582-tenant-telemetry");
        _shot++;
        System.IO.Directory.CreateDirectory(directory);
        await page.ScreenshotAsync(new PageScreenshotOptions { Path = Path.Combine(directory, $"{_shot:D2}-{name}.png") });
    }
}
