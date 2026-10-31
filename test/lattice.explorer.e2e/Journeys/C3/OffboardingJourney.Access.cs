using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C3;

/// <summary>C3.4: the stewards' rule and their (now empty) group are deleted.</summary>
public sealed partial class OffboardingJourney
{
    /// <summary>
    /// C3.4: olivia deletes rule <c>stewards-factory</c> - one record per governed tree, each from
    /// its own page - and then deletes the now-empty group <c>data-stewards</c>.
    /// </summary>
    private static async Task DeleteRuleAndGroupAsync(PersonaSession olivia)
    {
        using var step = C3Step.Begin(4, "olivia deletes rule stewards-factory, then the empty group data-stewards");
        var rule = Names.Rules.StewardsFactory;
        var records = ContosoAccess.A2RulesNamed(rule).Count();
        var content = olivia.Shell.Content;

        await olivia.Shell.GotoAsync($"/access/rules/{rule}");
        await olivia.Shell.ExpectHeadingAsync(rule);
        var candidates = content.GetByRole(AriaRole.Table, new() { Name = "Rules with this id" });
        await Expect(candidates.Locator("tbody tr")).ToHaveCountAsync(records);
        await step.ShotAsync(olivia, "rule-records", $"stewards-factory governs {records} trees, one record each: olivia chooses each in turn");

        for (var deleted = 0; deleted < records; deleted++)
        {
            await olivia.Shell.GotoAsync($"/access/rules/{rule}");
            await olivia.Shell.ExpectHeadingAsync(rule);
            var delete = content.GetByRole(AriaRole.Button, new() { Name = "Delete rule", Exact = true });
            if (records - deleted > 1)
            {
                await Expect(candidates.Locator("tbody tr")).ToHaveCountAsync(records - deleted);
                await candidates.Locator("tbody tr").First.GetByRole(AriaRole.Link).First.ClickAsync();
            }

            await Expect(delete).ToBeVisibleAsync();
            await delete.ClickAsync();
            var confirm = new ConfirmDialog(olivia.Page, "Delete this rule");
            await confirm.TypeAsync(rule);
            if (deleted == 0)
            {
                await step.ShotAsync(olivia, "confirm-delete-rule", "Delete this rule: the grant to data-stewards stops applying at once; olivia types stewards-factory");
            }

            await confirm.ConfirmAsync("Delete rule");
            await Expect(olivia.Shell.Toasts.Filter(new() { HasText = $"Rule {rule} deleted." }).First).ToBeVisibleAsync();
        }

        await olivia.Shell.GotoAsync($"/access/rules/{rule}");
        await Expect(content.GetByRole(AriaRole.Button, new() { Name = "Delete rule", Exact = true })).ToHaveCountAsync(0);
        await Expect(candidates).ToHaveCountAsync(0);
        await step.ShotAsync(olivia, "rule-gone", "Every record of stewards-factory is gone");

        var group = Names.Groups.DataStewards;
        await olivia.Shell.GotoAsync($"/access/groups/{group}");
        await olivia.Shell.ExpectHeadingAsync(group);
        await Expect(content.GetByText("This group has no direct members.")).ToBeVisibleAsync();
        await step.ShotAsync(olivia, "group-empty", "data-stewards now has no member, and no rule names it directly");

        await content.GetByRole(AriaRole.Button, new() { Name = "Delete group", Exact = true }).ClickAsync();
        var deleteGroup = new ConfirmDialog(olivia.Page, "Delete this group");
        await deleteGroup.TypeAsync(group);
        await step.ShotAsync(olivia, "confirm-delete-group", "Delete this group: olivia types data-stewards");
        await deleteGroup.ConfirmAsync("Delete group");
        await Expect(olivia.Shell.Toasts.Filter(new() { HasText = $"Group {group} deleted." })).ToHaveCountAsync(1);
        await olivia.Shell.ExpectHeadingAsync("Access");
        await Expect(content.GetByRole(AriaRole.Link, new() { Name = group, Exact = true })).ToHaveCountAsync(0);
        await step.ShotAsync(olivia, "group-deleted", "data-stewards is deleted and leaves the groups list");
        Coverage.Hit("use-case:access/group-delete");
        step.Complete();
    }
}
