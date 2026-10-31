using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>
/// CAPTURE-ONLY (first commit of the A9 pull request, replaced by the next): photographs the
/// rule-editor and rule-list defects A9 fixes, on the unchanged Explorer, for the pull
/// request's before/after pairs. It produces A9's contract state (factory-staff and its rule,
/// no scratch rule) so the chapter's verifier still passes.
/// </summary>
[JourneyChapter("A9", After = new[] { "A2" })]
public sealed class AccessInDepthJourney : JourneyChapter
{
    private const string ExistingScratchTree = "scratch-line-z";
    private const string MissingScratchTree = "scratch-line-a";

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        await ContosoHarness.SyncPolicyRulesAsync(context.Estate);
        var facades = ContractFacades.Of(context);

        // Capture set-up only: a scratch tree that sorts after the first page of rules, so a
        // rule saved on it is carried again by the page Load more reads.
        await facades.TreeAdmin.CreateTreeAsync(ExistingScratchTree);

        await using var olivia = await context.Estate.SignInAsAsync(Personas.Olivia);
        var steps = new A9Steps(olivia);
        var shell = olivia.Shell;
        var rules = new RulesListPage(shell);
        var rule = new RuleDetailsPage(shell);
        var groups = new GroupPages(shell);
        var staying = ContosoAccess.FactoryStaffTelemetryRead;
        var duplicated = ContosoAccess.ScratchRulePrefix + "listed-twice";

        await steps.StepAsync(0, "Before the fixes", async () =>
        {
            if (await rules.OpenFromSpineAsync())
            {
                await rules.OpenClusterWideAsync();
            }

            await groups.OpenListAsync();
            await groups.FillNewGroupAsync(ContosoAccess.FactoryStaff.GroupId, ContosoAccess.FactoryStaff.DisplayName);
            await groups.CreateAsync(ContosoAccess.FactoryStaff.GroupId);
            foreach (var member in ContosoAccess.FactoryStaff.Groups)
            {
                var option = await groups.OfferGroupMemberAsync(member);
                await groups.AddOfferedMemberAsync(option, member, ContosoAccess.FactoryStaff.GroupId);
            }

            await rules.OpenAsync();
            var editor = await rules.NewRuleAsync();
            await editor.RuleIdAsync(staying.RuleId);
            await editor.SubjectGroupAsync(Names.Groups.FactoryStaff);
            await editor.ScopeAsync(RuleEditorDialog.TreeScope);
            await editor.PickTreeAsync(staying.TreeId);
            await editor.OperationsAsync("Read", "Range read");
            await editor.SaveAsync();

            editor = await rules.NewRuleAsync();
            await editor.RuleIdAsync(duplicated);
            await editor.SubjectGroupAsync(Names.Groups.FactoryStaff);
            await editor.ScopeAsync(RuleEditorDialog.TreeScope);
            await editor.PickTreeAsync(ExistingScratchTree);
            await editor.OperationsAsync("Read");
            await editor.SaveAsync();
            await rules.LoadEverythingAsync();
            await rules.SearchAsync(ContosoAccess.ScratchRulePrefix);
            await steps.SubstepAsync("before-listed-twice", "BEFORE: a rule saved before Load more is listed again when its page arrives");

            await rules.Row(duplicated, ExistingScratchTree).First.GetByRole(AriaRole.Link, new() { Name = duplicated, Exact = true }).ClickAsync();
            await shell.ExpectHeadingAsync(duplicated);
            await rule.DeleteAsync(await rule.ConfirmDeleteAsync(duplicated), duplicated);

            await rules.WaitForListAsync();
            editor = await rules.NewRuleAsync();
            await editor.RuleIdAsync(ContosoAccess.ScratchRulePrefix + "key-reading");
            await editor.SubjectGroupAsync(Names.Groups.FactoryStaff);
            await editor.ScopeAsync(RuleEditorDialog.KeyScope);
            await editor.Tree.FillAsync(MissingScratchTree);
            await editor.Tree.PressAsync("Tab");
            await Expect(Pickers.ErrorOf(editor.Tree)).ToBeVisibleAsync();
            await steps.SubstepAsync("before-tree-refused", "BEFORE: a rule cannot name a tree that does not exist yet");
            await editor.CancelAsync();

            editor = await rules.NewRuleAsync();
            await editor.RuleIdAsync(staying.RuleId);
            await editor.SubjectGroupAsync(Names.Groups.FactoryStaff);
            await editor.ScopeAsync(RuleEditorDialog.ClusterScope);
            await editor.OperationsAsync("Telemetry");
            await editor.Form.GetByRole(AriaRole.Button, new() { Name = "Save rule", Exact = true }).ClickAsync();
            await Expect(editor.Form.Locator(".lt-field__error")).ToBeVisibleAsync();
            await steps.SubstepAsync("before-all-trees-id-clash", "BEFORE: an all-trees rule is refused an id another tree's rule uses");
            await editor.CancelAsync();
        });

        await ContractCalls.DeleteAndPurgeAsync(facades.TreeAdmin, ExistingScratchTree);
    }
}
