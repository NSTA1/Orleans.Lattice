using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A9;

/// <summary>
/// A9, access in depth (#5373). Contoso's policy-as-code pipeline has synced 230 rules into
/// the cluster. Olivia finds her way through them with the rule search, the ownership filter
/// and <b>Load more rules</b>; nests the stewards and the auditors under <c>factory-staff</c>;
/// writes the telemetry read rule that stays; authors one scratch rule per scope kind and reads
/// each back; sees the pipeline's conditional rule read-only, warned that conditions are not
/// evaluated; follows a rule id reused on two trees through its <c>?tree=</c> addresses; and
/// asks why dana may read the telemetry, and what dana may do across the factory trees.
/// </summary>
[JourneyChapter("A9", After = new[] { "A2" })]
public sealed class AccessInDepthJourney : JourneyChapter
{
    private const string ScratchTreeA = "scratch-line-a";
    private const string ScratchTreeB = "scratch-line-b";
    private const string ScratchShared = ContosoAccess.ScratchRulePrefix + "shared";
    private const string AllTrees = "all trees (cluster-wide)";

    private static readonly ScratchRule[] ScratchRules =
    [
        new(ContosoAccess.ScratchRulePrefix + "key-reading", RuleEditorDialog.KeyScope, ScratchTreeA, "Key", "reading/00000001",
            $"{ScratchTreeA} key reading/00000001", ["Read"], "Read"),
        new(ContosoAccess.ScratchRulePrefix + "prefix-readings", RuleEditorDialog.PrefixScope, ScratchTreeA, "Key prefix", "reading/",
            $"{ScratchTreeA} prefix reading/", ["Read", "Range read"], "Read, Range read"),
        new(ContosoAccess.ScratchRulePrefix + "whole-tree", RuleEditorDialog.TreeScope, ScratchTreeA, null, null,
            ScratchTreeA, ["Read", "Range read"], "Read, Range read"),
        new(ContosoAccess.ScratchRulePrefix + "all-trees", RuleEditorDialog.ClusterScope, null, null, null,
            AllTrees, ["Telemetry"], "Telemetry"),
    ];

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        // Off-UI, as the issue names it: the policy-as-code pipeline syncs its 230 rules.
        await ContosoHarness.SyncPolicyRulesAsync(context.Estate);
        var auth = ContractFacades.Of(context).Auth;

        await using var olivia = await context.Estate.SignInAsAsync(Personas.Olivia);
        var steps = new A9Steps(olivia);
        var shell = olivia.Shell;
        var rules = new RulesListPage(shell);
        var rule = new RuleDetailsPage(shell);
        var groups = new GroupPages(shell);
        var explain = new ExplainPage(shell);
        var page = olivia.Page;
        var group = $"group:{Names.Groups.FactoryStaff}";

        await steps.StepAsync(1, "Rules: the synced rules, 200 at a time", async () =>
        {
            var all = await ContractCalls.AllRulesAsync(auth);
            var total = all.Count;
            Assert.That(total, Is.GreaterThan(200), "the pipeline's rules fill more than one page");

            if (await rules.OpenFromSpineAsync())
            {
                Coverage.Hit("route:/t/{tenant}/access");
                await steps.SubstepAsync("tenant-rules", "Access opens at the default tenant: only its rules, and a link to every rule on the cluster");
                await rules.OpenClusterWideAsync();
            }

            await Expect(rules.Count).ToHaveTextAsync("200 rules, more to load");
            await Expect(rules.LoadMore).ToBeVisibleAsync();
            Coverage.Hit("route:/access/rules");
            await Expect(rules.PostureValue("Rules")).ToHaveTextAsync("Enforced");
            await Expect(rules.PostureValue("All-trees grants")).ToHaveTextAsync("On");
            await Expect(rules.PostureValue("Identity directory")).Not.ToHaveTextAsync("None");
            Coverage.Hit("use-case:access/posture-banner");
            await steps.SubstepAsync("first-page", "The rules page reads the first 200 rules and offers Load more rules, under the access posture");

            await rules.LoadAllAsync(total);
            Coverage.Hit("use-case:access/limits");
            await steps.SubstepAsync("every-rule-loaded", $"Load more rules reads the rest: all {total} rules are listed");

            const string term = "org-team-2";
            var matching = all.Count(candidate => Matches(candidate, term));
            await rules.SearchAsync(term);
            await Expect(rules.Count).ToHaveTextAsync($"{matching} of {total} rules");
            await Expect(rules.Row(ContosoHarness.SyncedRuleId(229), ContosoHarness.SyncedRuleTree(229))).ToHaveCountAsync(1);
            await Expect(rules.Row(ContosoHarness.ConditionalLegacyRule, ContosoHarness.ConditionalLegacyTree)).ToHaveCountAsync(1);
            await steps.SubstepAsync("search", $"Searching {term} matches rule ids, subjects and trees: {matching} of {total} rules");

            await rules.SearchAsync(string.Empty);
            var appOwned = all.Count(candidate => LatticeAppRuleIds.IsAppOwned(candidate.RuleId));
            await rules.FilterAsync("Authored");
            await Expect(rules.Count).ToHaveTextAsync(Shown(total - appOwned, total));
            await rules.FilterAsync("App-owned");
            await Expect(rules.Count).ToHaveTextAsync(Shown(appOwned, total));
            if (appOwned == 0)
            {
                await Expect(rules.Empty).ToBeVisibleAsync();
            }

            await steps.SubstepAsync("app-owned-filter", $"The ownership filter: {total - appOwned} authored rules, {appOwned} owned by an app");
            await rules.FilterAsync("All rules");
            await Expect(rules.Count).ToHaveTextAsync($"{total} rules");
            Coverage.Hit("use-case:access/rule-search");
        });

        await steps.StepAsync(2, "New group factory-staff, nesting the stewards and the auditors", async () =>
        {
            await groups.OpenListAsync();
            Coverage.Hit("route:/access/groups");
            Coverage.Hit("subroute:access/groups");
            await groups.FillNewGroupAsync(ContosoAccess.FactoryStaff.GroupId, ContosoAccess.FactoryStaff.DisplayName);
            await steps.SubstepAsync("new-group", "New group: factory-staff, a group stored here");
            await groups.CreateAsync(ContosoAccess.FactoryStaff.GroupId);
            Coverage.Hit("command:access.create-group");
            Coverage.Hit("route:/access/groups/{p1}");

            var first = true;
            foreach (var member in ContosoAccess.FactoryStaff.Groups)
            {
                var option = await groups.OfferGroupMemberAsync(member);
                await Expect(option.Locator(".lt-combobox__detail")).ToHaveTextAsync(new Regex(@"^\s*Stored\b"));
                if (first)
                {
                    await steps.SubstepAsync("nest-stored-group-offered", $"The member picker offers the stored group {member}, labelled Stored");
                    first = false;
                }

                await groups.AddOfferedMemberAsync(option, member, ContosoAccess.FactoryStaff.GroupId);
                await Expect(groups.MemberRow(member)).ToContainTextAsync("Group");
            }

            Coverage.Hit("use-case:access/subject-picker");
            Coverage.Hit("use-case:access/group-create-rename-members");
            await steps.SubstepAsync("nested-members", "factory-staff holds the groups data-stewards and auditors");

            await groups.FollowGroupLinkAsync(groups.Members, Names.Groups.Auditors, Names.Groups.Auditors);
            await Expect(groups.MemberRow(Personas.Dana.Id)).ToHaveCountAsync(1);
            await Expect(groups.MemberOf.GetByRole(AriaRole.Link, new() { Name = group, Exact = true })).ToBeVisibleAsync();
            Coverage.Hit("use-case:access/nested-groups");
            await steps.SubstepAsync("auditors-member-of", "auditors, holding dana, is now a member of factory-staff");
        });

        await steps.StepAsync(3, "New rule factory-staff-telemetry-read", async () =>
        {
            var staying = ContosoAccess.FactoryStaffTelemetryRead;
            var before = (await ContractCalls.AllRulesAsync(auth)).Count;
            await rules.OpenAsync();
            Coverage.Hit("route:/access/rules");
            Coverage.Hit("subroute:access/rules");
            var editor = await rules.NewRuleAsync();
            Coverage.Hit("command:access.create-rule");
            await editor.RuleIdAsync(staying.RuleId);
            await editor.SubjectGroupAsync(Names.Groups.FactoryStaff);
            await editor.ScopeAsync(RuleEditorDialog.TreeScope);
            await editor.PickTreeAsync(staying.TreeId);
            await editor.OperationsAsync("Read", "Range read");
            await Expect(Pickers.FlagOf(editor.Tree)).ToHaveCountAsync(0);
            await steps.SubstepAsync("new-rule", "New rule: factory-staff may read factory-telemetry");

            await editor.SaveAsync();
            await Expect(shell.Toasts.Filter(new() { HasText = $"Rule {staying.RuleId} saved." })).ToBeVisibleAsync();

            // Every rule is listed once after Load more, the new one included.
            await rules.LoadAllAsync(before + 1);
            await rules.SearchAsync(staying.RuleId);
            await Expect(rules.Count).ToHaveTextAsync($"1 of {before + 1} rules");
            await Expect(rules.Row(staying.RuleId, staying.TreeId)).ToContainTextAsync(group);
            Coverage.Hit("use-case:access/rule-create-edit-delete");
            await steps.SubstepAsync("rule-saved", "The rule is saved and listed: Allow, group:factory-staff, factory-telemetry, Read and Range read");
        });

        await steps.StepAsync(4, "One rule per scope kind, and the pipeline's conditional rule", async () =>
        {
            foreach (var scratch in ScratchRules)
            {
                await rules.OpenAsync();
                var editor = await rules.NewRuleAsync();
                var shot = scratch.Scope switch
                {
                    RuleEditorDialog.KeyScope => "key",
                    RuleEditorDialog.PrefixScope => "prefix",
                    RuleEditorDialog.TreeScope => "tree",
                    _ => "all-trees",
                };
                if (scratch.Scope == RuleEditorDialog.KeyScope)
                {
                    await Expect(editor.ConditionFields).ToHaveCountAsync(0);
                    await steps.SubstepAsync("no-condition-offered", "A new rule's editor offers no condition: conditions are not evaluated");
                }

                await editor.RuleIdAsync(scratch.RuleId);
                await editor.SubjectGroupAsync(Names.Groups.FactoryStaff);
                await editor.ScopeAsync(scratch.Scope);
                if (scratch.Tree is { } tree)
                {
                    await editor.NewTreeAsync(tree);
                    Coverage.Hit("use-case:access/rule-tree-not-yet-created");
                }

                if (scratch.KeyLabel is { } keyLabel)
                {
                    await editor.KeyOrPrefixAsync(keyLabel, scratch.KeyOrPrefix!);
                }

                await editor.OperationsAsync(scratch.Operations);
                await steps.SubstepAsync($"{shot}-scope-editor", $"A {ScopeWords(scratch.Scope)} rule, {scratch.RuleId}, on a scratch scope");
                await editor.SaveAsync();
                await Expect(shell.Toasts.Filter(new() { HasText = $"Rule {scratch.RuleId} saved." })).ToBeVisibleAsync();

                // Regression: a scratch tree sorts after the first page of rules, so the page
                // Load more reads carries the rule just saved again; it must be listed once.
                await rules.OpenRuleAsync(scratch.RuleId, scratch.ScopeLabel);
                await rule.ExpectAsync(scratch.RuleId, group, scratch.ScopeLabel, scratch.OperationsLabel);
                await Expect(rule.Condition).ToHaveCountAsync(0);
                Coverage.Hit("route:/access/rules/{p1}");
                await steps.SubstepAsync($"{shot}-scope-read-back", $"{scratch.RuleId} read back on its details page: {scratch.ScopeLabel}");
            }

            await rules.OpenAsync();
            await rules.OpenRuleAsync(ContosoHarness.ConditionalLegacyRule, ContosoHarness.ConditionalLegacyTree);
            await Expect(page).ToHaveURLAsync(new Regex($"/access/rules/{ContosoHarness.ConditionalLegacyRule}\\?tree={ContosoHarness.ConditionalLegacyTree}$"));
            await Expect(rule.Condition).ToHaveTextAsync(ContosoHarness.ConditionalLegacyCondition);
            await Expect(rule.ConditionWarning).ToHaveTextAsync(ConditionNotEvaluated);
            await steps.SubstepAsync("conditional-rule", "The pipeline's factory-conditional-legacy: its condition shown verbatim, warned that it is not evaluated");

            var edit = await rule.EditAsync();
            await Expect(edit.Condition).ToHaveValueAsync(ContosoHarness.ConditionalLegacyCondition);
            await Expect(edit.Condition).ToHaveAttributeAsync("readonly", new Regex(".*"));
            await Expect(edit.ConditionWarning).ToHaveTextAsync(ConditionNotEvaluated);
            await steps.SubstepAsync("conditional-rule-editor", "Editing it: the condition is read-only, kept as written, and warned");
            await edit.CancelAsync();
            Coverage.Hit("use-case:access/rule-scopes-and-conditions");

            var confirmShot = true;
            foreach (var scratch in ScratchRules)
            {
                await rules.OpenAsync();
                await rules.OpenRuleAsync(scratch.RuleId, scratch.ScopeLabel);
                var confirm = await rule.ConfirmDeleteAsync(scratch.RuleId);
                if (confirmShot)
                {
                    await steps.SubstepAsync("delete-confirmation", $"Delete this rule: {scratch.RuleId} stops applying at once");
                    confirmShot = false;
                }

                await rule.DeleteAsync(confirm, scratch.RuleId);
            }

            await rules.WaitForListAsync();
            await ExpectNoScratchRuleAsync(rules, auth);
            await steps.SubstepAsync("scratch-rules-deleted", "Every scratch rule is deleted");
        });

        await steps.StepAsync(5, "One rule id on two trees, each qualified by ?tree=", async () =>
        {
            foreach (var tree in new[] { ScratchTreeA, ScratchTreeB })
            {
                await rules.OpenAsync();
                var editor = await rules.NewRuleAsync();
                await editor.RuleIdAsync(ScratchShared);
                await editor.SubjectGroupAsync(Names.Groups.FactoryStaff);
                await editor.ScopeAsync(RuleEditorDialog.TreeScope);
                await editor.NewTreeAsync(tree);
                await editor.OperationsAsync("Read");
                if (tree == ScratchTreeB)
                {
                    await steps.SubstepAsync("same-id-second-tree", $"The id {ScratchShared} again, on a second tree: unique within each tree");
                }

                await editor.SaveAsync();
                await Expect(shell.Toasts.Filter(new() { HasText = $"Rule {ScratchShared} saved." }).Last).ToBeVisibleAsync();
            }

            await shell.GotoAsync($"/access/rules/{ScratchShared}");
            await shell.ExpectHeadingAsync(ScratchShared);
            await Expect(rule.Ambiguity).ToHaveTextAsync($"The id {ScratchShared} is used by a rule in more than one tree. Choose the one you mean.");
            await Expect(rule.Candidates).ToHaveCountAsync(2);
            await steps.SubstepAsync("id-used-in-two-trees", $"/access/rules/{ScratchShared} names two rules, so the page asks which one");

            foreach (var tree in new[] { ScratchTreeA, ScratchTreeB })
            {
                var row = Tables.Rows(page, shell.Content, ScratchShared, tree);
                await Expect(row.GetByRole(AriaRole.Link, new() { Name = ScratchShared, Exact = true })).ToHaveAttributeAsync("href", new Regex($"\\?tree={tree}$"));
                await row.GetByRole(AriaRole.Link, new() { Name = ScratchShared, Exact = true }).ClickAsync();
                await Expect(page).ToHaveURLAsync(new Regex($"/access/rules/{ScratchShared}\\?tree={tree}$"));
                await rule.ExpectAsync(ScratchShared, group, tree, "Read");
                await steps.SubstepAsync($"shared-id-{(tree == ScratchTreeA ? "first" : "second")}-tree", $"?tree={tree} opens the rule on {tree}");
                await page.GoBackAsync();
                await Expect(rule.Candidates).ToHaveCountAsync(2);
            }

            Coverage.Hit("query:access.tree");
            Coverage.Hit("use-case:access/addresses");

            foreach (var tree in new[] { ScratchTreeA, ScratchTreeB })
            {
                await shell.GotoAsync($"/access/rules/{ScratchShared}?tree={tree}");
                await rule.ExpectAsync(ScratchShared, group, tree, "Read");
                var confirm = await rule.ConfirmDeleteAsync(ScratchShared);
                await rule.DeleteAsync(confirm, ScratchShared);
            }

            await rules.WaitForListAsync();
            await ExpectNoScratchRuleAsync(rules, auth);
            await steps.SubstepAsync("no-scratch-rule-remains", "Both are deleted: no scratch rule remains");
        });

        await steps.StepAsync(6, "Explain, prefilled from the address: dana reads the telemetry through the nested group", async () =>
        {
            var dana = Personas.Dana.Id;
            var telemetry = Names.Trees.FactoryTelemetry;
            await explain.OpenAtAsync($"/access/explain?subject={dana}&tree={telemetry}");
            await Expect(explain.SubjectKind).ToHaveValueAsync("user");
            await Expect(explain.Subject).ToHaveValueAsync(dana);
            await Expect(explain.Operation).ToHaveValueAsync("read");
            await Expect(explain.Scope).ToHaveValueAsync(RuleEditorDialog.TreeScope);
            await Expect(explain.Tree).ToHaveValueAsync(telemetry);
            Coverage.Hit("route:/access/explain");
            Coverage.Hit("query:access.subject");
            Coverage.Hit("query:access.tree");
            await steps.SubstepAsync("prefilled-from-address", $"/access/explain?subject={dana}&tree={telemetry} fills in the question");

            await explain.ExplainAsync();
            await Expect(explain.Verdict).ToHaveTextAsync("Allowed");
            await Expect(explain.GroupLink(Names.Groups.Auditors)).ToBeVisibleAsync();
            await Expect(explain.GroupLink(Names.Groups.FactoryStaff)).ToBeVisibleAsync();
            await Expect(explain.RuleRow(ContosoAccess.FactoryStaffTelemetryRead.RuleId, telemetry)).ToContainTextAsync(group);
            await Expect(page).ToHaveURLAsync(new Regex($"[?&]{Regex.Escape("operation=read")}(&|$)"));
            await Expect(page).ToHaveURLAsync(new Regex("[?&]kind=user(&|$)"));
            Coverage.Hit("query:access.operation");
            Coverage.Hit("query:access.kind");
            Coverage.Hit("use-case:access/explain");
            await steps.SubstepAsync("allowed-through-nested-group", "Allowed: dana is in auditors, auditors in factory-staff, and factory-staff-telemetry-read decides");

            await explain.GroupLink(Names.Groups.Auditors).ClickAsync();
            await groups.ExpectGroupAsync(Names.Groups.Auditors);
            await Expect(groups.MemberRow(dana)).ToHaveCountAsync(1);
            await Expect(groups.MemberOf.GetByRole(AriaRole.Link, new() { Name = group, Exact = true })).ToBeVisibleAsync();
            await steps.SubstepAsync("chain", "The chain, followed: dana is a direct member of auditors, which is a member of factory-staff");
            await page.GoBackAsync();
            await Expect(explain.Verdict).ToHaveTextAsync("Allowed", new() { Timeout = 30_000 });
        });

        await steps.StepAsync(7, "Effective permissions for dana across the factory trees", async () =>
        {
            var dana = Personas.Dana.Id;
            await explain.EffectivePermissionsAsync();
            await Expect(explain.PermissionsHeading).ToHaveTextAsync($"Effective permissions for {dana}");
            await ExpectDanasPermissionsAsync(explain, group);
            await Expect(page).ToHaveURLAsync(new Regex("[?&]view=permissions(&|$)"));
            Coverage.Hit("use-case:access/effective-permissions");
            await steps.SubstepAsync("effective-permissions", $"Effective permissions for {dana}: the auditors' reads, and the telemetry through factory-staff");

            await explain.OpenAtAsync($"/access/explain?subject={dana}&kind=user&view=permissions");
            await Expect(explain.PermissionsHeading).ToHaveTextAsync($"Effective permissions for {dana}", new() { Timeout = 30_000 });
            await ExpectDanasPermissionsAsync(explain, group);
            Coverage.Hit("query:access.view");
            Coverage.Hit("query-value:access.view=permissions");
            await steps.SubstepAsync("effective-permissions-from-address", "?view=permissions opens the same answer from its address");
        });

        await steps.StepAsync(8, "Server authority: a delegated administrator's grant is revoked under an open page", async () =>
        {
            var priya = Personas.Priya.Id;
            var staffGroup = ContosoAccess.FactoryStaff.GroupId;
            const string accessAdministration = "access administration";

            await rules.OpenAsync();
            await Expect(rules.PostureValue("Access-admin delegation")).ToHaveTextAsync("On");
            var editor = await rules.NewRuleAsync();
            await editor.RuleIdAsync(DelegatedAdministration);
            await editor.SubjectUserAsync(priya);
            await editor.ScopeAsync(RuleEditorDialog.AccessAdministrationScope);
            await editor.OperationsAsync("Admin");
            await Expect(editor.Tree).ToHaveCountAsync(0);
            await steps.SubstepAsync("delegate-access-administration", $"Olivia delegates access administration to {priya}: Admin on access administration");
            await editor.SaveAsync();
            await Expect(shell.Toasts.Filter(new() { HasText = $"Rule {DelegatedAdministration} saved." })).ToBeVisibleAsync();

            await using var delegated = await context.Estate.SignInAsAsync(Personas.Priya);
            var theirs = new GroupPages(delegated.Shell);
            await OpenWhenGrantedAsync(delegated, theirs, staffGroup);
            await Expect(theirs.Details.GetByRole(AriaRole.Textbox, new() { Name = "Display name", Exact = true })).ToHaveValueAsync(ContosoAccess.FactoryStaff.DisplayName);
            Coverage.Hit("use-case:access/delegated-administration");
            await steps.SubstepAsync(delegated, "delegated-administrator", $"{priya}, now an access administrator, opens factory-staff's page");

            await rules.OpenRuleAsync(DelegatedAdministration, accessAdministration);
            await rule.ExpectAsync(DelegatedAdministration, $"user:{priya}", accessAdministration, "Admin");
            await rule.DeleteAsync(await rule.ConfirmDeleteAsync(DelegatedAdministration), DelegatedAdministration);
            await steps.SubstepAsync("revoked", $"Olivia deletes {DelegatedAdministration}; {priya}'s page is still open");

            await ExpectStalePageRefusedAsync(delegated, theirs);
            await Expect(theirs.DisplayNameError).ToContainTextAsync("You are not permitted to administer access on this cluster.");
            Coverage.Hit("use-case:access/server-authority");
            await steps.SubstepAsync(delegated, "stale-page-refused", $"{priya} saves from the stale page: the cluster refuses, and nothing changes");

            await rules.WaitForListAsync();
            await ExpectNoScratchRuleAsync(rules, auth);
            await explain.OpenAtAsync($"/access/explain?subject={priya}&kind=user&view=permissions");
            await Expect(explain.PermissionsHeading).ToHaveTextAsync($"Effective permissions for {priya}", new() { Timeout = 30_000 });
            await Expect(explain.RuleRows.Filter(new() { HasText = accessAdministration })).ToHaveCountAsync(0);
            Assert.That(
                (await ContractCalls.AllGroupsAsync(auth)).Single(candidate => candidate.GroupId == staffGroup).DisplayName,
                Is.EqualTo(ContosoAccess.FactoryStaff.DisplayName),
                "the refused save changed nothing");
            await steps.SubstepAsync("revoke-complete", $"The revoke is complete: no rule grants {priya} access administration, and no scratch rule remains");
        });
    }

    private const string DelegatedAdministration = ContosoAccess.ScratchRulePrefix + "access-admin";

    /// <summary>
    /// Opens <paramref name="group"/>'s page in the delegated administrator's session. The
    /// grant reaches every silo's compiled policy shortly after it commits, so a page that
    /// still says "not permitted" is read again, within a bound.
    /// </summary>
    private static async Task OpenWhenGrantedAsync(PersonaSession delegated, GroupPages pages, string group)
    {
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (true)
        {
            await delegated.Shell.GotoAsync($"/access/groups/{group}");
            try
            {
                await pages.ExpectGroupAsync(group);
                await Expect(pages.SaveName).ToBeEnabledAsync(new() { Timeout = 5_000 });
                return;
            }
            catch (PlaywrightException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(TimeSpan.FromSeconds(2));
            }
        }
    }

    /// <summary>
    /// Saves the group's unchanged display name from the revoked administrator's stale page
    /// until the cluster refuses it. The revoke reaches every silo's compiled policy shortly
    /// after it commits, and the write is harmless if it lands before then: the same name.
    /// </summary>
    private static async Task ExpectStalePageRefusedAsync(PersonaSession delegated, GroupPages pages)
    {
        var deadline = DateTime.UtcNow.AddSeconds(60);
        while (true)
        {
            await pages.SaveName.ClickAsync();
            try
            {
                await Expect(pages.DisplayNameError).ToBeVisibleAsync(new() { Timeout = 5_000 });
                return;
            }
            catch (PlaywrightException) when (DateTime.UtcNow < deadline)
            {
                // Saved before the revoke reached this silo: the same name, so nothing changed.
                await Task.Delay(TimeSpan.FromSeconds(3));
            }
        }
    }

    private const string ConditionNotEvaluated =
        "Conditions are not evaluated in this version. This rule applies unconditionally, as if it had no condition.";

    private static async Task ExpectDanasPermissionsAsync(ExplainPage explain, string group)
    {
        await Expect(explain.GroupLink(Names.Groups.Auditors)).ToBeVisibleAsync();
        await Expect(explain.GroupLink(Names.Groups.FactoryStaff)).ToBeVisibleAsync();
        foreach (var record in ContosoAccess.A2RulesNamed(Names.Rules.AuditorsFactoryRead))
        {
            var scope = record.TreeId == LatticeScope.ClusterWideTreeId ? AllTrees : record.TreeId;
            await Expect(explain.RuleRow(record.RuleId, scope)).ToContainTextAsync($"group:{Names.Groups.Auditors}");
        }

        await Expect(explain.RuleRow(ContosoAccess.FactoryStaffTelemetryRead.RuleId, Names.Trees.FactoryTelemetry)).ToContainTextAsync(group);
    }

    private static async Task ExpectNoScratchRuleAsync(RulesListPage rules, Orleans.Lattice.Api.Auth.Grpc.LatticeAuthApiGrpcClient auth)
    {
        var total = (await ContractCalls.AllRulesAsync(auth)).Count;
        await rules.LoadAllAsync(total);
        await rules.SearchAsync(ContosoAccess.ScratchRulePrefix);
        await Expect(rules.Count).ToHaveTextAsync(Shown(0, total));
        await Expect(rules.Empty).ToBeVisibleAsync();
    }

    private static string Shown(int shown, int total) => shown == total ? $"{total} rules" : $"{shown} of {total} rules";

    private static bool Matches(LatticeAuthorizationRule rule, string term) =>
        rule.RuleId.Contains(term, StringComparison.OrdinalIgnoreCase)
        || rule.Subject.Id.Contains(term, StringComparison.OrdinalIgnoreCase)
        || rule.Scope.TreeId.Contains(term, StringComparison.OrdinalIgnoreCase);

    private static string ScopeWords(string scope) => scope switch
    {
        RuleEditorDialog.KeyScope => "single-key",
        RuleEditorDialog.PrefixScope => "key-prefix",
        RuleEditorDialog.TreeScope => "whole-tree",
        _ => "all-trees",
    };

    /// <summary>A scratch rule A9.4 authors, reads back and deletes.</summary>
    private sealed record ScratchRule(
        string RuleId,
        string Scope,
        string? Tree,
        string? KeyLabel,
        string? KeyOrPrefix,
        string ScopeLabel,
        string[] Operations,
        string OperationsLabel);
}
