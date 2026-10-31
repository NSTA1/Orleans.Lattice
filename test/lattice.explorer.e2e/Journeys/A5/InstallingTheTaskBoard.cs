using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Apps = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Apps;
using Groups = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Groups;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A5;

/// <summary>
/// Journey A5 (#5376), installing the task board: olivia browses both app sources, installs
/// version 1 from <c>samples</c> with its roles bound to A2's teams, finds its app-owned rules
/// in Access, gathers the board's viewers into one nested group and rebinds the viewer role to
/// it, edits the consent, upgrades to vNext from <c>samples-next</c> consenting to its one new
/// operation, and reads every section of the app's page.
/// </summary>
[JourneyChapter("A5", After = new[] { "A2" })]
public sealed class InstallingTheTaskBoard : JourneyChapter
{
    /// <summary>The journey id its screenshots are filed under.</summary>
    public const string JourneyId = "a5";

    private const string TaskBoard = "Task board";

    /// <summary>Every app the estate's two sources offer: the task board from each, and 60 catalogue fillers.</summary>
    private const int CatalogueSize = 62;

    private static readonly string[] CeilingOperations = ["read", "range read", "write", "delete", "range delete"];

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        await using var olivia = await context.Estate.SignInAsAsync(Personas.Olivia);
        var run = new Steps(olivia);
        var apps = new AppsCataloguePage(olivia.Shell);
        var review = new AppReviewFlowPage(olivia.Shell);
        var access = new AccessForAppsPage(olivia.Shell);
        var sections = new AppSectionsPage(olivia.Shell);

        await run.StepAsync("1", async () =>
        {
            await apps.ExpectNoAppsYetAsync();
            await run.ShotAsync("no-apps-yet", "Apps: No apps yet - nothing is installed, and olivia may install");

            await apps.InstallFromPaletteAsync();
            await run.ShotAsync("catalogue-available", "The palette's Install app... opens the catalogue on Available: 2 sources, the first 50 apps");

            await apps.LoadMoreAsync(CatalogueSize);
            await run.ShotAsync("load-more", "Load more apps lists the remaining 12: 62 apps in all");

            await apps.SelectSourceAsync(Apps.SamplesNextSource);
            await Expect(apps.Count).ToHaveTextAsync("2 sources, 1 app");
            await Expect(apps.Row(TaskBoard, Apps.SamplesNextSource)).ToContainTextAsync(Apps.TaskBoardNextVersion);
            await run.ShotAsync("source-samples-next", "Source samples-next offers one app: the task board's vNext, 1.1.0");

            await apps.SelectSourceAsync(Apps.SamplesSource);
            await Expect(apps.Count).ToHaveTextAsync("2 sources, 50+ apps");
            await apps.SearchAsync("task");
            await Expect(apps.Count).ToHaveTextAsync("2 sources, 1 app");
            await Expect(apps.Row(TaskBoard, Apps.SamplesSource)).ToContainTextAsync(Apps.TaskBoardVersion);
            await run.ShotAsync("search-samples", "Source samples searched for 'task': the task board 1.0.0 alone");

            await apps.FilterAsync("Installed");
            await apps.ExpectNoMatchAsync("Nothing from this selection is installed here.");
            await run.ShotAsync("filter-installed", "The Installed filter: nothing from this selection is installed yet");

            await apps.FilterAsync("Updates");
            await apps.ExpectNoMatchAsync("Every installed app is on its source's newest version.");
            await run.ShotAsync("filter-updates", "The Updates filter: no installed app has an update");

            await apps.FilterAsync("All");
            await Expect(apps.Row(TaskBoard, Apps.SamplesSource)).ToContainTextAsync("Available");
            Coverage.Hit("use-case:apps/sources");
            await apps.OpenRowActionAsync("Review", TaskBoard, Apps.SamplesSource);
        });

        await run.StepAsync("2", async () =>
        {
            await review.ExpectReviewAsync();
            await Expect(review.Section("What it asks for")).ToContainTextAsync("read, write, delete and range read");
            await Expect(review.Section("What it asks for")).ToContainTextAsync(Apps.EditorRole);
            await Expect(review.Section("What it asks for")).ToContainTextAsync(Apps.ViewerRole);
            await Expect(review.Integration).ToContainTextAsync($"{A5Contract.TaskBoardTree} as LwwRegister");
            await run.ShotAsync("review-v1", "The v1.0.0 review: its manifest, roles, operations, scopes and replicated tree", fullPage: true);

            await review.ClickAsync("Install...");
            await review.ExpectBindingStepAsync("Bind roles to groups");
            await run.ShotAsync("bind-roles", "Install... opens Bind roles: each role takes one group");

            await review.BindAsync(Apps.EditorRole, Groups.DataStewards);
            await review.BindAsync(Apps.ViewerRole, Groups.Auditors);
            var shortcut = review.NotMemberOf(Groups.DataStewards).GetByRole(AriaRole.Link, new() { Name = $"Add me to {Groups.DataStewards}", Exact = true });
            await Expect(shortcut).ToBeVisibleAsync();
            await Expect(olivia.Shell.Content.Locator("[data-lt-holding-summary=none]")).ToContainTextAsync("You will hold no role in Task board");
            await run.ShotAsync("bound-not-member", "Editor to data-stewards, viewer to auditors: olivia is in neither, so Add me to data-stewards is offered");

            await shortcut.ClickAsync();
            await review.ExpectUrlAsync($"/access/groups/{Groups.DataStewards}$");
            await olivia.Shell.ExpectHeadingAsync(Groups.DataStewards);
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = "Add member", Exact = true })).ToBeVisibleAsync();
            await run.ShotAsync("add-me-shortcut", "Add me to data-stewards opens the group in Access, ready to add olivia");

            // Backed out: olivia adds nobody and returns; the install flow kept its stage and bindings.
            await olivia.Page.GoBackAsync();
            await review.ExpectBindingStepAsync("Bind roles to groups");
            await Expect(review.GroupFor(Apps.EditorRole)).ToHaveValueAsync(Groups.DataStewards);
            await Expect(review.GroupFor(Apps.ViewerRole)).ToHaveValueAsync(Groups.Auditors);
            await run.ShotAsync("backed-out", "Backed out without joining: the flow resumes at Bind roles with both bindings kept");

            await review.ClickAsync("Continue");
            await review.ExpectCeilingStepAsync();
            await review.ExpectCeilingAsync(CeilingOperations, ["read", "range read", "write", "delete"]);
            await Expect(review.Activation("ok")).ToBeVisibleAsync();
            await run.ShotAsync("confirm-ceiling", "Confirm the ceiling: read, range read, write and delete, exactly what v1 requests; it would activate", fullPage: true);

            await review.ClickAsync("Install v1.0.0");
            await review.ExpectStatusAsync("Task board v1.0.0 is installed and not enabled yet.");
            await Expect(review.AppAddress).ToHaveTextAsync("/apps/task-board");
            await run.ShotAsync("installed", "Installed v1.0.0, not enabled yet; its page is /apps/task-board");

            await review.ClickAsync("Enable now");
            await review.ExpectStatusAsync("Task board v1.0.0 is enabled.");
            await run.ShotAsync("enabled", "Enable now: Task board v1.0.0 is enabled");
            Coverage.Hit("use-case:apps/install");
        });

        await run.StepAsync("3", async () =>
        {
            await apps.ExpectInstalledAsync(Apps.TaskBoard, TaskBoard, Apps.TaskBoardVersion, "Enabled");
            await Expect(olivia.Shell.Content.GetByRole(AriaRole.Heading, new() { Name = "No role in an app", Exact = true })).ToBeVisibleAsync();
            await run.ShotAsync("apps-enabled", "Apps lists the task board enabled at 1.0.0; olivia holds none of its roles");

            await access.ShowAppOwnedRulesAsync(Apps.TaskBoard);
            await run.ShotAsync("app-owned-rules", "Access, App-owned: the task board's rules, each owned by app task-board");

            await access.OpenAppOwnedRuleAsync(Apps.TaskBoard);
            await run.ShotAsync("app-owned-rule", "An app-owned rule is read-only, attributed to the task-board app");

            await access.FollowToAppRolesAsync(Apps.TaskBoard);
            await sections.ExpectSectionAsync(Apps.TaskBoard, TaskBoard, "roles");
            await Expect(sections.Panel).ToContainTextAsync(Groups.DataStewards);
            await Expect(sections.Panel).ToContainTextAsync(Groups.Auditors);
            await run.ShotAsync("app-roles", "Its link leads to /apps/task-board/roles: editor bound to data-stewards, viewer to auditors");
        });

        await run.StepAsync("4", async () =>
        {
            var viewers = ContosoAccess.TaskBoardViewers;
            await access.CreateGroupAsync(viewers.GroupId, viewers.DisplayName, () => run.ShotAsync("new-group", "New group: task-board-viewers, display name Task board viewers"));
            foreach (var member in viewers.Groups)
            {
                await access.AddGroupMemberAsync(member);
            }

            await Expect(access.DirectMembers).ToHaveCountAsync(viewers.Groups.Count);
            await run.ShotAsync("group-nested", "task-board-viewers nests auditors and schema-owners");

            await review.OpenAsync(Apps.SamplesSource, Apps.TaskBoard, TaskBoard);
            await Expect(review.Lede).ToContainTextAsync("v1.0.0 is installed, from ");
            await review.ClickAsync("Change role bindings...");
            await review.ExpectBindingStepAsync("Change role bindings");
            await Expect(review.GroupFor(Apps.ViewerRole)).ToHaveValueAsync(Groups.Auditors);

            // A role binds to exactly one group: the step holds one picker per role, so the two
            // teams that view the board reach the role through the one nested group.
            await Expect(review.GroupFor(Apps.ViewerRole)).ToHaveCountAsync(1);
            await review.BindAsync(Apps.ViewerRole, viewers.GroupId);
            await run.ShotAsync("rebind-viewer", "Change role bindings...: one group per role, viewer rebound to task-board-viewers");

            await review.ClickAsync("Continue");
            await Expect(review.BindingChange(Apps.ViewerRole)).ToHaveAttributeAsync("data-lt-changed", "true");
            await Expect(review.BindingChange(Apps.ViewerRole)).ToContainTextAsync(Groups.Auditors);
            await Expect(review.BindingChange(Apps.ViewerRole)).ToContainTextAsync(viewers.GroupId);
            await Expect(review.BindingChange(Apps.EditorRole)).ToHaveAttributeAsync("data-lt-changed", "false");
            await run.ShotAsync("confirm-bindings", "Confirm the role bindings: viewer moves from auditors to task-board-viewers, editor unchanged");

            await review.ClickAsync("Apply bindings...");
            var apply = olivia.Page.GetByRole(AriaRole.Dialog, new() { Name = "Change the role bindings of Task board?" });
            await Expect(apply).ToContainTextAsync("Its access rules are replaced at once.");
            await run.ShotAsync("apply-bindings", "Apply bindings... asks once more: its access rules are replaced at once");

            await apply.GetByRole(AriaRole.Button, new() { Name = "Apply", Exact = true }).ClickAsync();
            await review.ExpectStatusAsync("The role bindings of Task board v1.0.0 are changed and its access rules replaced.");
            await run.ShotAsync("bindings-applied", "The bindings are changed and the app's access rules replaced");
            Coverage.Hit("use-case:apps/role-bindings");

            await review.ClickAsync("Back to the review");
            await review.ExpectReviewAsync();
            await review.ClickAsync("Edit consent...");
            await review.ExpectCeilingStepAsync();
            await Expect(review.Steps).ToContainTextAsync("Record consent");
            await review.ExpectCeilingAsync(CeilingOperations, ["read", "range read", "write", "delete"]);
            await run.ShotAsync("edit-consent", "Edit consent...: the recorded ceiling, pinned to 1.0.0; range delete is not in it", fullPage: true);

            await review.Operation("delete").UncheckAsync();
            await Expect(review.Activation("fails")).ToContainTextAsync("The role editor asks to delete, which the ceiling excludes");
            await run.ShotAsync("narrowed", "Narrowing the ceiling to drop delete: the editor role would fail activation");

            await review.Operation("delete").CheckAsync();
            await Expect(review.Activation("ok")).ToBeVisibleAsync();
            await review.ClickAsync("Back");
            await review.ExpectReviewAsync();
            Coverage.Hit("use-case:apps/edit-consent");
        });

        await run.StepAsync("5", async () =>
        {
            await olivia.Shell.GotoAsync("/apps/catalogue");
            await apps.FilterAsync("Updates");
            await apps.ExpectNoMatchAsync("Every installed app is on its source's newest version.");
            await run.ShotAsync("updates", "The Updates filter compares each install with its own source: samples has nothing newer");

            await apps.FilterAsync("All");
            await apps.SelectSourceAsync(Apps.SamplesNextSource);
            await Expect(apps.Row(TaskBoard, Apps.SamplesNextSource)).ToContainTextAsync(Apps.TaskBoardNextVersion);
            await run.ShotAsync("vnext-offered", "vNext appears from samples-next: the task board 1.1.0");

            await apps.OpenRowActionAsync("Review", TaskBoard, Apps.SamplesNextSource);
            await Expect(review.Lede).ToContainTextAsync("v1.0.0 is installed; ");
            await Expect(review.Lede).ToContainTextAsync("offers v1.1.0.");
            await Expect(review.UpgradeDiff).ToContainTextAsync($"Changes what the role {Apps.EditorRole} may do");
            await Expect(review.UpgradeDiff).ToContainTextAsync("Needs the ceiling to allow it to range delete");
            await Expect(review.UpgradeDiff.Locator("[data-lt-reconsent=true]")).ToBeVisibleAsync();
            await Expect(review.Integration).ToContainTextAsync($"{A5Contract.TaskBoardTree} as LwwRegister");
            await run.ShotAsync("upgrade-diff", "The upgrade diff: the editor role adds range delete, which needs re-consent; its tree stays replicated", fullPage: true);

            await review.ClickAsync($"Upgrade to v{Apps.TaskBoardNextVersion}...");
            await review.ExpectBindingStepAsync("Bind roles to groups");
            await Expect(review.GroupFor(Apps.EditorRole)).ToHaveValueAsync(Groups.DataStewards);
            await Expect(review.GroupFor(Apps.ViewerRole)).ToHaveValueAsync(ContosoAccess.TaskBoardViewers.GroupId);
            await review.ClickAsync("Continue");
            await review.ExpectCeilingStepAsync();
            await review.ExpectCeilingAsync(CeilingOperations, CeilingOperations);
            await Expect(review.Activation("ok")).ToBeVisibleAsync();
            await run.ShotAsync("re-consent", "Re-consent: the ceiling now includes the one new operation, range delete", fullPage: true);

            await review.ClickAsync($"Upgrade to v{Apps.TaskBoardNextVersion}");
            await review.ExpectStatusAsync($"Task board v{Apps.TaskBoardNextVersion} is");
            if (await olivia.Shell.Content.GetByRole(AriaRole.Button, new() { Name = "Enable now", Exact = true }).IsVisibleAsync())
            {
                await review.ClickAsync("Enable now");
            }

            await review.ExpectStatusAsync($"Task board v{Apps.TaskBoardNextVersion} is enabled.");
            await run.ShotAsync("upgraded", "Upgraded: Task board v1.1.0 is enabled");
            Coverage.Hit("use-case:apps/upgrade-and-re-consent");
        });

        await run.StepAsync("6", async () =>
        {
            await sections.OpenAsync(Apps.TaskBoard, TaskBoard, "overview");
            await Expect(sections.Panel).ToContainTextAsync(Apps.TaskBoardNextVersion);
            await Expect(sections.Panel).ToContainTextAsync(Apps.SamplesNextSource);
            await run.ShotAsync("overview", "Overview: version 1.1.0 from samples-next, enabled; olivia holds no role");

            await sections.ChooseTabAsync(Apps.TaskBoard, TaskBoard, "Trees");
            await Expect(sections.Panel).ToContainTextAsync("tasks");
            await run.ShotAsync("trees", "Trees: its one tree, tasks");

            await sections.ChooseTabAsync(Apps.TaskBoard, TaskBoard, "Roles");
            await Expect(sections.Panel).ToContainTextAsync(Groups.DataStewards);
            await Expect(sections.Panel).ToContainTextAsync(ContosoAccess.TaskBoardViewers.GroupId);
            await run.ShotAsync("roles", "Roles: editor bound to data-stewards, viewer to task-board-viewers");

            await sections.ChooseTabAsync(Apps.TaskBoard, TaskBoard, "Tools");
            await Expect(sections.Panel).ToContainTextAsync("This app declares no MCP tools.");
            await run.ShotAsync("tools", "Tools: it declares no MCP tools");

            await sections.ChooseTabAsync(Apps.TaskBoard, TaskBoard, "Subscriptions");
            await Expect(sections.Panel).ToContainTextAsync("This app declares no change-feed subscriptions.");
            await run.ShotAsync("subscriptions", "Subscriptions: it declares no change-feed subscriptions");

            await sections.ChooseTabAsync(Apps.TaskBoard, TaskBoard, "Replication");
            await Expect(sections.Panel).ToContainTextAsync("tasks");
            await Expect(sections.Panel).ToContainTextAsync("LwwRegister");
            await run.ShotAsync("replication", "Replication: its tasks tree is declared replicated, last writer wins");

            await sections.Panel.GetByRole(AriaRole.Link, new() { Name = "See replication for this app", Exact = true }).ClickAsync();
            await review.ExpectUrlAsync($"/replication\\?app={Apps.TaskBoard}");
            await olivia.Shell.ExpectHeadingAsync("Replication");
            Coverage.Hit("query:apps.app");
            await run.ShotAsync("replication-for-app", "See replication for this app opens Replication filtered to the task board");

            await sections.OpenAsync(Apps.TaskBoard, TaskBoard, "consent");
            await Expect(sections.Panel).ToContainTextAsync("The consent covers the installed manifest");
            await Expect(sections.Panel).ToContainTextAsync("range delete");
            await run.ShotAsync("consent", "Consent: the consent covers 1.1.0, range delete included");
            Coverage.Hit("use-case:apps/landing");
        });
    }

    /// <summary>The journey's steps: a coverage scope per step, and its checkpoints filed under it.</summary>
    /// <param name="session">olivia's session.</param>
    private sealed class Steps(PersonaSession session)
    {
        private readonly JourneyRecorder _recorder = JourneyRecorder.For(JourneyId);
        private string? _step;

        /// <summary>Runs step <c>A5.{number}</c> in its own coverage scope.</summary>
        /// <param name="number">The step number.</param>
        /// <param name="body">The step.</param>
        public async Task StepAsync(string number, Func<Task> body)
        {
            using var scope = Coverage.BeginScope($"A5.{number}");
            _step = $"{JourneyId}-{number}";
            await body();
            scope.Complete();
        }

        /// <summary>Captures and checks the current state as a substep of the running step.</summary>
        /// <param name="substep">The substep id.</param>
        /// <param name="title">What it shows.</param>
        /// <param name="fullPage">Whether to capture the whole page rather than the viewport.</param>
        public Task<JourneyManifestEntry> ShotAsync(string substep, string title, bool fullPage = true) =>
            session.SubstepAsync(_recorder, _step ?? throw new InvalidOperationException("A checkpoint outside a step."), substep, title, new SubstepOptions { FullPage = fullPage });
    }
}
