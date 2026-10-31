using System.Text.RegularExpressions;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A2;

/// <summary>
/// A2, the access model (#5372): before inviting anyone in, olivia models Contoso's teams as
/// groups, grants each its rules, proves the grants with Explain, and onboards the factory's
/// line feed, whose first write creates <c>factory-telemetry</c>. nina is left with nothing.
/// </summary>
/// <remarks>
/// The groups and rule records are exactly F6's pinned tables
/// (<see cref="ContosoAccess.A2Groups"/>, <see cref="ContosoAccess.A2Rules"/>), authored
/// through the Explorer. Two actions are off-UI, performed by the harness as the outside
/// actors they model: the corporate directory sync of the 230 team groups before step 1
/// (<see cref="ContosoHarness.SyncDirectoryGroupsAsync"/>), and the service operator starting
/// the line feed in step 8.
/// </remarks>
[JourneyChapter("A2", After = new[] { "A1" })]
public sealed class A2Journey : JourneyChapter
{
    /// <summary>The member id olivia mistypes in step 2.</summary>
    public const string MistypedMember = "pryia";

    /// <summary>The scratch rule olivia writes, edits and deletes in step 6.</summary>
    public const string ScratchRuleId = ContosoAccess.ScratchRulePrefix + "auditors-lines";

    private static readonly TimeSpan LineFeedPatience = TimeSpan.FromMinutes(2);

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        // Off-UI: the corporate directory sync has already filed Contoso's 230 team groups.
        await ContosoHarness.SyncDirectoryGroupsAsync(context.Estate);

        await using var olivia = await context.Estate.SignInAsAsync(Personas.Olivia);
        await olivia.Shell.ChooseAppearanceAsync(AppearanceOf(context.Recall(StoryKeys.OliviaAppearance)));

        var steps = new A2Steps(olivia);
        var rules = new RulesPage(olivia);
        var groups = new GroupsPage(olivia);
        var explain = new ExplainPage(olivia);
        var data = new DataDirectoryPage(olivia);

        await steps.RunAsync(1, "Access opens on Rules; the group catalogue pages", async () =>
        {
            await rules.ArriveAsync();
            await steps.ShotAsync("rules-posture", "Access opens on Rules, under the posture banner: rules enforced, the static directory");

            await groups.OpenAsync();
            await groups.ExpectCountAsync(new Regex("^200 groups, more to load$"));
            await steps.ShotAsync("groups-first-page", "Groups: the first 200 of the directory-synced teams, with more to load");

            await groups.LoadMoreAsync(Names.Groups.SyncedTeamCount);
            await steps.ShotAsync("groups-all-loaded", "Load more groups: all 230 synced teams are listed");
        });

        var stewards = ContosoAccess.A2Groups[0];
        await steps.RunAsync(2, "New group data-stewards; add priya; rename it", async () =>
        {
            await groups.OpenCreateFromPaletteAsync();
            await steps.ShotAsync("new-group-dialog", "Create a group, from the command palette: the New group dialog");

            await groups.FillCreateAsync(stewards.GroupId, displayName: null);
            await steps.ShotAsync("new-group-filled", "A stored group id the directory does not hold is accepted");

            await groups.SubmitCreateAsync(stewards.GroupId);
            await steps.ShotAsync("group-created", "data-stewards is created, with no members yet");

            await groups.AddMistypedUserAsync(MistypedMember);
            await steps.ShotAsync("mistyped-member-refused", "A mistyped member is refused by the directory, and nothing is added");

            await groups.AddUserAsync(stewards.GroupId, Personas.Priya.Id, () =>
                steps.ShotAsync("member-chosen", "priya chosen from the identity directory in the subject picker"));
            await steps.ShotAsync("member-added", "priya is a direct member of data-stewards");

            await groups.RenameAsync(stewards.GroupId, stewards.DisplayName, () =>
                steps.ShotAsync("rename-typed", "The group's display name, typed"));
            await steps.ShotAsync("renamed", "Save name: the group is now Data stewards");
        });

        var ingestion = ContosoAccess.A2Groups[^1];
        await steps.RunAsync(3, "The other three teams; a mistaken member removed", async () =>
        {
            foreach (var group in ContosoAccess.A2Groups.Skip(1))
            {
                await groups.OpenAsync();
                await groups.OpenCreateAsync();
                await groups.FillCreateAsync(group.GroupId, group.DisplayName);
                await groups.SubmitCreateAsync(group.GroupId);
                foreach (var user in group.Users)
                {
                    await groups.AddUserAsync(group.GroupId, user);
                }

                await steps.ShotAsync($"{group.GroupId}-ready", $"{group.GroupId} holds {string.Join(", ", group.Users)}");
            }

            // olivia adds nina to the ingestion services by mistake, and takes her out again.
            await groups.AddUserAsync(ingestion.GroupId, Personas.Nina.Id);
            await steps.ShotAsync("mistaken-member", "A mistaken addition: nina in ingestion-services");

            await groups.RemoveMemberAsync(ingestion.GroupId, Personas.Nina.Id, () =>
                steps.ShotAsync("remove-confirm", "Remove member: the destructive confirmation, armed with nina's name"));
            await steps.ShotAsync("mistake-removed", "ingestion-services holds the line feed alone again");
        });

        await steps.RunAsync(4, "New rule stewards-factory for the stored group data-stewards", async () =>
        {
            await rules.OpenAsync();
            var records = ContosoAccess.A2RulesNamed(Names.Rules.StewardsFactory).ToList();
            var first = true;
            foreach (var record in records)
            {
                if (first)
                {
                    await rules.OpenCreateFromPaletteAsync();
                    await steps.ShotAsync("new-rule-dialog", "Create an access rule, from the command palette: the New rule editor");
                    await rules.FillAsync(record);
                    await steps.ShotAsync("first-record-filled", $"stewards-factory on {record.TreeId}: data-stewards chosen as a stored group, a tree not created yet");
                    await rules.SaveAsync(record);
                    await steps.ShotAsync("first-record-saved", "The rule is saved and listed");
                    first = false;
                    continue;
                }

                await rules.OpenCreateAsync();
                await rules.FillAsync(record);
                await rules.SaveAsync(record);
            }

            await rules.SearchAsync(Names.Rules.StewardsFactory, records.Count);
            await steps.ShotAsync("stewards-factory-records", "stewards-factory: one record per factory tree it governs");
            await rules.ClearSearchAsync();
        });

        await steps.RunAsync(5, "The schema owners', auditors' and line feed's rules", async () =>
        {
            foreach (var ruleId in new[] { Names.Rules.SchemaOwnersFactory, Names.Rules.AuditorsFactoryRead, Names.Rules.LineFeedFactory })
            {
                foreach (var record in ContosoAccess.A2RulesNamed(ruleId))
                {
                    await rules.OpenCreateAsync();
                    await rules.FillAsync(record);
                    if (record.TreeId == LatticeScope.ClusterWideTreeId)
                    {
                        await steps.ShotAsync("telemetry-capability", "auditors-factory-read's cluster-wide record: the Telemetry capability");
                    }

                    await rules.SaveAsync(record);
                }

                await rules.SearchAsync(ruleId, ContosoAccess.A2RulesNamed(ruleId).Count());
                await steps.ShotAsync($"{ruleId}-records", $"{ruleId}: its records, as the access table pins them");
                await rules.ClearSearchAsync();
            }

            await steps.ShotAsync("all-rules", "Every rule record olivia authored");
        });

        await steps.RunAsync(6, "A scratch rule: New rule, Edit rule, Delete rule", async () =>
        {
            var scratch = new PinnedRule(ScratchRuleId, LatticeSubjectSelector.Group(Names.Groups.Auditors), Names.Trees.FactoryLines, LatticeOperation.Read);
            await rules.OpenCreateAsync();
            await rules.FillAsync(scratch);
            await rules.SaveAsync(scratch);
            await rules.SearchAsync(ScratchRuleId, 1);
            await rules.OpenRuleAsync(ScratchRuleId);
            await steps.ShotAsync("scratch-rule", "The scratch rule's page: Edit rule and Delete rule");

            await rules.EditAddOperationAsync("Range read");
            await steps.ShotAsync("edit-rule", "Edit rule: the id and tree are fixed; Range read is ticked");
            await rules.SaveEditAsync(ScratchRuleId, "Range read");
            await steps.ShotAsync("scratch-rule-edited", "The edited rule now allows Range read");

            await rules.DeleteAsync(ScratchRuleId, () =>
                steps.ShotAsync("delete-confirm", "Delete rule: the destructive confirmation, armed with the rule's id"));
            await steps.ShotAsync("scratch-rule-deleted", "The scratch rule is gone from the list");
        });

        await steps.RunAsync(7, "Explain: priya allowed, dana denied, nina matched by nothing", async () =>
        {
            await explain.OpenFromPaletteAsync();
            await steps.ShotAsync("explain", "Explain access, from the command palette");

            await explain.AskAsync(Personas.Priya.Id, "Write", Names.Trees.FactoryOrders, () =>
                steps.ShotAsync("priya-question", "May priya write factory-orders?"));
            await explain.ExpectAllowedByAsync(Names.Rules.StewardsFactory);
            await steps.ShotAsync("priya-allowed", "Allowed, by stewards-factory through data-stewards");

            await explain.AskAsync(Personas.Dana.Id, "Write", Names.Trees.FactoryOrders);
            await explain.ExpectDeniedAsync();
            await steps.ShotAsync("dana-denied", "dana, an auditor, may not write factory-orders");

            await explain.AskAsync(Personas.Nina.Id, "Read", Names.Trees.FactoryOrders);
            await explain.ExpectNoMatchingRuleAsync();
            await steps.ShotAsync("nina-no-rule", "nina may not even read it: no rule matches her, so the default denies");
        });

        await steps.RunAsync(8, "The line feed starts writing; factory-telemetry appears", async () =>
        {
            await data.OpenAsync();
            Assert.That(await data.Tree(Names.Trees.FactoryTelemetry).CountAsync(), Is.Zero, "the line feed has written nothing yet");
            Coverage.Hit("route:/data");
            await steps.ShotAsync("before-feed", "olivia's Data directory, before the line feed is started");

            // Off-UI: the service operator starts the line feed, now that it holds its rights.
            await context.Estate.LineFeed.StartAsync();
            await data.RefreshUntilListedAsync(Names.Trees.FactoryTelemetry, LineFeedPatience);
            await context.Estate.LineFeed.WaitUntilAsync(status => status.TelemetryWritten > 0, LineFeedPatience);
            await steps.ShotAsync("telemetry-appears", "Refresh: the line feed's first write created factory-telemetry");
        });
    }

    /// <summary>The appearance commands that apply a ledger appearance (<c>theme/contrast/density</c>).</summary>
    /// <param name="appearance">The appearance, such as <c>board/system/compact</c>.</param>
    internal static string[] AppearanceOf(string appearance)
    {
        var parts = appearance.Split('/');
        if (parts.Length != 3)
        {
            throw new ArgumentException($"'{appearance}' is not a theme/contrast/density appearance.", nameof(appearance));
        }

        return [$"appearance.theme.{parts[0]}", $"appearance.contrast.{parts[1]}", $"appearance.density.{parts[2]}"];
    }
}
