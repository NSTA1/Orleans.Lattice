using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U2;

/// <summary>
/// U2 (#5384), a schema owner's week. sam inherits olivia's strict policy on
/// <c>factory-orders</c> (A4) and what priya's day left (U1): two orders whose negative totals
/// no write since has touched. He scans for compliance, stops a scan and starts another by
/// address, remediates only the failing totals with a conditional set step (#5476), finds no
/// dead letters (every invalid write so far was refused), revises the policy, moves the tree to
/// version 2 and migrates it, tries a policy and versioning on the archive and takes both
/// away, and finds he can change no data and see no Cluster tools.
/// </summary>
/// <remarks>
/// One harness action is off the UI, as the issue names it: before the scan sam stops, the
/// harness holds that scan on the silos (<c>DelayComplianceScanAsync</c>), because a scan of a
/// few hundred orders otherwise finishes before any person, or browser, could press
/// <b>Stop scanning</b>. The hold is withdrawn as soon as the scan has been stopped.
/// </remarks>
[Journey(JourneyId)]
[JourneyChapter("U2", After = new[] { "A4", "U1" })]
public sealed class U2SchemaOwnerJourney : JourneyChapter
{
    /// <summary>The journey id its screenshots are filed under.</summary>
    public const string JourneyId = "u2";

    /// <summary>How long the harness holds the scan sam stops: long enough to stop it, bounded so it never outlives the step.</summary>
    private static readonly TimeSpan ScanHold = TimeSpan.FromMinutes(2);

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var estate = context.Estate;
        const string orders = Names.Trees.FactoryOrders;
        const string archive = Names.Trees.FactoryOrdersArchive;

        await using var sam = await estate.SignInAsAsync(Personas.Sam);
        var schema = new SchemaPage(sam.Shell);

        await U2Step.RunAsync(sam, 1, "The Schema directory: every tree, and its filter", async step =>
        {
            await sam.Shell.ExpectSignedInAsync(Personas.Sam.Id);
            await step.SubstepAsync("signed-in", "sam is signed in");

            await schema.OpenDirectoryAsync();
            await schema.ExpectDirectoryRowAsync(orders, "2 rules, strict ingest", "family 1 at version 1", "Not scanned");
            await step.SubstepAsync("under-schema", "The trees under schema: factory-orders, with A4's strict policy and version 1, not scanned this session");

            await schema.ShowAllTreesAsync();
            await schema.ExpectDirectoryRowAsync(archive, "None", "Unversioned", "Not scanned");
            await step.SubstepAsync("all-trees", "All trees (?show=all): ungoverned trees too, such as the October archive");

            await schema.FilterDirectoryAsync("orders");
            await Expect(schema.DirectoryRow(orders)).ToHaveCountAsync(1);
            await Expect(schema.DirectoryRow(archive)).ToHaveCountAsync(1);
            await step.SubstepAsync("filtered", "The filter narrows every tree to the ids containing 'orders'");
            Coverage.Hit("use-case:schema/directory");
        });

        await U2Step.RunAsync(sam, 2, "Scan compliance, stop the scan, and start one by address", async step =>
        {
            var picker = await schema.OpenScanPickerAsync(orders);
            await step.SubstepAsync("picker", "Scan compliance... offers the one tree sam can scan that has a policy");

            await estate.DelayComplianceScanAsync(orders, ScanHold);
            try
            {
                await picker.GetByRole(AriaRole.Button, new() { Name = "Scan", Exact = true }).ClickAsync();
                await Expect(schema.Shell.Heading).ToHaveTextAsync(orders);
                await Expect(schema.Tab("Compliance")).ToHaveAttributeAsync("aria-selected", "true");
                await schema.ExpectScanningAsync(orders);
                await step.SubstepAsync("scanning", "The scan runs on the cluster; the tab follows it and offers Stop scanning");

                await schema.StopScanAsync();
                await step.SubstepAsync("stopped", "Stop scanning cancels the scan before it finished");
            }
            finally
            {
                await estate.ClearComplianceScanDelayAsync(orders);
            }

            await schema.StartScanByAddressAsync(orders);
            var reasons = await schema.ExpectScanResultAsync(nonCompliant: 2);
            Assert.That(reasons, Has.All.Matches<(string Reason, int Count)>(reason => reason.Reason.Contains("total", StringComparison.Ordinal)),
                "order/000101 and order/000233 fail on their negative totals");
            await step.SubstepAsync("result", "?tab=compliance&scan=start scans again, drops scan from the address, and finds 2 non-compliant values: the negative totals");
        });

        await U2Step.RunAsync(sam, 3, "Remediate only the negative totals, then rescan", async step =>
        {
            await schema.ChooseTabAsync("remediation");
            await schema.WriteConditionalSetAsync("total", "0", lessThan: "0");
            await step.SubstepAsync("step-written", "A set step limited by a condition: total becomes 0 only where it is less than 0");

            await schema.AddStepAsync("Set total to 0 where total is less than 0");
            await step.SubstepAsync("step-added", "The step reads back as one sentence");

            await schema.ReviewRemediationAsync(steps: 1);
            await schema.ConfirmAsync("Remediate this tree", orders, "Start remediation",
                () => step.SubstepAsync("review", "Review and start...: the destructive confirmation names the tree"));
            await schema.ExpectOperationFinishedAsync();
            await step.SubstepAsync("finished", "The remediation ran on the cluster, every value passed, and the tree serves the result");

            await schema.RefreshStatusAsync();
            await schema.ClearResultAsync();
            await step.SubstepAsync("cleared", "Refresh status reads it again; Clear this result dismisses it");
            Coverage.Hit("tab:schema/remediation");
            Coverage.Hit("use-case:schema/remediation");

            await schema.ChooseTabAsync("compliance");
            await schema.ScanAgainAsync();
            await schema.ExpectScanResultAsync(nonCompliant: 0);
            await step.SubstepAsync("compliant", "A rescan finds every value compliant");
            Coverage.Hit("use-case:schema/compliance");
        });

        await U2Step.RunAsync(sam, 4, "Dead letters: none", async step =>
        {
            await schema.ChooseTabAsync("dead-letters");
            await Expect(schema.Panel.Locator("[role=status]")).ToHaveTextAsync("0 dead letters.");
            await Expect(schema.Panel.GetByRole(AriaRole.Heading, new() { Name = "No dead letters", Exact = true })).ToBeVisibleAsync();
            await Expect(schema.Button("Load dead letters")).ToBeDisabledAsync();
            await step.SubstepAsync("empty", "No dead letters: every invalid write so far was refused, so there is nothing to load");
        });

        await U2Step.RunAsync(sam, 5, "Edit the policy: an optional currency, and status as a regex", async step =>
        {
            await schema.ChooseTabAsync("policy");
            await schema.OpenRuleBuilderAsync(existing: true);
            await Expect(schema.RuleSentences).ToHaveCountAsync(2);
            await Expect(schema.RuleBuilder.Locator(".lt-schema-rulebuilder__aside")).ToContainTextAsync($"The first 100 values of {orders} in key order, of more.");
            Coverage.Hit("use-case:schema/limits");
            await step.SubstepAsync("builder", "Edit policy: A4's two rules as sentences, checked against the documented sample, the first 100 orders in key order");

            await schema.ComposeRuleAsync("currency", "One of a set");
            await schema.AllowValuesAsync("EUR", "USD", "GBP");
            await schema.TurnOnAsync("Also accept a missing value");
            await step.SubstepAsync("currency", "A One of a set card on currency, which also accepts a missing currency");
            await schema.CommitRuleAsync("Add rule", rules: 3);

            await schema.EditRuleAboutAsync("status");
            await schema.ChooseCardAsync("Starts, ends or contains");
            await schema.TakeOverPrefixAsRegexAsync("open");
            await schema.PatternInput.FillAsync("^(open|shipped|cancelled)$");
            await schema.TryPatternAsync("lost", matches: false);
            await step.SubstepAsync("regex", "Edit as regex turns the status check into a Custom pattern, adjusted by hand and tried");
            await schema.CommitRuleAsync("Update rule", rules: 3);

            await schema.SavePolicyAsync(orders, rules: 3);
            await step.SubstepAsync("saved", "The revised policy: total at least 0, status as a pattern, an optional currency");
        });

        await U2Step.RunAsync(sam, 6, "Advance to version 2, migrate, and change the config", async step =>
        {
            await schema.ChooseTabAsync("versions");
            await schema.ExpectVersionedAsync(family: 1, target: 1);
            await schema.OpenAdvanceAsync(target: 2);
            await schema.ReviewAdvanceAsync(target: 2);
            await schema.ConfirmAsync("Advance to version 2", orders, "Advance",
                () => step.SubstepAsync("advance", "Advance target version...: the confirmation warns it cannot be undone, and strands no version"));
            await schema.ExpectToastAsync($"{orders} now stamps new writes with version 2.");
            await schema.ExpectVersionedAsync(family: 1, target: 2);

            var migration = await schema.ReviewMigrationAsync(target: 2);
            await step.SubstepAsync("migrate", "Migrate stored values: rewrite every value at version 2, in the background");
            await migration.GetByRole(AriaRole.Button, new() { Name = "Start migration", Exact = true }).ClickAsync();
            await Expect(migration).ToHaveCountAsync(0);

            await schema.ChooseTabAsync("remediation");
            await schema.ExpectOperationFinishedAsync();
            await step.SubstepAsync("migrated", "The migration ran to completion");

            await schema.ChooseTabAsync("versions");
            await schema.OpenChangeConfigAsync(family: 1, target: 2);
            await step.SubstepAsync("change-config", "Change config replaces the config as typed");
            await schema.SaveConfigAsync(orders);
            await schema.ExpectVersionedAsync(family: 1, target: 2);
        });

        await U2Step.RunAsync(sam, 7, "The archive: a policy and versioning, set and taken away", async step =>
        {
            await schema.OpenTreeAsync(archive);
            await schema.OpenRuleBuilderAsync(existing: false);
            await schema.ComposeRuleAsync("total", "Number range");
            await schema.Composer.GetByLabel("Smallest", new() { Exact = true }).FillAsync("0");
            await Expect(schema.ComposerReads).ToContainTextAsync("total");
            await schema.CommitRuleAsync("Add rule", rules: 1);
            await step.SubstepAsync("set-policy", "Set a policy on the archive: one rule");
            await schema.SavePolicyAsync(archive, rules: 1);

            await schema.ClearPolicyAsync(archive,
                () => step.SubstepAsync("clear-policy", "Clear this tree's policy: every value is accepted again, and the rules are not kept"));
            await step.SubstepAsync("no-policy", "The archive has no policy");
            Coverage.Hit("use-case:schema/policy-edit-and-clear");

            await schema.ChooseTabAsync("versions");
            await schema.OpenTurnOnVersioningAsync(family: 3, target: 1);
            await step.SubstepAsync("turn-on", "Turn on versioning: a family and a target version");
            await schema.SaveConfigAsync(archive);
            await schema.ExpectVersionedAsync(family: 3, target: 1);

            await schema.TurnOffVersioningAsync(archive,
                () => step.SubstepAsync("turn-off", "Turn off versioning: new writes are no longer stamped"));
            await step.SubstepAsync("unversioned", "The archive is unversioned again");
            Coverage.Hit("use-case:schema/versions-advance-and-migrate");
        });

        await U2Step.RunAsync(sam, 8, "No data writes and no Cluster tools for a schema owner", async step =>
        {
            await sam.Shell.GotoAsync($"/data/{orders}?key={Uri.EscapeDataString(Names.Orders.Order233)}");
            var entry = sam.Shell.Content.Locator("section.lt-data-entry");
            await Expect(entry.Locator("pre.lt-data-value")).ToContainTextAsync("total");
            await step.SubstepAsync("data-read-only", "sam reads order/000233 in Data, but is offered no way to change it");

            // A write affordance is drawn once the cluster grants it; give the probe time to answer first.
            await Task.Delay(TimeSpan.FromSeconds(3));
            foreach (var command in new[] { "data.new-entry", "data.edit-entry", "data.delete-entry", "data.import" })
            {
                await Expect(sam.Shell.Content.Locator($"[data-lt-command=\"{command}\"]")).ToHaveCountAsync(0);
            }

            await sam.Shell.ExpectAreaWithheldAsync("cluster");
            await step.SubstepAsync("no-cluster", "The Cluster area, and its tools, are withheld from sam");
        });

        await U2Step.RunAsync(sam, 9, "The server has the last word: a revoked grant refuses sam's edit", async step =>
        {
            // The platform operator (olivia, through the Auth API: the off-UI actor the issue
            // names) narrows sam's grant on the archive to reading while sam's editor is open, and
            // later gives it back, so U2's produces and the chapters after it are unchanged.
            var olivia = ContractFacades.Of(context);
            var samSchema = ContractFacades.Of(context, Personas.Sam.Id).Schema;
            var granted = ContosoAccess.A2RulesNamed(Names.Rules.SchemaOwnersFactory).Single(rule => rule.TreeId == archive);
            var narrowed = granted with { Operations = ContosoAccess.ReadOps };

            await schema.OpenTreeAsync(archive);
            await schema.OpenRuleBuilderAsync(existing: false);
            await schema.ComposeRuleAsync("total", "Number range");
            await schema.Composer.GetByLabel("Smallest", new() { Exact = true }).FillAsync("0");
            await schema.CommitRuleAsync("Add rule", rules: 1);

            await ContractCalls.PutRuleAsync(olivia.Auth, narrowed);
            await WaitForPolicyRightAsync(samSchema, archive, granted: false);
            await schema.RuleBuilder.GetByRole(AriaRole.Button, new() { Name = "Save policy", Exact = true }).ClickAsync();
            await Expect(schema.RuleBuilder.GetByRole(AriaRole.Alert)).ToHaveTextAsync("You are not permitted to set the policy.");
            await step.SubstepAsync("refused", "olivia narrows sam's grant while his editor is open: the cluster refuses the save, and the editor says so");

            await schema.RuleBuilder.GetByRole(AriaRole.Button, new() { Name = "Cancel", Exact = true }).ClickAsync();
            Assert.That(await samSchema.GetPolicyAsync(archive), Is.Null, "the refused save stored nothing");
            await schema.OpenTreeAsync(archive);
            await Expect(schema.Panel.GetByRole(AriaRole.Heading, new() { Name = "No policy", Exact = true })).ToBeVisibleAsync();
            await Expect(schema.Button("Set a policy")).ToHaveCountAsync(0);
            await step.SubstepAsync("read-only", "Read afresh, the archive's policy is read-only for sam");

            await ContractCalls.PutRuleAsync(olivia.Auth, granted);
            Assert.That(await ContractCalls.RuleHoldsAsync(olivia.Auth, granted), Is.True, "sam's grant on the archive is restored as A2 wrote it");
            await WaitForPolicyRightAsync(samSchema, archive, granted: true);
            await schema.OpenTreeAsync(archive);
            await Expect(schema.Button("Set a policy")).ToBeVisibleAsync();
            await step.SubstepAsync("restored", "With the grant restored, sam may set the archive's policy again");
            Coverage.Hit("use-case:schema/server-authority");
        });
    }

    /// <summary>
    /// Waits until the cluster answers sam's capability probe on <paramref name="tree"/> with
    /// the policy right <paramref name="granted"/> or not, several times running, so every silo
    /// has the rule change.
    /// </summary>
    private static async Task WaitForPolicyRightAsync(Orleans.Lattice.Api.Schema.Grpc.LatticeSchemaApiGrpcClient schema, string tree, bool granted)
    {
        var agreed = 0;
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (agreed < 4)
        {
            agreed = (await schema.ProbeCapabilitiesAsync(tree)).CanManagePolicy == granted ? agreed + 1 : 0;
            if (clock.Elapsed > TimeSpan.FromMinutes(1))
            {
                Assert.Fail($"sam's policy right on {tree} did not become {granted} within a minute.");
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }
    }
}
