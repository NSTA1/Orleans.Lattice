using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Orleans.Lattice.Schema;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U6;

/// <summary>
/// U6 (#5385), the schema rule workshop: sam writes a full policy for the production-line
/// register, <c>factory-lines</c>, with every card the rule builder's gallery offers, reads each
/// back as a sentence against the sample, combines, moves and removes rules, finishes in the
/// Advanced view, keeps the policy-as-code pipeline's rule the builder cannot represent as it is,
/// and then turns versioning on, registers versions 1 and 2, and advances and migrates in one flow.
/// </summary>
/// <remarks>
/// Off-UI, before step 1, the policy-as-code pipeline authors its rule on <c>factory-lines</c>
/// through the Schema API (<see cref="ContosoHarness.AuthorLinesPipelineRuleAsync"/>), as the
/// chapter's contract names.
/// </remarks>
[JourneyChapter("U6", After = new[] { "A3", "U2" })]
public sealed class SchemaRuleWorkshopJourney : JourneyChapter
{
    private const string JourneyId = "u6";
    private const string Tree = Names.Trees.FactoryLines;

    /// <summary>Every rule the workshop's policy holds: the pipeline's rule and one per card (two formats).</summary>
    private const int WorkshopRules = 14;

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        await ContosoHarness.AuthorLinesPipelineRuleAsync(context.Estate);

        await using var sam = await context.Estate.SignInAsAsync(Personas.Sam);
        var run = new Run(sam, new SchemaWorkshopPage(sam.Shell), JourneyRecorder.For(JourneyId));

        await StepAsync("U6.1", () => EveryCardAsync(run));
        await StepAsync("U6.2", () => ExamplesAndSampleAsync(run));
        await StepAsync("U6.3", () => OrMoveAndRemoveAsync(run));
        await StepAsync("U6.4", () => AdvancedViewAsync(run));
        await StepAsync("U6.5", () => CustomRuleKeptAsync(context, run));
        await StepAsync("U6.6", () => AdvanceAndMigrateAsync(run));
    }

    /// <summary>Runs one step in a coverage scope of its own, so a later step's failure keeps this step's evidence.</summary>
    private static async Task StepAsync(string step, Func<Task> body)
    {
        using var scope = Coverage.BeginScope(step);
        await body();
        scope.Complete();
    }

    /// <summary>
    /// U6.1: in <c>factory-lines</c>' workspace, sam opens the rule builder on the pipeline's
    /// policy and adds a rule from every card of the gallery, each against the member it fits.
    /// </summary>
    private static async Task EveryCardAsync(Run run)
    {
        var page = run.Page;

        // #5385: a schema owner whose grant names trees, not the whole cluster, sees the Schema area.
        await Expect(run.Session.Shell.Stop("schema")).ToBeVisibleAsync();
        await run.ShotAsync(1, "home", "Sam's estate: the Schema area is listed for a schema owner of a few trees");

        await page.OpenTreeAsync(Tree);
        await Expect(page.Definition("Rules")).ToContainTextAsync("1 rule, all of which a value must satisfy");
        Coverage.Hit("tab:schema/policy");
        await run.ShotAsync(1, "workspace", "factory-lines' schema workspace: the pipeline's one rule, on the Policy tab");

        await page.EditPolicyAsync();
        await Expect(page.Rules).ToHaveCountAsync(1);
        await Expect(page.Preview).ToContainTextAsync("All pass");
        await run.ShotAsync(1, "builder", "The rule builder opens on the pipeline's rule; the sample of the lines all pass");

        var rules = 1;
        await AddCardAsync(run, ++rules, "required", "Required: line must be present", "line", "Required",
            example: new Regex(@"^Set in \d+ of \d+ sampled values\.$"),
            sentence: "line must be present as text, a number or true or false",
            configure: () => Expect(page.Composer.GetByRole(AriaRole.Switch, new() { Name = "It holds an object or a list", Exact = true }))
                .ToHaveAttributeAsync("aria-checked", "false"));

        await AddCardAsync(run, ++rules, "type", "Type: rate must be a number", "rate", "Type",
            example: new Regex(@"^Seen as a number \([\d,]+\)\.$"),
            sentence: "rate must be a number",
            configure: () => SchemaWorkshopPage.SelectAsync(page.Composer, "Type", "Number"));

        await AddCardAsync(run, ++rules, "one-of", "One of a set: line must be one of the lines seen", "line", "OneOf",
            example: new Regex(@"^(Seen: |Many values, such as )L\d+"),
            sentence: "line must be one of \"L",
            configure: () => Expect(page.Composer.GetByRole(AriaRole.List, new() { Name = "Allowed values, chosen", Exact = true })
                .GetByRole(AriaRole.Listitem).First).ToContainTextAsync("L"));

        await AddCardAsync(run, ++rules, "number-range", "Number range: rate is a whole number from 0 to 1,000", "rate", "NumberRange",
            example: new Regex(@"^Seen \d+ to \d+, all whole\.$"),
            sentence: "rate must be a whole number between 0 and 1,000",
            configure: async () =>
            {
                await SchemaWorkshopPage.FillAsync(page.Composer, "Smallest", "0");
                await SchemaWorkshopPage.FillAsync(page.Composer, "Largest", "1000");
                await page.SetSwitchAsync("Whole numbers only", on: true);
            });

        await AddCardAsync(run, ++rules, "text-length", "Text length: line is 2 to 3 characters", "line", "TextLength",
            example: new Regex(@"^Seen \d+ to \d+ characters\.$"),
            sentence: "line must be text of 2 to 3 characters",
            configure: async () =>
            {
                await SchemaWorkshopPage.FillAsync(page.Composer, "Fewest characters", "2");
                await SchemaWorkshopPage.FillAsync(page.Composer, "Most characters", "3");
            });

        await AddCardAsync(run, ++rules, "format-email", "Common format: owner is an email address", "owner", "Format",
            example: new Regex(@"^Every value seen is an email address\.$"),
            sentence: "owner must be an email address",
            configure: () => SchemaWorkshopPage.SelectAsync(page.Composer, "Format", "Email"));

        await AddCardAsync(run, ++rules, "format-iso-date", "Common format: since is an ISO date", "since", "Format",
            example: new Regex(@"^Every value seen is an ISO date\.$"),
            sentence: "since must be an ISO date",
            configure: () => SchemaWorkshopPage.SelectAsync(page.Composer, "Format", "Date"));

        await AddCardAsync(run, ++rules, "starts-with", "Starts, ends or contains: line starts with L", "line", "TextMatch",
            example: new Regex("^Every value seen starts with \"L\"\\.$"),
            sentence: "line must start with \"L\"",
            configure: async () =>
            {
                await SchemaWorkshopPage.SelectAsync(page.Composer, "Where", "StartsWith");
                await SchemaWorkshopPage.FillAsync(page.Composer, "Text", "L");
            });

        await AddCardAsync(run, ++rules, "list-length", "List length: tags hold 1 to 5 items", "tags", "ListLength",
            example: new Regex(@"^Seen \d+ to \d+ items\.$"),
            sentence: "tags must be a list of 1 to 5 items",
            configure: async () =>
            {
                await SchemaWorkshopPage.FillAsync(page.Composer, "Fewest items", "1");
                await SchemaWorkshopPage.FillAsync(page.Composer, "Most items", "5");
            });

        await AddCardAsync(run, ++rules, "every-item", "Every item: each of the tags is text", "tags", "EveryItem",
            example: new Regex(@"^Items seen are mostly text\.$"),
            sentence: "tags must be a list in which: Each item must be text",
            configure: () => Expect(page.Composer.Locator("#schema-kind-item-1-Type")).ToBeCheckedAsync());

        await AddCardAsync(run, ++rules, "well-formed", "Well-formed value: the whole value is one JSON document", "The whole value", "Encoding",
            example: new Regex(@"^For example: The value must be one JSON document\.$"),
            sentence: "The value must be one JSON document",
            configure: () => SchemaWorkshopPage.SelectAsync(page.Composer, "The value must be", "Json"));

        await AddCardAsync(run, ++rules, "largest-size", "Largest size: the whole value is at most 1,024 bytes", "The whole value", "MaxSize",
            example: new Regex(@"^For example: The value must be at most 65,536 bytes\.$"),
            sentence: "The value must be at most 1,024 bytes",
            configure: () => SchemaWorkshopPage.FillAsync(page.Composer, "Largest size, in bytes", "1024"));

        await AddCardAsync(run, ++rules, "custom-pattern", "Custom pattern: line matches ^L\\d+$, tried live on L7", "line", "Pattern",
            example: new Regex("^Test it against \"L\\d+\" and the rest of the sample\\.$"),
            sentence: @"line must match the pattern ^L\d+$",
            configure: async () =>
            {
                await SchemaWorkshopPage.FillAsync(page.Composer, "Pattern (a regular expression)", @"^L\d+$");
                await SchemaWorkshopPage.FillAsync(page.Composer, "Try a value", "X7");
                await Expect(page.Composer.Locator(".lt-schema-details__example")).ToContainTextAsync("Does not match.");
                await SchemaWorkshopPage.FillAsync(page.Composer, "Try a value", "L7");
                await Expect(page.Composer.Locator(".lt-schema-details__example")).ToContainTextAsync("Matches.");
                await Expect(page.Composer.Locator(".lt-schema-details__example")).ToContainTextAsync(new Regex(@"Matches (\d+) of \1 distinct text values seen\."));
            });

        Assert.That(rules, Is.EqualTo(WorkshopRules), "the pipeline's rule and one rule per card");
        await Expect(page.Preview).ToContainTextAsync("All pass");
        await run.ShotAsync(1, "every-card", "Fourteen rules: the pipeline's and one from every card; the whole sample passes them all");
        Coverage.Hit("use-case:schema/rule-builder");
        Coverage.Hit("use-case:schema/rule-builder-every-card");
    }

    /// <summary>Adds one card's rule: the member, the card and its live example, its details, the sentence it reads as.</summary>
    private static async Task AddCardAsync(
        Run run, int number, string substep, string title, string member, string kind, Regex example, string sentence, Func<Task> configure)
    {
        var page = run.Page;
        await page.OpenComposerAsync();
        await page.ChooseMemberAsync(member);
        await page.ChooseCardAsync(kind);
        await Expect(page.CardExample(kind)).ToHaveTextAsync(example);
        await configure();
        await Expect(page.Reads).ToContainTextAsync(sentence);
        await run.ShotAsync(1, substep, title);
        await page.CommitAsync("Add rule", number);
        await Expect(page.Sentence(number)).ToContainTextAsync(sentence);
    }

    /// <summary>
    /// U6.2: on a member, the cards that only judge the whole value are disabled and say why;
    /// on the whole value they are offered. Sam reads the sample again, and the check holds.
    /// </summary>
    private static async Task ExamplesAndSampleAsync(Run run)
    {
        var page = run.Page;
        await page.OpenComposerAsync();
        await page.ChooseMemberAsync("rate");
        foreach (var kind in new[] { "Encoding", "MaxSize" })
        {
            await Expect(page.CardRadio(kind)).ToBeDisabledAsync();
            await Expect(page.CardExample(kind)).ToHaveTextAsync("Only for the whole value.");
        }

        await Expect(page.CardExample("NumberRange")).ToHaveTextAsync(new Regex(@"^Seen \d+ to \d+, all whole\.$"));
        await run.ShotAsync(2, "disabled-on-a-member", "On rate, Well-formed value and Largest size are disabled: only for the whole value");

        await page.ChooseMemberAsync("The whole value");
        foreach (var kind in new[] { "Encoding", "MaxSize" })
        {
            await Expect(page.CardRadio(kind)).ToBeEnabledAsync();
        }

        await run.ShotAsync(2, "enabled-on-the-whole-value", "On the whole value, both whole-value cards are offered");
        await page.Composer.GetByRole(AriaRole.Button, new() { Name = "Cancel", Exact = true }).ClickAsync();
        await Expect(page.Composer).ToHaveCountAsync(0);

        await page.Preview.GetByRole(AriaRole.Button, new() { Name = "Read the sample again", Exact = true }).ClickAsync();
        await page.WaitForSampleAsync();
        await Expect(page.Preview).ToContainTextAsync("All pass");
        await Expect(page.Rules).ToHaveCountAsync(WorkshopRules);
        await run.ShotAsync(2, "sample-read-again", "The sample read again: every rule still passes it");
        Coverage.Hit("use-case:schema/sample-re-read");
    }

    /// <summary>U6.3: an alternative joins a rule with Or, a rule moves up and down, and one rule is removed.</summary>
    private static async Task OrMoveAndRemoveAsync(Run run)
    {
        var page = run.Page;
        const int startsWith = 9;
        await page.Button($"Add an alternative to rule {startsWith}").ClickAsync();
        await Expect(page.Composer.GetByRole(AriaRole.Heading, new() { Name = $"Add an alternative to rule {startsWith}", Exact = true })).ToBeVisibleAsync();
        await page.ChooseMemberAsync("line");
        await page.ChooseCardAsync("TextMatch");
        await SchemaWorkshopPage.SelectAsync(page.Composer, "Where", "EndsWith");
        await SchemaWorkshopPage.FillAsync(page.Composer, "Text", "-spare");
        await Expect(page.Reads).ToContainTextAsync("line must end with \"-spare\"");
        await run.ShotAsync(3, "alternative", "Or...: an alternative to rule 9, line ends with -spare");

        await page.CommitAsync("Add alternative", WorkshopRules);
        await Expect(page.Sentence(startsWith)).ToContainTextAsync("At least one of these must hold:");
        var alternatives = page.Rules.Nth(startsWith - 1).Locator("li.lt-schema-ruleset__alternative");
        await Expect(alternatives).ToHaveCountAsync(2);
        await Expect(alternatives.Nth(1)).ToContainTextAsync("line must end with \"-spare\"");
        await run.ShotAsync(3, "combined-with-or", "Rule 9 now holds when either alternative does");

        await page.Button($"Remove alternative 2 of rule {startsWith}").ClickAsync();
        await Expect(alternatives).ToHaveCountAsync(0);
        await Expect(page.Sentence(startsWith)).ToContainTextAsync("line must start with \"L\"");
        await run.ShotAsync(3, "alternative-removed", "Removing the second alternative leaves rule 9 on its own again");

        await page.Button($"Move rule {WorkshopRules} up").ClickAsync();
        await Expect(page.Sentence(WorkshopRules - 1)).ToContainTextAsync(@"line must match the pattern ^L\d+$");
        await Expect(page.Sentence(WorkshopRules)).ToContainTextAsync("The value must be at most 1,024 bytes");
        await run.ShotAsync(3, "moved-up", "The pattern rule moved up, above Largest size");

        await page.Button($"Move rule {WorkshopRules - 1} down").ClickAsync();
        await Expect(page.Sentence(WorkshopRules)).ToContainTextAsync(@"line must match the pattern ^L\d+$");
        await Expect(page.Sentence(WorkshopRules - 1)).ToContainTextAsync("The value must be at most 1,024 bytes");
        await run.ShotAsync(3, "moved-down", "And down again, back to the end");

        await page.Button($"Remove rule {WorkshopRules - 1}").ClickAsync();
        await Expect(page.Rules).ToHaveCountAsync(WorkshopRules - 1);
        await Expect(page.Rules.Filter(new() { HasText = "at most 1,024 bytes" })).ToHaveCountAsync(0);
        await run.ShotAsync(3, "removed", "Largest size removed: thirteen rules remain");
        Coverage.Hit("use-case:schema/rule-builder-every-card");
    }

    /// <summary>
    /// U6.4: the Advanced view shows the exact policy as read-only JSON; its composer adds a raw
    /// Largest size rule, which back in the builder reads as a normal, editable card.
    /// </summary>
    private static async Task AdvancedViewAsync(Run run)
    {
        var page = run.Page;
        await page.SetAdvancedAsync(on: true);
        await Expect(page.ExactPolicy).ToContainTextAsync("\"strictIngest\": false");
        await Expect(page.ExactPolicy).Not.ToContainTextAsync("maxByteLength");
        await run.ShotAsync(4, "exact-policy", "Advanced: the exact policy, as read-only JSON, with no size rule");

        await SchemaWorkshopPage.SelectAsync(page.RawComposer, "Rule", "MaxLength");
        await SchemaWorkshopPage.FillAsync(page.RawComposer, "Largest size, in bytes", "1024");
        await run.ShotAsync(4, "raw-rule", "The Advanced composer writes a raw Largest size rule of 1,024 bytes");

        await page.RawComposer.GetByRole(AriaRole.Button, new() { Name = "Add rule", Exact = true }).ClickAsync();
        await Expect(page.ExactPolicy).ToContainTextAsync("\"maxByteLength\": 1024");
        await Expect(page.Content.Locator("ol.lt-schema-rules > li")).ToHaveCountAsync(WorkshopRules);
        await run.ShotAsync(4, "raw-rule-added", "The raw rule is in the exact policy");

        await page.SetAdvancedAsync(on: false);
        await Expect(page.Rules).ToHaveCountAsync(WorkshopRules);
        await Expect(page.Sentence(WorkshopRules)).ToContainTextAsync("The value must be at most 1,024 bytes");
        await Expect(page.Sentence(WorkshopRules)).Not.ToContainTextAsync("kept as it is");
        await Expect(page.Button($"Edit rule {WorkshopRules}")).ToBeVisibleAsync();
        await run.ShotAsync(4, "decompiled", "Back in the builder, the raw rule reads as a normal Largest size card");
        Coverage.Hit("use-case:schema/advanced-view");
    }

    /// <summary>
    /// U6.5: the pipeline's rule, which no card can say, is a Custom rule kept as it is: it can be
    /// moved or removed, never edited. Sam moves it and saves; the saved policy still holds it
    /// verbatim.
    /// </summary>
    private static async Task CustomRuleKeptAsync(ChapterContext context, Run run)
    {
        var page = run.Page;
        var custom = page.Rules.Nth(0);
        await Expect(custom.Locator(":scope > .lt-schema-ruleset__sentence")).ToContainTextAsync("A custom rule:");
        await Expect(custom).ToContainTextAsync("kept as it is");
        await Expect(custom).ToContainTextAsync("Reported as: " + ContosoSchema.LinesPipelineRule.Description);
        await Expect(page.Button("Edit rule 1")).ToHaveCountAsync(0);
        await Expect(page.Button("Add an alternative to rule 1")).ToHaveCountAsync(0);
        await Expect(page.Button("Move rule 1 up")).ToBeDisabledAsync();
        await Expect(page.Button("Move rule 1 down")).ToBeEnabledAsync();
        await Expect(page.Button("Remove rule 1")).ToBeEnabledAsync();
        await run.ShotAsync(5, "custom-rule", "The pipeline's rule: a Custom rule, kept as it is - move or remove, no Edit and no Or");

        await page.Button("Move rule 1 down").ClickAsync();
        await Expect(page.Rules.Nth(1)).ToContainTextAsync("kept as it is");
        await Expect(page.Rules.Nth(0)).Not.ToContainTextAsync("kept as it is");
        await run.ShotAsync(5, "custom-rule-moved", "The Custom rule moved down to second place");

        await page.SaveAsync(Tree, WorkshopRules);
        await run.ShotAsync(5, "saved", "The policy is saved: fourteen rules, not strict");

        var saved = await ContractFacades.Of(context, Personas.Sam.Id).Schema.GetPolicyAsync(Tree);
        Assert.That(saved, Is.Not.Null, "the policy is saved");
        Assert.That(ContosoSchema.Holds(saved, ContosoSchema.LinesPipelineRule), Is.True, "the pipeline's rule is saved verbatim");
        Assert.That(saved!.Rules.Count, Is.EqualTo(WorkshopRules));
        Assert.That(saved.Rules[1], Is.EqualTo(ContosoSchema.LinesPipelineRule), "the pipeline's rule is where sam moved it");
        Assert.That(saved.StrictIngest, Is.False);

        await page.EditPolicyAsync();
        await Expect(page.Rules).ToHaveCountAsync(WorkshopRules);
        await Expect(page.Rules.Nth(1)).ToContainTextAsync("kept as it is");
        await Expect(page.Rules.Filter(new() { HasText = "kept as it is" })).ToHaveCountAsync(1);
        await run.ShotAsync(5, "reopened", "Reopened, the saved policy still shows the pipeline's rule as it was written");
        await page.Button("Cancel").ClickAsync();
        await Expect(page.Builder).ToHaveCountAsync(0);
        Coverage.Hit("use-case:schema/advanced-view");
    }

    /// <summary>
    /// U6.6: versioning on, in the lines' family at version 1; versions 1 and 2 registered;
    /// then <b>Advance and migrate...</b> to version 2 in one flow, followed to its end.
    /// </summary>
    private static async Task AdvanceAndMigrateAsync(Run run)
    {
        var page = run.Page;
        await page.OpenTabAsync("Versions");
        await Expect(page.Content.GetByRole(AriaRole.Heading, new() { Name = "Unversioned", Exact = true })).ToBeVisibleAsync();
        Coverage.Hit("tab:schema/versions");
        await run.ShotAsync(6, "unversioned", "The Versions tab: factory-lines is unversioned");

        await page.Button("Turn on versioning").ClickAsync();
        await SchemaWorkshopPage.FillAsync(page.Content, "Schema family", ContosoSchema.LinesFamily.ToString(System.Globalization.CultureInfo.InvariantCulture));
        await SchemaWorkshopPage.FillAsync(page.Content, "Target version", "1");
        await run.ShotAsync(6, "turn-on", "Turn on versioning: the lines' family 2, at version 1");

        await page.Button("Save config").ClickAsync();
        await page.ExpectToastAsync($"The version config of {Tree} is saved.");
        await Expect(page.Definition("Schema family")).ToHaveTextAsync("2");
        await Expect(page.Definition("Target version")).ToHaveTextAsync("1");
        await run.ShotAsync(6, "versioned", "Versioned: family 2, target version 1");

        await page.Content.Locator("details > summary").Filter(new() { HasText = "Register versions and migration paths" }).ClickAsync();
        var json = page.Content.GetByLabel("Registration JSON", new() { Exact = true });
        await json.FillAsync(ContosoSchema.LinesRegistrationJson);
        await run.ShotAsync(6, "register", "The lines' registration: version 1, and version 2, which adds shift defaulting to day");

        await page.Button("Register definitions").ClickAsync();
        await page.ExpectToastAsync("Schema definitions are registered.");
        await Expect(page.Content.GetByRole(AriaRole.Heading, new() { Name = "Family 2, which this tree stamps", Exact = true })).ToBeVisibleAsync();
        await Expect(page.Content.Locator("ol.lt-schema-hops")).ToContainTextAsync("Version 1 to 2");
        await run.ShotAsync(6, "registered", "Versions 1 and 2 are registered, with the hop between them");
        Coverage.Hit("use-case:schema/versioning");

        await page.Button("Advance target version...").ClickAsync();
        await Expect(page.Content.GetByLabel("New target version", new() { Exact = true })).ToHaveValueAsync("2");
        await run.ShotAsync(6, "advance", "Advance the target version: to 2");

        await page.Button("Advance and migrate...").ClickAsync();
        var confirm = page.Page.GetByRole(AriaRole.Alertdialog, new() { Name = "Advance to version 2 and migrate", Exact = true });
        await Expect(confirm).ToBeVisibleAsync();
        await confirm.GetByLabel("Tree name", new() { Exact = true }).FillAsync(Tree);
        await Expect(confirm.GetByRole(AriaRole.Button, new() { Name = "Advance and migrate", Exact = true })).ToBeEnabledAsync();
        await run.ShotAsync(6, "confirm", "Advance and migrate: confirmed by typing the tree's name");

        await confirm.GetByRole(AriaRole.Button, new() { Name = "Advance and migrate", Exact = true }).ClickAsync();
        await Expect(confirm).ToHaveCountAsync(0);
        await Expect(page.Tab("Remediation")).ToHaveAttributeAsync("aria-selected", "true");
        await Expect(page.OperationHeadline).ToHaveTextAsync(
            "The last operation finished, and the tree serves its result.", new() { Timeout = SchemaWorkshopPage.OperationPatienceMs });
        Coverage.Hit("tab:schema/remediation");
        await run.ShotAsync(6, "migrated", "The advance and migration ran in one operation, and finished");

        await page.OpenTabAsync("Versions");
        await Expect(page.Definition("Target version")).ToHaveTextAsync("2");
        await Expect(page.Content.Locator(".lt-schema-heading .lt-pill__text").Filter(new() { HasTextRegex = new Regex("^version 2$") })).ToHaveCountAsync(1);
        await run.ShotAsync(6, "at-version-2", "factory-lines now stamps version 2, and every stored line was rewritten at it");
        Coverage.Hit("use-case:schema/advance-and-migrate");
    }

    /// <summary>The session, its Schema page object and the journey's recorder.</summary>
    private sealed record Run(PersonaSession Session, SchemaWorkshopPage Page, JourneyRecorder Recorder)
    {
        /// <summary>Captures and checks a checkpoint of step <c>U6.<paramref name="step"/></c>.</summary>
        public Task ShotAsync(int step, string substep, string title) =>
            Session.SubstepAsync(Recorder, $"u6-{step}", substep, title);
    }
}
