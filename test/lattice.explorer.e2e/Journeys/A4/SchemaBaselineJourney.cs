using System.Text.RegularExpressions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A4;

/// <summary>
/// A4, the schema baseline (#5375): Olivia writes down what an order is - a non-negative
/// <c>total</c> and a known <c>status</c> - with the rule builder, sees the sample already
/// breaks it and saves anyway with strict ingest on, turns versioning on with versions 1 and 2
/// registered, and watches the line feed's next invalid order refused by the policy.
/// </summary>
[Journey(A4Steps.JourneyId)]
[JourneyChapter("A4", After = new[] { "A3" })]
public sealed partial class SchemaBaselineJourney : JourneyChapter
{
    private const string Tree = Names.Trees.FactoryOrders;

    /// <summary>The line feed's refusal of an order by the tree's policy, as its last error records it.</summary>
    private static readonly Regex PolicyRefusal = RefusalPattern();

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        await using var olivia = await context.Estate.SignInAsAsync(Personas.Olivia);
        var steps = new A4Steps(olivia);
        var schema = new SchemaAreaPage(olivia.Shell);

        await steps.StepAsync(1, async () =>
        {
            await schema.OpenDirectoryAsync();
            await schema.ExpectNotUnderSchemaAsync(Tree);
            await steps.SubstepAsync("under-schema", "The Schema directory: factory-orders is not under schema");

            await schema.ShowAllTreesAsync(Tree);
            await schema.ExpectRowAsync(Tree, policy: "None", versioning: "Unversioned");
            await steps.SubstepAsync("all-trees", "All trees, filtered to factory-orders: no policy, unversioned");

            await schema.OpenTreeFromDirectoryAsync(Tree);
            await schema.ExpectNoPolicyAsync();
            await steps.SubstepAsync("no-policy", "factory-orders' schema workspace: the Policy tab offers Set a policy");
            Coverage.Hit("use-case:schema/area");
        });

        await steps.StepAsync(2, async () =>
        {
            var builder = await schema.SetAPolicyAsync();
            await steps.SubstepAsync("builder", "The rule builder, with a sample of factory-orders read and no rules yet");

            await builder.AddARuleAsync("total");
            await builder.ChooseCardAsync("Number range");
            await builder.SetRangeAsync(smallest: "0", largest: string.Empty);
            await builder.ExpectComposerReadsAsync("total must be a number of at least 0", "99 of 100 values pass");
            await steps.SubstepAsync("total-card", "The Number range card on total, at least 0: order/000017 fails it");
            await builder.CommitRuleAsync("total must be a number of at least 0");

            await builder.AddARuleAsync("status");
            await builder.ChooseCardAsync("One of a set");
            await builder.SetAllowedValuesAsync(Names.Orders.Statuses);
            await builder.ExpectComposerReadsAsync("status must be one of \"open\", \"shipped\" or \"cancelled\"", "100 of 100 values pass");
            await steps.SubstepAsync("status-card", "The One of a set card on status: open, shipped or cancelled");
            await builder.CommitRuleAsync("status must be one of \"open\", \"shipped\" or \"cancelled\"");

            await builder.TurnStrictIngestOnAsync();
            await builder.ExpectSampleCheckAsync(failing: 1, checkedCount: 100, firstFailure: Names.Orders.Order17);
            await steps.SubstepAsync("sample-check", "Two rules and strict ingest on; the sample check flags order/000017");

            await builder.SaveIntoWarningAsync("1 of 100");
            await steps.SubstepAsync("save-warning", "Saving warns that 1 of 100 sampled values fail the new rules");

            await builder.SaveAnywayAsync(Tree);
            await schema.ExpectStrictPolicyAsync(rules: 2);
            var saved = await ContractFacades.Of(context).Schema.GetPolicyAsync(Tree);
            Assert.That(ContosoSchema.SameAs(saved, ContosoSchema.OrdersBaseline), Is.True,
                "The policy the builder saved is the pinned baseline: total >= 0 and status in the set, strict ingest on.");
            await steps.SubstepAsync("policy-saved", "Saved anyway: a strict policy of two rules; the stored values are left as they are");
            Coverage.Hit("use-case:schema/policy");
        });

        await steps.StepAsync(3, async () =>
        {
            var versions = new SchemaVersionsPage(olivia.Shell, Tree);
            await schema.SelectTabAsync("Versions", "versions");
            await versions.ExpectUnversionedAsync();
            await steps.SubstepAsync("unversioned", "The Versions tab: factory-orders is unversioned");

            await versions.OpenTurnOnAsync();
            await steps.SubstepAsync("turn-on", "Turn on versioning: family 1, target version 1, strict ingest off");

            await versions.SaveConfigAsync();
            await steps.SubstepAsync("versioned", "Versioning is on: new writes are stamped family 1, version 1");

            await versions.PasteRegistrationAsync(ContosoSchema.OrdersRegistrationJson);
            await steps.SubstepAsync("register", "The Register box holds versions 1 and 2, and the hop that adds currency, EUR by default");

            await versions.RegisterAsync();
            await versions.ExpectRegisteredAsync("Order", "Order with currency", "currency");
            await steps.SubstepAsync("registered", "Registered: version 1 is the target; version 2 is newer, one hop away");

            await versions.RefreshDefinitionsAsync();
            await versions.ExpectRegisteredAsync("Order", "Order with currency", "currency");
            await versions.ExpectTargetAsync(1);
            var config = await ContractFacades.Of(context).Schema.GetVersionConfigAsync(Tree);
            Assert.That(config?.TargetVersion, Is.EqualTo(1u), "The target stays version 1 after the definitions are registered.");
            await steps.SubstepAsync("refreshed", "Definitions refreshed: both versions are there, and the target stays version 1");
            SchemaVersionsPage.RecordVersioning();
        });

        await steps.StepAsync(4, async () =>
        {
            await schema.OpenDirectoryAsync();
            await schema.ExpectRowAsync(Tree, policy: "2 rules, strict ingest", versioning: "family 1 at version 1");
            await steps.SubstepAsync("governed", "The Schema directory: factory-orders is under schema, with its policy and versioning");
            Coverage.Hit("use-case:schema/area");
        });

        await steps.StepAsync(5, async () =>
        {
            // Off-UI: the harness watches the line feed, the external writer, send its next
            // invalid order, and finds it refused by the policy rather than stored.
            var refused = await NextRefusedOrderAsync(context.Estate);
            var stored = await ContractCalls.GetAsync(ContractFacades.Of(context).Data, Tree, refused);
            Assert.That(stored, Is.Null, $"The line feed's invalid order {refused} was refused, so nothing is stored under it.");

            await schema.OpenDirectoryAsync();
            await schema.OpenTreeFromDirectoryAsync(Tree);
            await schema.ExpectHeadingPillsAsync(version: 1);
            await schema.SelectTabAsync("Dead letters", "dead-letters");
            await schema.ExpectNoDeadLettersAsync();
            await steps.SubstepAsync("no-dead-letters", $"The line feed's {refused} was refused, not diverted: Dead letters is empty");
        });
    }

    /// <summary>
    /// Waits, for at most two minutes (the line feed sends an invalid order about every ten
    /// seconds), until the line feed sends an invalid order the tree's policy refuses, and
    /// returns that order's key. A policy change reaches the cluster's other silo shortly after
    /// the one that saved it, so an invalid order in that window may still be stored; the wait
    /// is for the first refusal.
    /// </summary>
    /// <param name="estate">The estate.</param>
    private static async Task<string> NextRefusedOrderAsync(JourneyEstate estate)
    {
        var before = await estate.LineFeed.StatusAsync();
        var status = await estate.LineFeed.WaitUntilAsync(
            now => now.InvalidOrdersWritten > before.InvalidOrdersWritten
                && now.LastError != before.LastError
                && now.LastError is { } error
                && PolicyRefusal.IsMatch(error),
            TimeSpan.FromMinutes(2));
        return PolicyRefusal.Match(status.LastError!).Groups["key"].Value;
    }

    [GeneratedRegex(@"^factory-orders/(?<key>order/\d{6}): InvalidArgument The value was rejected by the tree's schema policy\.")]
    private static partial Regex RefusalPattern();
}
