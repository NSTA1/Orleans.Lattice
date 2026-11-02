using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A2, the access model (#5372). Consumes the directory sync
/// (<see cref="ContosoHarness.SyncDirectoryGroupsAsync"/>). Produces the four groups and the
/// rule records of <see cref="ContosoAccess.A2Groups"/> and <see cref="ContosoAccess.A2Rules"/>,
/// and the line feed running with <c>factory-telemetry</c> created by its first write. C3
/// later removes priya, <c>stewards-factory</c> and <c>data-stewards</c>.
/// </summary>
public sealed class A2Contract() : ChapterContract("A2")
{
    private static readonly TimeSpan LineFeedPatience = TimeSpan.FromMinutes(2);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        await ContosoHarness.SyncDirectoryGroupsAsync(context.Estate);
        var auth = ContractFacades.Of(context).Auth;
        foreach (var group in ContosoAccess.A2Groups)
        {
            await ContractCalls.UpsertGroupAsync(auth, group);
        }

        foreach (var rule in ContosoAccess.A2Rules)
        {
            await ContractCalls.PutRuleAsync(auth, rule);
        }

        var feed = context.Estate.LineFeed;
        await feed.StartAsync();
        await feed.WaitUntilAsync(status => status.TelemetryWritten > 0, LineFeedPatience);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var offboarded = context.IsProduced("C3");
        var facades = ContractFacades.Of(context);
        var auth = facades.Auth;
        foreach (var group in ContosoAccess.A2Groups.Where(group => !(offboarded && group.GroupId == Names.Groups.DataStewards)))
        {
            Require(await ContractCalls.GroupHoldsAsync(auth, group), $"group {group.GroupId} holds {string.Join(", ", group.Users)}");
        }

        foreach (var rule in ContosoAccess.A2Rules.Where(rule => !(offboarded && rule.RuleId == Names.Rules.StewardsFactory)))
        {
            Require(await ContractCalls.RuleHoldsAsync(auth, rule), $"rule {rule.RuleId} on {rule.TreeId} allows {rule.Operations} to {rule.Subject.Id}");
        }

        var groups = await ContractCalls.AllGroupsAsync(auth);
        Require(
            Enumerable.Range(1, Names.Groups.SyncedTeamCount).All(number => groups.Any(group => group.GroupId == Names.Groups.SyncedTeam(number))),
            $"the {Names.Groups.SyncedTeamCount} directory-synced groups exist");

        var status = await context.Estate.LineFeed.StatusAsync();
        Require(status.Started && status.TelemetryWritten > 0, $"the line feed is running and writing ({status})");
        Require((await facades.TreeAdmin.CheckTreeExistsAsync(Names.Trees.FactoryTelemetry)).Exists, "factory-telemetry exists");
    }
}
