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
        var missing = Enumerable.Range(1, Names.Groups.SyncedTeamCount)
            .Select(Names.Groups.SyncedTeam)
            .Where(id => !groups.Any(group => group.GroupId == id))
            .ToList();
        Require(
            missing.Count == 0,
            missing.Count == 0
                ? $"the {Names.Groups.SyncedTeamCount} directory-synced groups exist"
                : $"the {Names.Groups.SyncedTeamCount} directory-synced groups exist (#5564 diagnostics: {await DescribeMissingGroupsAsync(context, auth, missing, groups.Count)})");

        var status = await context.Estate.LineFeed.StatusAsync();
        Require(status.Started && status.TelemetryWritten > 0, $"the line feed is running and writing ({status})");
        Require((await facades.TreeAdmin.CheckTreeExistsAsync(Names.Trees.FactoryTelemetry)).Exists, "factory-telemetry exists");
    }

    /// <summary>
    /// For #5564: the listing missed <paramref name="missing"/>. Names each id, the shard it hashes
    /// to in <c>sys-membership-groups</c>, and what a point read of it answers now, so a silent
    /// partial listing can be told apart from a missing record.
    /// </summary>
    private static async Task<string> DescribeMissingGroupsAsync(ChapterContext context, Orleans.Lattice.Api.Auth.Grpc.LatticeAuthApiGrpcClient auth, IReadOnlyList<string> missing, int listed)
    {
        const string groupsTree = "sys-membership-groups";
        const int defaultVirtualShards = 4096, defaultShards = 64; // LatticeConstants.DefaultVirtualShardCount / DefaultShardCount (internal)
        var virtualShards = defaultVirtualShards;
        var mapNote = "default map";
        try
        {
            var map = await ContractFacades.Of(context).TreeAdmin.GetShardMapAsync(groupsTree);
            if (map.HasCustomMap)
            {
                virtualShards = map.VirtualShardCount;
                mapNote = $"custom map v{map.MapVersion}, {map.PhysicalShardCount} physical shards";
            }
        }
        catch (Exception ex)
        {
            mapNote = $"shard map unreadable ({ex.GetType().Name})";
        }

        var lines = new List<string>();
        foreach (var id in missing)
        {
            var slot = ShardMap.GetVirtualSlot(id, virtualShards);
            var legacyShard = ShardMap.GetVirtualSlot(id, defaultShards);
            string read;
            try
            {
                read = (await auth.GetGroupAsync(new() { GroupId = id })).Group is null ? "GetGroup: absent" : "GetGroup: present";
            }
            catch (Exception ex)
            {
                read = $"GetGroup failed: {ex.GetType().Name} {ex.Message.Split('\n')[0]}";
            }

            lines.Add($"{id} slot {slot}/{virtualShards} (default shard {legacyShard}/{defaultShards}): {read}");
        }

        return $"listed {listed} groups, {missing.Count} synced missing; {mapNote}; " + string.Join("; ", lines);
    }
}
