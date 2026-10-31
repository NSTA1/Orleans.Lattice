using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Orleans.Lattice.Membership;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// Offboarding and lifecycle (C3, #5391). Produces: priya in no group and named by no rule;
/// every <c>stewards-factory</c> record and the <c>data-stewards</c> group gone; the task board
/// uninstalled; <c>factory-orders</c>' replication disabled in both regions; no backup schedule
/// on <c>factory-telemetry</c> or <c>factory-lines</c>; no <c>orders-baseline</c>; no orphan
/// catalogue row (the row <c>lines-full</c> left when its manifest was deleted from storage is
/// pruned); the three snapshot trees purged; and the <c>orders-by-status</c> view dropped.
/// </summary>
public sealed class C3Contract() : ChapterContract("C3")
{
    private static readonly string[] SnapshotTrees =
        [Names.Trees.FactoryOrdersSnapshot, Names.Trees.TelemetrySnapOnline, Names.Trees.LinesSnapOffline];

    private static readonly TimeSpan OperationPatience = TimeSpan.FromMinutes(5);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var facades = ContractFacades.Of(context);
        var auth = facades.Auth;

        if (await ContractCalls.GroupExistsAsync(auth, Names.Groups.DataStewards))
        {
            if ((await ContractCalls.MembersAsync(auth, Names.Groups.DataStewards)).Contains(Personas.Priya.Id))
            {
                await auth.RemoveMemberAsync(new AuthMemberEdge { GroupId = Names.Groups.DataStewards, MemberId = Personas.Priya.Id, MemberKind = MembershipMemberKind.User });
            }
        }

        var apps = facades.Apps;
        if (await A5Contract.InstalledAsync(apps) is { } app)
        {
            if (app.State == AppLifecycleState.Enabled)
            {
                await apps.DisableAsync(Names.Apps.TaskBoard);
            }

            await apps.UninstallAsync(Names.Apps.TaskBoard);
        }

        foreach (var rule in ContosoAccess.A2RulesNamed(Names.Rules.StewardsFactory))
        {
            await auth.RemoveRuleAsync(new AuthRuleRef { TreeId = rule.TreeId, RuleId = rule.RuleId });
        }

        if (await ContractCalls.GroupExistsAsync(auth, Names.Groups.DataStewards))
        {
            await auth.RemoveGroupAsync(new AuthGroupRef { GroupId = Names.Groups.DataStewards });
        }

        foreach (var region in new[] { JourneyEstate.West, JourneyEstate.East })
        {
            await ContractFacades.Of(context, region: region).Replication.DisableReplicationAsync(Names.Trees.FactoryOrders);
        }

        var backup = facades.Backup;
        await backup.CancelScheduleAsync(A6Contract.TelemetryScope, incremental: false);
        await backup.CancelScheduleAsync(A10Contract.LinesScope, incremental: true);
        if (await ContractCalls.FindBackupAsync(backup, Names.Backups.OrdersBaseline, Names.Trees.FactoryOrders) is { } baseline)
        {
            await backup.DeleteBackupAsync(baseline.Id);
        }

        if (await ContractCalls.FindBackupAsync(backup, Names.Backups.LinesFull, Names.Trees.FactoryLines) is { } linesFull)
        {
            await context.Estate.DeleteBackupArtifactAsync(linesFull.Id);
        }

        var scrub = await backup.StartCatalogScrubAsync(pruneOrphans: true);
        await ContractWait.SucceededAsync("the catalogue scrub", () => backup.GetBackupOperationStatusAsync(scrub.OperationId), OperationPatience);

        foreach (var tree in SnapshotTrees)
        {
            await ContractCalls.DeleteAndPurgeAsync(facades.TreeAdmin, tree);
        }

        if ((await facades.TreeAdmin.ListViewsAsync()).Views.Any(view => view.ViewName == Names.Views.OrdersByStatus))
        {
            await facades.TreeAdmin.DropViewAsync(Names.Views.OrdersByStatus);
        }
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var facades = ContractFacades.Of(context);
        var auth = facades.Auth;

        Require(!await ContractCalls.GroupExistsAsync(auth, Names.Groups.DataStewards), "data-stewards is gone");
        var priyasGroups = (await auth.ListSubjectGroupsAsync(new AuthMemberRef { MemberId = Personas.Priya.Id })).Values;
        Require(priyasGroups.Count == 0, $"priya is in no group ({string.Join(", ", priyasGroups)})");
        var rules = await ContractCalls.AllRulesAsync(auth);
        Require(!rules.Any(rule => rule.RuleId == Names.Rules.StewardsFactory), "stewards-factory is gone");
        Require(!rules.Any(rule => rule.Subject == LatticeSubjectSelector.User(Personas.Priya.Id) || rule.Subject == LatticeSubjectSelector.Group(Names.Groups.DataStewards)), "no rule names priya or her group");

        var app = await A5Contract.InstalledAsync(facades.Apps);
        Require(app is null, $"the task board is uninstalled ({app?.State})");

        foreach (var region in new[] { JourneyEstate.West, JourneyEstate.East })
        {
            var config = await ContractFacades.Of(context, region: region).Replication.GetReplicationConfigAsync();
            Require(!config.Trees.Any(tree => tree.TreeId == Names.Trees.FactoryOrders && tree.Enabled), $"factory-orders' replication is off in {region}");
        }

        var backup = facades.Backup;
        var telemetry = await backup.GetScopeStatusAsync(A6Contract.TelemetryScope);
        Require(telemetry is null or { FullScheduleRegistered: false, IncrementalScheduleRegistered: false }, "factory-telemetry has no schedule");
        var lines = await backup.GetScopeStatusAsync(A10Contract.LinesScope);
        Require(lines is null or { FullScheduleRegistered: false, IncrementalScheduleRegistered: false }, "factory-lines has no schedule");
        Require(await ContractCalls.FindBackupAsync(backup, Names.Backups.OrdersBaseline) is null, "orders-baseline is deleted");

        var check = await backup.StartCatalogScrubAsync(pruneOrphans: false);
        var checkStatus = await ContractWait.SucceededAsync("the catalogue check", () => backup.GetBackupOperationStatusAsync(check.OperationId), OperationPatience);
        RequireEqual(checkStatus.Result.GetValueOrDefault(BackupOperationResultKeys.OrphanCount, "0"), "0", "no orphan catalogue row remains");

        foreach (var tree in SnapshotTrees)
        {
            Require(await ContractCalls.TreeIsPurgedAsync(facades.TreeAdmin, tree), $"{tree} is purged");
        }

        Require(!(await facades.TreeAdmin.ListViewsAsync()).Views.Any(view => view.ViewName == Names.Views.OrdersByStatus), "orders-by-status is dropped");
    }
}
