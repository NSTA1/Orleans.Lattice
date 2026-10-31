using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Apps = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Apps;
using Groups = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Groups;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A5, installing the task board (#5376). Produces the task board at vNext (1.1.0, from
/// <c>samples-next</c>) enabled, with the ceiling <see cref="NextCeiling"/>, the bindings
/// <see cref="Bindings"/>, and its <c>tasks</c> tree (<see cref="TaskBoardTree"/>, recorded at
/// <see cref="StoryKeys.TaskBoardTree"/>) declared replicated by its manifest. C3 uninstalls it.
/// </summary>
public sealed class A5Contract() : ChapterContract("A5")
{
    /// <summary>The installed app's <c>tasks</c> tree: an app tree is named <c>a/{slug}/{tree}</c>.</summary>
    public const string TaskBoardTree = "a/" + Apps.TaskBoard + "/tasks";

    /// <summary>The version 1 ceiling: what version 1's roles need.</summary>
    public const LatticeOperation FirstCeiling = ContosoAccess.EditOps;

    /// <summary>The vNext ceiling, re-consented on upgrade: vNext's editor adds RangeDelete.</summary>
    public const LatticeOperation NextCeiling = ContosoAccess.EditOps | LatticeOperation.RangeDelete;

    /// <summary>The role bindings A5 leaves: editor to the stewards, viewer to the auditors and the schema owners.</summary>
    public static ImmutableArray<AppRoleBindingDescriptor> Bindings { get; } =
    [
        new() { RoleName = Apps.EditorRole, GroupId = Groups.DataStewards },
        new() { RoleName = Apps.ViewerRole, GroupId = Groups.Auditors },
        new() { RoleName = Apps.ViewerRole, GroupId = Groups.SchemaOwners },
    ];

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var apps = ContractFacades.Of(context).Apps;
        var installed = await apps.DescribeAsync(Apps.TaskBoard);
        if (installed is null or { State: AppLifecycleState.NotInstalled or AppLifecycleState.Uninstalled })
        {
            await apps.InstallAsync(new AppInstallRequest
            {
                Slug = Apps.TaskBoard,
                Version = Apps.TaskBoardVersion,
                SourceKey = Apps.SamplesSource,
                Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = FirstCeiling },
                RoleBindings = [Bindings[0], Bindings[1]],
            });
            await apps.EnableAsync(Apps.TaskBoard);
            installed = await apps.DescribeAsync(Apps.TaskBoard);
        }

        if (installed!.Version != Apps.TaskBoardNextVersion)
        {
            await apps.UpdateRoleBindingsAsync(new AppRoleBindingsUpdate { Slug = Apps.TaskBoard, Version = installed.Version, RoleBindings = Bindings });
            await apps.InstallAsync(new AppInstallRequest
            {
                Slug = Apps.TaskBoard,
                Version = Apps.TaskBoardNextVersion,
                SourceKey = Apps.SamplesNextSource,
                Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = NextCeiling },
                RoleBindings = Bindings,
            });
        }
        else if (!SameBindings(installed.RoleBindings))
        {
            await apps.UpdateRoleBindingsAsync(new AppRoleBindingsUpdate { Slug = Apps.TaskBoard, Version = installed.Version, RoleBindings = Bindings });
        }

        if ((await apps.DescribeAsync(Apps.TaskBoard))!.State != AppLifecycleState.Enabled)
        {
            await apps.EnableAsync(Apps.TaskBoard);
        }

        var admin = ContractFacades.Of(context).TreeAdmin;
        await ContractWait.UntilAsync("the task board's tasks tree", async () => (await admin.CheckTreeExistsAsync(TaskBoardTree)).Exists);
        context.Record(StoryKeys.TaskBoardTree, TaskBoardTree);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        RequireEqual(context.Recall(StoryKeys.TaskBoardTree), TaskBoardTree, "the task board's tree is recorded");
        if (context.IsProduced("C3"))
        {
            return;
        }

        var app = await ContractFacades.Of(context).Apps.DescribeAsync(Apps.TaskBoard);
        Require(app is not null, "the task board is installed");
        RequireEqual(app!.Version, Apps.TaskBoardNextVersion, "the task board is at vNext");
        RequireEqual(app.State, AppLifecycleState.Enabled, "the task board is enabled");
        Require(SameBindings(app.RoleBindings), "editor is bound to data-stewards, viewer to auditors and schema-owners");
        Require(app.Ceiling is { } ceiling && (ceiling.AllowedOperations & NextCeiling) == NextCeiling, "vNext's ceiling is consented");
        Require(app.Replication.Length > 0, "its tree is declared replicated by its manifest");
        Require((await ContractFacades.Of(context).TreeAdmin.CheckTreeExistsAsync(TaskBoardTree)).Exists, "its tasks tree exists");
    }

    private static bool SameBindings(ImmutableArray<AppRoleBindingDescriptor> bindings) =>
        bindings.Length == Bindings.Length
        && Bindings.All(expected => bindings.Any(actual => actual.RoleName == expected.RoleName && actual.GroupId == expected.GroupId));
}
