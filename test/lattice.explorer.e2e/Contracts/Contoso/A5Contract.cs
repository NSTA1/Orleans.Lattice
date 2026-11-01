using System.Collections.Immutable;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Apps = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Apps;
using Groups = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Groups;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A5, installing the task board (#5376). Produces the task board at vNext (1.1.0) enabled,
/// upgraded within <c>samples</c> once that source's publisher released it (the harness's
/// <c>PublishAppNextVersionAsync</c>; <c>samples-next</c> offers the same version), with the ceiling <see cref="NextCeiling"/>, the bindings
/// <see cref="Bindings"/>, and its <c>tasks</c> tree (<see cref="TaskBoardTree"/>, recorded at
/// <see cref="StoryKeys.TaskBoardTree"/>) declared replicated by its manifest. An app role binds to
/// exactly one group, so step 4 rebinds viewer from <c>auditors</c> to
/// <see cref="ContosoAccess.TaskBoardViewers"/>, which nests the auditors and the schema owners.
/// C3 uninstalls it.
/// </summary>
public sealed class A5Contract() : ChapterContract("A5")
{
    /// <summary>The installed app's <c>tasks</c> tree: an app tree is named <c>a/{slug}/{tree}</c>.</summary>
    public const string TaskBoardTree = "a/" + Apps.TaskBoard + "/tasks";

    /// <summary>The version 1 ceiling: what version 1's roles need.</summary>
    public const LatticeOperation FirstCeiling = ContosoAccess.EditOps;

    /// <summary>The vNext ceiling, re-consented on upgrade: vNext's editor adds RangeDelete.</summary>
    public const LatticeOperation NextCeiling = ContosoAccess.EditOps | LatticeOperation.RangeDelete;

    /// <summary>The role bindings of the version 1 install: editor to the stewards, viewer to the auditors.</summary>
    public static ImmutableArray<AppRoleBindingDescriptor> FirstBindings { get; } =
    [
        new() { RoleName = Apps.EditorRole, GroupId = Groups.DataStewards },
        new() { RoleName = Apps.ViewerRole, GroupId = Groups.Auditors },
    ];

    /// <summary>The role bindings A5 leaves: editor to the stewards, viewer to <see cref="ContosoAccess.TaskBoardViewers"/>.</summary>
    public static ImmutableArray<AppRoleBindingDescriptor> Bindings { get; } =
    [
        new() { RoleName = Apps.EditorRole, GroupId = Groups.DataStewards },
        new() { RoleName = Apps.ViewerRole, GroupId = ContosoAccess.TaskBoardViewers.GroupId },
    ];

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        await ContractCalls.UpsertGroupAsync(ContractFacades.Of(context).Auth, ContosoAccess.TaskBoardViewers);
        var apps = ContractFacades.Of(context).Apps;
        var installed = await InstalledAsync(apps);
        if (installed is null)
        {
            await apps.InstallAsync(new AppInstallRequest
            {
                Slug = Apps.TaskBoard,
                Version = Apps.TaskBoardVersion,
                SourceKey = Apps.SamplesSource,
                Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = FirstCeiling },
                RoleBindings = FirstBindings,
            });
            await apps.EnableAsync(Apps.TaskBoard);
        }

        var current = (await apps.DescribeAsync(Apps.TaskBoard))!;
        if (current.Version != Apps.TaskBoardNextVersion)
        {
            await apps.UpdateRoleBindingsAsync(new AppRoleBindingsUpdate { Slug = Apps.TaskBoard, Version = current.Version, RoleBindings = Bindings });

            // vNext appears: the samples source's publisher releases 1.1.0, the version
            // samples-next also offers, so the install upgrades within its own source.
            await context.Estate.PublishAppNextVersionAsync(Apps.SamplesSource);
            await apps.InstallAsync(new AppInstallRequest
            {
                Slug = Apps.TaskBoard,
                Version = Apps.TaskBoardNextVersion,
                SourceKey = Apps.SamplesSource,
                Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = NextCeiling },
                RoleBindings = Bindings,
            });
        }
        else if (!SameBindings(current.RoleBindings))
        {
            await apps.UpdateRoleBindingsAsync(new AppRoleBindingsUpdate { Slug = Apps.TaskBoard, Version = current.Version, RoleBindings = Bindings });
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

        Require(await ContractCalls.GroupHoldsAsync(ContractFacades.Of(context).Auth, ContosoAccess.TaskBoardViewers), "task-board-viewers nests the auditors and the schema owners");
        var app = await ContractFacades.Of(context).Apps.DescribeAsync(Apps.TaskBoard);
        Require(app is not null, "the task board is installed");
        RequireEqual(app!.Version, Apps.TaskBoardNextVersion, "the task board is at vNext");
        RequireEqual(app.Provenance.Source, Apps.SamplesSource, "vNext was installed from the samples source it was first installed from");
        RequireEqual(app.State, AppLifecycleState.Enabled, "the task board is enabled");
        Require(SameBindings(app.RoleBindings), "editor is bound to data-stewards, viewer to task-board-viewers");
        Require(app.Ceiling is { } ceiling && (ceiling.AllowedOperations & NextCeiling) == NextCeiling, "vNext's ceiling is consented");
        Require(app.Replication.Length > 0, "its tree is declared replicated by its manifest");
        Require((await ContractFacades.Of(context).TreeAdmin.CheckTreeExistsAsync(TaskBoardTree)).Exists, "its tasks tree exists");
    }

    /// <summary>
    /// The task board's live install in the caller's scope, or <see langword="null"/> when it is
    /// not installed. Read from the installed-app list: describing an app that is not installed
    /// is refused (FailedPrecondition), not answered empty.
    /// </summary>
    /// <param name="apps">The Apps API.</param>
    public static async Task<AppSummary?> InstalledAsync(Orleans.Lattice.Api.Apps.Grpc.LatticeAppsApiGrpcClient apps) =>
        (await apps.ListAsync()).Apps.FirstOrDefault(app =>
            app.Slug == Apps.TaskBoard && app.State is not (AppLifecycleState.NotInstalled or AppLifecycleState.Uninstalled));

    private static bool SameBindings(ImmutableArray<AppRoleBindingDescriptor> bindings) =>
        bindings.Length == Bindings.Length
        && Bindings.All(expected => bindings.Any(actual => actual.RoleName == expected.RoleName && actual.GroupId == expected.GroupId));
}
