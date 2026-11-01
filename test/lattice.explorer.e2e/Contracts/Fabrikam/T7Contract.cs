using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Apps = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Apps;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Fabrikam;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Fabrikam;

/// <summary>
/// The tenant-rooted walk (T7, #5395). Produces the task board (1.0.0 from <c>samples</c>)
/// installed and enabled in acme by hal, viewer bound to the tenant group
/// <c>acme-analysts</c>; and ada's backup <c>acme-inv-1</c> of <c>acme-inventory</c>, its id
/// at <see cref="StoryKeys.Backup"/> of its name.
/// </summary>
public sealed class T7Contract() : ChapterContract("T7")
{
    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var apps = ContractFacades.Of(context, Personas.Hal.Id, tenant: Names.Tenants.Acme).Apps;
        if (await apps.DescribeAsync(Apps.TaskBoard) is null or { State: AppLifecycleState.NotInstalled or AppLifecycleState.Uninstalled })
        {
            await apps.InstallAsync(new AppInstallRequest
            {
                Slug = Apps.TaskBoard,
                Version = Apps.TaskBoardVersion,
                SourceKey = Apps.SamplesSource,
                Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = A5Contract.FirstCeiling },
                RoleBindings = [new() { RoleName = Apps.ViewerRole, GroupId = Names.Groups.AcmeAnalysts }],
            });
        }

        if ((await apps.DescribeAsync(Apps.TaskBoard))!.State != AppLifecycleState.Enabled)
        {
            await apps.EnableAsync(Apps.TaskBoard);
        }

        var backup = ContractFacades.Of(context, Personas.Ada.Id, tenant: Names.Tenants.Acme).Backup;
        var id = (await ContractCalls.FindBackupAsync(backup, Names.Backups.AcmeInv1))?.Id;
        if (id is null)
        {
            var handle = await backup.StartBackupAsync(new LatticeBackupCaptureRequest(Names.Backups.AcmeInv1, BackupScopeSelector.WholeTree(Names.Trees.AcmeInventory)));
            id = await ContractCalls.CapturedBackupIdAsync(backup, handle.OperationId, "the acme-inv-1 capture");
        }

        context.Record(StoryKeys.Backup(Names.Backups.AcmeInv1), id);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var region = context.IsProduced("T5") ? FabrikamAccess.East : FabrikamAccess.West;
        var app = await ContractFacades.Of(context, Personas.Hal.Id, region, Names.Tenants.Acme).Apps.DescribeAsync(Apps.TaskBoard);
        Require(app is { State: AppLifecycleState.Enabled }, $"the task board is enabled in acme ({app?.State})");
        Require(app!.RoleBindings.Any(binding => binding.RoleName == Apps.ViewerRole && binding.GroupId == Names.Groups.AcmeAnalysts), "viewer is bound to acme-analysts");

        var id = context.Recall(StoryKeys.Backup(Names.Backups.AcmeInv1));
        var described = await ContractFacades.Of(context, Personas.Ada.Id, region, Names.Tenants.Acme).Backup.DescribeBackupAsync(id);
        Require(described is { Manifest.Name: Names.Backups.AcmeInv1 }, "acme-inv-1 is catalogued");
    }
}
