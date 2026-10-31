using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A schema owner's week (U2, #5384), established as sam. Produces <c>factory-orders</c>
/// remediated (every negative total set to 0 by <see cref="ContosoSchema.NegativeTotalToZero"/>)
/// so a compliance scan finds nothing non-compliant, the policy revised to
/// <see cref="ContosoSchema.OrdersRevised"/>, the target advanced to version 2 and the stored
/// values migrated; and the archive with no policy and no versioning. C1 later restores the
/// orders from before the remediation.
/// </summary>
public sealed class U2Contract() : ChapterContract("U2")
{
    /// <summary>The compliance scan result key counting non-compliant values.</summary>
    public const string NonCompliantCount = "nonCompliantCount";

    private static readonly TimeSpan OperationPatience = TimeSpan.FromMinutes(6);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var schema = ContractFacades.Of(context, Personas.Sam.Id).Schema;
        var orders = Names.Trees.FactoryOrders;

        if (!ContosoSchema.SameAs(await schema.GetPolicyAsync(orders), ContosoSchema.OrdersRevised))
        {
            var remediation = await schema.StartRemediationAsync(orders, ContosoSchema.NegativeTotalToZero, ContosoSchema.OrdersBaseline);
            await ContractWait.SucceededAsync("the negative-total remediation", () => schema.GetSchemaOperationStatusAsync(remediation.OperationId), OperationPatience);
            await schema.SetPolicyAsync(orders, ContosoSchema.OrdersRevised);
        }

        if ((await schema.GetVersionConfigAsync(orders))?.TargetVersion != 2)
        {
            await schema.AdvanceTargetVersionAsync(orders, 2);
        }

        var migration = await schema.StartMigrationAsync(orders);
        await ContractWait.SucceededAsync("the migration to version 2", () => schema.GetSchemaOperationStatusAsync(migration.OperationId), OperationPatience);

        await schema.ClearPolicyAsync(Names.Trees.FactoryOrdersArchive);
        await schema.ClearVersionConfigAsync(Names.Trees.FactoryOrdersArchive);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var schema = ContractFacades.Of(context).Schema;
        var orders = Names.Trees.FactoryOrders;

        RequireEqual((await schema.GetVersionConfigAsync(orders))?.TargetVersion, 2u, "factory-orders targets version 2");
        Require(ContosoSchema.SameAs(await schema.GetPolicyAsync(orders), ContosoSchema.OrdersRevised), "the revised policy: an optional currency and status as a pattern");
        Require(await schema.GetPolicyAsync(Names.Trees.FactoryOrdersArchive) is null, "the archive has no policy");
        Require(await schema.GetVersionConfigAsync(Names.Trees.FactoryOrdersArchive) is null, "the archive is unversioned");

        if (!context.IsProduced("C1"))
        {
            var scan = await schema.StartComplianceScanAsync(orders);
            var result = await ContractWait.SucceededAsync("the compliance scan", () => schema.GetComplianceScanStatusAsync(scan.OperationId), OperationPatience);
            RequireEqual(result.Result.GetValueOrDefault(NonCompliantCount), "0", "factory-orders is compliant at scan time");
        }
    }
}
