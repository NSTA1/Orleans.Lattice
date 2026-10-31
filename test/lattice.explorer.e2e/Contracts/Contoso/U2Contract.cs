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
            await RunSchemaOperationAsync(
                schema,
                "the negative-total remediation",
                context.Estate.LineFeed,
                async () => (await schema.StartRemediationAsync(orders, ContosoSchema.NegativeTotalToZero, ContosoSchema.OrdersBaseline)).OperationId);
            await schema.SetPolicyAsync(orders, ContosoSchema.OrdersRevised);
        }

        if ((await schema.GetVersionConfigAsync(orders))?.TargetVersion != 2)
        {
            await schema.AdvanceTargetVersionAsync(orders, 2);
        }

        await MigrateAsync(schema, orders, "the migration to version 2", context.Estate.LineFeed);

        await schema.ClearPolicyAsync(Names.Trees.FactoryOrdersArchive);
        await schema.ClearVersionConfigAsync(Names.Trees.FactoryOrdersArchive);
    }

    /// <summary>
    /// Starts a migration of <paramref name="treeId"/> and waits for it to succeed, starting it
    /// again when a slice outran the grain call timeout (#5440); any other failure is final.
    /// </summary>
    /// <param name="schema">The Schema API.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="what">The migration, for messages.</param>
    /// <param name="feed">
    /// The line feed, paused while the migration runs (and resumed after, unless it was already
    /// paused) so its writes do not slow the slices past the call timeout.
    /// </param>
    public static Task MigrateAsync(Orleans.Lattice.Api.Schema.Grpc.LatticeSchemaApiGrpcClient schema, string treeId, string what, Estate.LineFeedControl feed) =>
        RunSchemaOperationAsync(schema, what, feed, async () => (await schema.StartMigrationAsync(treeId)).OperationId);

    /// <summary>
    /// Starts a tracked schema operation and waits for it to succeed, starting it again when a
    /// slice outran the grain call timeout (#5440); any other failure is final.
    /// </summary>
    /// <param name="schema">The Schema API.</param>
    /// <param name="what">The operation, for messages.</param>
    /// <param name="feed">The line feed, paused while the operation runs (see <see cref="MigrateAsync"/>).</param>
    /// <param name="start">Starts the operation and returns its id.</param>
    public static async Task RunSchemaOperationAsync(Orleans.Lattice.Api.Schema.Grpc.LatticeSchemaApiGrpcClient schema, string what, Estate.LineFeedControl feed, Func<Task<string>> start)
    {
        // #5440: pause the line feed so its writes do not push a slice past the call timeout;
        // remove the pause (and the retry below) once #5440 is fixed.
        var resume = !(await feed.StatusAsync()).Paused;
        await feed.PauseAsync();
        try
        {
            for (var attempt = 1; ; attempt++)
            {
                var operationId = await start();
                try
                {
                    await ContractWait.SucceededAsync(what, () => schema.GetSchemaOperationStatusAsync(operationId), OperationPatience);
                    return;
                }
                catch (InvalidOperationException ex) when (attempt < 5 && ex.Message.Contains("TimeoutException", StringComparison.Ordinal))
                {
                    // Known defect #5440: a slow slice fails the whole operation; start it again.
                    TestContext.Progress.WriteLine($"{what}: attempt {attempt} hit #5440; retrying.");
                }
            }
        }
        finally
        {
            if (resume)
            {
                await feed.ResumeAsync();
            }
        }
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
