using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A schema owner's week (U2, #5384), established as sam. Produces <c>factory-orders</c>
/// remediated (every negative total set to 0 by <see cref="ContosoSchema.NegativeTotalToZero"/>)
/// so a compliance scan finds nothing non-compliant, the policy revised to
/// <see cref="ContosoSchema.OrdersRevised"/> and the target advanced to version 2 (the stored
/// values are not migrated by this stand-in, see <see cref="EstablishAsync"/>); and the archive
/// with no policy and no versioning. C1 later restores the
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

        // The stored values are not migrated here: a whole-tree migration saturates the estate
        // (#5569, its fix is in phase 2) and the one phase-1 consumer, U4, reads only the target
        // version and the policy. U2's own journey still migrates through the UI.

        await schema.ClearPolicyAsync(Names.Trees.FactoryOrdersArchive);
        await schema.ClearVersionConfigAsync(Names.Trees.FactoryOrdersArchive);
    }

    /// <summary>
    /// A whole-tree migration saturates the journey estate (#5569): its own calls then time out
    /// behind replays from -1 and it stops advancing. The wrapper runs only when the migration's
    /// wait gave up with the operation still running.
    /// </summary>
    private static readonly KnownDefect MigrationStarved = new KnownDefect("#5569", "a whole-tree schema migration saturates the journey estate, so the migration stops advancing while it is still running");

    /// <summary>How long a migration may go without moving to a new phase or completing more units.</summary>
    private static readonly TimeSpan MigrationStall = TimeSpan.FromMinutes(6);

    /// <summary>The absolute deadline for a migration, however steadily it advances.</summary>
    private static readonly TimeSpan MigrationCeiling = TimeSpan.FromMinutes(8);

    /// <summary>Starts a migration of <paramref name="treeId"/> and waits for it to succeed while it advances.</summary>
    /// <param name="schema">The Schema API.</param>
    /// <param name="treeId">The tree.</param>
    /// <param name="what">The migration, for messages.</param>
    public static async Task MigrateAsync(Orleans.Lattice.Api.Schema.Grpc.LatticeSchemaApiGrpcClient schema, string treeId, string what)
    {
        var migration = await schema.StartMigrationAsync(treeId);
        var last = new System.Runtime.CompilerServices.StrongBox<Orleans.Lattice.Api.Operations.LatticeOperationStatus?>();
        TimeoutException? timedOut = null;
        try
        {
            await ContractWait.SucceededWhileAdvancingAsync(what, () => schema.GetSchemaOperationStatusAsync(migration.OperationId), MigrationStall, MigrationCeiling, last);
        }
        catch (TimeoutException ex)
        {
            timedOut = ex;
        }

        if (timedOut is null)
        {
            return;
        }

        if (last.Value is not { IsTerminal: false })
        {
            throw timedOut;
        }

        await MigrationStarved.ReproducesAsync(() =>
        {
            Assert.That(last.Value!.IsTerminal, Is.False, "the migration is still running when its wait gives up");
            return Task.CompletedTask;
        });
        Assert.Ignore($"KNOWN DEFECT #5569: {what} was still running: {timedOut.Message}");
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
            var nonCompliant = result.Result.GetValueOrDefault(NonCompliantCount);
            RequireEqual(nonCompliant, "0", "factory-orders is compliant at scan time" + (nonCompliant == "0" ? string.Empty : $" ({Breakdown(result.Result)})"));
        }
    }

    /// <summary>The non-compliant breakdown of a compliance scan's result, as reason: count pairs, for a failure message.</summary>
    /// <param name="result">The scan operation's result map.</param>
    private static string Breakdown(IReadOnlyDictionary<string, string> result)
    {
        var rows = new List<string>();
        for (var i = 0; result.TryGetValue($"rule.{i}.reason", out var reason); i++)
        {
            rows.Add($"{result.GetValueOrDefault($"rule.{i}.count")}x {reason}");
        }

        return rows.Count == 0 ? "no breakdown recorded" : string.Join("; ", rows);
    }
}
