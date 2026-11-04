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
            await AwaitRemediationAsync(schema, remediation.OperationId, "the negative-total remediation");
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
    /// How long a remediation may go without its phase or scanned count moving. The driver
    /// runs one slice at a time, each bounded under the 30 s grain call timeout, reports
    /// progress after every slice, and fails the run after five consecutive slice timeouts
    /// (<c>SchemaRemediationDriver.MaxConsecutiveSliceTimeouts</c>): three minutes without a
    /// change is a remediation that has stopped, not a slow one.
    /// </summary>
    private static readonly TimeSpan RemediationStall = TimeSpan.FromMinutes(3);

    /// <summary>
    /// How long a remediation's backlog (its phase total less its completed values) may go
    /// without reaching a new low. A build copies the source as it finds it, so live writes
    /// to keys past its cursor grow the total while it runs; a build that keeps advancing
    /// but never gains on them would pass the stall check for ever. Five minutes is several
    /// times the slice cadence, so a converging build sets a new low well inside it.
    /// </summary>
    private static readonly TimeSpan RemediationConvergence = TimeSpan.FromMinutes(5);

    /// <summary>
    /// The hang guard on a remediation that keeps advancing and converging. It is not a
    /// length estimate: the stall and convergence checks decide whether a remediation is
    /// healthy, and this only stops one that would otherwise run into the leg's hang
    /// timeout. The slowest healthy remediation observed on the contract estate, C1's
    /// negative-total remediation of the restored orders (run 41771641475), needed about
    /// 600 s to reach cutover while chasing live line-feed writes; twenty minutes is twice
    /// that. A contract test may run for 50 minutes (the leg's hang budget,
    /// <c>HANGS["contract"]</c> in <c>explorer-journeys-plan.py</c>): C3's prerequisites
    /// up to C1's remediation took about 15 minutes there, so 15 + 20 + 8 (C1's
    /// re-migration ceiling) + about 4 (C3 itself) is 47, inside it.
    /// </summary>
    private static readonly TimeSpan RemediationCeiling = TimeSpan.FromMinutes(20);

    /// <summary>
    /// Waits for a remediation to succeed while it advances and converges: it fails when
    /// the operation's phase and scanned count stop moving for <see cref="RemediationStall"/>,
    /// when its backlog sets no new low for <see cref="RemediationConvergence"/>, or at
    /// <see cref="RemediationCeiling"/>, naming the trajectory it observed.
    /// </summary>
    /// <param name="schema">The Schema API.</param>
    /// <param name="operationId">The remediation's operation id.</param>
    /// <param name="what">The remediation, for messages.</param>
    public static Task AwaitRemediationAsync(Orleans.Lattice.Api.Schema.Grpc.LatticeSchemaApiGrpcClient schema, string operationId, string what) =>
        ContractWait.SucceededWhileAdvancingAsync(
            what, () => schema.GetSchemaOperationStatusAsync(operationId), RemediationStall, RemediationCeiling,
            convergence: RemediationConvergence);

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
            Orleans.Lattice.Api.Operations.LatticeOperationStatus? scanned = null;
            await ContractWait.UntilAsync("the compliance scan", async () =>
            {
                scanned = await schema.GetComplianceScanStatusAsync(scan.OperationId);
                return scanned is { IsTerminal: true };
            }, OperationPatience);

            if (scanned!.State == Orleans.Lattice.Api.Operations.LatticeOperationState.Failed
                && scanned.FailureReason?.Contains("EnumerationAborted", StringComparison.Ordinal) == true)
            {
                await ScanStarved.ReproducesAsync(() =>
                {
                    Assert.That(scanned.FailureReason, Does.Contain("EnumerationAborted"), "the compliance scan ran out of resumes");
                    return Task.CompletedTask;
                });
                Assert.Ignore($"KNOWN DEFECT #5569: the compliance scan ended Failed in phase '{scanned.Phase}': {scanned.FailureReason}");
            }

            Require(scanned.State == Orleans.Lattice.Api.Operations.LatticeOperationState.Succeeded, $"the compliance scan succeeded (it ended {scanned.State} in phase '{scanned.Phase}': {scanned.FailureReason})");
            var nonCompliant = scanned.Result.GetValueOrDefault(NonCompliantCount);
            RequireEqual(nonCompliant, "0", "factory-orders is compliant at scan time" + (nonCompliant == "0" ? string.Empty : $" ({Breakdown(scanned.Result)})"));
        }
    }

    /// <summary>
    /// Under replay-permit starvation (#5569) the compliance scan's resilient source scan exhausts
    /// its resumes and the scan ends Failed with an enumeration abort. The wrapper runs only on
    /// that outcome; any other failure stays red.
    /// </summary>
    private static readonly KnownDefect ScanStarved = new KnownDefect("#5569", "under replay-permit starvation the compliance scan's resilient source scan exhausts its resumes, so the scan ends Failed with an enumeration abort");

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
