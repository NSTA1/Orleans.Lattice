using System.Text.Json;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// Incident and recovery (C1, #5389). Produces the archive recovered (deleted, then recovered,
/// its 5,000 entries back); <c>factory-orders</c> restored from <c>orders-baseline</c> by a
/// point-in-time replace after a burst of policy-valid wrong prices, the corrections re-applied
/// (48 apply, 2 refused), remediated again, migrated to version 2, with no wrong price, in west
/// and, once replication converges, in east; and three dead letters. <c>orders-baseline</c>
/// predates U2's remediation, so the restore brings its three negative orders back, and a
/// migration aborts on a value its policy rejects (#5659): sam re-runs U2's
/// <see cref="ContosoSchema.NegativeTotalToZero"/> before migrating. A restore writes below the
/// policy's choke point, so the dead letters come from the east ledger
/// (<see cref="ReplayCapturedOrdersFromEastAsync"/>): it replays the three negative orders as
/// <c>orders-baseline</c> captured them in one atomic batch to east, and west - strict - diverts
/// each replicated entry instead of applying it, so west keeps their remediated totals.
/// </summary>
public sealed class C1Contract() : ChapterContract("C1")
{
    /// <summary>The total above which a stored order is a wrong (x100) price.</summary>
    public const decimal WrongPriceFloor = 1000m;

    /// <summary>How many wrong prices the seeder's burst writes (the journey injects 500).</summary>
    public const int SeederBurst = 3;

    private static readonly TimeSpan OperationPatience = TimeSpan.FromMinutes(6);

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var olivia = ContractFacades.Of(context);
        var admin = olivia.TreeAdmin;

        if (!(await admin.GetTreeDeletionStatusAsync(Names.Trees.FactoryOrdersArchive)).IsDeleted)
        {
            await ContractFacades.Of(context, Personas.Priya.Id).TreeAdmin.DeleteTreeAsync(Names.Trees.FactoryOrdersArchive);
        }

        await admin.RecoverTreeAsync(Names.Trees.FactoryOrdersArchive);
        await ContractWait.UntilAsync("the archive's recovery", () => ContractCalls.TreeIsLiveAsync(admin, Names.Trees.FactoryOrdersArchive));

        var feed = context.Estate.LineFeed;
        var wrong = (await feed.StatusAsync()).WrongPricesWritten;
        await feed.InjectWrongPriceBurstAsync(SeederBurst);
        await feed.WaitUntilAsync(status => status.WrongPricesWritten >= wrong + SeederBurst, TimeSpan.FromMinutes(2));

        var backup = olivia.Backup;
        var restore = await backup.StartRestoreAsync(new LatticeRestoreRequest(
            context.Recall(StoryKeys.OrdersBaselineBackup), Names.Trees.FactoryOrders, mode: LatticeRestoreMode.ShadowCutover));
        await ContractWait.SucceededAsync("the orders-baseline restore", () => backup.GetBackupOperationStatusAsync(restore.OperationId), OperationPatience);

        var priya = ContractFacades.Of(context, Personas.Priya.Id).Data;
        foreach (var correction in StoryData.Corrections)
        {
            await U1Contract.TrySetAsync(priya, correction);
        }

        var schema = ContractFacades.Of(context, Personas.Sam.Id).Schema;
        var remediation = await schema.StartRemediationAsync(Names.Trees.FactoryOrders, ContosoSchema.NegativeTotalToZero, ContosoSchema.OrdersRevised);
        await ContractWait.SucceededAsync("the negative-total remediation of the restored orders", () => schema.GetSchemaOperationStatusAsync(remediation.OperationId), OperationPatience);
        await U2Contract.MigrateAsync(schema, Names.Trees.FactoryOrders, "the re-migration to version 2");

        await ReplayCapturedOrdersFromEastAsync(context.Estate);
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var west = ContractFacades.Of(context);
        Require(await ContractCalls.TreeIsLiveAsync(west.TreeAdmin, Names.Trees.FactoryOrdersArchive), "the archive is recovered");
        RequireEqual(await ContractCalls.CountPrefixAsync(west.Data, Names.Trees.FactoryOrdersArchive, "order/"), Names.Files.ArchiveRows, "the recovered archive holds 5,000 entries");

        var wrongInWest = await WrongPricesAsync(west.Data);
        Require(wrongInWest.Count == 0, $"no wrong price remains in west ({string.Join(", ", wrongInWest)})");

        foreach (var number in StoryData.CorrectedOrders)
        {
            var value = await ContractCalls.GetAsync(west.Data, Names.Trees.FactoryOrders, Names.Orders.Key(number));
            Require(value is not null && A3Contract.TotalOf(value) == StoryData.CorrectedTotal(number), $"{Names.Orders.Key(number)} carries its re-applied correction");
        }

        RequireEqual((await west.Schema.GetVersionConfigAsync(Names.Trees.FactoryOrders))?.TargetVersion, 2u, "factory-orders is migrated to version 2");

        foreach (var key in Names.Orders.NegativeTotals)
        {
            var value = await ContractCalls.GetAsync(west.Data, Names.Trees.FactoryOrders, key);
            Require(value is not null && A3Contract.TotalOf(value) >= 0, $"{key} keeps its remediated total in west");
        }

        var letters = new List<string>();
        await foreach (var letter in west.Schema.ListDeadLettersAsync(Names.Trees.FactoryOrders))
        {
            letters.Add(letter.Key);
        }

        foreach (var key in Names.Orders.NegativeTotals)
        {
            Require(letters.Contains(key), $"{key}, replayed as captured, is a dead letter");
        }

        if (!context.IsProduced("C3"))
        {
            var east = ContractFacades.Of(context, region: JourneyEstate.East).Data;
            IReadOnlyList<string> wrongInEast = [];
            await ContractWait.UntilAsync("east to converge on the restored orders", async () =>
            {
                wrongInEast = await WrongPricesAsync(east);
                return wrongInEast.Count == 0;
            });
            Require(wrongInEast.Count == 0, "no wrong price remains in east");
        }
    }

    /// <summary>
    /// The east ledger, an upstream system writing to the east region: replays the three
    /// negative orders exactly as <c>orders-baseline</c> captured them, as one atomic batch.
    /// The batch replicates to west, where the strict policy diverts each entry to the dead
    /// letters (a replicated atomic batch is an intercepted system-origin write).
    /// </summary>
    /// <param name="estate">The Contoso estate.</param>
    public static async Task ReplayCapturedOrdersFromEastAsync(JourneyEstate estate)
    {
        var east = ContractFacades.Of(estate, region: JourneyEstate.East).Data;
        var schema = ContractFacades.Of(estate).Schema;
        var westData = ContractFacades.Of(estate).Data;
        async Task DiagAsync(string when)
        {
            foreach (var key in Names.Orders.NegativeTotals)
            {
                var e = await ContractCalls.GetAsync(east, Names.Trees.FactoryOrders, key);
                var w = await ContractCalls.GetAsync(westData, Names.Trees.FactoryOrders, key);
                string Show(byte[]? v) => v is null ? "<absent>" : System.Text.Encoding.UTF8.GetString(v.Where(b => b >= 32 && b < 127).ToArray());
                NUnit.Framework.TestContext.Progress.WriteLine($"C1 replay: {DateTime.UtcNow:HH:mm:ss.fff} {when} {key} east={Show(e)} west={Show(w)}");
            }
        }

        await WaitForSettledReplicationAsync(estate);
        await DiagAsync("before-batch");
        await east.SetManyAtomicAsync(new DataAtomicRequest
        {
            TreeId = Names.Trees.FactoryOrders,
            OperationId = "east-ledger-replay-" + estate.Id,
            Batch = new DataAtomicBatch
            {
                Upserts = [.. Names.Orders.NegativeTotals.Select(key =>
                    new DataEntry { Key = key, Value = StoryData.Order(int.Parse(key["order/".Length..], System.Globalization.CultureInfo.InvariantCulture)).Value })],
            },
        });

        await DiagAsync("after-batch");
        var polls = 0;
        await ContractWait.UntilAsync("the replayed orders to reach west's dead letters", async () =>
        {
            if (polls++ % 20 == 0)
            {
                await DiagAsync("poll");
            }

            var keys = new HashSet<string>(StringComparer.Ordinal);
            await foreach (var letter in schema.ListDeadLettersAsync(Names.Trees.FactoryOrders))
            {
                keys.Add(letter.Key);
            }

            return Names.Orders.NegativeTotals.All(keys.Contains);
        });
    }

    /// <summary>How many consecutive polls must find the replication settled.</summary>
    private const int SettledPolls = 10;

    /// <summary>
    /// Waits until <c>factory-orders</c>' replication is settled in both directions: in each
    /// region no link of the tree is stalled (in particular, none waits on a re-seed), every
    /// outbound link is healthy, and the tree's physical copy has not changed, all held for
    /// <see cref="SettledPolls"/> consecutive polls, so no import or cutover is in flight.
    /// C1's restore and remediation re-seed both regions; a batch written while one of
    /// them is still re-seeding is re-appended into the new copy as plain last-writer-wins rows,
    /// which strict ingest does not judge (#5684), so the east ledger waits for the estate to settle.
    /// </summary>
    /// <param name="estate">The Contoso estate.</param>
    private static async Task WaitForSettledReplicationAsync(JourneyEstate estate)
    {
        var regions = new[] { JourneyEstate.West, JourneyEstate.East };
        string?[] lastPhysical = [null, null];
        var detail = "no poll yet";
        var stablePolls = 0;
        try
        {
            await ContractWait.UntilAsync("factory-orders' replication to settle in both directions", async () =>
            {
                var settled = true;
                var parts = new List<string>();
                for (var i = 0; i < regions.Length; i++)
                {
                    var facades = ContractFacades.Of(estate, region: regions[i]);
                    var page = await facades.ReplicationStatus.GetPeerStatusAsync(
                        new ReplicationPeerStatusQuery { TreeId = Names.Trees.FactoryOrders, PageSize = ReplicationPeerStatusQuery.MaxPageSize });
                    var links = page.Peers.Where(link => link.TreeId == Names.Trees.FactoryOrders).ToList();
                    var physical = (await facades.TreeAdmin.ResolveTreeAliasAsync(Names.Trees.FactoryOrders)).PhysicalTreeId;
                    var regionSettled = links.Any(link => link.Direction == ReplicationLinkDirection.Outbound)
                        && links.All(link => link.StallReason is null
                            && (link.Health == ReplicationLinkHealth.Healthy
                                || (link.Health == ReplicationLinkHealth.Unknown && link.Direction == ReplicationLinkDirection.Inbound)))
                        && physical == lastPhysical[i];
                    settled &= regionSettled;
                    lastPhysical[i] = physical;
                    parts.Add($"{regions[i]}: {physical}, " + string.Join("; ", links.Select(link =>
                        $"{link.Direction} {link.PeerRegionId} {link.Health} behind={link.EntriesBehind} inflight={link.InFlight} stall={link.StallReason}")));
                }

                detail = string.Join(" | ", parts);
                stablePolls = settled ? stablePolls + 1 : 0;
                return stablePolls >= SettledPolls;
            }, OperationPatience);
        }
        catch (TimeoutException ex)
        {
            throw new TimeoutException($"{ex.Message} Last observed: {detail}", ex);
        }

        NUnit.Framework.TestContext.Progress.WriteLine($"C1 replay: settled - {detail}");
    }

    private static async Task<IReadOnlyList<string>> WrongPricesAsync(LatticeDataApiGrpcClient data)
    {
        var wrong = new List<string>();
        foreach (var entry in await ContractCalls.RangeAsync(data, Names.Trees.FactoryOrders, "order/", ContractCalls.PrefixEnd("order/")))
        {
            if (TryTotal(entry.Value) is { } total && total >= WrongPriceFloor)
            {
                wrong.Add(entry.Key);
            }
        }

        return wrong;
    }

    private static decimal? TryTotal(byte[] value)
    {
        try
        {
            using var document = JsonDocument.Parse(value);
            return document.RootElement.TryGetProperty("total", out var total) && total.TryGetDecimal(out var parsed) ? parsed : null;
        }
        catch (JsonException)
        {
            return null;
        }
    }
}
