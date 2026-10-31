using System.Text.Json;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// Incident and recovery (C1, #5389). Produces the archive recovered (deleted, then recovered,
/// its 5,000 entries back); <c>factory-orders</c> restored from <c>orders-baseline</c> by a
/// point-in-time replace after a burst of policy-valid wrong prices, the corrections re-applied
/// (48 apply, 2 refused), migrated to version 2, with no wrong price, in west and, once
/// replication converges, in east; and three dead letters. A restore writes below the
/// policy's choke point, so the dead letters come from the east ledger
/// (<see cref="ReplayCapturedOrdersFromEastAsync"/>): it replays the three negative orders as
/// <c>orders-baseline</c> captured them in one atomic batch to east, and west - strict - diverts
/// each replicated entry instead of applying it, leaving both regions holding the same values.
/// Until #5443 is fixed the restore into the replicated <c>factory-orders</c> aborts, so the
/// restore step is a <see cref="KnownDefect"/> and the seeder deletes the wrong prices instead.
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
        await new KnownDefect("#5443", "A restore into a replicated tree aborts because the peer's participant authorizes as anonymous").ReproducesAsync(async () =>
        {
            var restore = await backup.StartRestoreAsync(new LatticeRestoreRequest(
                context.Recall(StoryKeys.OrdersBaselineBackup), Names.Trees.FactoryOrders, mode: LatticeRestoreMode.ShadowCutover));
            try
            {
                await ContractWait.SucceededAsync("the orders-baseline restore", () => backup.GetBackupOperationStatusAsync(restore.OperationId), OperationPatience);
            }
            catch (InvalidOperationException ex) when (ex.Message.Contains("could not prepare", StringComparison.Ordinal))
            {
                return;
            }

            throw new InvalidOperationException("the orders-baseline restore into the replicated factory-orders succeeded");
        });

        // Until #5443 is fixed the restore cannot run, so the seeder removes the wrong prices
        // the restore would have discarded (they are feed orders written after the baseline).
        foreach (var key in await WrongPricesAsync(olivia.Data))
        {
            await olivia.Data.DeleteAsync(new DataDeleteRequest { TreeId = Names.Trees.FactoryOrders, Key = key });
        }

        var priya = ContractFacades.Of(context, Personas.Priya.Id).Data;
        foreach (var correction in StoryData.Corrections)
        {
            await U1Contract.TrySetAsync(priya, correction);
        }

        var schema = ContractFacades.Of(context, Personas.Sam.Id).Schema;
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

        var schema = ContractFacades.Of(estate).Schema;
        await ContractWait.UntilAsync("the replayed orders to reach west's dead letters", async () =>
        {
            var keys = new HashSet<string>(StringComparer.Ordinal);
            await foreach (var letter in schema.ListDeadLettersAsync(Names.Trees.FactoryOrders))
            {
                keys.Add(letter.Key);
            }

            return Names.Orders.NegativeTotals.All(keys.Contains);
        });
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
