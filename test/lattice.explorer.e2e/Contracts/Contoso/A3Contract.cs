using System.Text;
using System.Text.Json;
using Grpc.Core;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;

/// <summary>
/// A3, provisioning the factory (#5374). Produces <c>factory-orders</c> (4 shards, full-value
/// history retention set before any order is written, the 240
/// orders of <see cref="StoryData.Orders"/>, alias <c>orders</c>, publish-events on - the
/// tunable step 6 changes - the <c>orders-by-status</c> view over the silo's
/// <c>order-status</c> provider, the line feed attached and the <c>order-status</c> tag index
/// reconciled), <c>factory-lines</c> (<see cref="StoryData.Lines"/>), a 7-day history
/// retention window on <c>factory-telemetry</c>, and no <c>factory-scratch</c>.
/// </summary>
public sealed class A3Contract() : ChapterContract("A3")
{
    /// <summary>The physical shard count A3 creates <c>factory-orders</c> with.</summary>
    public const int OrdersShardCount = 4;

    /// <summary>The runtime view provider <c>orders-by-status</c> is created from, and its payload.</summary>
    public const string ViewProviderKey = "order-status";

    /// <summary>The telemetry's history retention window (step 6).</summary>
    public static readonly TimeSpan TelemetryRetention = TimeSpan.FromDays(7);

    /// <summary>
    /// The history retention mode of <c>factory-orders</c>, set in step 1 before any order is
    /// written, so every revision keeps its value (U4's Show as of reads an earlier one).
    /// </summary>
    public const TreeHistoryRetentionMode OrdersRetentionMode = TreeHistoryRetentionMode.FullValue;

    /// <inheritdoc />
    public override async Task EstablishAsync(ChapterContext context)
    {
        var facades = ContractFacades.Of(context);
        var admin = facades.TreeAdmin;

        await admin.CreateTreeAsync(Names.Trees.FactoryOrders, shardCount: OrdersShardCount);

        // Retention is stamped per revision at write time, so it is set before the seed (U4 reads
        // an earlier value through Show as of).
        await admin.SetHistoryRetentionAsync(Names.Trees.FactoryOrders, OrdersRetentionMode, null);
        await ContractCalls.SetManyAsync(facades.Data, Names.Trees.FactoryOrders, StoryData.Orders);

        await admin.CreateTreeAsync(Names.Trees.FactoryLines);
        await ContractCalls.SetManyAsync(facades.Data, Names.Trees.FactoryLines, StoryData.Lines);

        await admin.SetTreeAliasAsync(Names.Aliases.Orders, Names.Trees.FactoryOrders);
        await admin.SetHistoryRetentionAsync(Names.Trees.FactoryTelemetry, mode: null, TelemetryRetention);
        await admin.SetTreeConfigAsync(Names.Trees.FactoryOrders, new TreeConfigurationUpdate { ApplyPublishEvents = true, PublishEvents = true });

        await admin.CreateViewAsync(Names.Views.OrdersByStatus, Names.Trees.FactoryOrders, ViewProviderKey, "{}"u8.ToArray());
        await ContractWait.UntilAsync("orders-by-status to be ready", async () =>
            !string.IsNullOrEmpty((await admin.GetViewStatusAsync(Names.Views.OrdersByStatus)).ActiveTreeId));

        var feed = context.Estate.LineFeed;
        var before = (await feed.StatusAsync()).OrdersWritten;
        await feed.AttachOrdersAsync();
        await feed.WaitUntilAsync(status => status.OrdersWritten >= before + 2, TimeSpan.FromMinutes(2));

        await ContractWait.UntilAsync("the line feed's order-status tag index", async () =>
            (await admin.ListTagIndexesAsync()).Indexes.Any(index => index.IndexName == Names.TagIndexes.OrderStatus));
        var reconcile = await admin.StartTagIndexReconcileAsync(Names.TagIndexes.OrderStatus);
        await ContractWait.SucceededAsync("the order-status reconcile", () => admin.GetTreeAdminOperationStatusAsync(reconcile.OperationId));
    }

    /// <inheritdoc />
    public override async Task VerifyAsync(ChapterContext context)
    {
        var facades = ContractFacades.Of(context);
        var admin = facades.TreeAdmin;

        if (!context.IsProduced("C1"))
        {
            RequireEqual((await admin.InspectShardMapAsync(Names.Trees.FactoryOrders)).PhysicalShardCount, OrdersShardCount, "factory-orders has 4 shards");
        }

        var seeded = await ContractCalls.RangeAsync(facades.Data, Names.Trees.FactoryOrders, Names.Orders.Key(1), Names.Orders.Key(Names.Orders.SeededCount + 1));
        RequireEqual(seeded.Count, Names.Orders.SeededCount, "factory-orders holds the 240 seeded orders");
        if (!context.IsProduced("U1"))
        {
            foreach (var key in Names.Orders.NegativeTotals)
            {
                var value = seeded.Single(entry => entry.Key == key).Value;
                Require(TotalOf(value) < 0, $"{key} has a negative total");
            }
        }

        Require((await context.Estate.LineFeed.StatusAsync()).OrdersAttached, "the line feed writes factory-orders");

        var alias = await admin.ResolveTreeAliasAsync(Names.Aliases.Orders);
        Require(alias.IsAliased, "the alias orders exists");
        if (!context.IsProduced("C1"))
        {
            RequireEqual(alias.PhysicalTreeId, Names.Trees.FactoryOrders, "the alias orders resolves to factory-orders");
        }

        Require((await admin.GetTreeConfigAsync(Names.Trees.FactoryOrders)).PublishEvents == true, "factory-orders publishes events (the tunable step 6 saves)");
        if (!context.IsProduced("C1"))
        {
            RequireEqual((await admin.GetHistoryRetentionAsync(Names.Trees.FactoryOrders)).Mode, OrdersRetentionMode, "factory-orders keeps every revision's value (full-value history)");
        }

        if (!context.IsProduced("C3"))
        {
            TreeViewStatus? view = null;
            try
            {
                view = await admin.GetViewStatusAsync(Names.Views.OrdersByStatus);
            }
            catch (RpcException ex) when (ContractWait.IsTransient(ex.StatusCode))
            {
                // #5519: while the maintainer rebuilds after a source swap (U2's remediation) it
                // answers no status read, even after the logged retries. Tolerant: the view's
                // registration still proves what it projects.
                await new KnownDefect("#5519", "A view maintainer rebuilding after a source swap answers no status read").ReproducesAsync(() => Task.CompletedTask);
                var listed = (await admin.ListViewsAsync()).Views.SingleOrDefault(candidate => candidate.ViewName == Names.Views.OrdersByStatus);
                Require(listed?.SourceTreeId == Names.Trees.FactoryOrders, "orders-by-status is registered over factory-orders");
            }

            if (view is not null)
            {
                RequireEqual(view.SourceTreeId, Names.Trees.FactoryOrders, "orders-by-status projects factory-orders");
                Require(!string.IsNullOrEmpty(view.ActiveTreeId), "orders-by-status is ready");
            }
        }

        var index = await admin.GetTagIndexStatusAsync(Names.TagIndexes.OrderStatus);
        Require(index.CoveredTrees.Contains(Names.Trees.FactoryOrders), "the order-status tag index covers factory-orders");

        // A10 deletes one line, which its repair does not bring back.
        var lines = context.IsProduced("A10") ? Names.Lines.Count - 1 : Names.Lines.Count;
        RequireEqual(await ContractCalls.CountPrefixAsync(facades.Data, Names.Trees.FactoryLines, "line/"), lines, $"factory-lines holds {lines} lines");

        if (!context.IsProduced("A8"))
        {
            RequireEqual((await admin.GetHistoryRetentionAsync(Names.Trees.FactoryTelemetry)).Window, TelemetryRetention, "factory-telemetry keeps 7 days of history");
        }

        Require(!await ContractCalls.TreeIsLiveAsync(admin, Names.Trees.FactoryScratch), "factory-scratch does not exist");
    }

    /// <summary>An order value's total.</summary>
    /// <param name="value">The order's JSON.</param>
    public static decimal TotalOf(byte[] value)
    {
        using var document = JsonDocument.Parse(value);
        return document.RootElement.GetProperty("total").GetDecimal();
    }

    /// <summary>An order value's status.</summary>
    /// <param name="value">The order's JSON.</param>
    public static string? StatusOf(byte[] value)
    {
        using var document = JsonDocument.Parse(value);
        return document.RootElement.GetProperty("status").GetString();
    }

    /// <summary>An order value's JSON as text, for messages.</summary>
    /// <param name="value">The order's JSON.</param>
    public static string Text(byte[] value) => Encoding.UTF8.GetString(value);
}
