using System.Buffers.Binary;
using System.Text.Json;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Faults;

/// <summary>
/// The line feed models the factory's upstream ingestion service (#5359, scope item 8): it
/// does nothing until started, its writes are refused until <c>line-feed</c> is granted
/// rights, and once granted it writes binary telemetry, moves orders from <c>open</c> to
/// <c>shipped</c> (with an invalid order every few), owns the <c>order-status</c> tag index,
/// pauses and resumes, and writes a burst of wrong prices on demand.
/// </summary>
[TestFixture]
[Category("UI")]
[Category("AzureStorageEmulator")]
[Category("EndToEnd")]
public sealed class LineFeedTests
{
    private static readonly TimeSpan Patience = TimeSpan.FromMinutes(1);

    private static JourneyEstate Estate => FaultEstate.Current;

    private static LatticeDataApiGrpcClient Data =>
        LatticeDataApiGrpcClient.Create(Estate.Invoker(), JourneyEstate.Serialization);

    /// <summary>The whole service lifecycle, in the order a real onboarding takes it.</summary>
    [Test]
    public async Task The_line_feed_is_refused_until_granted_then_writes_telemetry_and_orders()
    {
        var feed = Estate.LineFeed;
        Assert.That((await feed.StatusAsync()).Started, Is.False, "the service is idle until onboarded");

        await feed.StartAsync();
        var refused = await feed.WaitUntilAsync(status => status.ConsecutiveFailures >= 2, Patience);
        Assert.That(refused.TelemetryWritten, Is.Zero, "line-feed holds no rights yet, so every write is refused");

        var auth = LatticeAuthApiGrpcClient.Create(Estate.Invoker(), JourneyEstate.Serialization);
        foreach (var tree in new[] { LineFeedControl.TelemetryTree, LineFeedControl.OrdersTree })
        {
            await auth.PutRuleAsync(new AuthPutRule
            {
                Rule = new LatticeAuthorizationRule(
                    ruleId: $"e2e-line-feed-{tree}",
                    subject: LatticeSubjectSelector.User(JourneyEstateOptions.LineFeedIdentity),
                    scope: LatticeScope.Tree(tree),
                    operations: LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write,
                    effect: LatticeEffect.Allow),
            });
        }

        await feed.WaitUntilAsync(status => status.TelemetryWritten >= 4, Patience);
        var reading = await Data.GetAsync(new DataGetRequest { TreeId = LineFeedControl.TelemetryTree, Key = "reading/press-1/00000000" });
        Assert.Multiple(() =>
        {
            Assert.That(reading.Found, Is.True, "the first reading created the telemetry tree");
            Assert.That(reading.Value, Has.Length.EqualTo(sizeof(double)), "a reading is an 8-byte binary double");
            Assert.That(double.IsFinite(BinaryPrimitives.ReadDoubleLittleEndian(reading.Value)), Is.True);
        });

        await feed.AttachOrdersAsync();
        await feed.InjectWrongPriceBurstAsync(1);
        var orders = await feed.WaitUntilAsync(status => status.InvalidOrdersWritten >= 1 && status.WrongPricesWritten >= 1, TimeSpan.FromMinutes(2));
        Assert.That(orders.OrdersWritten, Is.GreaterThanOrEqualTo(4));

        var first = await Data.GetAsync(new DataGetRequest { TreeId = LineFeedControl.OrdersTree, Key = $"order/{LineFeedControl.FirstOrder:D6}" });
        Assert.That(StatusOf(first.Value), Is.EqualTo("shipped"), "the first order moved from open to shipped");

        var tagged = await WaitForTaggedOrdersAsync();
        Assert.That(tagged, Is.GreaterThan(0), "the line feed's indexer tags orders in tag-order-status");

        await feed.PauseAsync();
        await Task.Delay(TimeSpan.FromSeconds(1));
        var paused = (await feed.StatusAsync()).TelemetryWritten;
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.That((await feed.StatusAsync()).TelemetryWritten, Is.EqualTo(paused), "a paused feed writes nothing");

        await feed.ResumeAsync();
        await feed.WaitUntilAsync(status => status.TelemetryWritten > paused, Patience);

        // The silo's order-status view provider projects each order to its status, which is
        // how the story's orders-by-status view is created through the New view dialog.
        var admin = Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiGrpcClient.Create(Estate.Invoker(), JourneyEstate.Serialization);
        var view = await admin.CreateViewAsync("e2e-orders-by-status", LineFeedControl.OrdersTree, "order-status", "{}"u8.ToArray());
        Assert.That(view.ProviderKey, Is.EqualTo("order-status"));
        var projected = await WaitForProjectedStatusAsync(admin, view.ViewName, $"order/{LineFeedControl.FirstOrder:D6}");
        Assert.That(projected, Is.EqualTo("shipped"), "the view holds the order's status, not the order");
    }

    private static async Task<string?> WaitForProjectedStatusAsync(
        Orleans.Lattice.Api.TreeAdmin.Grpc.LatticeTreeAdminApiGrpcClient admin, string viewName, string key)
    {
        var deadline = DateTime.UtcNow + Patience;
        while (true)
        {
            var status = await admin.GetViewStatusAsync(viewName);
            if (!string.IsNullOrEmpty(status.ActiveTreeId))
            {
                var read = await Data.GetAsync(new DataGetRequest { TreeId = status.ActiveTreeId, Key = key });
                if (read.Found)
                {
                    return System.Text.Encoding.UTF8.GetString(read.Value);
                }
            }

            if (DateTime.UtcNow > deadline)
            {
                return null;
            }

            await Task.Delay(500);
        }
    }

    private static string? StatusOf(byte[] value)
    {
        using var document = JsonDocument.Parse(value);
        return document.RootElement.GetProperty("status").GetString();
    }

    private static async Task<int> WaitForTaggedOrdersAsync()
    {
        var deadline = DateTime.UtcNow + Patience;
        while (true)
        {
            var page = await Data.ReadRangeAsync(new Orleans.Lattice.Api.Data.DataRangeRequest
            {
                TreeId = "tag-" + LineFeedControl.OrderStatusIndex,
                PageSize = 100,
            });
            if (page.Entries.Count > 0 || DateTime.UtcNow > deadline)
            {
                return page.Entries.Count;
            }

            await Task.Delay(500);
        }
    }
}
