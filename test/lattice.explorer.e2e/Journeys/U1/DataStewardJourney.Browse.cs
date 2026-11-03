using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>Steps 1 to 3: the directory, the address line, paging, live updates and an expired cursor.</summary>
public sealed partial class DataStewardJourney
{
    /// <summary>How long priya leaves a paged scan idle so the factory-orders cursor's one-minute idle lifetime lapses.</summary>
    private static readonly TimeSpan CursorIdle = TimeSpan.FromSeconds(90);

    /// <summary>The trees priya reads through <c>stewards-factory</c>, as the directory lists them.</summary>
    private static readonly string[] FactoryTrees = [Names.Trees.FactoryOrders, Names.Trees.FactoryTelemetry, Names.Trees.FactoryLines];

    /// <summary>A name the directory may show priya: a factory tree, the orders alias, a view over the orders, or the tag index's tree.</summary>
    private static bool IsFactoryName(string name) =>
        name.StartsWith("factory-", StringComparison.Ordinal)
        || name is Names.Aliases.Orders or Names.Views.OrdersByStatus or Names.Views.OrdersHistory or "tag-" + Names.TagIndexes.OrderStatus;

    private async Task SignInAndBrowseTheDirectoryAsync()
    {
        await _priya.Shell.ExpectSignedInAsync(Personas.Priya.Id);
        var spine = await _priya.Shell.ReadSpineAsync();
        Assert.Multiple(() =>
        {
            Assert.That(spine.CanOpen("data"), Is.True, $"stewards-factory lets priya open Data: {spine}");
            // #5588: her tree lifecycle grant on the archive and history trees admits Cluster,
            // though she holds no cluster telemetry.
            Assert.That(spine.CanOpen("cluster"), Is.True, $"priya's tree lifecycle grant admits Cluster (#5588): {spine}");
            Assert.That(spine.CanOpen("access"), Is.False, $"priya administers no access: {spine}");
        });
        await CheckpointAsync("1", "spine", "priya is signed in; the spine offers what stewards-factory permits");

        // In the story A5 has installed the task board and bound its editor role to
        // data-stewards, so priya also reads the board's tree. Only the tree A5 recorded is
        // allowed, and only when the run's ledger holds it.
        var taskBoardTree = _context.Ledger.TryGet(StoryKeys.TaskBoardTree, out var recorded) ? recorded : null;
        await _directory.OpenAsync();
        var names = await _directory.NamesAsync();
        Assert.Multiple(() =>
        {
            Assert.That(names, Is.SupersetOf(FactoryTrees), "the factory trees stewards-factory lets priya read");
            Assert.That(
                names.Where(name => !IsFactoryName(name) && name != taskBoardTree),
                Is.Empty,
                "only factory trees, what reads from them and, once A5 has granted data-stewards the task board, its tree are listed");
            if (taskBoardTree is not null)
            {
                Assert.That(names, Does.Contain(taskBoardTree), "A5 bound the task board's editor role to data-stewards, so priya reads its tree");
            }
        });
        await CheckpointAsync("1", "directory", taskBoardTree is null
            ? "The Data directory lists only the factory trees and the views over the orders"
            : "The Data directory lists the factory trees, the views over the orders and the task board's tree A5 granted data-stewards");

        await _directory.FilterAsync("telemetry");
        Assert.That(await _directory.NamesAsync(), Does.Contain(Names.Trees.FactoryTelemetry));
        await CheckpointAsync("1", "text-filter", "The text filter narrows the listing to the telemetry tree");
        await _directory.Filter.FillAsync(string.Empty);
        await Expect(_directory.Rows).ToHaveCountAsync(names.Count);

        await _directory.ShowAsync("Views");
        await Expect(_directory.Row(Names.Views.OrdersByStatus)).ToHaveCountAsync(1);
        await Expect(_directory.Row(Names.Views.OrdersHistory)).ToHaveCountAsync(1);
        await Expect(_directory.Row(Names.Trees.FactoryOrders)).ToHaveCountAsync(0);
        await CheckpointAsync("1", "views-only", "Show: Views lists the views over the orders alone: orders-by-status and orders-history");

        await _directory.ShowAsync("Trees");
        await Expect(_directory.Row(Names.Trees.FactoryOrders)).ToHaveCountAsync(1);
        await Expect(_directory.Row(Names.Views.OrdersByStatus)).ToHaveCountAsync(0);
        await _directory.ShowAsync("All");

        // Contoso is not tenanted: no tenant shares a tree with priya, so the kind filter
        // has no "shared" choice to offer.
        await Expect(_directory.Kinds.GetByRole(AriaRole.Button, new() { Name = "Shared with this tenant" })).ToHaveCountAsync(0);
        Coverage.Hit("use-case:data/directory-filters");

        await _priya.Shell.GotoAsync("/data?filter=lines");
        await Expect(_directory.Filter).ToHaveValueAsync("lines");
        await _directory.WaitForListingAsync();
        await Expect(_directory.Row(Names.Trees.FactoryLines)).ToHaveCountAsync(1);
        await Expect(_directory.Row(Names.Trees.FactoryOrders)).ToHaveCountAsync(0);
        Coverage.Hit("query:data.filter");
        await CheckpointAsync("1", "filter-address", "A ?filter= address opens the directory already filtered");

        await _directory.RefreshAsync();
        await Expect(_directory.Row(Names.Trees.FactoryLines)).ToHaveCountAsync(1);
        await CheckpointAsync("1", "refreshed", "Refresh (data.refresh) reads the catalogue again");
    }

    private async Task CompleteAnAddressAndPageTheOrdersAsync()
    {
        var suggestions = await _priya.Shell.CompleteAsync("/data/fac");
        var exact = new Regex("(^|[\\s/])" + Regex.Escape(Names.Trees.FactoryOrders) + "(?![-\\w])", RegexOptions.Multiline);
        var index = suggestions.ToList().FindIndex(text => exact.IsMatch(text));
        Assert.That(index, Is.GreaterThanOrEqualTo(0), "the address line completes /data/fac to factory-orders: " + string.Join(" | ", suggestions));
        await CheckpointAsync("2", "completion", "Typing /data/fac completes to the factory trees");

        await _priya.Shell.Suggestions.Nth(index).ClickAsync();
        await Expect(_priya.Shell.AddressInput).ToBeHiddenAsync();
        await Expect(_tree.Heading).ToHaveTextAsync(Names.Trees.FactoryOrders);
        await _tree.WaitForKeysAsync();
        await CheckpointAsync("2", "completed-tree", "Accepting the completion opens factory-orders");

        // The alias: orders names the same tree. A read is authorized on the name it addresses,
        // so stewards-factory grants her orders as well as factory-orders (#5597).
        await _priya.Shell.GotoAsync("/data/" + Names.Aliases.Orders);
        await Expect(_tree.Heading).ToHaveTextAsync(Names.Aliases.Orders);
        await _tree.WaitForKeysAsync();
        await _tree.SetPrefixAsync("order/00");
        Assert.That(await _tree.KeysAsync(), Has.Count.EqualTo(25), "the orders alias reads the seeded factory-orders keys");
        Coverage.Hit("use-case:data/alias-resolution");
        await CheckpointAsync("2", "alias", "/data/orders reaches factory-orders through the alias");
        await _priya.Shell.GotoAsync("/data/" + Names.Trees.FactoryOrders);
        await Expect(_tree.Heading).ToHaveTextAsync(Names.Trees.FactoryOrders);
        await _tree.WaitForKeysAsync();

        // A prefix the line feed never writes, so the pages hold still while she pages them.
        await _tree.SetPrefixAsync("order/00");
        Assert.That(await _tree.KeysAsync(), Has.Count.EqualTo(25), "the default page size");
        await Expect(_tree.RangeStatus).ToContainTextAsync("under prefix");
        await CheckpointAsync("2", "prefix", "The prefix filter shows the seeded orders, 25 to a page");

        var first = await _tree.KeysAsync();
        await _tree.NextPage.ClickAsync();
        await Expect(_tree.PagePosition).ToHaveTextAsync("Page 2");
        await Expect(_tree.KeyLinks.First).Not.ToHaveTextAsync(first[0]);
        await Expect(_tree.RangeStatus).ToHaveTextAsync(new Regex("^26-"));
        var second = await _tree.KeysAsync();
        Assert.That(string.CompareOrdinal(second[0], first[^1]), Is.GreaterThan(0), "page 2 continues after page 1");
        await CheckpointAsync("2", "next-page", "Next page continues the scan");

        await _tree.PreviousPage.ClickAsync();
        await Expect(_tree.PagePosition).ToHaveTextAsync("Page 1");
        await Expect(_tree.KeyLinks).ToHaveTextAsync([.. first]);

        await _tree.SetPageSizeAsync(50);
        Assert.That(await _tree.KeysAsync(), Has.Count.EqualTo(50));
        Coverage.Hit("use-case:data/keys-paging");
        await CheckpointAsync("2", "page-size", "Page size 50 shows fifty orders");

        var live = await _tree.KeysAsync();
        await _tree.SetScanModeAsync("Snapshot");
        await Expect(_tree.LiveUpdates).ToBeDisabledAsync();
        await Expect(_tree.KeyLinks).ToHaveTextAsync([.. live]);
        await CheckpointAsync("2", "snapshot-scan", "A snapshot scan; live updates follow live scans only");

        await _tree.SetScanModeAsync("Live");
        await Expect(_tree.LiveUpdates).ToBeEnabledAsync();
        Coverage.Hit("use-case:data/snapshot-scan");
    }

    private async Task FollowLiveUpdatesAndRecoverAnExpiredCursorAsync()
    {
        await _priya.Shell.GotoAsync("/data/" + Names.Trees.FactoryOrders);
        await Expect(_tree.Heading).ToHaveTextAsync(Names.Trees.FactoryOrders);
        await _tree.WaitForKeysAsync();

        // A decade of order numbers the line feed is about to reach: empty now, filled live.
        var prefix = await UpcomingOrdersPrefixAsync();
        await _tree.SetPrefixAsync(prefix, expectKeys: false);
        await _tree.SetLiveUpdatesAsync(true);
        await CheckpointAsync("3", "awaiting-orders", $"Live updates on, under {prefix}: no order there yet");

        await Expect(_tree.KeyLinks.Nth(1)).ToBeVisibleAsync(new() { Timeout = 90_000 });
        await Expect(_tree.KeysTable).ToContainTextAsync("shipped", new() { Timeout = 30_000 });
        await CheckpointAsync("3", "orders-arrive", "The line feed's orders arrive and move from open to shipped as she watches");

        await _tree.SetLiveUpdatesAsync(false);
        await Task.Delay(TimeSpan.FromSeconds(1));
        var shown = await _tree.KeysAsync();
        var paused = shown.Count;
        // The line feed opens the decade's remaining orders while the table is paused: wait
        // until the Data API holds keys under the prefix that the table does not show.
        var meanwhile = await KeysWrittenSinceAsync(prefix, shown, TimeSpan.FromSeconds(60));
        await Task.Delay(TimeSpan.FromSeconds(2));
        Assert.That(await _tree.KeyLinks.CountAsync(), Is.EqualTo(paused), "paused, the table holds still while the line feed writes");
        await CheckpointAsync("3", "paused", "Live updates paused: the table holds still while the line feed writes");

        // #5647: resuming reads the page again, so the orders written while paused arrive
        // even if no further write reaches the prefix after the resume.
        await _tree.SetLiveUpdatesAsync(true);
        foreach (var key in meanwhile)
        {
            await Expect(_tree.KeyLinks.Filter(new() { HasTextRegex = new Regex("^" + Regex.Escape(key) + "$") }))
                .ToHaveCountAsync(1, new() { Timeout = 60_000 });
        }

        Coverage.Hit("use-case:data/live-updates");
        await CheckpointAsync("3", "resumed", "Live updates on again: the orders written meanwhile arrive");

        // The cursor: page 2 of the seeded orders, left idle past the cursor's lifetime.
        await _tree.SetPrefixAsync("order/00");
        await _tree.SetPageSizeAsync(25);
        await _tree.NextPage.ClickAsync();
        await Expect(_tree.PagePosition).ToHaveTextAsync("Page 2");
        await ExpireTheCursorAsync();
        await CheckpointAsync("3", "cursor-expired", "Left idle past its lifetime, the scan's cursor has expired");

        await _tree.RestartFromFirstPage.ClickAsync();

        // The pager moves to page 1 only once the new scan's first page is read; under #5569 that
        // read can take up to three server timeouts, or end in the tab's own failure state.
        var firstPage = _tree.PagePosition.Filter(new() { HasTextRegex = new Regex("^Page 1$") });
        await Expect(firstPage.Or(_tree.KeysDidNotLoad)).ToBeVisibleAsync(new() { Timeout = 120_000 });
        if (await _tree.KeysDidNotLoad.IsVisibleAsync())
        {
            Assert.Fail($"Restart from the first page: the keys did not load: {(await _tree.KeysDidNotLoad.Locator("xpath=..").InnerTextAsync()).ReplaceLineEndings(" ")}");
        }

        await Expect(_tree.PagePosition).ToHaveTextAsync("Page 1");
        await Expect(_tree.KeyLinks.First).ToHaveTextAsync(Names.Orders.Key(1), new() { Timeout = 60_000 });
        Coverage.Hit("use-case:data/cursor-expiry");
        await CheckpointAsync("3", "restarted", "Restart from the first page recovers the scan");
    }

    /// <summary>
    /// Leaves the scan idle past its cursor's lifetime and asks for the next page, until the
    /// cluster has released the cursor. The cursor is reaped by a reminder, so the moment it
    /// lapses is not exact: a page that still loads is idled past the lifetime again.
    /// </summary>
    private async Task ExpireTheCursorAsync()
    {
        for (var attempt = 1; attempt <= 4; attempt++)
        {
            if (await _tree.CursorExpired.IsVisibleAsync())
            {
                return;
            }

            var position = await TextNowAsync(_tree.PagePosition);
            await Task.Delay(CursorIdle);
            await _tree.NextPage.ClickAsync();
            var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (DateTime.UtcNow < deadline)
            {
                if (await _tree.CursorExpired.IsVisibleAsync())
                {
                    return;
                }

                var failed = _priya.Shell.Content.GetByRole(AriaRole.Heading, new() { Name = "The keys did not load" });
                if (await failed.IsVisibleAsync())
                {
                    Assert.Fail("U1.3: resuming a scan whose cursor lapsed showed 'The keys did not load' instead of the cursor-expired state: "
                        + await _priya.Shell.Content.InnerTextAsync());
                }

                // The pager's label moves on the click, before the page loads; only a loaded
                // page fills the status line, and the expired state can still replace it.
                if (await _tree.KeyLinks.First.IsVisibleAsync()
                    && Regex.IsMatch(await TextNowAsync(_tree.RangeStatus), "^\\d")
                    && await TextNowAsync(_tree.PagePosition) is { Length: > 0 } now && now != position)
                {
                    await Task.Delay(TimeSpan.FromSeconds(2));
                    if (await _tree.CursorExpired.IsVisibleAsync())
                    {
                        return;
                    }

                    if (await _tree.PagePosition.IsVisibleAsync() && Regex.IsMatch(await TextNowAsync(_tree.RangeStatus), "^\\d"))
                    {
                        break;
                    }
                }

                await Task.Delay(250);
            }

            TestContext.Out.WriteLine($"U1.3: the cursor had not expired after {CursorIdle.TotalSeconds:0} s idle (attempt {attempt}); idling again.");
        }

        await Expect(_tree.CursorExpired).ToBeVisibleAsync();
    }

    /// <summary>The trimmed text of <paramref name="locator"/> now, or empty when it is not on the page.</summary>
    private static async Task<string> TextNowAsync(ILocator locator)
    {
        try
        {
            return await locator.IsVisibleAsync() ? (await locator.InnerTextAsync(new() { Timeout = 1_000 })).Trim() : string.Empty;
        }
        catch (TimeoutException)
        {
            return string.Empty;
        }
    }

    /// <summary>
    /// Waits until the Data API holds keys under <paramref name="prefix"/> that are not in
    /// <paramref name="shown"/>, and returns them. It reads the cluster as the administrator,
    /// so it is the ground truth the paused table is compared with.
    /// </summary>
    private async Task<IReadOnlyList<string>> KeysWrittenSinceAsync(string prefix, IReadOnlyList<string> shown, TimeSpan timeout)
    {
        var data = LatticeDataApiGrpcClient.Create(_context.Estate.Invoker(), JourneyEstate.Serialization);
        var end = prefix[..^1] + (char)(prefix[^1] + 1);
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var page = await data.ReadRangeAsync(new DataRangeRequest { TreeId = Names.Trees.FactoryOrders, StartInclusive = prefix, EndExclusive = end, PageSize = 100 });
            var added = page.Entries.Select(entry => entry.Key).Where(key => !shown.Contains(key, StringComparer.Ordinal)).ToList();
            if (added.Count > 0)
            {
                return added;
            }

            if (DateTime.UtcNow >= deadline)
            {
                Assert.Fail($"U1.3: the line feed wrote no order under {prefix} in {timeout.TotalSeconds:0} s beyond the {shown.Count} the table showed: {await _context.Estate.LineFeed.StatusAsync()}");
            }

            await Task.Delay(500);
        }
    }

    /// <summary>
    /// A prefix covering ten order numbers the line feed will open within the next few
    /// seconds. Each order it opens takes two writes (open, then shipped), and every invalid
    /// order it sends is refused but still takes a number.
    /// </summary>
    private async Task<string> UpcomingOrdersPrefixAsync()
    {
        var status = await _context.Estate.LineFeed.StatusAsync();
        Assert.That(status.OrdersAttached, Is.True, $"the line feed writes orders: {status}");
        var next = Names.Orders.LineFeedFirstOrder + (int)((status.OrdersWritten + 1) / 2) + (int)status.InvalidOrdersWritten;
        var decade = ((next + 15) / 10) * 10;
        return "order/" + decade.ToString("D6", System.Globalization.CultureInfo.InvariantCulture)[..5];
    }
}
