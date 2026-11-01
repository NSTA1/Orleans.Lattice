using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts.Contoso;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A7;

/// <summary>
/// A7, going multi-region (#5379). Contoso's east plant needs the orders locally: olivia reads
/// the replication estate - the line register east enrols by static configuration, and the task
/// board's app-owned enrolment - enrols <c>factory-orders</c> with an explicit merge mode and
/// bootstrap source in both regions, and watches it converge on the east console.
/// </summary>
/// <remarks>
/// Off-UI actions, performed by the harness as the external actor each models: the line feed
/// (the plant's ingestion service) is paused from step 4 to the end of step 7, so a caught-up
/// link and a live-key count can be compared while nothing writes, and resumed afterwards.
/// </remarks>
[JourneyChapter("A7", After = new[] { "A3", "A5" })]
public sealed class GoingMultiRegionJourney : JourneyChapter
{
    private const string Journey = "a7";
    private const string MergeModeLabel = "LWW register";
    private static readonly TimeSpan Convergence = TimeSpan.FromMinutes(4);

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var estate = context.Estate;
        var recorder = JourneyRecorder.For(Journey);
        var orders = Names.Trees.FactoryOrders;

        await using var west = await estate.SignInAsAsync(Personas.Olivia);
        var westCompact = await west.Shell.IsCompactAsync();
        var westEstate = new ReplicationEstateScreen(west.Shell);
        var westTrees = new ReplicationTreesScreen(west.Shell);

        // ----- 1. The replication estate -----
        using (var step = Coverage.BeginScope("A7.1"))
        {
            await westEstate.OpenAsync();
            await westEstate.WaitForPeerAsync(JourneyEstate.East, TimeSpan.FromMinutes(2));
            await Expect(westEstate.LocalNode(JourneyEstate.West)).ToBeVisibleAsync();
            Coverage.Hit("route:/replication");
            await west.SubstepAsync(recorder, "1", "estate-map", "A7.1 - The estate map draws this region, west, and its peer east");

            await westEstate.RefreshFromPaletteAsync();
            Coverage.Hit("command:replication.refresh");
            await west.SubstepAsync(recorder, "1", "refreshed", "A7.1 - Refresh replication status re-reads every peer link");

            await westEstate.FilterAsync(westEstate.HealthFilter, "Healthy", "health", "healthy");
            await Expect(westEstate.StatusLine).ToContainTextAsync(new Regex("of [\\d,]+ links? match"));
            Coverage.Hit("query:replication.health");
            await west.SubstepAsync(recorder, "1", "filter-health", "A7.1 - Filtered to healthy links");
            await westEstate.FilterAsync(westEstate.HealthFilter, "Any health", "health", null);

            await westEstate.FilterAsync(westEstate.RegionFilter, JourneyEstate.East, "region", JourneyEstate.East);
            await Expect(westEstate.PeerNode(JourneyEstate.East)).ToHaveAttributeAsync("aria-current", "true");
            Coverage.Hit("query:replication.region");
            await west.SubstepAsync(recorder, "1", "filter-region", "A7.1 - Filtered to the links with east");
            step.Complete();
        }

        // ----- 2. The enrolled trees: app-owned in west, static-only in east -----
        await using var east = await OpenEastConsoleAsync(estate);
        var eastCompact = await east.Shell.IsCompactAsync();
        var eastEstate = new ReplicationEstateScreen(east.Shell);
        var eastTrees = new ReplicationTreesScreen(east.Shell);
        using (var step = Coverage.BeginScope("A7.2"))
        {
            await west.Shell.RunCommandAsync(ReplicationEstateScreen.TreesCommandTitle);
            await westTrees.ExpectLoadedAsync();
            Coverage.Hit("command:replication.trees");
            Coverage.Hit("route:/replication/trees");
            Coverage.Hit("subroute:replication/trees");
            var taskBoard = westTrees.Row(A5TaskBoardTree, westCompact);
            await Expect(taskBoard).ToContainTextAsync("Enabled");
            await Expect(taskBoard.GetByRole(AriaRole.Link, new() { Name = "Managed by " + Names.Apps.TaskBoard, Exact = true })).ToBeVisibleAsync();
            if (!westCompact)
            {
                Assert.That(await ReplicationTables.CellAsync(taskBoard, "Owner"), Is.EqualTo("App " + Names.Apps.TaskBoard));
            }

            await west.SubstepAsync(recorder, "2", "app-owned", "A7.2 - The task board's tree is enrolled by its app, and managed from it");

            // The App filter lists the apps that own an enrolled tree here. The estate page's lists
            // only apps whose trees have reported a link, which an idle app tree may not have yet.
            await westEstate.FilterAsync(westEstate.AppFilter, Names.Apps.TaskBoard, "app", Names.Apps.TaskBoard);
            await Expect(taskBoard).ToBeVisibleAsync();
            Coverage.Hit("query:replication.app");
            Coverage.Hit("use-case:replication/filters");
            await west.SubstepAsync(recorder, "2", "filter-app", "A7.2 - Filtered to the trees the task board owns");
            await westEstate.FilterAsync(westEstate.AppFilter, "Any app", "app", null);

            await Expect(east.Shell.Heading).ToBeVisibleAsync();
            await east.SubstepAsync(recorder, "2", "east-console", "A7.2 - olivia signed in to the east console, under /console on the east gateway");

            await eastEstate.OpenAsync();
            await eastEstate.TreesSection.ClickAsync();
            await eastTrees.ExpectLoadedAsync();
            var lines = eastTrees.Row(Names.Trees.FactoryLines, eastCompact);
            await Expect(lines).ToContainTextAsync("Static");
            await Expect(lines).ToContainTextAsync("Declared at deployment");
            await Expect(lines.GetByRole(AriaRole.Button)).ToHaveCountAsync(0);
            if (!eastCompact)
            {
                Assert.That(await ReplicationTables.CellAsync(lines, "Enrolment"), Is.EqualTo("Static"));
            }

            Coverage.Hit("use-case:replication/enrolment-shapes");
            await east.SubstepAsync(recorder, "2", "static-only", "A7.2 - East enrols factory-lines by its configuration alone: declared at deployment, no toggle");
            step.Complete();
        }

        // ----- 3. Enable replication for factory-orders, west then east -----
        using (var step = Coverage.BeginScope("A7.3"))
        {
            await westTrees.OpenEnableAsync();
            await west.SubstepAsync(recorder, "3", "west-dialog", "A7.3 - The Enable replication dialog in west");
            Assert.That(await westTrees.NameTreeAsync(orders), Is.True, "west's tree picker offers factory-orders");
            await westTrees.ChooseMergeModeAsync(MergeModeLabel);
            await Expect(westTrees.BootstrapField).ToHaveValueAsync(string.Empty);
            await west.SubstepAsync(recorder, "3", "west-composed", "A7.3 - factory-orders, last-writer-wins, no bootstrap: west is the source");
            await westTrees.EnableButton.ClickAsync();
            await Expect(westTrees.EnableDialog).ToBeHiddenAsync(new() { Timeout = 30_000 });
            await Expect(west.Shell.Toasts.Filter(new() { HasText = $"Replication enabled for {orders} ({MergeModeLabel})." })).ToBeVisibleAsync();
            var westRow = westTrees.Row(orders, westCompact);
            await Expect(westRow).ToContainTextAsync("Enabled", new() { Timeout = 30_000 });
            await Expect(westRow).ToContainTextAsync(MergeModeLabel);
            await Expect(westRow).ToContainTextAsync("Runtime");
            await west.SubstepAsync(recorder, "3", "west-enabled", "A7.3 - factory-orders enabled in west, runtime enrolment");

            await eastTrees.OpenEnableAsync();
            await eastTrees.NameTreeAsync(orders);
            await eastTrees.ChooseMergeModeAsync(MergeModeLabel);
            await eastTrees.NameBootstrapAsync(JourneyEstate.West);
            await east.SubstepAsync(recorder, "3", "east-composed", "A7.3 - In east: factory-orders, last-writer-wins, bootstrapped from west");
            var enableStarted = System.Diagnostics.Stopwatch.StartNew();
            await eastTrees.EnableButton.ClickAsync();

            // While the enable waits on west, the dialog says so. The enable can finish before the
            // note is drawn, so the in-flight picture is taken only when the note is up.
            var inFlight = eastTrees.EnableDialog.GetByRole(AriaRole.Status).Filter(new() { HasText = $"asking {JourneyEstate.West} for a snapshot" });
            try
            {
                await inFlight.WaitForAsync(new() { Timeout = 5_000 });
                await east.SubstepAsync(recorder, "3", "east-in-flight", "A7.3 - While the enable waits on west, the dialog says what it is waiting for");
            }
            catch (TimeoutException)
            {
                TestContext.Progress.WriteLine($"A7.3: the enable finished before its in-flight note was drawn ({enableStarted.Elapsed.TotalSeconds:0.0} s)");
            }

            // Regression (#5379): east does not hold factory-orders yet; with a bootstrap source
            // named, the picker accepts it rather than refusing a tree only the bootstrap brings.
            // The enable returns only once west has answered the bootstrap request, which can take
            // well over a minute on a cold receiver (#5560), so the wait is bounded by that operation
            // rather than by a UI timeout. Meanwhile the dialog says what it is waiting for.
            await Expect(east.Shell.Toasts.Filter(new() { HasText = $"Replication enabled for {orders} ({MergeModeLabel}). A snapshot bootstrap was requested." }))
                .ToBeVisibleAsync(new() { Timeout = (float)EnableBootstrapBound.TotalMilliseconds });
            TestContext.Progress.WriteLine($"A7.3: east's bootstrapped enable of {orders} took {enableStarted.Elapsed.TotalSeconds:0.0} s (#5560)");
            await Expect(eastTrees.EnableDialog).ToBeHiddenAsync();
            await Expect(east.Page.GetByText($"No tree is named {orders}. Choose one from the list.")).ToHaveCountAsync(0);
            var eastRow = eastTrees.Row(orders, eastCompact);
            await Expect(eastRow).ToContainTextAsync("Enabled", new() { Timeout = 30_000 });
            await Expect(eastRow).ToContainTextAsync(MergeModeLabel);
            Coverage.Hit("use-case:replication/enrol");
            await east.SubstepAsync(recorder, "3", "east-enabled", "A7.3 - factory-orders enabled in east, a snapshot bootstrap from west requested");
            step.Complete();
        }

        // ----- 4 to 7 compare while the line feed is paused -----
        await estate.LineFeed.PauseAsync();
        try
        {
            // ----- 4. The link catches up; Clear filters -----
            using (var step = Coverage.BeginScope("A7.4"))
            {
                await westTrees.ExpectLoadedAsync();
                await westEstate.EstateSection.ClickAsync();
                await west.Shell.ExpectHeadingAsync("Replication");
                await westEstate.FilterAsync(westEstate.RegionFilter, JourneyEstate.East, "region", JourneyEstate.East);
                var link = westEstate.LinkRows(orders, westCompact).Filter(new() { HasText = westCompact ? "To " + JourneyEstate.East : "Outbound" });
                await WaitUntilAsync(async () => await link.CountAsync() > 0, westEstate.RefreshButton, Convergence, "factory-orders' outbound link to east is listed");
                await west.SubstepAsync(recorder, "4", "link", "A7.4 - factory-orders' outbound link to east, filtered to east");

                await WaitUntilAsync(
                    async () => westCompact
                        ? (await link.First.InnerTextAsync()).Contains("Caught up", StringComparison.Ordinal)
                        : await ReplicationTables.CellAsync(link.First, "Entries behind") == "0",
                    westEstate.RefreshButton,
                    Convergence,
                    "factory-orders' outbound link to east falls to 0 entries behind");
                await west.SubstepAsync(recorder, "4", "caught-up", "A7.4 - Entries behind has fallen to 0: east has caught up");

                await westEstate.ClearFilters.ClickAsync();
                await Expect(west.Page).Not.ToHaveURLAsync(new Regex("[?&](health|region|app)="));
                await Expect(westEstate.RegionFilter.Locator("option:checked")).ToHaveTextAsync("Any region");
                await Expect(westEstate.ClearFilters).ToHaveCountAsync(0);
                await west.SubstepAsync(recorder, "4", "filters-cleared", "A7.4 - Clear filters resets the toolbar and the address");
                step.Complete();
            }

            // ----- 5. The tree's page refreshes only while it is visible -----
            using (var step = Coverage.BeginScope("A7.5"))
            {
                var tree = new ReplicationTreeScreen(west.Shell);
                var rowHeader = westEstate.LinkRows(orders, westCompact).First.GetByRole(AriaRole.Link, new() { Name = orders, Exact = true });
                await rowHeader.ClickAsync();
                await tree.ExpectLoadedAsync(orders);
                await Expect(tree.Lede).ToContainTextAsync($"Replicated - {MergeModeLabel} - Runtime enrolment");
                await tree.ExpectRefreshingAsync();
                await tree.ExpectReadAgainAsync(TimeSpan.FromSeconds(20));
                Coverage.Hit("route:/replication/trees/{p1}/{p2?}/{p3?}/{p4?}/{p5?}/{p6?}");
                await west.SubstepAsync(recorder, "5", "visible", "A7.5 - factory-orders' links, refreshing on a cadence while the page is visible");

                await tree.HideAsync();

                // A read already in flight when the page hid may still land; let it.
                await Task.Delay(TimeSpan.FromSeconds(2));
                var hiddenAt = await tree.ReadAtAsync();
                await Task.Delay(TimeSpan.FromSeconds(12));
                Assert.That(await tree.ReadAtAsync(), Is.EqualTo(hiddenAt), "the page did not re-read its links while hidden");

                // Regression (#5379): the status line itself says the refresh is paused.
                await tree.ExpectPausedAsync();
                await west.SubstepAsync(recorder, "5", "hidden", "A7.5 - Hidden: the cadence pauses, and the read time stands still");

                await tree.ShowAsync();
                await tree.ExpectRefreshingAsync();
                await Expect(tree.StatusLine).Not.ToContainTextAsync("Read at " + hiddenAt, new() { Timeout = 20_000 });
                Coverage.Hit("use-case:replication/visibility-paused-refresh");
                await west.SubstepAsync(recorder, "5", "resumed", "A7.5 - Shown again: the page re-reads at once and the cadence resumes");
                step.Complete();
            }

            // ----- 6. The Cluster overview's region diagram -----
            using (var step = Coverage.BeginScope("A7.6"))
            {
                var regions = new ClusterRegionsScreen(west.Shell);
                await regions.OpenAsync();
                await Expect(regions.Diagram).ToContainTextAsync(JourneyEstate.West);
                await Expect(regions.Diagram).ToContainTextAsync("this region");
                await Expect(regions.Peers.Filter(new() { HasText = JourneyEstate.East })).ToHaveCountAsync(1);
                Coverage.Hit("use-case:cluster/region-diagram");
                await west.SubstepAsync(recorder, "6", "region-diagram", "A7.6 - The Cluster overview draws west and its peer east");
                step.Complete();
            }

            // ----- 7. The east console holds what west holds -----
            using (var step = Coverage.BeginScope("A7.7"))
            {
                var eastData = new DataTreeScreen(east.Shell);
                var westData = new DataTreeScreen(west.Shell);
                await eastData.OpenDirectoryAsync();
                await Expect(eastData.TreeLink(orders)).ToBeVisibleAsync(new() { Timeout = 60_000 });
                await east.SubstepAsync(recorder, "7", "east-data", "A7.7 - The east console's Data lists factory-orders");
                await eastData.OpenTreeAsync(orders);
                await westData.OpenDirectoryAsync();
                await westData.OpenTreeAsync(orders);

                long westKeys = 0, eastKeys = -1;
                var deadline = DateTime.UtcNow + Convergence;
                while (true)
                {
                    westKeys = await westData.LiveKeyCountAsync();
                    eastKeys = await eastData.LiveKeyCountAsync();
                    if (westKeys == eastKeys || DateTime.UtcNow > deadline)
                    {
                        break;
                    }

                    await Task.Delay(TimeSpan.FromSeconds(5));
                    await westData.ReloadAsync();
                    await eastData.ReloadAsync();
                }

                Assert.That(eastKeys, Is.EqualTo(westKeys), "east's factory-orders holds as many live keys as west's");
                Assert.That(eastKeys, Is.GreaterThanOrEqualTo(Names.Orders.SeededCount), "east holds at least the 240 seeded orders");
                await west.SubstepAsync(recorder, "7", "west-live-keys", "A7.7 - West's factory-orders and its live-key count");
                Coverage.Hit("use-case:replication/east-console");
                await east.SubstepAsync(recorder, "7", "east-live-keys", "A7.7 - East's factory-orders holds the same live-key count as west");
                step.Complete();
            }
        }
        finally
        {
            await estate.LineFeed.ResumeAsync();
        }
    }

    /// <summary>The task board's replicated tree, as A5 leaves it (<c>a/{slug}/tasks</c>).</summary>
    private static string A5TaskBoardTree => A5Contract.TaskBoardTree;

    /// <summary>How long a receiver's bootstrapped enable may take before the journey fails it (#5560 measured about 77 s).</summary>
    private static readonly TimeSpan EnableBootstrapBound = TimeSpan.FromSeconds(180);

    /// <summary>
    /// Opens olivia's session on the east console - <c>/console</c> on the east gateway, where its
    /// two Explorer replicas sit - and signs her in through its own sign-in form.
    /// </summary>
    private static async Task<PersonaSession> OpenEastConsoleAsync(JourneyEstate estate)
    {
        var session = await PersonaSession.OpenAsync(
            estate.EastRegion.ConsoleUri,
            Personas.Olivia,
            JourneyEstate.East,
            new PersonaSessionOptions(),
            estate.HostFaults,
            estate.ArtifactsDirectory);
        try
        {
            await session.SignInAsync();
            Assert.That(session.Page.Url, Does.StartWith(estate.EastRegion.ConsoleUri.ToString()), "the console stays under /console after sign-in");
            return session;
        }
        catch
        {
            await session.DisposeAsync();
            throw;
        }
    }

    /// <summary>Waits until <paramref name="condition"/> holds, pressing <paramref name="refresh"/> between reads.</summary>
    private static async Task WaitUntilAsync(Func<Task<bool>> condition, ILocator refresh, TimeSpan timeout, string what)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!await condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                Assert.Fail($"Within {timeout.TotalMinutes:0} minutes: {what}.");
            }

            await Task.Delay(TimeSpan.FromSeconds(3));
            await refresh.ClickAsync();
            await Expect(refresh).ToHaveTextAsync("Refresh", new() { Timeout = 30_000 });
        }
    }
}
