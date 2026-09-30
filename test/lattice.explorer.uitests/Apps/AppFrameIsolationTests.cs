using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Samples.Explorer.TaskBoard;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Apps;

/// <summary>
/// The epic's named security test (E4 and E5): an app's UI runs in a sandboxed frame
/// that can reach nothing of the Explorer's but the bridge, and the bridge refuses
/// everything the app was not granted and everything the user may not do - proved in
/// Chromium, Firefox and WebKit, against actively hostile bundles.
/// </summary>
/// <remarks>
/// <para>
/// Every bundle in X1's <c>Fixtures/HostileBundles</c> runs, plus this suite's own in
/// <c>Apps/Bundles</c>, through the hostile-app head (<see cref="HostileAppHead"/>): the
/// real frame host and broker, with the app's workspace and the cluster behind the bridge
/// played by the harness. Between them they attempt <c>document.cookie</c>,
/// <c>localStorage</c> and the other storages, <c>parent.document</c>, <c>fetch</c>, XHR and
/// WebSocket, a popup, <c>top.location</c>, navigating the frame out of the bootstrap path
/// or to another origin with data in the URL, and reloading it inside that path, forged handshake messages to the parent, a physical
/// and an undeclared tree, an operation outside the vocabulary, a flood, an entry fragment
/// carrying a script, and a tampered asset. Each attempt must be denied.
/// </para>
/// <para>
/// Two attempts need the real cluster, so they run against the test world with the pilot
/// app: reading a tree outside the manifest that the signed-in user <em>can</em> read, and a
/// write by a viewer who holds broad rights of their own.
/// </para>
/// <para>
/// No case can pass vacuously. A case whose bundle must run waits for the bundle's own
/// report, which only its script can send, and fails loudly naming the bundle if the
/// report never comes.
/// </para>
/// </remarks>
[TestFixture(UiBrowsers.Chromium)]
[TestFixture(UiBrowsers.Firefox)]
[TestFixture(UiBrowsers.WebKit)]
[Category("UI")]
public sealed class AppFrameIsolationTests(string engine) : UiTestBase(engine)
{
    private static readonly TimeSpan EscapeWait = TimeSpan.FromSeconds(30);

    /// <summary>Every hostile bundle's name.</summary>
    public static IEnumerable<string> HostileBundles() =>
        HostileAppHead.HostileBundleFolders().Select(Path.GetFileName).OfType<string>();

    [TestCaseSource(nameof(HostileBundles))]
    public async Task A_hostile_bundle_is_contained(string name)
    {
        // Hostile bundles provoke policy violations on purpose; the suite's no-violation
        // guard is for the Explorer's own pages.
        ExpectCspViolations();
        var hostile = await UiHosts.HostileAsync();
        var bundle = hostile.Bundles[name];
        hostile.Bridge.Clear();
        while (hostile.Escapes.Wait(0))
        {
        }

        var page = await OpenAsync(hostile.Head, $"/apps/{name}/open", WorldIdentities.Admin);
        await Expect(AppFrames.Host(page)).ToBeVisibleAsync();
        var explorerUrl = page.Url;

        if (bundle.Expect.TryGetProperty("crossOriginNavigationBlocked", out _))
        {
            if (Engine == UiBrowsers.WebKit)
            {
                // Documented limitation (#4020): some WebKit builds do not check the embedder's
                // frame-src against a navigation the frame starts itself (a Windows build lets it
                // out; the Linux CI build refuses it), so the request may be sent, carrying at most
                // what the app's consented bridge let it read. The behaviour is platform-dependent,
                // so it is recorded rather than asserted either way.
                var leaked = hostile.Escapes.WaitAsync(EscapeWait);
                var blocked = page.EvaluateAsync<string>("() => window.__ltFrameBlocked");
                var first = await Task.WhenAny(leaked, blocked, Task.Delay(EscapeWait));
                var outcome = first == leaked && await leaked ? "leaked (the request reached the other origin)"
                    : first == blocked && blocked.IsCompletedSuccessfully ? $"blocked ({blocked.Result})"
                    : "not observed within the wait";
                TestContext.Out.WriteLine($"WebKit self-navigation to another origin: {outcome}.");
                Assert.That(page.Url, Is.EqualTo(explorerUrl), "The app navigated the Explorer page.");

                // The pending evaluation may fault when the context closes; observe it so it is never unobserved.
                _ = blocked.ContinueWith(static task => task.Exception, TaskScheduler.Default);
                return;
            }

            // Self-navigation egress (#4020): the Explorer's own frame-src refused the frame's
            // navigation to another origin before any request was sent, and reported it on the
            // Explorer's document. Were it not refused, no violation would come and the request
            // would reach the escape path under the other origin.
            var directive = await page.EvaluateAsync<string>("() => window.__ltFrameBlocked").WaitAsync(EscapeWait);
            Assert.Multiple(() =>
            {
                Assert.That(directive, Is.EqualTo("frame-src"));
                Assert.That(hostile.Escapes.Wait(0), Is.False, $"The {name} bundle's frame reached another origin.");
                Assert.That(page.Url, Is.EqualTo(explorerUrl), "The app navigated the Explorer page.");
            });
            return;
        }

        if (bundle.ExpectedFailure is { } failure)
        {
            if (bundle.Expect.TryGetProperty("escapeRequested", out _))
            {
                // The attempt was made: the frame really did ask for the page outside the
                // bootstrap path.
                Assert.That(await hostile.Escapes.WaitAsync(EscapeWait), Is.True,
                    $"The {name} bundle never ran: its frame never asked for {HostileAppHead.EscapePath}.");
            }

            await Expect(AppFrames.Failure(page)).ToContainTextAsync(FailureTitle(failure));

            if (failure is "BundleInvalid" or "DigestMismatch")
            {
                // Refused before a byte reached a frame: there is no frame at all.
                await Expect(AppFrames.Element(page)).ToHaveCountAsync(0);
            }

            if (bundle.Expect.TryGetProperty("escapeRendered", out _))
            {
                // Blocked: the page it navigated to never ran.
                var messages = await page.EvaluateAsync<string[]>("() => window.__ltMessages");
                Assert.That(messages, Does.Not.Contain("escaped"),
                    "An Explorer page outside the bootstrap path rendered inside the app's frame.");
            }
        }
        else
        {
            var report = await WaitForReportAsync(page, bundle);

            if (bundle.ExpectedToast is { } toast)
            {
                AssertReport(report, toast);
            }

            if (bundle.Expect.TryGetProperty("rateLimitedAtLeast", out var atLeast))
            {
                var limited = int.Parse(Regex.Match(report, "limited=(\\d+)").Groups[1].Value, System.Globalization.CultureInfo.InvariantCulture);
                Assert.That(limited, Is.GreaterThanOrEqualTo(atLeast.GetInt32()), $"Only {limited} of the flood's requests were rate limited.");
            }

            if (bundle.Expect.TryGetProperty("explorerUrlUnchanged", out _))
            {
                Assert.That(page.Url, Is.EqualTo(explorerUrl), "The app navigated the Explorer page.");
            }

            if (bundle.Expect.TryGetProperty("secondHelloSent", out _))
            {
                var frame = await AppFrames.DocumentAsync(page);
                Assert.That(await frame.EvaluateAsync<int>("() => window.__ltHellos"), Is.EqualTo(1),
                    "Forged lattice.ready messages won the frame a second lattice.hello.");
            }

            if (name == "physical-tree")
            {
                Assert.That(hostile.Bridge.Calls, Is.Empty, "A refused tree reached the cluster behind the bridge.");
            }
        }
    }

    [Test]
    public async Task A_read_of_a_tree_outside_the_manifest_is_denied_although_the_user_may_read_it()
    {
        var world = await UiHosts.WorldAsync();
        await world.InstallTaskBoardAsync();

        // The premise: alice may read the demo tree herself.
        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.DemoTree}", WorldIdentities.Alice);
        await Expect(Shell.Content(page)).ToContainTextAsync("machine-000");

        await AppFrames.OpenAsync(page, world.Head, TaskBoardApp.Slug);
        var frame = await AppFrames.DocumentAsync(page);

        var outcome = await AppFrames.RequestAsync(frame, "data.read", $"{{\"action\":\"get\",\"tree\":\"{ExplorerWorld.DemoTree}\",\"key\":\"machine-000\"}}");
        Assert.That(outcome, Is.EqualTo("denied"), "The app read a tree its manifest does not declare.");

        // And the app's own tree still answers, so the refusal is about the tree, not a broken frame.
        var own = await AppFrames.RequestAsync(frame, "data.read", "{\"action\":\"scan\",\"tree\":\"tasks\",\"prefix\":\"tasks/\"}");
        Assert.That(own, Is.EqualTo("allowed"));
    }

    [Test]
    public async Task A_write_by_a_viewer_who_holds_broad_rights_is_denied()
    {
        var world = await UiHosts.WorldAsync();
        await world.InstallTaskBoardAsync();

        // The premise: bob holds broad rights of his own - the Data area lists the demo tree
        // to him although no group grants it.
        var page = await OpenAsync(world.Head, $"/data/{ExplorerWorld.DemoTree}", WorldIdentities.Bob);
        await Expect(Shell.Content(page)).ToContainTextAsync("machine-000");

        await AppFrames.OpenAsync(page, world.Head, TaskBoardApp.Slug);
        var frame = await AppFrames.DocumentAsync(page);

        var write = await AppFrames.RequestAsync(frame, "data.write", "{\"action\":\"set\",\"tree\":\"tasks\",\"key\":\"tasks/forced\",\"value\":\"e30=\"}");
        var delete = await AppFrames.RequestAsync(frame, "data.delete", "{\"action\":\"delete\",\"tree\":\"tasks\",\"key\":\"tasks/forced\"}");
        Assert.Multiple(() =>
        {
            Assert.That(write, Is.EqualTo("denied"), "A viewer wrote through the app because of rights that are not the app's.");
            Assert.That(delete, Is.EqualTo("denied"), "A viewer deleted through the app because of rights that are not the app's.");
        });

        var read = await AppFrames.RequestAsync(frame, "data.read", "{\"action\":\"scan\",\"tree\":\"tasks\",\"prefix\":\"tasks/\"}");
        Assert.That(read, Is.EqualTo("allowed"), "The viewer's own role still reads the board.");
    }

    /// <summary>
    /// An app role is held by binding, never by capability (#3902): a viewer who holds
    /// broad rights of their own is told they hold <c>viewer</c> and nothing else, so the
    /// task board, which shows its write controls only to an <c>editor</c>, shows them none.
    /// </summary>
    [Test]
    public async Task A_viewer_who_holds_broad_rights_is_told_only_viewer_and_shown_no_write_controls()
    {
        var world = await UiHosts.WorldAsync();
        await world.InstallTaskBoardAsync();

        var page = await NewPageAsync(world.Head, WorldIdentities.Bob);
        await AppFrames.OpenAsync(page, world.Head, TaskBoardApp.Slug);
        var frame = await AppFrames.DocumentAsync(page);

        var roles = await frame.EvaluateAsync<string[]>(
            "() => lattice.ready.then(() => lattice.request('context.read', {})).then(context => context.roles)");
        Assert.That(roles, Is.EqualTo(new[] { "viewer" }), "The frame was told a role the viewer is not bound to.");

        var board = AppFrames.Content(page);
        await Expect(board.Locator("#tb-read-only")).ToBeVisibleAsync();
        await Expect(board.Locator("#tb-add")).ToBeHiddenAsync();
        await Expect(board.Locator("#tb-add-title")).ToBeHiddenAsync();
        await Expect(board.Locator("#tb-add-button")).ToBeHiddenAsync();
        await Expect(board.Locator("#tb-detail-actions")).ToBeHiddenAsync();
    }

    /// <summary>
    /// Compares a bundle's report with the expected one, attempt by attempt. Where the
    /// fixture expects <c>denied</c>, the AppKit inside the frame may already have refused
    /// the request as <c>invalid</c> - a tree name that is not a manifest name, an operation
    /// outside the vocabulary - before it reached the host; the request never left the frame,
    /// which is a refusal too. Every other outcome must match exactly.
    /// </summary>
    private static void AssertReport(string report, string expected)
    {
        var actual = report.Split(' ');
        var wanted = expected.Split(' ');
        Assert.That(actual, Has.Length.EqualTo(wanted.Length), $"The report '{report}' does not cover the attempts '{expected}'.");
        for (var i = 0; i < wanted.Length; i++)
        {
            if (wanted[i].EndsWith("=denied", StringComparison.Ordinal))
            {
                var attempt = wanted[i][..wanted[i].IndexOf('=', StringComparison.Ordinal)];
                Assert.That(actual[i], Is.EqualTo(attempt + "=denied").Or.EqualTo(attempt + "=invalid"),
                    $"The attempt '{attempt}' was not refused: {report}");
            }
            else
            {
                Assert.That(actual[i], Is.EqualTo(wanted[i]), $"The report was '{report}'.");
            }
        }
    }
    private static async Task<string> WaitForReportAsync(IPage page, HostileBundle bundle)
    {
        var prefix = bundle.DisplayName + ": ";
        var report = Shell.ToastMessages(page).Filter(new() { HasTextRegex = new Regex("^" + Regex.Escape(prefix)) });
        try
        {
            await Expect(report).ToHaveCountAsync(1);
        }
        catch (PlaywrightException ex)
        {
            Assert.Fail(
                $"The hostile bundle '{bundle.Name}' never reported through the bridge, so it never ran and this case proved "
                + "nothing. A frame that does not load cannot demonstrate isolation. " + ex.Message);
        }

        return (await report.InnerTextAsync()).Trim();
    }

    private static string FailureTitle(string failure) => failure switch
    {
        "NoUi" => "This app has no interface",
        "DigestMismatch" or "BundleDigestMismatch" => "The app's interface failed verification",
        "BundleInvalid" => "The app's interface could not be loaded",
        "HandshakeTimeout" => "The app did not start",
        "ProtocolUnsupported" => "This app needs a newer Explorer",
        "FrameFailed" => "The app failed to start",
        "Reloaded" => "The app was closed",
        "Revoked" => "This app has changed",
        "Unavailable" => "The app could not be opened",
        _ => throw new ArgumentOutOfRangeException(nameof(failure), failure, "Not a failure the frame host shows."),
    };
}
