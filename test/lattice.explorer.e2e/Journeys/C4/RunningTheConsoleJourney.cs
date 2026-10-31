using System.Reflection;
using System.Runtime.CompilerServices;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.FeatureCoverage;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;
using static Microsoft.Playwright.Assertions;
using Hosting = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso.Hosting;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C4;

/// <summary>
/// Journey C4 (#5392), running the console: Contoso's platform team operates the console
/// itself. East's console runs under <c>/console</c> on two replicas that share a Data
/// Protection key ring in Blob storage, as their production ingress does. Olivia proves a
/// session survives a replica switch and a restart of both replicas, that the kiosk head seeded
/// from its environment opens signed in, and that the console sends the security headers they
/// require. Closes the Contoso story; read-only (its contract asserts the estate C3 left is
/// unchanged).
/// </summary>
/// <remarks>
/// Off-UI actions, each performed by the harness as the external actor it models: the gateway
/// operator draining a replica (<see cref="JourneyEstate.ForceConsoleSwitchAsync"/>), the
/// platform team restarting a replica (<see cref="JourneyEstate.RestartExplorerAsync"/>), and,
/// for the response headers, the browser's own view of each response.
/// </remarks>
[JourneyChapter("C4", After = new[] { "C3" })]
public sealed class RunningTheConsoleJourney : JourneyChapter
{
    private const string JourneyId = "c4";

    private static readonly TimeSpan ReplicaPatience = TimeSpan.FromMinutes(2);

    private static JourneyRecorder Recorder => JourneyRecorder.For(JourneyId);

    /// <inheritdoc />
    public override async Task RunAsync(ChapterContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var estate = context.Estate;
        var east = estate.EastRegion;
        Assert.That(east.ConsoleUri.AbsolutePath, Is.EqualTo(Hosting.ConsoleBasePath + "/"), "the east console is mounted under its base path");
        Assert.That(estate.KioskName, Is.EqualTo(Hosting.Kiosk), "the kiosk head is Contoso's");

        await using var console = await ConsoleHostPage.OpenAsync(estate, Personas.Olivia, east.ConsoleUri, JourneyEstate.East);
        await TheEastConsoleUnderItsBasePathAsync(estate, console);
        await ASwitchToTheOtherReplicaKeepsTheSessionAsync(estate, console);
        await RestartingBothReplicasKeepsTheSessionAsync(estate, console);

        await using var kiosk = await ConsoleHostPage.OpenAsync(estate, Personas.Olivia, estate.KioskUri, JourneyEstate.West);
        await TheKioskOpensSignedInAsync(estate, kiosk);
        await TheConsoleSendsItsSecurityHeadersAsync(console, kiosk);
    }

    [JourneyStep("c4-1", "The east console under /console: every area, static asset and app frame route resolves, and no link escapes the base path")]
    private static async Task TheEastConsoleUnderItsBasePathAsync(JourneyEstate estate, ConsoleHostPage console)
    {
        var step = Step();
        using var scope = Coverage.BeginScope(ScopeOf(step));
        var shell = console.Shell;

        await console.ExpectUnderBasePathAsync();
        await shell.OpenSignInAsync();
        await console.ExpectNothingEscapesBasePathAsync("the sign-in dialog");
        await console.Session.SubstepAsync(Recorder, step.Id, "sign-in-dialog",
            "The east console, served under /console by the gateway, offers its own sign-in form, whose post stays under the base path");

        await shell.SignInAsync(Personas.Olivia.Id, Personas.Olivia.Password);
        await console.ExpectUnderBasePathAsync();
        await shell.ExpectHeadingAsync("Estate");
        await shell.ExpectConnectionAsync("Connected");
        var ports = estate.Regions.Values.SelectMany(region => region.Silos).SelectMany(silo => new[] { silo.HttpPort, silo.GrpcPort, silo.SiloPort });
        Assert.That(ports, Does.Not.Contain(console.BaseUri.Port), "the console is its own head, in front of the silos, never one of them");
        await console.ExpectNothingEscapesBasePathAsync("the estate home");
        Coverage.Hit("use-case:shell/hosted-head");
        Coverage.Hit("use-case:shell/isolated-head");
        await console.Session.SubstepAsync(Recorder, step.Id, "estate-home",
            "Signed in, olivia's console reaches the estate through the gateway, and the home page's links and files stay under /console");

        var spine = await shell.ReadSpineAsync();
        var areas = spine.Open.Where(area => area != "home").ToList();
        Assert.That(areas, Does.Contain("data"), $"the console offers olivia its areas ({spine})");
        foreach (var area in areas)
        {
            await shell.GoToAreaAsync(area);
            await console.ExpectUnderBasePathAsync();
            var url = console.Page.Url;
            var heading = (await shell.Heading.TextContentAsync())?.Trim() ?? string.Empty;
            Assert.That(heading, Is.Not.Empty, $"the {area} area draws its heading");

            // The deep link resolves on the server too, not only inside the live circuit.
            var response = await console.ReloadAsync();
            Assert.That(response.Status, Is.EqualTo(200), $"{url} answers when loaded directly");
            await Expect(console.Page).ToHaveURLAsync(url);
            await shell.ExpectHeadingAsync(heading);
            await console.ExpectNothingEscapesBasePathAsync($"the {area} area");
            await console.Session.SubstepAsync(Recorder, step.Id, $"area-{area}",
                $"The {heading} area resolves under /console, loaded directly, and nothing on it leaves the base path");
        }

        var script = await console.RequestAsync("_framework/blazor.web.js");
        Assert.That(script.Status, Is.EqualTo(200), "the circuit script resolves under /console");
        Assert.That((await script.BodyAsync()).Length, Is.GreaterThan(0), "the circuit script was published");

        var frame = await console.RequestAsync("_apps/frame/v1/frame.html");
        Assert.That(frame.Status, Is.EqualTo(200), "the app frame bootstrap resolves under /console");
        Assert.That(frame.Headers.GetValueOrDefault("content-type"), Does.StartWith("text/html"), "the app frame bootstrap is a document");
        Assert.That(await frame.TextAsync(), Does.Contain("data-appkit-protocol=\"1\""), "the app frame bootstrap is AppKit's protocol v1 document");
        var boot = await console.RequestAsync("_apps/frame/v1/boot.js");
        Assert.That(boot.Status, Is.EqualTo(200), "the frame bootstrap's script resolves beside it");

        await shell.GoToAreaAsync("home");
        await shell.ExpectHeadingAsync("Estate");
        Coverage.Hit("option:BasePath");
        Coverage.Hit("use-case:shell/base-path");
        Coverage.Hit("use-case:shell/app-frame-route");
        await console.Session.SubstepAsync(Recorder, step.Id, "app-frame-route",
            "Back home: the circuit script and the app frame bootstrap (/console/_apps/frame/v1/frame.html) both resolve under the base path");
        scope.Complete();
    }

    [JourneyStep("c4-2", "The gateway is forced to switch olivia to the other replica: the session holds, because the Data Protection keys are shared")]
    private static async Task ASwitchToTheOtherReplicaKeepsTheSessionAsync(JourneyEstate estate, ConsoleHostPage console)
    {
        var step = Step();
        using var scope = Coverage.BeginScope(ScopeOf(step));
        var shell = console.Shell;

        var first = await ConsoleHostPage.ReplicaOfAsync(await console.ReloadAsync());
        var again = await ConsoleHostPage.ReplicaOfAsync(await console.ReloadAsync());
        Assert.That(again, Is.EqualTo(first), "the gateway's affinity keeps a browser on the replica that served it");
        await shell.ExpectSignedInAsync(Personas.Olivia.Id);
        await shell.ExpectHeadingAsync("Estate");
        Coverage.Hit("use-case:shell/session-affinity");
        await console.Session.SubstepAsync(Recorder, step.Id, "served-by-one-replica",
            $"Reloaded twice, olivia's console is served by the same replica (east-explorer-{first}) each time: the gateway holds her to it");

        try
        {
            await estate.ForceConsoleSwitchAsync(first);
            var second = await ConsoleHostPage.ReplicaOfAsync(await console.ReloadAsync());
            Assert.That(second, Is.Not.EqualTo(first), "the gateway switched olivia to the other replica");
            await console.ExpectUnderBasePathAsync();
            await shell.ExpectSignedInAsync(Personas.Olivia.Id);
            await shell.ExpectHeadingAsync("Estate");
            await console.Session.SubstepAsync(Recorder, step.Id, "switched-replica",
                $"With east-explorer-{first} drained, east-explorer-{second} serves her next page, and she is still signed in: it reads her cookie with the shared keys");

            await shell.GoToAreaAsync("data");
            await console.ExpectUnderBasePathAsync();
            await shell.ExpectHeadingAsync("Data");
            await shell.ExpectConnectionAsync("Connected");
            Coverage.Hit("use-case:shell/shared-key-ring");
            Coverage.Hit("option:ConfigureDataProtection");
            Coverage.Hit("option:DataProtectionApplicationName");
            await console.Session.SubstepAsync(Recorder, step.Id, "data-on-other-replica",
                "On the other replica her rights are intact: the Data area opens and the console stays connected");
        }
        finally
        {
            await estate.ForceConsoleSwitchAsync(from: -1);
        }

        await shell.GoToAreaAsync("home");
        scope.Complete();
    }

    [JourneyStep("c4-3", "Restart both east replicas, one after the other: the session survives")]
    private static async Task RestartingBothReplicasKeepsTheSessionAsync(JourneyEstate estate, ConsoleHostPage console)
    {
        var step = Step();
        using var scope = Coverage.BeginScope(ScopeOf(step));
        var shell = console.Shell;
        Assert.That(estate.EastRegion.ExplorerUris, Has.Count.EqualTo(2), "the east console runs on two replicas");

        var serving = await ConsoleHostPage.ReplicaOfAsync(await console.ReloadAsync());
        var other = 1 - serving;
        await shell.ExpectSignedInAsync(Personas.Olivia.Id);
        await console.Session.SubstepAsync(Recorder, step.Id, "before-restarts",
            $"Before the platform team restarts anything: olivia signed in, served by east-explorer-{serving}");

        // The platform team restarts the replica serving her while her console is closed, and
        // she opens it again: the restarted process reads her cookie with the ring in Blob.
        await console.LeaveAsync();
        await estate.RestartExplorerAsync(JourneyEstate.East, serving);
        await WaitForEveryReplicaAsync(estate);
        var afterFirst = await ConsoleHostPage.ReplicaOfAsync(await console.ReopenAsync());
        Assert.That(afterFirst, Is.EqualTo(serving), "the gateway returns olivia to the replica that served her, now restarted");
        await console.ExpectUnderBasePathAsync();
        await shell.ExpectSignedInAsync(Personas.Olivia.Id);
        await shell.ExpectHeadingAsync("Estate");
        await console.Session.SubstepAsync(Recorder, step.Id, "after-first-restart",
            $"east-explorer-{serving} restarted: olivia opens the console again and the new process serves her, still signed in");

        await console.LeaveAsync();
        await estate.RestartExplorerAsync(JourneyEstate.East, other);
        await WaitForEveryReplicaAsync(estate);
        try
        {
            await estate.ForceConsoleSwitchAsync(serving);
            var afterSecond = await ConsoleHostPage.ReplicaOfAsync(await console.ReopenAsync());
            Assert.That(afterSecond, Is.EqualTo(other), "the gateway switched olivia to the other restarted replica");
            await console.ExpectUnderBasePathAsync();
            await shell.ExpectSignedInAsync(Personas.Olivia.Id);
            await shell.ExpectHeadingAsync("Estate");
            Coverage.Hit("use-case:shell/shared-key-ring");
            await console.Session.SubstepAsync(Recorder, step.Id, "after-second-restart",
                $"east-explorer-{other} restarted too, and serves her signed in: no process that saw her sign in is left, and her session holds");
        }
        finally
        {
            await estate.ForceConsoleSwitchAsync(from: -1);
        }

        scope.Complete();
    }

    [JourneyStep("c4-4", "The kiosk head (contoso-kiosk, AllowEnvironmentCredentialSeed on) opens signed in as its seeded identity, without the sign-in dialog")]
    private static async Task TheKioskOpensSignedInAsync(JourneyEstate estate, ConsoleHostPage kiosk)
    {
        var step = Step();
        using var scope = Coverage.BeginScope(ScopeOf(step));
        var shell = kiosk.Shell;

        await Expect(shell.SignInDialog).ToHaveCountAsync(0);
        await shell.ExpectSignedInAsync(Personas.Olivia.Id);
        await shell.ExpectHeadingAsync("Estate");
        await shell.ExpectConnectionAsync("Connected");

        // The head's endpoint came from its environment (LATTICE_EXPLORER_ENDPOINT): the west gateway.
        var west = estate.WestRegion;
        var seeded = new List<string> { $"https://127.0.0.1:{west.GatewayAddress.Port}" };
        if (west.GatewayPlaintextPort is { } plaintext)
        {
            seeded.Add($"http://127.0.0.1:{plaintext}");
        }

        Assert.That(await kiosk.ReadEndpointAsync(), Is.AnyOf(seeded.ToArray()), "the kiosk's endpoint is the one its environment seeded");
        Coverage.Hit("option:UseEnvironmentBootstrap");
        Coverage.Hit("use-case:shell/configuration");
        await kiosk.Session.SubstepAsync(Recorder, step.Id, "kiosk-signed-in",
            "contoso-kiosk opens straight onto the estate, connected to the endpoint its environment names, with no sign-in dialog");

        var session = await shell.OpenIdentityMenuAsync();
        await Expect(session).ToContainTextAsync(Personas.Olivia.Id);
        Coverage.Hit("option:AllowEnvironmentCredentialSeed");
        Coverage.Hit("use-case:shell/kiosk-credential-seed");
        await kiosk.Session.SubstepAsync(Recorder, step.Id, "kiosk-session",
            "Its session names olivia, the identity its environment credential seeded");
        if (await shell.IsCompactAsync())
        {
            await shell.CloseMenuAsync();
        }
        else
        {
            await kiosk.Page.Keyboard.PressAsync("Escape");
            await Expect(session).ToBeHiddenAsync();
        }

        scope.Complete();
    }

    [JourneyStep("c4-5", "The response headers on the shell and the frame bootstrap match the documented security response headers")]
    private static async Task TheConsoleSendsItsSecurityHeadersAsync(ConsoleHostPage console, ConsoleHostPage kiosk)
    {
        var step = Step();
        using var scope = Coverage.BeginScope(ScopeOf(step));

        // Under a subpath (east, through its gateway) and at the root mount (the kiosk head).
        foreach (var head in new[] { console, kiosk })
        {
            var where = head.BasePath == "/" ? "the kiosk head (root mount)" : $"the east console ({head.BasePath})";
            var shell = await head.ReloadAsync();
            await head.Shell.ExpectHeadingAsync("Estate");
            SecurityResponseHeaders.ExpectShell(await shell.AllHeadersAsync(), $"{where}: the shell");

            var asset = await head.RequestAsync("_framework/blazor.web.js");
            Assert.That(asset.Status, Is.EqualTo(200));
            SecurityResponseHeaders.ExpectShell(asset.Headers, $"{where}: a static asset");

            var frame = await head.RequestAsync("_apps/frame/v1/frame.html");
            Assert.That(frame.Status, Is.EqualTo(200));
            SecurityResponseHeaders.ExpectFrameBootstrap(frame.Headers, $"{where}: the app frame bootstrap");

            var boot = await head.RequestAsync("_apps/frame/v1/boot.js");
            Assert.That(boot.Status, Is.EqualTo(200));
            SecurityResponseHeaders.ExpectFrameFile(boot.Headers, $"{where}: the frame bootstrap's script");
        }

        Coverage.Hit("use-case:shell/security-headers");
        await console.Session.SubstepAsync(Recorder, step.Id, "east-console-headers",
            "The east console's shell refuses framing (CSP frame-ancestors 'none', X-Frame-Options DENY); only its frame bootstrap may be framed, by the console itself");
        await kiosk.Session.SubstepAsync(Recorder, step.Id, "kiosk-headers",
            "The kiosk head, mounted at the root, sends the same headers: the shell refuses framing and only the frame bootstrap allows its own console");
        scope.Complete();
    }

    private static async Task WaitForEveryReplicaAsync(JourneyEstate estate)
    {
        var deadline = DateTime.UtcNow + ReplicaPatience;
        IReadOnlyList<GatewayDestination> replicas = [];
        while (DateTime.UtcNow < deadline)
        {
            replicas = await estate.ConsoleDestinationsAsync();
            if (replicas.Count == estate.EastRegion.ExplorerUris.Count && replicas.All(replica => replica.Healthy))
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(500));
        }

        Assert.Fail($"The east gateway did not find every console replica healthy within {ReplicaPatience.TotalMinutes:0} minutes: "
            + string.Join(", ", replicas.Select(replica => $"{replica.Id}={replica.Health}")));
    }

    private static JourneyStepAttribute Step([CallerMemberName] string method = "") =>
        typeof(RunningTheConsoleJourney).GetMethod(method, BindingFlags.NonPublic | BindingFlags.Static)?.GetCustomAttribute<JourneyStepAttribute>()
            ?? throw new InvalidOperationException($"{method} carries no [JourneyStep].");

    // Step c4-3 is scoped C4.3, as the issue numbers it.
    private static string ScopeOf(JourneyStepAttribute step) => "C4." + step.Id[(step.Id.LastIndexOf('-') + 1)..];
}
