using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

/// <summary>
/// One persona's browser session on an Explorer head (issue #5360): its own Playwright
/// browser context - cookies, storage, circuit and trace - so several personas can be signed
/// in at once, each seeing what their own rights allow.
/// </summary>
/// <remarks>
/// <para>
/// Open one with <c>estate.SignInAsAsync(persona)</c> (signed in through the Explorer's own
/// sign-in form) or <c>estate.OpenSessionAsync(persona)</c> (signed out). The context runs
/// at the journey's fixed viewport, in the Paper colour scheme, with reduced motion, in UTC,
/// so its screenshots are deterministic.
/// </para>
/// <para>
/// <see cref="SubstepAsync(string, string, SubstepOptions?)"/> is the checkpoint: a screenshot
/// filed in the journey's manifest, then axe-core, the browser console and the estate's
/// host-fault logs. The session's trace is kept beside the estate's logs when the test fails
/// (or always, when <c>LATTICE_E2E_TRACES=always</c>).
/// </para>
/// </remarks>
public sealed class PersonaSession : IAsyncDisposable
{
    /// <summary>Set to <c>always</c> to keep every session's trace, not only a failed test's.</summary>
    public const string TracesVariable = "LATTICE_E2E_TRACES";

    private static int _sequence;

    private readonly HostFaultLog? _hostFaults;
    private readonly string _artifacts;
    private bool _disposed;

    private PersonaSession(
        Persona persona,
        string region,
        JourneyViewport viewport,
        IBrowserContext context,
        IPage page,
        HostFaultLog? hostFaults,
        string artifacts)
    {
        Persona = persona;
        Region = region;
        Viewport = viewport;
        Context = context;
        Page = page;
        Shell = new ShellPage(page);
        Console = new BrowserConsoleLog();
        Console.Attach(page);
        _hostFaults = hostFaults;
        _artifacts = artifacts;
    }

    /// <summary>The persona.</summary>
    public Persona Persona { get; }

    /// <summary>The region whose Explorer head the session uses.</summary>
    public string Region { get; }

    /// <summary>The viewport the session runs at.</summary>
    public JourneyViewport Viewport { get; }

    /// <summary>The session's own browser context.</summary>
    public IBrowserContext Context { get; }

    /// <summary>The session's page.</summary>
    public IPage Page { get; }

    /// <summary>The Explorer's chrome on the session's page.</summary>
    public ShellPage Shell { get; }

    /// <summary>What the session's page has said since its last checkpoint.</summary>
    public BrowserConsoleLog Console { get; }

    /// <summary>
    /// Captures and checks a checkpoint of the running test's <see cref="JourneyStepAttribute"/>
    /// step, filed in the fixture's journey (<see cref="JourneyRecorder.ForCurrentTest"/>).
    /// </summary>
    /// <param name="substep">The substep id, lower-case kebab-case, unique within the step.</param>
    /// <param name="title">The caption: what this state shows.</param>
    /// <param name="options">Further masks, or <see langword="null"/>.</param>
    /// <exception cref="InvalidOperationException">The running test carries no <see cref="JourneyStepAttribute"/>.</exception>
    /// <exception cref="JourneyCheckpointException">A check failed; the screenshot was still recorded.</exception>
    public Task<JourneyManifestEntry> SubstepAsync(string substep, string title, SubstepOptions? options = null)
    {
        var step = JourneyStepAttribute.Current() ?? throw new InvalidOperationException(
            $"{TestContext.CurrentContext.Test.Name} names no step: give the test method [JourneyStep(\"<id>\", \"<title>\")].");
        return SubstepAsync(JourneyRecorder.ForCurrentTest(), step.Id, substep, title, options);
    }

    /// <summary>Captures and checks a checkpoint filed in <paramref name="recorder"/>'s journey under <paramref name="step"/>.</summary>
    /// <param name="recorder">The journey's recorder.</param>
    /// <param name="step">The step id.</param>
    /// <param name="substep">The substep id.</param>
    /// <param name="title">The caption.</param>
    /// <param name="options">Further masks, or <see langword="null"/>.</param>
    /// <exception cref="JourneyCheckpointException">A check failed; the screenshot was still recorded.</exception>
    public Task<JourneyManifestEntry> SubstepAsync(JourneyRecorder recorder, string step, string substep, string title, SubstepOptions? options = null)
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        return JourneyCheckpoint.CaptureAsync(Page, recorder, step, substep, title, Persona.Id, Console, _hostFaults, options);
    }

    /// <summary>Tolerates, from now on, every browser error matching <paramref name="pattern"/>, such as a reconnect a fault provokes.</summary>
    /// <param name="pattern">The pattern.</param>
    public void TolerateConsole(Regex pattern) => Console.Tolerate(pattern);

    /// <summary>Signs the persona in through the Explorer's own sign-in form, with their own password.</summary>
    public Task SignInAsync() => Shell.SignInAsync(Persona.Id, Persona.Password);

    /// <summary>Stops the trace and closes the context. The trace is kept only if the test fails (or always, under <c>LATTICE_E2E_TRACES=always</c>).</summary>
    public async ValueTask DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;
        var always = string.Equals(Environment.GetEnvironmentVariable(TracesVariable), "always", StringComparison.OrdinalIgnoreCase);
        try
        {
            var traces = System.IO.Path.Combine(_artifacts, "traces");
            Directory.CreateDirectory(traces);
            var path = System.IO.Path.Combine(traces, $"{Persona.Id}-{Region}-{Interlocked.Increment(ref _sequence):D2}.trace.zip");
            await Context.Tracing.StopAsync(new TracingStopOptions { Path = path });

            // Disposed inside its test, before NUnit knows the outcome: the journey's
            // [Journey] attribute discards this trace once the test has passed.
            if (!always && TestContext.CurrentContext.Test.ID is { Length: > 0 } test)
            {
                JourneyTraces.Register(test, path);
            }
        }
        catch (PlaywrightException)
        {
            // A context whose browser is gone has no trace left to stop.
        }

        await Context.DisposeAsync();
    }

    /// <summary>Opens a session for <paramref name="persona"/> on <paramref name="explorer"/>, signed out, at <see cref="PersonaSessionOptions.Path"/>.</summary>
    /// <param name="explorer">The Explorer head's base address.</param>
    /// <param name="persona">The persona.</param>
    /// <param name="region">The head's region.</param>
    /// <param name="options">How the session is opened.</param>
    /// <param name="hostFaults">The estate's host-fault log, or <see langword="null"/>.</param>
    /// <param name="artifacts">Where a kept trace is written.</param>
    internal static async Task<PersonaSession> OpenAsync(
        Uri explorer,
        Persona persona,
        string region,
        PersonaSessionOptions options,
        HostFaultLog? hostFaults,
        string artifacts)
    {
        var viewport = options.Viewport ?? JourneyViewport.Run;
        var browser = await JourneyBrowser.ChromiumAsync();
        var context = await browser.NewContextAsync(new BrowserNewContextOptions
        {
            BaseURL = explorer.ToString(),
            IgnoreHTTPSErrors = true,
            ViewportSize = new ViewportSize { Width = viewport.Width, Height = viewport.Height },
            DeviceScaleFactor = 1,
            ColorScheme = ColorScheme.Light,
            ReducedMotion = ReducedMotion.Reduce,
            ForcedColors = ForcedColors.None,
            Locale = "en-US",
            TimezoneId = "UTC",
        });
        try
        {
            context.SetDefaultNavigationTimeout(90_000);
            context.SetDefaultTimeout(30_000);
            await context.AddInitScriptAsync(JourneyBrowser.ReadyScript);
            await context.Tracing.StartAsync(new TracingStartOptions
            {
                Title = $"{persona.Id} on {region}",
                Screenshots = true,
                Snapshots = true,
            });
            var page = await context.NewPageAsync();
            var session = new PersonaSession(persona, region, viewport, context, page, hostFaults, artifacts);
            await JourneyBrowser.GotoAsync(page, options.Path);
            return session;
        }
        catch
        {
            await context.DisposeAsync();
            throw;
        }
    }
}
