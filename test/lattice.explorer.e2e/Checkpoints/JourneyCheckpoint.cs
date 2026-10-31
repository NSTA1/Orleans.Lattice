using System.Security.Cryptography;
using System.Text;
using Deque.AxeCore.Playwright;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.UiTests.Accessibility;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// One checkpoint of a journey: at each distinct UI state, a deterministic screenshot filed
/// in the journey's manifest, then the checks - axe-core conformance, the browser console,
/// and the estate's host-fault logs - any failure of which fails the journey.
/// </summary>
/// <remarks>
/// The screenshot is taken first, so a failed checkpoint still shows the state that failed.
/// It is full-page at the journey's fixed viewport, after fonts have loaded and motion has
/// settled, with animations disabled, the caret hidden and every volatile region masked
/// (<see cref="JourneyMasks"/>).
/// </remarks>
public static class JourneyCheckpoint
{
    /// <summary>Captures and checks one checkpoint.</summary>
    /// <param name="page">The page in the state to capture.</param>
    /// <param name="recorder">The journey's recorder.</param>
    /// <param name="step">The step id.</param>
    /// <param name="substep">The substep id, lower-case kebab-case.</param>
    /// <param name="title">The caption: what the screenshot shows.</param>
    /// <param name="persona">The persona whose session it is, or <see langword="null"/>.</param>
    /// <param name="console">What the page has said since the last checkpoint.</param>
    /// <param name="hostFaults">The estate's host-fault log, or <see langword="null"/> when no estate is involved.</param>
    /// <param name="options">Further masks, or <see langword="null"/>.</param>
    /// <returns>The manifest entry recorded.</returns>
    /// <exception cref="JourneyCheckpointException">A check failed; the screenshot was still recorded.</exception>
    public static async Task<JourneyManifestEntry> CaptureAsync(
        IPage page,
        JourneyRecorder recorder,
        string step,
        string substep,
        string title,
        string? persona,
        BrowserConsoleLog console,
        HostFaultLog? hostFaults,
        SubstepOptions? options = null)
    {
        ArgumentNullException.ThrowIfNull(page);
        ArgumentNullException.ThrowIfNull(recorder);
        ArgumentNullException.ThrowIfNull(console);
        JourneyNames.RequireSlug(step, nameof(step));
        JourneyNames.RequireSlug(substep, nameof(substep));
        ArgumentException.ThrowIfNullOrWhiteSpace(title);
        options ??= new SubstepOptions();

        var problems = new List<string>();
        await page.EvaluateAsync("() => document.fonts.ready.then(() => true)");
        var moving = await AxeConformance.WaitForMotionToSettleAsync(page);
        if (moving > 0)
        {
            problems.Add($"{moving} animation(s) were still running ten seconds after the page was asked to settle.");
        }

        var (index, file, path) = recorder.Reserve(step, substep);
        var png = await page.ScreenshotAsync(new PageScreenshotOptions
        {
            Path = path,
            FullPage = options.FullPage,
            Animations = ScreenshotAnimations.Disabled,
            Caret = ScreenshotCaret.Hide,
            Scale = ScreenshotScale.Css,
            Mask = [.. JourneyMasks.Default(page), .. options.Mask],
            MaskColor = JourneyMasks.Colour,
        });
        var sha256 = Convert.ToHexString(SHA256.HashData(png)).ToLowerInvariant();

        var surface = $"{recorder.JourneyId} {step}/{substep} ({title})";
        var axe = await page.RunAxe(AxeConformance.RunOptions);
        var missing = AxeConformance.MissingTags(axe);
        if (missing.Count > 0)
        {
            problems.Add(AxeConformance.DescribeVacuity(missing, surface));
        }

        var blocking = AxeConformance.Blocking(axe);
        if (blocking.Count > 0)
        {
            problems.Add(AxeConformance.Describe(blocking, surface));
        }

        var (consoleFailures, consoleNotes) = console.Drain();
        if (consoleFailures.Count > 0)
        {
            problems.Add("The browser reported:" + Environment.NewLine + string.Join(Environment.NewLine, consoleFailures.Select(line => "  " + line)));
        }

        var notes = new List<string>(consoleNotes);
        if (hostFaults is not null)
        {
            var (hostFailures, hostNotes) = hostFaults.Drain();
            if (hostFailures.Count > 0)
            {
                problems.Add("The estate's hosts logged:" + Environment.NewLine + string.Join(Environment.NewLine, hostFailures.Select(line => "  " + line)));
            }

            notes.AddRange(hostNotes);
        }

        var entry = new JourneyManifestEntry(index, step, substep, title, persona, file, sha256, Passed: problems.Count == 0);
        recorder.Record(entry);

        if (notes.Count > 0)
        {
            TestContext.Out.WriteLine($"Checkpoint {file} noted:{Environment.NewLine}{string.Join(Environment.NewLine, notes.Select(line => "  " + line))}");
        }

        if (problems.Count > 0)
        {
            var report = new StringBuilder($"Checkpoint {file} ({title}) failed:");
            foreach (var problem in problems)
            {
                report.AppendLine().Append("- ").Append(problem);
            }

            throw new JourneyCheckpointException(report.ToString());
        }

        return entry;
    }
}
