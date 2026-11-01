using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using Orleans.Lattice.Explorer.UiTests.Accessibility;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// The checkpoint's axe timing (#5565), in a real browser with no estate: a run past its bound
/// fails naming the slowest rules, and a slow run's rules are timed one at a time. The
/// second test also measures, rule by rule, the page shape journey U6 stalled on (a long
/// mono pattern that wraps anywhere, beside a gallery of radio cards), and logs it to the
/// test's output for the CI log and the TRX.
/// </summary>
[TestFixture]
[Category("EndToEnd")]
public sealed class AxeTimingSelfTests
{
    private const string EmailPattern = @"^[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}\z";

    private IBrowserContext _context = null!;
    private IPage _page = null!;

    /// <summary>A fresh context and page at the run's viewport.</summary>
    [SetUp]
    public async Task OpenPageAsync()
    {
        var browser = await JourneyBrowser.ChromiumAsync();
        _context = await browser.NewContextAsync(new BrowserNewContextOptions
        {
            ViewportSize = new ViewportSize { Width = JourneyViewport.Run.Width, Height = JourneyViewport.Run.Height },
            DeviceScaleFactor = 1,
            ColorScheme = ColorScheme.Light,
            ReducedMotion = ReducedMotion.Reduce,
        });
        _page = await _context.NewPageAsync();
    }

    /// <summary>Closes the context.</summary>
    [TearDown]
    public async Task ClosePageAsync() => await _context.DisposeAsync();

    /// <summary>A run that outlasts its bound fails the checkpoint, naming axe, the bound and the slowest rules.</summary>
    [Test]
    public async Task A_run_past_its_bound_fails_naming_its_slowest_rules()
    {
        await _page.SetContentAsync(Page("A page", string.Empty));

        var run = await AxeTiming.RunAsync(_page, "a self-test page", AxeConformance.RunOptions, timeout: TimeSpan.Zero, slowThreshold: TimeSpan.Zero);

        Assert.Multiple(() =>
        {
            Assert.That(run.WithinBound, Is.False);
            Assert.That(run.Result, Is.Not.Null, "the run drained after the bound, so its result names the rules to time");
            Assert.That(run.Rules, Is.Not.Empty);
            Assert.That(run.Rules.Select(rule => rule.RuleId), Does.Contain("color-contrast"));
            Assert.That(run.Rules.Where(rule => rule.Error is not null), Is.Empty, "every rule ran alone without error");
            Assert.That(run.Rules.Select(rule => rule.Duration), Is.Ordered.Descending);
            Assert.That(run.Failure, Does.Contain("axe did not finish within 0.0 s on a self-test page"));
            Assert.That(run.Failure, Does.Contain(run.Rules[0].RuleId), "the failure names the slowest rule");
        });
    }

    /// <summary>
    /// A run under the bound passes, and with a zero threshold its rules are timed. Measured on
    /// a baseline page and on the U6 shapes, so the log shows what each adds.
    /// </summary>
    /// <param name="shape">The page shape.</param>
    [TestCase("baseline")]
    [TestCase("pattern")]
    [TestCase("pattern-x20")]
    [TestCase("pattern-no-wrap-anywhere")]
    public async Task A_slow_run_has_its_rules_timed_one_at_a_time(string shape)
    {
        var extra = shape switch
        {
            "baseline" => string.Empty,
            "pattern" => Readout(wrapAnywhere: true),
            "pattern-x20" => string.Concat(Enumerable.Repeat(Readout(wrapAnywhere: true), 20)),
            "pattern-no-wrap-anywhere" => Readout(wrapAnywhere: false),
            _ => throw new ArgumentOutOfRangeException(nameof(shape)),
        };
        await _page.SetContentAsync(Page($"Shape {shape}", extra));

        var run = await AxeTiming.RunAsync(_page, $"the {shape} page", AxeConformance.RunOptions, slowThreshold: TimeSpan.Zero);
        TestContext.Out.WriteLine($"{shape}: whole run {run.Elapsed.TotalSeconds:0.000} s; rules alone total {run.Rules.Sum(rule => rule.Duration.TotalSeconds):0.000} s.");
        TestContext.Out.WriteLine(AxeTiming.DescribeRules(run.Rules, $"the {shape} page"));

        Assert.Multiple(() =>
        {
            Assert.That(run.Result, Is.Not.Null);
            Assert.That(run.Rules, Is.Not.Empty, "a run at or over the threshold has its rules timed");
            Assert.That(run.Rules.Where(rule => rule.Error is not null), Is.Empty);
        });
    }

    private static string Readout(bool wrapAnywhere) =>
        $"""
        <div class="regex">
          <p class="regex-label">The same check as a pattern</p>
          <code class="pattern" style="overflow-wrap: {(wrapAnywhere ? "anywhere" : "normal")}">{System.Net.WebUtility.HtmlEncode(EmailPattern)}</code>
          <button type="button">Edit as regex</button>
        </div>
        """;

    private static string Page(string heading, string extra)
    {
        var cards = string.Concat(Enumerable.Range(1, 12).Select(index =>
            $"""
            <label class="option" for="kind-{index}">
              <input class="radio" type="radio" name="kind" id="kind-{index}" value="{index}"{(index == 6 ? " checked" : string.Empty)}>
              <span class="title">Card {index}</span>
              <span class="meaning">What card {index} checks, in a sentence or two.</span>
              <span class="example">Every value seen is an email address.</span>
            </label>
            """));
        return $$"""
            <!doctype html>
            <html lang="en">
            <head><title>Axe timing self-test</title>
            <style>
              body { font-family: sans-serif; margin: 24px; max-width: 760px; }
              .gallery { display: grid; grid-template-columns: repeat(auto-fill, minmax(13rem, 1fr)); gap: 8px; }
              .option { display: grid; grid-template-columns: auto minmax(0, 1fr); gap: 4px 8px; padding: 8px 12px; border: 1px solid #555; border-radius: 6px; }
              .option:has(.radio:checked) { border-color: #000; box-shadow: inset 0 0 0 1px #000; }
              .radio { grid-row: 1 / span 3; width: 1.5rem; height: 1.5rem; margin: 0; }
              .regex { display: grid; gap: 8px; justify-items: start; padding: 12px; border: 1px solid #777; background: #f4f4f4; }
              .pattern { max-width: 100%; font-family: monospace; font-size: 0.875rem; }
              button { min-width: 44px; min-height: 44px; }
            </style></head>
            <body>
              <main>
                <h1>{{heading}}</h1>
                <fieldset><legend>What it must be</legend><div class="gallery">{{cards}}</div></fieldset>
                {{extra}}
              </main>
            </body>
            </html>
            """;
    }
}
