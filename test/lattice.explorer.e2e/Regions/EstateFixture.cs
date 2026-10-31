using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Browser;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Regions;

/// <summary>
/// One estate booted once per fixture, from <see cref="Options"/>, with a browser context per
/// test whose trace and every process log are kept beside the estate's artifacts when the test
/// fails, and its screenshots written in order under <c>screenshots/</c>.
/// </summary>
public abstract class EstateFixture
{
    private readonly List<IBrowserContext> _contexts = [];
    private JourneyEstate? _estate;

    /// <summary>The fixture's estate.</summary>
    protected JourneyEstate Estate => _estate ?? throw new InvalidOperationException("The estate has not started.");

    /// <summary>The estate this fixture boots.</summary>
    protected abstract JourneyEstateOptions Options { get; }

    /// <summary>Boots the estate.</summary>
    [OneTimeSetUp]
    public async Task StartEstateAsync() => _estate = await JourneyEstate.StartAsync(Options);

    /// <summary>Stops every process and removes the estate's storage.</summary>
    [OneTimeTearDown]
    public async Task StopEstateAsync()
    {
        if (_estate is not null)
        {
            await _estate.DisposeAsync();
            _estate = null;
        }
    }

    /// <summary>Keeps the traces of a failed test, and every process log, beside the estate's artifacts.</summary>
    [TearDown]
    public async Task CloseContextsAsync()
    {
        var failed = TestContext.CurrentContext.Result.Outcome.Status == NUnit.Framework.Interfaces.TestStatus.Failed;
        for (var i = 0; i < _contexts.Count; i++)
        {
            await _contexts[i].Tracing.StopAsync(new TracingStopOptions
            {
                Path = failed && _estate is not null
                    ? Path.Combine(_estate.ArtifactsDirectory, $"{TestContext.CurrentContext.Test.Name}-{i}.trace.zip")
                    : null,
            });
            await _contexts[i].DisposeAsync();
        }

        _contexts.Clear();
        if (failed && _estate is not null)
        {
            foreach (var log in Directory.EnumerateFiles(_estate.ArtifactsDirectory, "*.log"))
            {
                TestContext.AddTestAttachment(log);
            }
        }
    }

    /// <summary>A fresh browser context on <paramref name="console"/>, closed after the test.</summary>
    /// <param name="console">An Explorer head's or console's base address.</param>
    protected async Task<IPage> OpenAsync(Uri console)
    {
        var context = await JourneyBrowser.NewContextAsync(console);
        _contexts.Add(context);
        return await context.NewPageAsync();
    }

    /// <summary>Writes a screenshot of this fixture named <paramref name="name"/>.</summary>
    /// <param name="page">The page.</param>
    /// <param name="name">A file name such as <c>01-east-console.png</c>.</param>
    protected Task ScreenshotAsync(IPage page, string name) =>
        JourneyBrowser.ScreenshotAsync(page, Path.Combine(Estate.ArtifactsDirectory, "screenshots", name));
}
