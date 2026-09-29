using System.Text.RegularExpressions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests.Apps;

/// <summary>
/// F5's AppKit bootstrap fixture page (<c>test/lattice.explorer/AppKit/Fixtures/boot-fixture.html</c>)
/// run for real: the page plays the Explorer host against the real <c>frame.html</c> and
/// <c>boot.js</c> the Explorer serves, under the real frame policy, in Chromium, Firefox
/// and WebKit, and reports whether the bootstrap loaded the bundle in order and failed
/// every malformed bundle with the protocol's code.
/// </summary>
/// <remarks>
/// The page decides each outcome on a protocol message, never a timer, and marks
/// <c>&lt;html&gt;</c> with <c>data-state="done"</c> and <c>data-pass</c> when it has one. It
/// is served from the Explorer's origin because the bootstrap document may only be framed
/// by a page of the same origin.
/// </remarks>
[TestFixture(UiBrowsers.Chromium)]
[TestFixture(UiBrowsers.Firefox)]
[TestFixture(UiBrowsers.WebKit)]
[Category("UI")]
public sealed class AppKitBootFixtureTests(string engine) : UiTestBase(engine)
{
    /// <summary>The scenarios the fixture page declares.</summary>
    public static IEnumerable<string> Scenarios() =>
        Regex.Matches(
                File.ReadAllText(RepositoryPaths.Resolve("test/lattice.explorer/AppKit/Fixtures/boot-fixture.js")),
                @"^\s*""(?<name>[a-z-]+)"": \{ outcome:",
                RegexOptions.Multiline)
            .Select(match => match.Groups["name"].Value);

    [TestCaseSource(nameof(Scenarios))]
    public async Task The_bootstrap_passes_the_fixture_scenario(string scenario)
    {
        var hostile = await UiHosts.HostileAsync();
        var page = await NewPageAsync(hostile.Head);
        await page.GotoAsync(hostile.Head.Url($"{HostileAppHead.BootFixturePath}boot-fixture.html?scenario={scenario}"));

        var root = page.Locator("html");
        await Expect(root).ToHaveAttributeAsync("data-scenarios", new Regex("(^| )" + Regex.Escape(scenario) + "( |$)"));
        await Expect(root).ToHaveAttributeAsync("data-state", "done");

        var result = await root.GetAttributeAsync("data-result");
        Assert.That(await root.GetAttributeAsync("data-pass"), Is.EqualTo("true"), $"The '{scenario}' scenario failed: {result}");
    }
}
