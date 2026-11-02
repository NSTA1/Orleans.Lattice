using System.Text.RegularExpressions;
using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;

/// <summary>The directory spine: its stops, which are shown, and going to an area.</summary>
public sealed partial class ShellPage
{
    /// <summary>The spine stop for <paramref name="area"/> (<c>home</c>, <c>data</c>, <c>access</c>, ...).</summary>
    /// <param name="area">The area key.</param>
    public ILocator Stop(string area) => Spine.Locator($"a[data-lt-command=\"go.{area}\"]");

    /// <summary>
    /// Waits until the spine has loaded, then reads which areas it offers: every stop, and
    /// the ones the caller can open (a stop the caller may not open is drawn with its reason).
    /// When compact, the directory sheet is opened to read it and closed again.
    /// </summary>
    /// <returns>The area keys, in directory order, <c>home</c> first.</returns>
    public async Task<SpineReading> ReadSpineAsync()
    {
        var compact = await IsCompactAsync();
        if (compact)
        {
            await OpenDirectoryAsync();
        }

        await Expect(Spine.Locator("[aria-busy=true]")).ToHaveCountAsync(0);
        await Expect(Stop("home")).ToBeVisibleAsync();
        var stops = await Spine.Locator("a[data-lt-command^=\"go.\"]").EvaluateAllAsync<StopState[]>(
            """
            links => links.map(link => ({
              area: link.getAttribute('data-lt-command').substring(3),
              open: !link.classList.contains('lt-shell-directory__link--unavailable'),
            }))
            """);

        if (compact)
        {
            await CloseDirectoryAsync();
        }

        return new SpineReading(
            [.. stops.Select(stop => stop.Area)],
            [.. stops.Where(stop => stop.Open).Select(stop => stop.Area)]);
    }

    /// <summary>Goes to <paramref name="area"/> from its spine stop, through the directory sheet when compact.</summary>
    /// <param name="area">The area key.</param>
    public async Task GoToAreaAsync(string area)
    {
        if (await IsCompactAsync())
        {
            await OpenDirectoryAsync();
            await Stop(area).ClickAsync();
            await Expect(DirectorySheet).ToBeHiddenAsync();
        }
        else
        {
            await Stop(area).ClickAsync();
        }

        if (area != "home")
        {
            await Expect(Page).ToHaveURLAsync(new Regex($"/{Regex.Escape(area)}(/|\\?|$)"));
        }

        await Expect(Heading).ToBeVisibleAsync();
    }

    private sealed class StopState
    {
        public string Area { get; set; } = string.Empty;

        public bool Open { get; set; }
    }
}
