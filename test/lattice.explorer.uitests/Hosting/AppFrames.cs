using Microsoft.Playwright;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// How the suite reaches into an app's frame: finding it, waiting for it to load, and
/// running code in it the way a user at the browser console would.
/// </summary>
internal static class AppFrames
{
    /// <summary>The frame host's section on an app's Open tab.</summary>
    public static ILocator Host(IPage page) => page.Locator("section.appframe");

    /// <summary>The app's sandboxed frame element.</summary>
    public static ILocator Element(IPage page) => Host(page).Locator("iframe");

    /// <summary>The app frame's content, for locators.</summary>
    public static IFrameLocator Content(IPage page) => Element(page).ContentFrame;

    /// <summary>The frame host's failure, when it shows one.</summary>
    public static ILocator Failure(IPage page) => Host(page).GetByRole(AriaRole.Alert);

    /// <summary>
    /// Opens <paramref name="slug"/>'s Open tab and waits until its frame document is up.
    /// A role grant reaches the workspace a moment after an install is enabled, so a page
    /// that still says the app is not open yet is loaded again, a bounded number of times.
    /// </summary>
    /// <param name="page">A signed-in page.</param>
    /// <param name="head">The head.</param>
    /// <param name="slug">The app slug.</param>
    public static async Task OpenAsync(IPage page, ExplorerHead head, string slug)
    {
        for (var attempt = 0; ; attempt++)
        {
            await Shell.GotoAsync(page, head, $"/apps/{slug}/open");
            var opened = Element(page);
            var closed = Shell.Content(page).GetByRole(AriaRole.Heading, new() { NameRegex = new System.Text.RegularExpressions.Regex("is not open to you yet") });
            await Expect(opened.Or(closed).Or(Failure(page))).ToBeVisibleAsync();
            if (await opened.IsVisibleAsync() || attempt >= 10)
            {
                break;
            }
        }

        await Expect(Element(page)).ToBeVisibleAsync();
    }

    /// <summary>The frame document of the app on <paramref name="page"/>, once it has loaded.</summary>
    /// <param name="page">A page showing an app's Open tab.</param>
    public static async Task<IFrame> DocumentAsync(IPage page)
    {
        var bootstrap = new System.Text.RegularExpressions.Regex("_apps/frame/");
        await Expect(Element(page)).ToHaveAttributeAsync("src", bootstrap);
        var frame = await (await Element(page).ElementHandleAsync()).ContentFrameAsync()
            ?? throw new InvalidOperationException("The app frame element has no content frame.");
        await frame.WaitForURLAsync(bootstrap);
        await frame.WaitForLoadStateAsync(LoadState.Load);
        return frame;
    }
    /// <summary>
    /// Runs a bridge request from inside the app's frame, as a user typing into the
    /// browser console on the frame would, and returns <c>allowed</c> or the error code.
    /// </summary>
    /// <param name="frame">The app's frame document.</param>
    /// <param name="operation">The bridge operation.</param>
    /// <param name="argumentsJson">The request's arguments, as JSON.</param>
    public static Task<string> RequestAsync(IFrame frame, string operation, string argumentsJson) =>
        frame.EvaluateAsync<string>(
            "([op, args]) => lattice.ready.then(() => lattice.request(op, JSON.parse(args))).then(() => 'allowed', e => (e && e.code) || String(e))",
            new object[] { operation, argumentsJson });
}
