using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Shell;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.A1;

/// <summary>
/// The header's connection indicator as A1 reads it (#5371): the connection state and the
/// endpoint it names, in the header when expanded and in the <b>Menu</b> sheet's
/// <b>Connection</b> section when compact.
/// </summary>
/// <param name="shell">The shell the indicator belongs to.</param>
public sealed class ConnectionIndicatorPage(ShellPage shell)
{
    /// <summary>The shell.</summary>
    public ShellPage Shell { get; } = shell ?? throw new ArgumentNullException(nameof(shell));

    /// <summary>
    /// Asserts the indicator reads <paramref name="state"/> and names <paramref name="endpointAuthority"/>,
    /// the <c>host:port</c> of the endpoint the head is configured with.
    /// </summary>
    /// <param name="state">The connection state in words, such as <c>Connected</c>.</param>
    /// <param name="endpointAuthority">The endpoint's <c>host:port</c>.</param>
    public async Task ExpectAsync(string state, string endpointAuthority)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(state);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointAuthority);
        await Shell.ExpectConnectionAsync(state);
        if (await Shell.IsCompactAsync())
        {
            await Shell.OpenMenuAsync();
            await Expect(ConnectionSection).ToContainTextAsync(endpointAuthority);
            await Shell.CloseMenuAsync();
            return;
        }

        await Expect(Shell.Banner.Locator("code").Filter(new() { HasText = endpointAuthority })).ToHaveCountAsync(1);
    }

    /// <summary>
    /// Opens the read-only <b>Cluster connection</b> dialog from the indicator - the endpoint
    /// button in the header (named <c>Connection details: ...</c>), or <b>Connection details</b>
    /// in the compact <b>Menu</b> sheet - and returns it.
    /// </summary>
    public async Task<ILocator> OpenConnectionDialogAsync()
    {
        if (await Shell.IsCompactAsync())
        {
            await Shell.OpenMenuAsync();
            await ConnectionSection.GetByRole(AriaRole.Button, new() { Name = "Connection details", Exact = true }).ClickAsync();
        }
        else
        {
            await Shell.Banner.Locator("button[data-lt-connection-details]").ClickAsync();
        }

        var dialog = Shell.Page.GetByRole(AriaRole.Dialog, new() { Name = "Cluster connection" });
        await Expect(dialog).ToBeVisibleAsync();
        return dialog;
    }

    /// <summary>Asserts the read-only dialog names <paramref name="endpointAuthority"/> and offers no way to change it, then closes it.</summary>
    /// <param name="dialog">The open dialog.</param>
    /// <param name="endpointAuthority">The endpoint's <c>host:port</c>.</param>
    public async Task ExpectReadOnlyAndCloseAsync(ILocator dialog, string endpointAuthority)
    {
        ArgumentNullException.ThrowIfNull(dialog);
        ArgumentException.ThrowIfNullOrWhiteSpace(endpointAuthority);
        await Expect(dialog).ToContainTextAsync(endpointAuthority);
        await Expect(dialog).ToContainTextAsync("cannot be changed from the browser");
        await Expect(dialog.GetByRole(AriaRole.Textbox)).ToHaveCountAsync(0);
        await Shell.Page.Keyboard.PressAsync("Escape");
        await Expect(dialog).ToBeHiddenAsync();
        if (await Shell.IsCompactAsync() && await Shell.MenuSheet.IsVisibleAsync())
        {
            await Shell.CloseMenuAsync();
        }
    }

    private ILocator ConnectionSection => Shell.MenuSheet.GetByRole(AriaRole.Region, new() { Name = "Connection" });
}
