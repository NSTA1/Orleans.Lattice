using System.Text.RegularExpressions;
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
    private static readonly Regex ConnectionControl = new("connection", RegexOptions.IgnoreCase);

    /// <summary>The shell.</summary>
    public ShellPage Shell { get; } = shell ?? throw new ArgumentNullException(nameof(shell));

    /// <summary>
    /// Asserts the indicator reads <paramref name="state"/> and names <paramref name="endpointAuthority"/>,
    /// the <c>host:port</c> of the endpoint the head is configured with.
    /// </summary>
    /// <param name="state">The connection state in words, such as <c>Sign-in required</c>.</param>
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
    /// Asserts the indicator offers no control that opens a connection dialog: no
    /// <b>Connection settings</b> (right on a head that refuses browser configuration) and
    /// nothing in its place that opens the read-only <b>Cluster connection</b> dialog.
    /// </summary>
    public async Task ExpectNoConnectionDialogAffordanceAsync()
    {
        if (await Shell.IsCompactAsync())
        {
            await Shell.OpenMenuAsync();
            await Expect(ConnectionSection.GetByRole(AriaRole.Button, new() { NameRegex = ConnectionControl })).ToHaveCountAsync(0);
            await Shell.CloseMenuAsync();
            return;
        }

        await Expect(Shell.Banner.GetByRole(AriaRole.Button, new() { NameRegex = ConnectionControl })).ToHaveCountAsync(0);
    }

    private ILocator ConnectionSection => Shell.MenuSheet.GetByRole(AriaRole.Region, new() { Name = "Connection" });
}
