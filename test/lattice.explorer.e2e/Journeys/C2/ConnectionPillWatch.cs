using Microsoft.Playwright;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>
/// Records every reading of the header's connection indicator in the page itself, from the
/// moment it is installed, so a reconnect too brief for any screenshot to catch is still
/// evidence: the history holds each distinct text the indicator showed, in order.
/// </summary>
/// <remarks>
/// The compact chrome folds the indicator into the <b>Menu</b> sheet, which is not drawn
/// while closed, so there the watch has nothing to read and says so (<see cref="IsWatching"/>).
/// </remarks>
internal sealed class ConnectionPillWatch
{
    private const string Install =
        """
        () => {
          const read = () => {
            const pill = document.querySelector('header [role=status], [role=banner] [role=status]');
            return pill ? pill.textContent.replace(/\s+/g, ' ').trim() : null;
          };
          if (window.__c2PillObserver) { window.__c2PillObserver.disconnect(); }
          const first = read();
          window.__c2Pill = first === null ? [] : [first];
          const observer = new MutationObserver(() => {
            const text = read();
            const history = window.__c2Pill;
            if (text !== null && history[history.length - 1] !== text) { history.push(text); }
          });
          observer.observe(document.body, { subtree: true, childList: true, characterData: true });
          window.__c2PillObserver = observer;
          return first !== null;
        }
        """;

    private readonly IPage _page;

    private ConnectionPillWatch(IPage page, bool watching)
    {
        _page = page;
        IsWatching = watching;
    }

    /// <summary>Whether the header draws the indicator, so its readings are recorded.</summary>
    public bool IsWatching { get; }

    /// <summary>Starts recording the indicator on <paramref name="page"/>'s current document.</summary>
    /// <param name="page">The page.</param>
    public static async Task<ConnectionPillWatch> StartAsync(IPage page)
    {
        ArgumentNullException.ThrowIfNull(page);
        return new ConnectionPillWatch(page, await page.EvaluateAsync<bool>(Install));
    }

    /// <summary>Every distinct reading since the watch started, in order.</summary>
    public async Task<IReadOnlyList<string>> HistoryAsync() =>
        await _page.EvaluateAsync<string[]>("() => window.__c2Pill ?? []");
}
