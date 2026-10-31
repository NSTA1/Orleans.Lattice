using System.Globalization;
using System.Text.RegularExpressions;
using Microsoft.Playwright;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;
using static Microsoft.Playwright.Assertions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.C2;

/// <summary>
/// One replicated tree's page in the Replication area, as C2 reads it: the tree's link to a
/// peer, which the page refreshes on its own every few seconds while it is visible.
/// </summary>
/// <param name="session">The persona's session.</param>
internal sealed partial class ReplicationTreeView(PersonaSession session)
{
    private const string ReadLink =
        """
        ([peer, direction]) => {
          const table = document.querySelector('main#lt-shell-content table');
          if (table) {
            const headers = [...table.querySelectorAll('thead th')].map(th => th.textContent.replace(/\s+/g, ' ').trim());
            for (const row of table.querySelectorAll('tbody tr')) {
              const cells = [...row.querySelectorAll('th, td')].map(cell => cell.textContent.replace(/\s+/g, ' ').trim());
              const at = name => cells[headers.findIndex(header => header.startsWith(name))] ?? '';
              if (at('Peer') === peer && at('Direction') === direction) {
                return { health: at('Health'), behind: at('Entries behind'), stall: at('Stall reason'), errors: at('Errors'), text: cells.join(' | ') };
              }
            }
            return null;
          }
          const lead = direction === 'Outbound' ? 'To ' : 'From ';
          for (const row of document.querySelectorAll('main#lt-shell-content li, main#lt-shell-content [role=row]')) {
            const text = row.textContent.replace(/\s+/g, ' ').trim();
            if (text.includes(lead + peer + ' - ')) {
              const health = (text.match(/\b(Healthy|Lagging|Stalled|Unknown)\b/) ?? [''])[0];
              const behind = text.includes('Caught up') ? '0' : ((text.match(/([\d,]+) entr/) ?? ['', ''])[1]);
              return { health, behind, stall: '', errors: '', text };
            }
          }
          return null;
        }
        """;

    private ILocator Content => session.Shell.Content;

    /// <summary>Opens <paramref name="tree"/>'s page: Replication, then <b>Enrolled trees</b>, then the tree.</summary>
    /// <param name="tree">The tree id.</param>
    public async Task OpenAsync(string tree)
    {
        await session.Shell.GoToAreaAsync("replication");
        await Content.GetByRole(AriaRole.Link, new() { Name = "Enrolled trees", Exact = true }).ClickAsync();
        await Expect(session.Page).ToHaveURLAsync(new Regex("/replication/trees(\\?|$)"));
        await Content.GetByRole(AriaRole.Link, new() { Name = tree, Exact = true }).First.ClickAsync();
        await Expect(session.Page).ToHaveURLAsync(new Regex($"/replication/trees/{Regex.Escape(tree)}(\\?|$)"));
    }

    /// <summary>Reads the tree's link to <paramref name="peer"/> in <paramref name="direction"/>, or <see langword="null"/> when the page lists none.</summary>
    /// <param name="peer">The peer region.</param>
    /// <param name="direction"><c>Outbound</c> or <c>Inbound</c>.</param>
    public async Task<ReplicationLinkReading?> ReadAsync(string peer, string direction)
    {
        var raw = await session.Page.EvaluateAsync<RawLink?>(ReadLink, new[] { peer, direction });
        if (raw is null)
        {
            return null;
        }

        var digits = Digits().Replace(raw.Behind ?? string.Empty, string.Empty);
        long? behind = long.TryParse(digits, NumberStyles.None, CultureInfo.InvariantCulture, out var value) ? value : null;
        return new ReplicationLinkReading(raw.Health ?? string.Empty, behind, raw.Stall ?? string.Empty, raw.Errors ?? string.Empty, raw.Text ?? string.Empty);
    }

    /// <summary>Waits until the link's reading satisfies <paramref name="condition"/>, as the page refreshes itself.</summary>
    /// <param name="peer">The peer region.</param>
    /// <param name="direction"><c>Outbound</c> or <c>Inbound</c>.</param>
    /// <param name="what">What is awaited, for the timeout's message.</param>
    /// <param name="condition">The condition.</param>
    /// <param name="timeout">How long to wait.</param>
    public async Task<ReplicationLinkReading> WaitForAsync(string peer, string direction, string what, Func<ReplicationLinkReading, bool> condition, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        ReplicationLinkReading? last = null;
        while (true)
        {
            last = await ReadAsync(peer, direction);
            if (last is not null && condition(last))
            {
                return last;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new System.TimeoutException($"After {timeout.TotalSeconds:0} s the {direction} link to {peer} never showed {what}; it last read: {last?.Text ?? "no such link"}.");
            }

            await Task.Delay(TimeSpan.FromSeconds(1));
        }
    }

    [GeneratedRegex("[^0-9]")]
    private static partial Regex Digits();

    private sealed class RawLink
    {
        public string? Health { get; set; }

        public string? Behind { get; set; }

        public string? Stall { get; set; }

        public string? Errors { get; set; }

        public string? Text { get; set; }
    }
}
