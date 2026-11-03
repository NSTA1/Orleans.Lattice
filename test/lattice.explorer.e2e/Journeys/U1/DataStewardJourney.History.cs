using Microsoft.Playwright;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Cast;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Pending;
using static Microsoft.Playwright.Assertions;
using Names = Orleans.Lattice.Explorer.EndToEnd.Tests.Story.StoryNames.Contoso;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Journeys.U1;

/// <summary>
/// The History tab's revisions of a key priya has just written. factory-orders has a
/// change-history view (A3), which materialises asynchronously: the tab reads it once when it
/// opens and then follows only later changes, so a revision the view had not yet materialised
/// at that moment is not on the tab. The server's own history read is the oracle.
/// </summary>
public sealed partial class DataStewardJourney
{
    /// <summary>How long the tab waits for a revision before the server is asked.</summary>
    private static readonly TimeSpan HistoryTabPatience = TimeSpan.FromSeconds(30);

    private static readonly KnownDefect HistoryViewLags = new KnownDefect("#5569", "under replay-permit starvation the change-history view's drain lags its source by minutes, so a revision priya has just written is not yet in History");

    /// <summary>
    /// Waits until the open History tab shows at least <paramref name="count"/> revisions of
    /// <paramref name="key"/> in factory-orders. When the tab does not show them, the server's
    /// history read decides: once it holds them, the page is reloaded and the tab must show them;
    /// if the view still lacks them after <see cref="PanelSettled.Bound"/>, that is #5569's view
    /// lag, recorded as a known defect.
    /// </summary>
    /// <param name="key">The key whose History tab is open.</param>
    /// <param name="count">The revisions the key has.</param>
    /// <returns><see langword="true"/> when the tab shows them; <see langword="false"/> when the view lags (#5569).</returns>
    private async Task<bool> WaitForHistoryRevisionsAsync(string key, int count)
    {
        var revision = _priya.Shell.Content.Locator(".lt-data-timeline__rev").Nth(count - 1);
        try
        {
            await revision.WaitForAsync(new() { State = WaitForSelectorState.Visible, Timeout = (float)HistoryTabPatience.TotalMilliseconds });
            return true;
        }
        catch (System.TimeoutException)
        {
        }

        var state = ContractFacades.Of(_context, Personas.Priya.Id).State;
        EntryHistoryResponse? last = null;
        try
        {
            await ContractWait.UntilAsync($"the history of {key} to hold {count} revisions", async () =>
            {
                last = await state.GetEntryHistoryAsync(new EntryHistoryRequest
                {
                    TreeId = Names.Trees.FactoryOrders,
                    Key = key,
                    Limit = count,
                    Reverse = true,
                });
                return last.Revisions.Count >= count;
            }, PanelSettled.Bound);
        }
        catch (System.TimeoutException) when (last is { Bound: EntryHistoryBound.BoundedByAge })
        {
            var held = last.Revisions.Count;
            await HistoryViewLags.ReproducesAsync(() =>
            {
                Assert.That(held, Is.LessThan(count), $"the change-history view still lacks a revision of {key}");
                return Task.CompletedTask;
            });
            return false;
        }

        // The server holds them: the tab read before the view caught up, so read it again.
        await _priya.Page.ReloadAsync();
        await Expect(revision).ToBeVisibleAsync(new() { Timeout = (float)HistoryTabPatience.TotalMilliseconds });
        return true;
    }
}
