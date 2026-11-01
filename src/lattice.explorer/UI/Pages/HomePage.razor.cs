using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Layout;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Pages;

/// <summary>
/// Home: the estate overview, listing every shown area with its one-line status,
/// drawn as a spine.
/// </summary>
public partial class HomePage : IDisposable
{
    /// <summary>The note under an area, or a status, shown from the last answer because the latest check met no answer.</summary>
    internal const string StaleNote = "Last known: the cluster did not answer the latest check.";

    private readonly Dictionary<string, ExplorerAreaStatus?> _statuses = new(StringComparer.Ordinal);
    private readonly ComponentLifetime _lifetime = new();
    private IReadOnlyList<ExplorerAreaEntry>? _asked;

    [Inject]
    internal ExplorerAreaDirectory Directory { get; set; } = default!;

    private ExplorerLocation CurrentLocation => Location ?? ExplorerLocation.Initial;

    /// <summary>Stops asking for statuses.</summary>
    public void Dispose()
    {
        _lifetime.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var entries = CurrentLocation.Entries;
        if (!CurrentLocation.EntriesLoaded || ReferenceEquals(entries, _asked))
        {
            return;
        }

        _asked = entries;
        var token = _lifetime.Token;
        var pending = entries
            .Where(entry => entry.IsVisible)
            .Select(async entry => (entry.Area.Key, Status: await Directory.GetHomeStatusAsync(entry.Area, token)))
            .ToArray();

        await foreach (var answered in Task.WhenEach(pending).WithCancellation(token))
        {
            var (key, status) = await answered;
            if (!ReferenceEquals(entries, _asked))
            {
                return;
            }

            _statuses[key] = status;
            StateHasChanged();
        }
    }

    private string AreaHref(ExplorerAreaEntry entry) =>
        Navigator.Canonicalize(ExplorerAddress.ForArea(entry.Area.Key).WithTenant(Address.Tenant)).ToHref();

    private ExplorerAreaStatus? StatusOf(ExplorerAreaEntry entry) =>
        _statuses.TryGetValue(entry.Area.Key, out var status) ? status : null;

    private bool AnyNeedsRecheck =>
        CurrentLocation.Entries.Any(entry => entry.NeedsRecheck)
        || CurrentLocation.Entries.Any(entry => entry.IsVisible && StatusOf(entry) is { NeedsRecheck: true });

    private static string StateOf(ExplorerAreaEntry entry) =>
        entry.IsUnchecked ? "unchecked" : entry.IsStale ? "stale" : "current";
}
