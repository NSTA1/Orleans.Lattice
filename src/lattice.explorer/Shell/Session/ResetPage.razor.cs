using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Core.Session;

namespace Orleans.Lattice.Explorer.Shell.Session;

/// <summary>
/// The reset-view page at <c>/reset</c>: it discloses everything the preference
/// contract remembers for the current identity and cluster, and clears all of it
/// only when asked, announcing the outcome.
/// </summary>
/// <remarks>
/// It lists <see cref="IExplorerShellPreferences.Keys"/> rather than a
/// hand-written list, so a preference any feature registers is disclosed and
/// cleared here without this page changing.
/// </remarks>
public partial class ResetPage
{
    /// <summary>The page's address relative to the document base, for links that point at it.</summary>
    internal const string Href = "reset";

    private readonly EventCallback _resetClicked;
    private bool _busy;
    private bool _reset;

    /// <summary>Creates the page, binding its one callback once rather than per render.</summary>
    public ResetPage() => _resetClicked = EventCallback.Factory.Create(this, ResetAsync);

    [Inject]
    private IExplorerShellPreferences Preferences { get; set; } = default!;

    /// <inheritdoc />
    protected override Task OnInitializedAsync() => Preferences.EnsureLoadedAsync();

    private async Task ResetAsync()
    {
        if (_busy)
        {
            return;
        }

        _busy = true;
        try
        {
            await Preferences.ResetAsync();
            _reset = true;
        }
        finally
        {
            _busy = false;
        }
    }
}
