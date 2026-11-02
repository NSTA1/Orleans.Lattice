using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.WebUtilities;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;

namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>
/// The session overlay, rendered in the <c>overlay.session</c> chrome slot: the
/// connection gate on first run and whenever no valid endpoint is configured,
/// the connection settings and sign-in dialogs when the circuit asks for them,
/// and the re-authentication interstitial when a sign-in can no longer be
/// renewed.
/// </summary>
/// <remarks>
/// <para>
/// It shows at most one surface, in priority order: re-authentication first
/// (nothing else can succeed until it does), then the mandatory connection gate,
/// then whatever the circuit asked for through <see cref="SessionChromeState"/>.
/// </para>
/// <para>
/// It also drives the circuit's one-time initialisation - loading the persisted
/// configuration and any stored credential - exactly as the old
/// <c>ConfigurationGate</c> did, so every other session control can simply read
/// Core's state and re-render on its change events.
/// </para>
/// <para>
/// When the server sign-in endpoint redirects back after the cluster refused the
/// posted username and password, the interactive circuit reopens the sign-in
/// dialog with that notice and drops the notice from the address, so a reload or
/// a bookmark does not repeat it.
/// </para>
/// </remarks>
public partial class SessionOverlay
{
    private readonly EventCallback _close;

    /// <summary>Creates the overlay, binding its one close callback once rather than per render.</summary>
    public SessionOverlay() => _close = EventCallback.Factory.Create(this, Close);

    [Inject]
    private SessionChromeState State { get; set; } = default!;

    [Inject]
    private IExplorerSession Explorer { get; set; } = default!;

    [Inject]
    private IExplorerAuthSession Auth { get; set; } = default!;

    [Inject]
    private NavigationManager Navigation { get; set; } = default!;

    /// <inheritdoc />
    public void Dispose()
    {
        State.Changed -= OnChanged;
        Explorer.ConfigurationChanged -= OnChanged;
    }

    /// <inheritdoc />
    protected override async Task OnInitializedAsync()
    {
        State.Changed += OnChanged;
        Explorer.ConfigurationChanged += OnChanged;
        await State.EnsureInitializedAsync();

        // Only the interactive circuit acts on the notice: the prerender's own scope
        // is discarded, and navigating there would redirect the document and drop
        // the notice before the circuit could show it. The address is read first,
        // so a document without the notice never asks for the renderer at all.
        if (TryTakeRefusedNotice(Navigation.Uri, out var remaining) && RendererInfo.IsInteractive)
        {
            if (!Auth.IsAuthenticated)
            {
                State.OpenSignInWithNotice(ExplorerSignInRefusedException.PasswordRefusedMessage);
            }

            Navigation.NavigateTo(remaining, replace: true);
        }
    }

    /// <summary>
    /// Reports whether <paramref name="uri"/> carries the server sign-in endpoint's
    /// refused notice, and the same address without it.
    /// </summary>
    /// <param name="uri">The absolute address.</param>
    /// <param name="remaining">The address without the notice, when it carries one.</param>
    /// <returns><see langword="true"/> when the address carries the refused notice.</returns>
    internal static bool TryTakeRefusedNotice(string uri, out string remaining)
    {
        remaining = uri;
        if (!Uri.TryCreate(uri, UriKind.Absolute, out var parsed) || parsed.Query.Length <= 1)
        {
            return false;
        }

        var query = QueryHelpers.ParseQuery(parsed.Query);
        if (!query.TryGetValue(SessionSignInOptions.NoticeQueryKey, out var values)
            || !values.Any(v => string.Equals(v, SessionSignInOptions.RefusedNoticeValue, StringComparison.Ordinal)))
        {
            return false;
        }

        var kept = new List<KeyValuePair<string, string?>>();
        foreach (var (key, keyValues) in query)
        {
            if (string.Equals(key, SessionSignInOptions.NoticeQueryKey, StringComparison.Ordinal))
            {
                continue;
            }

            foreach (var value in keyValues)
            {
                kept.Add(new(key, value));
            }
        }

        var path = parsed.GetLeftPart(UriPartial.Path);
        remaining = QueryHelpers.AddQueryString(path, kept) + parsed.Fragment;
        return true;
    }

    private void Close() => State.CloseOverlay();

    private void OnChanged() => _ = InvokeAsync(StateHasChanged);
}
