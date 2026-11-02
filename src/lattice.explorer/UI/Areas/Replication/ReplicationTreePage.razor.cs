using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// <c>/replication/trees/{tree-path}</c>: one tree's per-peer links, refreshed on a
/// cadence that runs only while the page is visible.
/// </summary>
public partial class ReplicationTreePage
{
    private readonly ComponentLifetime _cancellation = new();
    private ReplicationRefreshLoop? _loop;
    private string? _loadedTree;
    private IReadOnlyList<ReplicationPeerStatusEntry>? _links;
    private ReplicationFault? _fault;
    private ReplicationFault? _refreshFault;
    private ReplicationTreeConfigEntry? _entry;
    private string? _appSlug;
    private DateTimeOffset _readAt;
    private long _readVersion;

    [Inject]
    internal ReplicationDataSource Data { get; set; } = default!;

    [Inject]
    internal IReplicationPageVisibility Visibility { get; set; } = default!;

    [Inject]
    internal TimeProvider Time { get; set; } = default!;

    [Inject]
    internal ReplicationOptions Options { get; set; } = default!;

    [Inject]
    internal NavigationManager Navigation { get; set; } = default!;

    /// <summary>The cadence, once the page has rendered in a browser.</summary>
    internal ReplicationRefreshLoop? Loop => _loop;

    internal string TreeId => ReplicationAddresses.TreeIdOf(Address) ?? string.Empty;

    private string StatusLine =>
        ReplicationFormat.Count(_links?.Count ?? 0, "link", "links")
        + $". Read at {_readAt.UtcDateTime:HH:mm:ss} UTC"
        + (_loop is { IsRunning: true } ? $", refreshing every {Options.RefreshInterval.TotalSeconds:0} s." : ", paused while the page is hidden.");

    /// <inheritdoc />
    public void Dispose()
    {
        Visibility.Changed -= OnVisibilityChanged;
        _loop?.Dispose();
        _cancellation.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        var tree = TreeId;
        if (string.Equals(tree, _loadedTree, StringComparison.Ordinal))
        {
            return;
        }

        _loadedTree = tree;
        var cancellationToken = _cancellation.Renew();
        _links = null;
        _fault = null;
        _refreshFault = null;
        _entry = null;
        _appSlug = ReplicationTreeOwnership.TryGetAppSlug(tree, out var slug) ? slug : null;
        if (tree.Length == 0)
        {
            Navigation.NotFound();
            return;
        }

        ReplicationRead<ReplicationConfigReport> config;
        try
        {
            config = await Data.GetConfigAsync(refresh: false, cancellationToken);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return;
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return;
        }

        _entry = config.Value?.Trees.FirstOrDefault(entry => string.Equals(entry.TreeId, tree, StringComparison.Ordinal));
        await ReadAsync();
        if (cancellationToken.IsCancellationRequested)
        {
            return;
        }

        if (_entry is null && _links is { Count: 0 })
        {
            // Nothing the caller may see names this tree: it does not resolve here.
            Navigation.NotFound();
        }
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (firstRender)
        {
            _loop = new ReplicationRefreshLoop(Time, Visibility, Options.RefreshInterval, InvokeAsync, RefreshAsync);
            await _loop.StartAsync();
            if (_cancellation.IsLeft)
            {
                return;
            }

            // Subscribed after the cadence, so the status line is drawn once the cadence has
            // stopped or resumed: hiding the page reads no links, and nothing else would
            // redraw the line that says the refresh is paused.
            Visibility.Changed += OnVisibilityChanged;
            StateHasChanged();
        }
    }

    private void OnVisibilityChanged() => _ = InvokeAsync(StateHasChanged);

    private async Task RefreshAsync()
    {
        await ReadAsync();
        StateHasChanged();
    }

    private async Task ReadAsync()
    {
        var tree = TreeId;
        var cancellationToken = _cancellation.Token;
        if (tree.Length == 0)
        {
            return;
        }

        var version = ++_readVersion;
        try
        {
            var read = await Data.GetTreeLinksAsync(tree, cancellationToken);
            if (cancellationToken.IsCancellationRequested || version != _readVersion
                || !string.Equals(tree, TreeId, StringComparison.Ordinal))
            {
                return;
            }

            if (read.Value is { } estate)
            {
                _links = ReplicationEstate.WorstFirst(estate.Links.Where(link => string.Equals(link.TreeId, tree, StringComparison.Ordinal)));
                _readAt = estate.ReadAt;
                _fault = null;
                _refreshFault = null;
            }
            else if (_links is null)
            {
                _fault = read.Fault;
            }
            else
            {
                // A failed refresh keeps the last good read on screen, and says so.
                _refreshFault = read.Fault;
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The page went away.
        }
    }

    private string AppHref(string slug) => Navigator.Canonicalize(ReplicationTreeOwnership.AppReplicationAddress(slug)).ToHref();
}
