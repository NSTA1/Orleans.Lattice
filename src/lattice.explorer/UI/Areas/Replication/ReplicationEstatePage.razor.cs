using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Areas.Replication;

/// <summary>
/// <c>/replication</c>: the estate's replication links as an order diagram and a
/// sortable table, filtered by <c>?health=</c>, <c>?region=</c> and <c>?app=</c>.
/// </summary>
public partial class ReplicationEstatePage
{
    private ReplicationEstate? _estate;
    private ReplicationFault? _fault;
    private ReplicationFilter _filter = ReplicationFilter.None;
    private IReadOnlyList<ReplicationPeerStatusEntry> _filtered = [];
    private IReadOnlyList<string> _apps = [];
    private bool _refreshing;
    private bool _subscribed;
    private readonly ComponentLifetime _cancellation = new();

    [Inject]
    internal ReplicationDataSource Data { get; set; } = default!;

    internal IReadOnlyList<ReplicationPeerStatusEntry> FilteredLinks => _filtered;

    private string TreesHref => Navigator.Canonicalize(ReplicationAddresses.Trees).ToHref();

    private string FaultTitle => _fault?.Kind switch
    {
        ReplicationFaultKind.Denied => "Replication status is not open to you",
        ReplicationFaultKind.NotServed => "Replication status is not served here",
        _ => "Replication status could not be read",
    };

    private string StatusLine
    {
        get
        {
            if (_estate is null)
            {
                return string.Empty;
            }

            var shown = _filter.IsActive
                ? $"{ReplicationFormat.Count(_filtered.Count)} of {ReplicationFormat.Count(_estate.Links.Count, "link", "links")} match."
                : ReplicationFormat.Count(_estate.Links.Count, "link", "links") + ".";
            return $"{shown} Read at {_estate.ReadAt.UtcDateTime:HH:mm:ss} UTC.";
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        Data.Invalidated -= OnInvalidated;
        _cancellation.Leave();
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        _filter = ReplicationFilter.From(Address);
        if (!_subscribed)
        {
            _subscribed = true;
            Data.Invalidated += OnInvalidated;
            await LoadAsync(refresh: false);
        }
        else
        {
            Apply();
        }
    }

    private async Task RefreshAsync()
    {
        _refreshing = true;
        try
        {
            await LoadAsync(refresh: true);
        }
        finally
        {
            _refreshing = false;
        }
    }

    private async Task LoadAsync(bool refresh)
    {
        try
        {
            var read = await Data.GetEstateAsync(refresh, _cancellation.Token);
            _estate = read.Value;
            _fault = read.Fault;
            Apply();
        }
        catch (OperationCanceledException)
        {
            // The page went away.
        }
    }

    private void Apply()
    {
        if (_estate is null)
        {
            _filtered = [];
            _apps = [];
            return;
        }

        _filtered = ReplicationEstate.WorstFirst(_estate.Links.Where(_filter.Matches));
        _apps =
        [
            .. _estate.Links
                .Select(link => ReplicationTreeOwnership.TryGetAppSlug(link.TreeId, out var slug) ? slug : null)
                .OfType<string>()
                .Distinct(StringComparer.Ordinal)
                .Order(StringComparer.Ordinal),
        ];
    }

    private void OnInvalidated() => _ = InvokeAsync(async () =>
    {
        await RefreshAsync();
        StateHasChanged();
    });
}
