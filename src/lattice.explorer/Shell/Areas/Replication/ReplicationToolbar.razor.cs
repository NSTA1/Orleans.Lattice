using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.Shell.Design.Components;
using Orleans.Lattice.Explorer.Shell.Navigation;
using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Areas.Replication;

/// <summary>
/// The Replication area's filter toolbar: health, peer region and owning app, each
/// written to the address so a filtered view can be shared and linked to.
/// </summary>
public partial class ReplicationToolbar
{
    private const string AnyValue = "";

    /// <summary>The peer regions to offer, in display order.</summary>
    [Parameter]
    public IReadOnlyList<string> Regions { get; set; } = [];

    /// <summary>The app slugs to offer, in display order.</summary>
    [Parameter]
    public IReadOnlyList<string> Apps { get; set; } = [];

    [CascadingParameter]
    internal ExplorerLocation? Location { get; set; }

    [Inject]
    internal ExplorerNavigator Navigator { get; set; } = default!;

    internal ReplicationFilter Filter => ReplicationFilter.From(Current);

    private ExplorerAddress Current => Location?.Address ?? Navigator.Current ?? ReplicationAddresses.Estate;

    private IReadOnlyList<LtSelectOption> HealthOptions { get; } =
    [
        new LtSelectOption(AnyValue, "Any health"),
        .. ReplicationHealth.WorstFirst.Select(health => new LtSelectOption(ReplicationHealth.QueryValue(health), ReplicationHealth.Label(health))),
    ];

    private IReadOnlyList<LtSelectOption> RegionOptions => Options("Any region", Regions, Filter.Region);

    private IReadOnlyList<LtSelectOption> AppOptions => Options("Any app", Apps, Filter.App);

    private string HealthValue => Filter.Health is { } health ? ReplicationHealth.QueryValue(health) : AnyValue;

    private string RegionValue => Filter.Region ?? AnyValue;

    private string AppValue => Filter.App ?? AnyValue;

    private static IReadOnlyList<LtSelectOption> Options(string any, IReadOnlyList<string> values, string? selected)
    {
        var options = new List<LtSelectOption>(values.Count + 2) { new(AnyValue, any) };
        options.AddRange(values.Select(value => new LtSelectOption(value, value)));
        if (selected is not null && !values.Contains(selected, StringComparer.Ordinal))
        {
            // A filter from a link that names nothing present is kept visible, not dropped.
            options.Add(new LtSelectOption(selected, selected));
        }

        return options;
    }

    private Task SetHealthAsync(string value) => SetAsync(ReplicationAddresses.HealthQuery, value);

    private Task SetRegionAsync(string value) => SetAsync(ReplicationAddresses.RegionQuery, value);

    private Task SetAppAsync(string value) => SetAsync(ReplicationAddresses.AppQuery, value);

    private Task SetAsync(string key, string? value)
    {
        Navigator.NavigateTo(Current.WithQuery(key, string.IsNullOrEmpty(value) ? null : value), replace: true);
        return Task.CompletedTask;
    }

    private Task ClearAsync()
    {
        Navigator.NavigateTo(
            Current.WithQuery(ReplicationAddresses.HealthQuery, null)
                .WithQuery(ReplicationAddresses.RegionQuery, null)
                .WithQuery(ReplicationAddresses.AppQuery, null),
            replace: true);
        return Task.CompletedTask;
    }
}
