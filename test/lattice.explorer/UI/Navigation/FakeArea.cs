using Orleans.Lattice.Explorer.UI.Navigation;

namespace Orleans.Lattice.Explorer.Tests.UI.Navigation;

/// <summary>
/// A scriptable native area: its availability, badge, Home status, completions
/// and commands are whatever the test says, including a probe that throws or
/// never answers.
/// </summary>
internal sealed class FakeArea : IExplorerArea
{
    /// <summary>Creates a visible area.</summary>
    /// <param name="key">The area key.</param>
    /// <param name="displayName">The display name.</param>
    /// <param name="order">The directory order.</param>
    public FakeArea(string key, string displayName, int order = 0)
    {
        Key = key;
        DisplayName = displayName;
        DirectoryOrder = order;
    }

    /// <inheritdoc />
    public string Key { get; }

    /// <inheritdoc />
    public string DisplayName { get; }

    /// <inheritdoc />
    public int DirectoryOrder { get; }

    /// <inheritdoc />
    public bool IsTenantScoped { get; init; } = true;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; init; }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands { get; init; } = [];

    /// <summary>What the availability probe does.</summary>
    public Func<CancellationToken, ValueTask<AreaAvailability>> Availability { get; set; } =
        _ => ValueTask.FromResult(AreaAvailability.Visible);

    /// <summary>What the Home status probe does.</summary>
    public Func<CancellationToken, ValueTask<string?>> HomeStatus { get; set; } = _ => ValueTask.FromResult<string?>(null);

    /// <summary>What the badge probe does.</summary>
    public Func<CancellationToken, ValueTask<string?>> Badge { get; set; } = _ => ValueTask.FromResult<string?>(null);

    /// <summary>How the address line groups a path into chain nodes; <see langword="null"/> for one per segment.</summary>
    public Func<Orleans.Lattice.Explorer.UI.Navigation.Address.ExplorerAddress, IReadOnlyList<int>?> ChainSpans { get; set; } = _ => null;

    /// <inheritdoc />
    public IReadOnlyList<int>? GetChainSpans(Orleans.Lattice.Explorer.UI.Navigation.Address.ExplorerAddress address) => ChainSpans(address);

    /// <summary>Whether an address in the area renders standalone; never, unless the test says so.</summary>
    public Func<Orleans.Lattice.Explorer.UI.Navigation.Address.ExplorerAddress, bool> Standalone { get; set; } = _ => false;

    /// <inheritdoc />
    public bool IsStandaloneAt(Orleans.Lattice.Explorer.UI.Navigation.Address.ExplorerAddress address) => Standalone(address);

    /// <summary>How many times availability was asked.</summary>
    public int AvailabilityCalls { get; private set; }

    /// <inheritdoc />
    public ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        AvailabilityCalls++;
        return Availability(cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken) => HomeStatus(cancellationToken);

    /// <inheritdoc />
    public ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken) => Badge(cancellationToken);
}
