using System.Globalization;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Navigation;
using Orleans.Lattice.Explorer.UI.Navigation.Address;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Data area: the tree and view directory at <c>/data</c> and a tree's
/// workspace at <c>/data/{tree-path}</c>. It is visible whenever the caller can
/// read the tree catalogue through the Core state-API reader, hidden when the
/// head serves no state API or the caller is refused, and unavailable - with a
/// reason - when the cluster cannot be reached.
/// </summary>
internal sealed class DataArea : IExplorerArea
{
    /// <summary>The area key: the first address segment.</summary>
    public const string AreaKey = "data";

    /// <summary>The Apps area's key, which an app-owned tree's owner links into.</summary>
    public const string AppsAreaKey = "apps";

    /// <summary>The command that reloads the directory.</summary>
    public const string RefreshCommandId = "data.refresh";

    private readonly DataDirectory _directory;
    private readonly IServiceProvider _services;

    /// <summary>Creates the area.</summary>
    /// <param name="directory">The circuit's tree directory.</param>
    /// <param name="completions">The area's address completions.</param>
    /// <param name="services">The circuit's services, from which the state connection is read lazily.</param>
    public DataArea(DataDirectory directory, DataCompletionSource completions, IServiceProvider services)
    {
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(completions);
        ArgumentNullException.ThrowIfNull(services);
        _directory = directory;
        _services = services;
        Completions = completions;
        Commands =
        [
            new ExplorerCommand(RefreshCommandId, "Refresh the tree directory")
            {
                Detail = "Reload every tree and view you can reach.",
                Target = ExplorerAddress.ForArea(AreaKey),
                InvokeAsync = async cancellationToken => await _directory.RefreshAsync(cancellationToken).ConfigureAwait(false),
            },
        ];
    }

    /// <inheritdoc />
    public string Key => AreaKey;

    /// <inheritdoc />
    public string DisplayName => "Data";

    /// <inheritdoc />
    public int DirectoryOrder => 10;

    /// <inheritdoc />
    public IAddressCompletionSource? Completions { get; }

    /// <inheritdoc />
    public IReadOnlyList<ExplorerCommand> Commands { get; }

    /// <inheritdoc />
    public async ValueTask<AreaAvailability> GetAvailabilityAsync(CancellationToken cancellationToken)
    {
        if (!_directory.HasReader)
        {
            return AreaAvailability.Hidden;
        }

        if (ConnectionStatus() is { IsDisconnected: true } status && _directory.Loaded is null)
        {
            return AreaAvailability.Unavailable(status.RequiresAuthentication
                ? "Sign in to browse this cluster's data."
                : "Connect to a cluster to browse its data.");
        }

        try
        {
            return await _directory.ProbeAsync(cancellationToken).ConfigureAwait(false)
                ? AreaAvailability.Visible
                : AreaAvailability.Hidden;
        }
        catch (Exception exception) when (!cancellationToken.IsCancellationRequested)
        {
            return AreaAvailability.Unavailable(DataErrors.Describe(exception, "read the tree catalogue"));
        }
    }

    /// <inheritdoc />
    public ValueTask<string?> GetDirectoryBadgeAsync(CancellationToken cancellationToken) =>
        ValueTask.FromResult(_directory.Loaded is { } entries
            ? entries.Count.ToString("N0", CultureInfo.InvariantCulture)
            : null);

    /// <inheritdoc />
    public async ValueTask<string?> GetHomeStatusAsync(CancellationToken cancellationToken)
    {
        var entries = await _directory.LoadAsync(cancellationToken).ConfigureAwait(false);
        var views = entries.Count(entry => entry.Kind == DataTreeKind.View);
        var trees = entries.Count - views;
        return string.Create(CultureInfo.InvariantCulture, $"{Plural(trees, "tree")} and {Plural(views, "view")}.");
    }

    internal static string Plural(long count, string noun) =>
        string.Create(CultureInfo.InvariantCulture, $"{count:N0} {noun}{(count == 1 ? string.Empty : "s")}");

    private LatticeConnectionStatus? ConnectionStatus()
    {
        try
        {
            return (_services.GetService(typeof(ILatticeStateConnection)) as ILatticeStateConnection)?.Status;
        }
        catch (InvalidOperationException)
        {
            return null;
        }
    }
}
