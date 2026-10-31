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

    /// <summary>The command that opens the New view dialog over the administered tree the workspace shows.</summary>
    public const string CreateViewCommandId = "data.create-view";

    /// <summary>The command that opens the Export dialog of the tree the workspace shows (issue #5366).</summary>
    public const string ExportCommandId = "data.export";

    private readonly DataDirectory _directory;
    private readonly IServiceProvider _services;
    private readonly DataViewCommandScope _viewCommand;
    private readonly DataExportCommandScope _exportCommand;
    private readonly DataCommandSignals _signals;
    private readonly ExplorerCommand _refresh;

    /// <summary>Creates the area.</summary>
    /// <param name="directory">The circuit's tree directory.</param>
    /// <param name="completions">The area's address completions.</param>
    /// <param name="services">The circuit's services, from which the state connection is read lazily.</param>
    /// <param name="viewCommand">The administered tree the workspace offers the New view command over.</param>
    /// <param name="exportCommand">The tree the workspace offers the Export command over.</param>
    /// <param name="signals">Carries palette commands to the open tree page.</param>
    public DataArea(
        DataDirectory directory,
        DataCompletionSource completions,
        IServiceProvider services,
        DataViewCommandScope viewCommand,
        DataExportCommandScope exportCommand,
        DataCommandSignals signals)
    {
        ArgumentNullException.ThrowIfNull(directory);
        ArgumentNullException.ThrowIfNull(completions);
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(viewCommand);
        ArgumentNullException.ThrowIfNull(exportCommand);
        ArgumentNullException.ThrowIfNull(signals);
        _directory = directory;
        _services = services;
        _viewCommand = viewCommand;
        _exportCommand = exportCommand;
        _signals = signals;
        Completions = completions;
        _refresh = new ExplorerCommand(RefreshCommandId, "Refresh the tree directory")
        {
            Detail = "Reload every tree and view you can reach.",
            Target = ExplorerAddress.ForArea(AreaKey),
            InvokeAsync = async cancellationToken => await _directory.RefreshAsync(cancellationToken).ConfigureAwait(false),
        };
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
    /// <remarks>Every path segment below <c>/data</c> belongs to the logical tree id, so the path is one node.</remarks>
    public IReadOnlyList<int>? GetChainSpans(ExplorerAddress address)
    {
        ArgumentNullException.ThrowIfNull(address);
        return address.Path.Count == 0 ? null : [address.Path.Count];
    }

    /// <summary>
    /// Refreshing the directory, always; creating a view, only while a tree
    /// workspace the caller administers is open, targeting that tree's New view
    /// dialog; and exporting, only while a tree workspace the caller may export is
    /// open, opening that tree's Export dialog.
    /// </summary>
    public IReadOnlyList<ExplorerCommand> Commands
    {
        get
        {
            var view = _viewCommand.Source;
            var export = _exportCommand.Source;
            if (view is null && export is null)
            {
                return [_refresh];
            }

            var commands = new List<ExplorerCommand>(3) { _refresh };
            if (view is not null)
            {
                commands.Add(new ExplorerCommand(CreateViewCommandId, "New view...")
                {
                    Detail = "Create a materialised view over " + view.LogicalId + ".",
                    Target = view.Address
                        .WithQuery(DataTabs.TabQuery, DataTabs.Views)
                        .WithQuery(DataTabs.NewQuery, DataTabs.NewViewValue),
                });
            }

            if (export is not null)
            {
                commands.Add(new ExplorerCommand(ExportCommandId, "Export...")
                {
                    Detail = "Download " + export.LogicalId + ", a key prefix or a key range as NDJSON or CSV.",
                    Target = export.Address,
                    InvokeAsync = _ => _signals.RequestAsync(ExportCommandId),
                });
            }

            return commands;
        }
    }

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
        var views = 0;
        var shared = 0;
        foreach (var entry in entries)
        {
            if (entry.IsShared)
            {
                shared++;
            }
            else if (entry.Kind == DataTreeKind.View)
            {
                views++;
            }
        }

        var trees = entries.Count - views - shared;
        return shared == 0
            ? string.Create(CultureInfo.InvariantCulture, $"{Plural(trees, "tree")} and {Plural(views, "view")}.")
            : string.Create(CultureInfo.InvariantCulture, $"{Plural(trees, "tree")} and {Plural(views, "view")}, and {shared:N0} shared with this tenant.");
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
