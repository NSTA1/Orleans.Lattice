using System.Globalization;
using Microsoft.AspNetCore.Components;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Downloads;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Export dialog on a tree page (issue #5366): choose a scope - the whole tree, a
/// key prefix or a key range - and a format, prepare a single-use download, then follow
/// it to its verdict. Owns the visible control of the <c>data.export</c> command's dialog.
/// </summary>
public partial class DataExportDialog : IDisposable
{
    private static readonly TimeSpan RenderInterval = TimeSpan.FromMilliseconds(250);

    private DataExportScopeKind _scope;
    private DataExportFormat _format;
    private string? _prefix;
    private string? _from;
    private string? _to;
    private string? _error;
    private DataExportRequest? _prepared;
    private ExplorerDownloadTicket? _ticket;
    private long _lastRenderTicks;
    private bool _wasOpen;

    /// <summary>The logical id of the tree to export, as the Data API names it under the caller's tenant.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>The tree's name as the user reads it.</summary>
    [Parameter, EditorRequired]
    public string TreeName { get; set; } = string.Empty;

    /// <summary>Whether the dialog is open.</summary>
    [Parameter]
    public bool Open { get; set; }

    /// <summary>Raised when the dialog opens or closes itself.</summary>
    [Parameter]
    public EventCallback<bool> OpenChanged { get; set; }

    /// <summary>The prefix the keys panel is filtered by, offered as the prefix scope when the dialog opens.</summary>
    [Parameter]
    public string? Prefix { get; set; }

    /// <summary>Where the dialog sits: centred, or a sheet at the compact width.</summary>
    [Parameter]
    public LtDialogPlacement Placement { get; set; } = LtDialogPlacement.Center;

    [Inject]
    internal ExplorerDownloads Downloads { get; set; } = default!;

    [Inject]
    internal IDataExportSources Sources { get; set; } = default!;

    internal static IReadOnlyList<LtSelectOption> ScopeOptions { get; } =
    [
        new(nameof(DataExportScopeKind.Tree), "The whole tree"),
        new(nameof(DataExportScopeKind.Prefix), "Keys with a prefix"),
        new(nameof(DataExportScopeKind.Range), "A key range"),
    ];

    internal static IReadOnlyList<LtSelectOption> FormatOptions { get; } =
    [
        new(nameof(DataExportFormat.Ndjson), "NDJSON (one JSON object per line)"),
        new(nameof(DataExportFormat.Csv), "CSV (key, value, encoding)"),
    ];

    private string ScopeValue => _scope.ToString();

    private string FormatValue => _format.ToString();

    private string FormatHint => _format == DataExportFormat.Csv
        ? "A JSON value is written as JSON text; any other value as base64."
        : "A JSON value is written as JSON; any other value as base64 marked \"encoding\":\"base64\".";

    /// <summary>The state the dialog shows, as a hook for tests and journeys.</summary>
    private string StateName => _ticket?.State.ToString().ToLowerInvariant() ?? "choose";

    /// <inheritdoc />
    public void Dispose()
    {
        Unwatch();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (Open && !_wasOpen && _ticket is null)
        {
            _error = null;
            if (!string.IsNullOrEmpty(Prefix))
            {
                _scope = DataExportScopeKind.Prefix;
                _prefix = Prefix;
            }
        }

        _wasOpen = Open;
    }

    private void OnScopeChanged(string value)
    {
        _scope = Enum.TryParse<DataExportScopeKind>(value, out var scope) ? scope : DataExportScopeKind.Tree;
        _error = null;
    }

    private void OnFormatChanged(string value) =>
        _format = Enum.TryParse<DataExportFormat>(value, out var format) ? format : DataExportFormat.Ndjson;

    private void Prepare()
    {
        var request = new DataExportRequest
        {
            TreeId = TreeId,
            TreeName = TreeName,
            Scope = _scope,
            Prefix = _prefix,
            From = _from,
            To = _to,
            Format = _format,
        };

        if (request.Validate() is { } problem)
        {
            _error = problem;
            return;
        }

        if (Downloads.CaptureConnection() is not { } connection)
        {
            _error = "Connect to a cluster before exporting.";
            return;
        }

        try
        {
            var ticket = Downloads.Mint(new DataExportDownload(request, Sources.ForDownload(connection)));
            _error = null;
            Watch(ticket);
            _prepared = request;
        }
        catch (InvalidOperationException exception)
        {
            _error = exception.Message;
        }
    }

    private void Reset()
    {
        Unwatch();
        _error = null;
    }

    private void Withdraw()
    {
        if (_ticket is not { } ticket)
        {
            return;
        }

        var ready = ticket.State == ExplorerDownloadState.Ready;
        Downloads.Withdraw(ticket);
        if (ready)
        {
            // Cancelling a download not yet started returns to the choices; stopping a
            // running one waits for the trailer that says it was stopped.
            Unwatch();
        }
    }

    private async Task CloseAsync()
    {
        if (_ticket is { State: ExplorerDownloadState.Ready } ticket)
        {
            Downloads.Withdraw(ticket);
        }

        if (_ticket is { State: not ExplorerDownloadState.Running })
        {
            Unwatch();
        }

        await OpenChanged.InvokeAsync(false);
    }

    private async Task OnOpenChangedAsync(bool open)
    {
        if (!open)
        {
            await CloseAsync();
            return;
        }

        await OpenChanged.InvokeAsync(true);
    }

    private void Watch(ExplorerDownloadTicket ticket)
    {
        Unwatch();
        _ticket = ticket;
        ticket.Changed += OnTicketChanged;
    }

    private void Unwatch()
    {
        if (_ticket is { } ticket)
        {
            ticket.Changed -= OnTicketChanged;
        }

        _ticket = null;
        _prepared = null;
    }

    private void OnTicketChanged()
    {
        // Progress arrives once per page on the download's own thread; it is drawn at
        // most every quarter second, but every change of state is drawn at once.
        if (_ticket is { State: ExplorerDownloadState.Running })
        {
            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastRenderTicks) < RenderInterval.TotalMilliseconds)
            {
                return;
            }

            Interlocked.Exchange(ref _lastRenderTicks, now);
        }

        _ = InvokeAsync(StateHasChanged);
    }

    internal static string ScopeText(DataExportRequest request) => request.Scope switch
    {
        DataExportScopeKind.Prefix => "Keys starting with " + request.Prefix,
        DataExportScopeKind.Range => (request.From, request.To) switch
        {
            ({ Length: > 0 } from, { Length: > 0 } to) => "From " + from + " up to " + to,
            ({ Length: > 0 } from, _) => "From " + from + " to the last key",
            _ => "From the first key up to " + request.To,
        },
        _ => "The whole tree",
    };

    internal static string ProgressText(ExplorerDownloadTicket ticket) =>
        string.Create(CultureInfo.InvariantCulture, $"{ticket.Items:N0} {(ticket.Items == 1 ? "entry" : "entries")}, {Size(ticket.Bytes)} written");

    internal static string ResultText(ExplorerDownloadTrailer trailer)
    {
        var count = string.Create(CultureInfo.InvariantCulture, $"{trailer.Items:N0} {(trailer.Items == 1 ? "entry" : "entries")}");
        return trailer.Complete
            ? $"Exported {count}. Every key in the scope is in the file."
            : $"Exported {count}, but the file is not complete. {trailer.Reason}";
    }

    internal static string RefusalText(ExplorerDownloadState state) => state switch
    {
        ExplorerDownloadState.Expired => "The download was not started within a minute, so it expired. Prepare it again.",
        ExplorerDownloadState.Refused => "The download was refused because it was not started from this tab. Prepare it again here.",
        _ => "The download was withdrawn: you signed in or out, or changed cluster. Prepare it again.",
    };

    internal static string Size(long bytes) => bytes switch
    {
        < 1024 => string.Create(CultureInfo.InvariantCulture, $"{bytes:N0} bytes"),
        < 1024 * 1024 => string.Create(CultureInfo.InvariantCulture, $"{bytes / 1024.0:N1} KB"),
        < 1024L * 1024 * 1024 => string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024.0 * 1024):N1} MB"),
        _ => string.Create(CultureInfo.InvariantCulture, $"{bytes / (1024.0 * 1024 * 1024):N2} GB"),
    };
}
