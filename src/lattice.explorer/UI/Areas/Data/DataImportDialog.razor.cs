using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Forms;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Downloads;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Data;

/// <summary>
/// The Import dialog on a tree page (issue #5365): choose a file and how to read it, then
/// stream it into the tree - upserting through the Data API, or seeding an empty tree with
/// a bulk load after a pre-flight pass - and follow it to its result, with every refused
/// line listed and downloadable. Owns the visible control of the <c>data.import</c>
/// command's dialog.
/// </summary>
public partial class DataImportDialog : IDisposable
{
    private static readonly TimeSpan RenderInterval = TimeSpan.FromMilliseconds(250);

    private IBrowserFile? _file;
    private string? _fileName;
    private DataImportFormat _format = DataImportFormat.Ndjson;
    private DataImportMode _mode = DataImportMode.Upsert;
    private string? _keyTemplate = "/id";
    private DataImportValueSource _valueSource = DataImportValueSource.Record;
    private string? _valueField;
    private char _delimiter = ',';
    private bool _inferTypes;
    private string? _error;
    private DataImportRun? _run;
    private DataImportPhase _drawnPhase;
    private ExplorerDownloadTicket? _ticket;
    private long _lastRenderTicks;
    private bool _wasOpen;
    private bool _announced;

    /// <summary>The tree id the facades write, as they name it under the caller's tenant.</summary>
    [Parameter, EditorRequired]
    public string TreeId { get; set; } = string.Empty;

    /// <summary>The tree's name as the user reads it.</summary>
    [Parameter, EditorRequired]
    public string TreeName { get; set; } = string.Empty;

    /// <summary>Whether the caller may write the tree, which an upsert needs; offered only when it may.</summary>
    [Parameter]
    public bool CanUpsert { get; set; }

    /// <summary>Whether the caller holds the tree's bulk-load grant, which a seed needs; offered only when it does.</summary>
    [Parameter]
    public bool CanSeed { get; set; }

    /// <summary>Whether the dialog opens on a seed, as it does for a tree just created.</summary>
    [Parameter]
    public bool SeedFirst { get; set; }

    /// <summary>Whether the dialog is open.</summary>
    [Parameter]
    public bool Open { get; set; }

    /// <summary>Raised when the dialog opens or closes itself.</summary>
    [Parameter]
    public EventCallback<bool> OpenChanged { get; set; }

    /// <summary>Raised when an import finishes having written something, so the page can refresh.</summary>
    [Parameter]
    public EventCallback Imported { get; set; }

    /// <summary>Where the dialog sits: centred, or a sheet at the compact width.</summary>
    [Parameter]
    public LtDialogPlacement Placement { get; set; } = LtDialogPlacement.Center;

    [Inject]
    internal DataImportSessions Sessions { get; set; } = default!;

    [Inject]
    internal ExplorerDownloads Downloads { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    internal static IReadOnlyList<LtSelectOption> FormatOptions { get; } =
    [
        new(nameof(DataImportFormat.Ndjson), "NDJSON (one JSON value per line)"),
        new(nameof(DataImportFormat.Csv), "CSV (a header row, then one row per entry)"),
        new(nameof(DataImportFormat.LatticeExport), "Lattice export (a file from Export...)"),
        new(nameof(DataImportFormat.KeyValue), "key=value lines"),
    ];

    internal static IReadOnlyList<LtSelectOption> DelimiterOptions { get; } =
    [
        new(",", "Comma"),
        new(";", "Semicolon"),
        new("\t", "Tab"),
        new("|", "Vertical bar"),
    ];

    private string FileInputId => "lt-data-import-file";

    private bool Reading => _run is { IsFinished: false };

    private bool UsesKeyTemplate => _format is DataImportFormat.Ndjson or DataImportFormat.Csv;

    private string StateName => _run?.Phase.ToString().ToLowerInvariant() ?? "choose";

    private string DelimiterValue => _delimiter.ToString();

    private IReadOnlyList<LtSelectOption> ModeOptions
    {
        get
        {
            var options = new List<LtSelectOption>(2);
            if (Rights.CanUpsert)
            {
                options.Add(new(nameof(DataImportMode.Upsert), "Add or replace entries"));
            }

            if (Rights.CanSeed)
            {
                options.Add(new(nameof(DataImportMode.Seed), "Seed an empty tree (bulk load)"));
            }

            return options;
        }
    }

    private IReadOnlyList<LtSelectOption> ValueOptions => _format == DataImportFormat.Csv
        ?
        [
            new(nameof(DataImportValueSource.Record), "The row, as a JSON object"),
            new(nameof(DataImportValueSource.Field), "One column's text"),
        ]
        :
        [
            new(nameof(DataImportValueSource.Record), "The whole line"),
            new(nameof(DataImportValueSource.Field), "The value at a JSON Pointer"),
        ];

    private string FileHint => _file is null
        ? "NDJSON, CSV, an Export file, or key=value lines, up to " + DataExportDialog.Size(Limits.MaxImportBytes) + "."
        : _fileName + ", " + DataExportDialog.Size(_file.Size) + ".";

    private string FormatHint => _format switch
    {
        DataImportFormat.Csv => "RFC 4180, UTF-8 with or without a byte-order mark. The first row names the columns.",
        DataImportFormat.LatticeExport => "The NDJSON an Export writes: each line's key and value are restored exactly.",
        DataImportFormat.KeyValue => "The first = on a line splits the key from the value, which is stored as UTF-8 text.",
        _ => "Each line is one JSON value; blank lines are skipped.",
    };

    private string ModeHint => _mode == DataImportMode.Seed
        ? "The tree must be empty and the file in strictly ascending key order. The whole file is checked before anything is written."
        : "Entries are written in batches, in any order. A line that cannot be read or written is listed, and the rest still land.";

    private string KeyHint => _format == DataImportFormat.Csv
        ? "A column, such as id, or a template such as order/{id}."
        : "A JSON Pointer, such as /id, or a template such as order/{/id}.";

    private DataImportRights Rights => new(CanUpsert, CanSeed);

    private DataImportLimits Limits => Services.GetService(typeof(DataImportLimits)) as DataImportLimits ?? DataImportLimits.Default;

    /// <inheritdoc />
    public void Dispose()
    {
        Unwatch();
        GC.SuppressFinalize(this);
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (Open && !_wasOpen)
        {
            _error = null;
            if (Sessions.Find(TreeId) is { } existing)
            {
                Watch(existing);
            }
            else if (_run is null)
            {
                _mode = (SeedFirst && CanSeed) || !CanUpsert ? DataImportMode.Seed : DataImportMode.Upsert;
            }
        }

        _wasOpen = Open;
    }

    internal static string Count(long count, string one, string many) =>
        string.Create(CultureInfo.InvariantCulture, $"{count:N0} {(count == 1 ? one : many)}");

    internal static string ReadText(DataImportRun run) =>
        run.FileBytes > 0
            ? DataExportDialog.Size(run.BytesRead) + " of " + DataExportDialog.Size(run.FileBytes) + ", " + Count(run.LinesRead, "line", "lines")
            : Count(run.LinesRead, "line", "lines");

    internal static string RateText(DataImportRun run) =>
        run.EntriesPerSecond > 0
            ? string.Create(CultureInfo.InvariantCulture, $"{run.EntriesPerSecond:N0} entries a second")
            : "-";

    internal static string ResultText(DataImportRun run)
    {
        var written = Count(run.EntriesCommitted, "entry", "entries");
        var refused = run.Rejections.Count;
        return run.Phase switch
        {
            DataImportPhase.Succeeded when refused == 0 => run.TreeKeys is { } keys
                ? $"Seeded {written}. The tree holds {Count(keys, "live key", "live keys")}."
                : $"Imported {written}. Every line was written.",
            DataImportPhase.Succeeded => $"Imported {written}. {Count(refused, "line was", "lines were")} refused; each is listed below.",
            DataImportPhase.Stopped => $"Stopped. {written} were written before the stop, and nothing after it.",
            DataImportPhase.Cancelled => $"Cancelled when the session ended. {written} were written.",
            _ => (run.Failure ?? "The import stopped.") + (run.EntriesCommitted > 0 ? $" {written} were written before it stopped." : string.Empty),
        };
    }

    internal static string PhaseText(string phase) => phase == "Checking" ? "Checking the file" : "Importing";

    /// <summary>The format a file's name suggests, or <see langword="null"/> when it suggests none.</summary>
    /// <param name="fileName">The file's name.</param>
    internal static (DataImportFormat Format, char Delimiter)? FormatFor(string fileName) =>
        Path.GetExtension(fileName).ToLowerInvariant() switch
        {
            ".csv" => (DataImportFormat.Csv, ','),
            ".tsv" => (DataImportFormat.Csv, '\t'),
            ".ndjson" or ".jsonl" or ".json" => (DataImportFormat.Ndjson, ','),
            ".txt" => (DataImportFormat.KeyValue, ','),
            _ => null,
        };

    private void OnFileChosen(InputFileChangeEventArgs args)
    {
        _file = args.File;
        _fileName = args.File.Name;
        _error = null;
        if (FormatFor(_fileName) is { } guess && guess.Format != _format)
        {
            OnFormatChanged(guess.Format.ToString());
            _delimiter = guess.Delimiter;
        }
    }

    private void OnFormatChanged(string value)
    {
        _format = Enum.TryParse<DataImportFormat>(value, out var format) ? format : DataImportFormat.Ndjson;
        _keyTemplate = _format == DataImportFormat.Csv ? "id" : "/id";
        _valueSource = DataImportValueSource.Record;
        _valueField = null;
        _error = null;
    }

    private void OnModeChanged(string value) =>
        _mode = Enum.TryParse<DataImportMode>(value, out var mode) && Rights.Allows(mode) ? mode : _mode;

    private void OnValueSourceChanged(string value) =>
        _valueSource = Enum.TryParse<DataImportValueSource>(value, out var source) ? source : DataImportValueSource.Record;

    private void OnDelimiterChanged(string value) => _delimiter = value is { Length: 1 } ? value[0] : ',';

    private Task StartAsync()
    {
        _error = Validate(out var settings);
        if (_error is not null || settings is null || _file is not { } file)
        {
            return Task.CompletedTask;
        }

        ILatticeDataApi? data = null;
        ILatticeTreeAdmin? admin = null;
        try
        {
            if (settings.Mode == DataImportMode.Seed)
            {
                admin = Services.GetShellFacade<ILatticeTreeAdmin>();
            }
            else
            {
                data = Services.GetShellFacade<ILatticeDataApi>();
            }
        }
        catch (InvalidOperationException)
        {
            _error = "This Explorer is not connected to a cluster that serves imports.";
            return Task.CompletedTask;
        }

        var maxBytes = Limits.MaxImportBytes;
        var run = new DataImportRun(TreeId, settings, () => file.OpenReadStream(maxBytes), file.Size, data, admin);
        try
        {
            Sessions.Start(run);
        }
        catch (Exception exception) when (exception is InvalidOperationException or ObjectDisposedException)
        {
            run.Dispose();
            _error = exception.Message;
            return Task.CompletedTask;
        }

        Watch(run);
        return Task.CompletedTask;
    }

    private string? Validate(out DataImportSettings? settings)
    {
        settings = null;
        if (_file is null)
        {
            return "Choose a file to import.";
        }

        if (_file.Size > Limits.MaxImportBytes)
        {
            return $"The file is {DataExportDialog.Size(_file.Size)}; the largest file this Explorer imports is {DataExportDialog.Size(Limits.MaxImportBytes)}.";
        }

        if (!Rights.Allows(_mode))
        {
            return "You may not import into this tree that way.";
        }

        settings = new DataImportSettings
        {
            Format = _format,
            Mode = _mode,
            KeyTemplate = _keyTemplate,
            ValueSource = _valueSource,
            ValueField = _valueField,
            Delimiter = _delimiter,
            InferTypes = _inferTypes,
        };

        if (settings.Validate() is { } problem)
        {
            settings = null;
            return problem;
        }

        return null;
    }

    private void Stop() => _run?.Stop();

    private void Reset()
    {
        Unwatch();
        Sessions.Forget(TreeId);
        _file = null;
        _fileName = null;
        _error = null;
    }

    private void PrepareRejections()
    {
        if (_run is not { IsFinished: true } run)
        {
            return;
        }

        try
        {
            _ticket = Downloads.Mint(new DataImportRejectionsDownload(run.Rejections, TreeName));
            _ticket.Changed += OnTicketChanged;
        }
        catch (InvalidOperationException exception)
        {
            _error = exception.Message;
        }
    }

    private async Task CloseAsync()
    {
        if (Reading)
        {
            return;
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

    private void Watch(DataImportRun run)
    {
        if (ReferenceEquals(run, _run))
        {
            return;
        }

        Unwatch();
        _run = run;
        _announced = run.IsFinished;
        _drawnPhase = run.Phase;
        run.Changed += OnRunChanged;
    }

    private void Unwatch()
    {
        if (_run is { } run)
        {
            run.Changed -= OnRunChanged;
        }

        if (_ticket is { } ticket)
        {
            ticket.Changed -= OnTicketChanged;
        }

        _run = null;
        _ticket = null;
    }

    private void OnRunChanged()
    {
        // Progress arrives once per batch on the import's own thread; it is drawn at most
        // every quarter second, but every change of phase is drawn at once.
        if (_run is not { } run)
        {
            return;
        }

        var phase = run.Phase;
        if (phase == _drawnPhase)
        {
            var now = Environment.TickCount64;
            if (now - Interlocked.Read(ref _lastRenderTicks) < RenderInterval.TotalMilliseconds)
            {
                return;
            }

            Interlocked.Exchange(ref _lastRenderTicks, now);
        }

        _drawnPhase = phase;
        _ = InvokeAsync(async () =>
        {
            StateHasChanged();
            if (run.IsFinished && run.EntriesCommitted > 0 && !_announced)
            {
                _announced = true;
                await Imported.InvokeAsync();
            }
        });
    }

    private void OnTicketChanged() => _ = InvokeAsync(StateHasChanged);
}
