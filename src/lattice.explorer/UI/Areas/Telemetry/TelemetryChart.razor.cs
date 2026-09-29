using System.Globalization;
using System.Text;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.UI.Design.Components;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>
/// One chart on a Telemetry board: it evaluates its catalogue entry through the
/// transport-neutral <see cref="ILatticeTelemetry"/> facade and draws the answer
/// as an order-diagram line chart, a table of current readings, or - in the
/// board's table view - a table of values by time.
/// </summary>
/// <remarks>
/// A refusal to read the entry (the caller's grant or the metric allow-list) is
/// reported to the board through <see cref="OnDenied"/>, which omits the chart
/// with a note rather than showing an error. Every other failure stays on the
/// chart, with a retry where one could succeed.
/// </remarks>
public partial class TelemetryChart : IDisposable
{
    private readonly string _headingId = LtIds.Next("lt-telemetry-chart");
    private readonly string _readoutId = LtIds.Next("lt-telemetry-readout");

    private TelemetryQueryRequest? _asked;
    private int _askedGeneration = -1;
    private CancellationTokenSource? _load;
    private TelemetryChartGeometry? _geometry;
    private IReadOnlyList<TelemetryValuesRow> _rows = [];
    private string? _failure;
    private bool _retryable;
    private int? _selected;
    private bool _pinned;

    /// <summary>The catalogue entry the chart draws.</summary>
    [Parameter, EditorRequired]
    public TelemetryQueryDescriptor Descriptor { get; set; } = default!;

    /// <summary>
    /// The request to evaluate, or <see langword="null"/> when <see cref="Problem"/>
    /// says why the board's window cannot be asked of this entry.
    /// </summary>
    [Parameter]
    public TelemetryQueryRequest? Request { get; set; }

    /// <summary>Why the board's window does not fit this entry, shown instead of a chart.</summary>
    [Parameter]
    public string? Problem { get; set; }

    /// <summary>A qualification shown beside the chart, such as a filter that does not apply to it.</summary>
    [Parameter]
    public string? Note { get; set; }

    /// <summary>Whether a time series is drawn as a table of values rather than a line chart.</summary>
    [Parameter]
    public bool ShowTable { get; set; }

    /// <summary>Bumped by the board to re-read the entry with the same request.</summary>
    [Parameter]
    public int Generation { get; set; }

    /// <summary>The link to the board's table view, offered from the figure.</summary>
    [Parameter]
    public string? TableHref { get; set; }

    /// <summary>Builds the link that narrows the board to one logical tree.</summary>
    [Parameter]
    public Func<string, string>? TreeHref { get; set; }

    /// <summary>Builds the link to a logical tree in the Data area.</summary>
    [Parameter]
    public Func<string, string>? DataHref { get; set; }

    /// <summary>Raised with the entry's query id when the cluster refuses to evaluate it for this caller.</summary>
    [Parameter]
    public EventCallback<string> OnDenied { get; set; }

    /// <summary>Raised with the tenant scope the facade applied to an answer.</summary>
    [Parameter]
    public EventCallback<TelemetryTenantScope> OnScope { get; set; }

    [Inject(Key = ShellFacades.Key)]
    internal ILatticeTelemetry Telemetry { get; set; } = default!;

    internal TelemetryChartGeometry? Geometry => _geometry;

    private static string ViewBox { get; } = string.Create(
        CultureInfo.InvariantCulture,
        $"0 0 {TelemetryChartGeometry.Width} {TelemetryChartGeometry.Height}");

    private string UnitText => TelemetryFormat.Unit(Descriptor.Unit);

    private string PlotLabel =>
        $"{Descriptor.Title} over time, {_geometry!.Series.Count} series. Use the left and right arrow keys to read the values at each time.";

    private string Summary
    {
        get
        {
            var geometry = _geometry!;
            var start = TelemetryFormat.Time(geometry.Times[0], withDate: true);
            var end = TelemetryFormat.Time(geometry.Times[^1], withDate: geometry.SpansDays);
            return $"{geometry.Series.Count} series from {start} to {end} UTC.";
        }
    }

    private string ReadoutText
    {
        get
        {
            var geometry = _geometry!;
            if (_selected is not { } selected)
            {
                return "Point at the chart, or focus it and use the arrow keys, to read the values at a time.";
            }

            var builder = new StringBuilder();
            builder.Append(TelemetryFormat.Time(geometry.Times[selected], withDate: geometry.SpansDays)).Append(" UTC: ");
            var first = true;
            foreach (var line in geometry.Lines)
            {
                if (!first)
                {
                    builder.Append("; ");
                }

                first = false;
                builder.Append(line.Series.Name).Append(' ').Append(Format(line.Series.Values[selected]));
            }

            return builder.Append('.').ToString();
        }
    }

    /// <inheritdoc />
    public void Dispose()
    {
        _load?.Cancel();
        _load?.Dispose();
        _load = null;
    }

    /// <inheritdoc />
    protected override async Task OnParametersSetAsync()
    {
        if (Equals(Request, _asked) && Generation == _askedGeneration)
        {
            return;
        }

        _asked = Request;
        _askedGeneration = Generation;
        await LoadAsync();
    }

    private static string Coordinate(double value) => TelemetryChartGeometry.Coordinate(value);

    private static string? DashOf(TelemetryPlotLine line) => line.Dash.Length == 0 ? null : line.Dash;

    // Razor reserves <text> inside a code block, so SVG text in a loop is built here.
    private static RenderFragment SvgText(string className, double x, double y, string anchor, string content) => builder =>
    {
        builder.OpenElement(0, "text");
        builder.AddAttribute(1, "class", className);
        builder.AddAttribute(2, "x", Coordinate(x));
        builder.AddAttribute(3, "y", Coordinate(y));
        builder.AddAttribute(4, "text-anchor", anchor);
        builder.AddContent(5, content);
        builder.CloseElement();
    };

    private string Format(double value) => TelemetryFormat.Value(value, Descriptor.Unit, Descriptor.Semantic);

    private Task RetryAsync() => LoadAsync();

    private async Task LoadAsync()
    {
        _load?.Cancel();
        _load?.Dispose();
        _load = null;
        _geometry = null;
        _rows = [];
        _failure = null;
        _selected = null;
        _pinned = false;

        if (Request is not { } request)
        {
            return;
        }

        var load = new CancellationTokenSource();
        _load = load;

        TelemetryQueryResponse response;
        try
        {
            response = await Telemetry.QueryAsync(request, load.Token);
        }
        catch (OperationCanceledException) when (load.IsCancellationRequested)
        {
            return;
        }
        catch (Exception exception) when (IsDenial(exception))
        {
            if (ReferenceEquals(load, _load))
            {
                await OnDenied.InvokeAsync(Descriptor.QueryId);
            }

            return;
        }
        catch (Exception exception)
        {
            if (ReferenceEquals(load, _load))
            {
                (_failure, _retryable) = Describe(exception);
            }

            return;
        }

        if (!ReferenceEquals(load, _load))
        {
            return;
        }

        var geometry = TelemetryChartGeometry.Build(response, Descriptor.Unit, Descriptor.Semantic);
        _rows = [.. geometry.Times.Select((time, index) =>
            new TelemetryValuesRow(time, [.. geometry.Series.Select(series => series.Values[index])]))];
        _geometry = geometry;
        await OnScope.InvokeAsync(response.Scope);
    }

    private static bool IsDenial(Exception exception) =>
        exception is UnauthorizedAccessException or TelemetryQueryNotFoundException;

    private static (string Message, bool Retryable) Describe(Exception exception) => exception switch
    {
        TelemetryQueryBoundsException => ("The cluster would not draw this chart over that window. Choose a shorter range or a coarser step.", false),
        TelemetryBackendException => ("The metrics backend did not answer. Try again in a moment.", true),
        ShellTransportException { IsTransient: true } => ("The cluster did not answer. Try again in a moment.", true),
        NotSupportedException => ("This cluster does not serve telemetry queries.", false),
        ArgumentException => ("The cluster refused this chart's parameters.", false),
        _ => ("This chart could not be read.", true),
    };

    private void Point(int index)
    {
        if (!_pinned)
        {
            _selected = index;
        }
    }

    private void Pin(int index)
    {
        _selected = index;
        _pinned = true;
    }

    private void OnPointerLeave(MouseEventArgs args)
    {
        if (!_pinned)
        {
            _selected = null;
        }
    }

    private void OnKeyDown(KeyboardEventArgs args)
    {
        var count = _geometry?.Times.Count ?? 0;
        if (count == 0)
        {
            return;
        }

        int? next = args.Key switch
        {
            "ArrowLeft" => Math.Max(0, (_selected ?? count) - 1),
            "ArrowRight" => _selected is { } at ? Math.Min(count - 1, at + 1) : count - 1,
            "Home" => 0,
            "End" => count - 1,
            "Escape" => null,
            _ => _selected,
        };

        _pinned = next is not null;
        _selected = next;
    }
}
