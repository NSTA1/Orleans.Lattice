using System.Net.Http.Json;
using System.Text.Json.Serialization;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;

/// <summary>What the line feed reports about itself.</summary>
/// <param name="Started">Whether it was started.</param>
/// <param name="Paused">Whether it is paused.</param>
/// <param name="OrdersAttached">Whether it writes orders.</param>
/// <param name="TelemetryWritten">Telemetry readings written.</param>
/// <param name="OrdersWritten">Order writes that succeeded, transitions included.</param>
/// <param name="InvalidOrdersWritten">Invalid (negative-total) orders sent.</param>
/// <param name="WrongPricesWritten">Policy-valid but wrong (x100) totals written.</param>
/// <param name="ConsecutiveFailures">Writes in a row that failed.</param>
/// <param name="LastError">The last write failure, if any.</param>
public sealed record LineFeedStatus(
    [property: JsonPropertyName("started")] bool Started,
    [property: JsonPropertyName("paused")] bool Paused,
    [property: JsonPropertyName("ordersAttached")] bool OrdersAttached,
    [property: JsonPropertyName("telemetryWritten")] long TelemetryWritten,
    [property: JsonPropertyName("ordersWritten")] long OrdersWritten,
    [property: JsonPropertyName("invalidOrdersWritten")] long InvalidOrdersWritten,
    [property: JsonPropertyName("wrongPricesWritten")] long WrongPricesWritten,
    [property: JsonPropertyName("consecutiveFailures")] int ConsecutiveFailures,
    [property: JsonPropertyName("lastError")] string? LastError);

/// <summary>
/// The harness's handle on the line feed: the factory's upstream ingestion service, a
/// process writing through the west gateway as <c>line-feed</c>. It is idle until
/// <see cref="StartAsync"/>, as a real service is idle until it is onboarded, and its writes
/// are refused until the story grants it rights.
/// </summary>
public sealed class LineFeedControl
{
    /// <summary>The telemetry tree its readings create.</summary>
    public const string TelemetryTree = "factory-telemetry";

    /// <summary>The orders tree it writes once attached.</summary>
    public const string OrdersTree = "factory-orders";

    /// <summary>The application-side tag index it owns over <see cref="OrdersTree"/>.</summary>
    public const string OrderStatusIndex = "order-status";

    /// <summary>The first order number it allocates, clear of the 240 seeded orders.</summary>
    public const int FirstOrder = 100_001;

    /// <summary>
    /// The telemetry key overwritten with every accepted reading (the line's latest reading),
    /// so the one key in the estate with hundreds of revisions: a History tab that pages.
    /// </summary>
    public const string LatestReadingKey = "reading/latest";

    private readonly HttpClient _http;

    internal LineFeedControl(JourneyProcess process, int controlPort)
    {
        Process = process;
        _http = new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{controlPort}/"), Timeout = TimeSpan.FromSeconds(30) };
    }

    internal JourneyProcess Process { get; }

    /// <summary>Starts writing telemetry.</summary>
    public Task<LineFeedStatus> StartAsync() => PostAsync("control/start");

    /// <summary>Also writes orders, moving each from <c>open</c> to <c>shipped</c>, with an invalid order every few.</summary>
    public Task<LineFeedStatus> AttachOrdersAsync() => PostAsync("control/attach-orders");

    /// <summary>Stops writing.</summary>
    public Task<LineFeedStatus> PauseAsync() => PostAsync("control/pause");

    /// <summary>Writes again.</summary>
    public Task<LineFeedStatus> ResumeAsync() => PostAsync("control/resume");

    /// <summary>Writes the next <paramref name="count"/> orders with policy-valid but wrong (x100) totals.</summary>
    /// <param name="count">How many orders.</param>
    public Task<LineFeedStatus> InjectWrongPriceBurstAsync(int count) => PostAsync($"control/wrong-price-burst?count={count}");

    /// <summary>What it has done so far.</summary>
    public async Task<LineFeedStatus> StatusAsync() =>
        await _http.GetFromJsonAsync<LineFeedStatus>("control/status")
            ?? throw new InvalidOperationException("The line feed reported no status.");

    /// <summary>Waits until <paramref name="condition"/> holds for the line feed's status.</summary>
    /// <param name="condition">The condition.</param>
    /// <param name="timeout">How long to wait.</param>
    public async Task<LineFeedStatus> WaitUntilAsync(Func<LineFeedStatus, bool> condition, TimeSpan timeout)
    {
        ArgumentNullException.ThrowIfNull(condition);
        var deadline = DateTime.UtcNow + timeout;
        while (true)
        {
            var status = await StatusAsync();
            if (condition(status))
            {
                return status;
            }

            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException($"The line feed did not reach the expected state within {timeout.TotalSeconds:0} seconds; it reports {status}.");
            }

            await Task.Delay(250);
        }
    }

    private async Task<LineFeedStatus> PostAsync(string path)
    {
        using var response = await _http.PostAsync(path, content: null);
        response.EnsureSuccessStatusCode();
        return await response.Content.ReadFromJsonAsync<LineFeedStatus>()
            ?? throw new InvalidOperationException("The line feed reported no status.");
    }
}
