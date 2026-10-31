using System.Buffers.Binary;
using System.Globalization;
using System.Text;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Hosting;

namespace Orleans.Lattice.Explorer.EndToEnd.Hosts.LineFeed;

/// <summary>What the line feed has done, as its control surface reports it.</summary>
/// <param name="Started">Whether <see cref="LineFeedService.Start"/> was called.</param>
/// <param name="Paused">Whether it is paused.</param>
/// <param name="OrdersAttached">Whether it also writes orders.</param>
/// <param name="TelemetryWritten">How many telemetry readings it wrote.</param>
/// <param name="OrdersWritten">How many order writes succeeded, transitions included.</param>
/// <param name="InvalidOrdersWritten">How many invalid (negative-total) orders it sent.</param>
/// <param name="WrongPricesWritten">How many policy-valid but wrong (x100) totals it wrote.</param>
/// <param name="ConsecutiveFailures">How many writes in a row have failed.</param>
/// <param name="LastError">The last write failure, or <see langword="null"/>.</param>
internal sealed record LineFeedStatus(
    bool Started,
    bool Paused,
    bool OrdersAttached,
    long TelemetryWritten,
    long OrdersWritten,
    long InvalidOrdersWritten,
    long WrongPricesWritten,
    int ConsecutiveFailures,
    string? LastError);

/// <summary>
/// The factory's upstream ingestion service. It does nothing until it is started, as a
/// real service is onboarded; then every tick it writes a telemetry reading and, once
/// orders are attached, moves an order along (<c>open</c> to <c>shipped</c>), sending an
/// invalid order every <c>LineFeed:InvalidEvery</c> orders. Every write goes through the
/// Data API as <c>line-feed</c>, so until that identity is granted rights its writes are
/// refused and reported, never retried around.
/// </summary>
internal sealed class LineFeedService : BackgroundService
{
    /// <summary>The telemetry tree; the first reading creates it.</summary>
    public const string TelemetryTree = "factory-telemetry";

    /// <summary>The orders tree.</summary>
    public const string OrdersTree = "factory-orders";

    /// <summary>The first order number the line feed allocates, clear of the seeded orders.</summary>
    public const int FirstOrder = 100_001;

    private static readonly string[] Sensors = ["press-1", "press-2", "oven-1", "conveyor-1"];

    private readonly LatticeDataApiGrpcClient _client;
    private readonly TimeSpan _interval;
    private readonly int _invalidEvery;
    private readonly ILogger<LineFeedService> _logger;
    private readonly Lock _gate = new();
    private int _wrongPriceBurst;
    private bool _started;
    private bool _paused;
    private bool _ordersAttached;
    private long _telemetry;
    private long _orders;
    private long _invalid;
    private long _wrongPrices;
    private long _sequence;
    private int _nextOrder = FirstOrder;
    private int? _openOrder;
    private int _consecutiveFailures;
    private string? _lastError;

    /// <summary>Creates the service from configuration.</summary>
    public LineFeedService(IConfiguration configuration, IServiceProvider services, ILogger<LineFeedService> logger)
    {
        _logger = logger;
        _interval = TimeSpan.FromMilliseconds(configuration.GetValue("LineFeed:IntervalMilliseconds", 500));
        _invalidEvery = Math.Max(2, configuration.GetValue("LineFeed:InvalidEvery", 10));

        var endpoint = configuration["LineFeed:Endpoint"]
            ?? throw new InvalidOperationException("LineFeed:Endpoint (the region's gateway) is required.");
        var user = configuration["LineFeed:User"] ?? "line-feed";
        var password = configuration["LineFeed:Password"]
            ?? throw new InvalidOperationException("LineFeed:Password is required.");

        var handler = new SocketsHttpHandler { EnableMultipleHttp2Connections = true };
        if (JourneyHostEnvironment.LoadCertificateAuthority() is { } authority)
        {
            handler.SslOptions.RemoteCertificateValidationCallback = JourneyHostEnvironment.TrustOnly(authority);
        }

        var channel = GrpcChannel.ForAddress(endpoint, new GrpcChannelOptions { HttpHandler = handler, DisposeHttpClient = true });
        var authorization = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes(user + ":" + password));
        var invoker = channel.Intercept(metadata =>
        {
            metadata.Add("authorization", authorization);
            return metadata;
        });
        _client = LatticeDataApiGrpcClient.Create(invoker, services);
    }

    /// <summary>Starts writing telemetry.</summary>
    public void Start()
    {
        lock (_gate)
        {
            _started = true;
            _paused = false;
        }
    }

    /// <summary>Also writes orders from now on.</summary>
    public void AttachOrders()
    {
        lock (_gate)
        {
            _ordersAttached = true;
        }
    }

    /// <summary>Stops writing until <see cref="Resume"/>.</summary>
    public void Pause()
    {
        lock (_gate)
        {
            _paused = true;
        }
    }

    /// <summary>Writes again after <see cref="Pause"/>.</summary>
    public void Resume()
    {
        lock (_gate)
        {
            _paused = false;
        }
    }

    /// <summary>
    /// Writes the next <paramref name="count"/> orders with a total one hundred times too
    /// large: valid by any schema, wrong in fact, as a faulty deploy writes them.
    /// </summary>
    /// <param name="count">How many orders.</param>
    public void InjectWrongPriceBurst(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(count);
        lock (_gate)
        {
            _wrongPriceBurst += count;
        }
    }

    /// <summary>What the line feed has done so far.</summary>
    public LineFeedStatus Status()
    {
        lock (_gate)
        {
            return new LineFeedStatus(
                _started, _paused, _ordersAttached, _telemetry, _orders, _invalid, _wrongPrices, _consecutiveFailures, _lastError);
        }
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(_interval);
        while (await timer.WaitForNextTickAsync(stoppingToken))
        {
            bool started, paused, orders;
            lock (_gate)
            {
                (started, paused, orders) = (_started, _paused, _ordersAttached);
            }

            if (!started || paused)
            {
                continue;
            }

            if (await WriteAsync(TelemetryTree, ReadingKey(_sequence), Reading(_sequence), counter: 0, stoppingToken))
            {
                // A refused reading is offered again, so the series has no gaps once granted.
                _sequence++;
            }

            if (orders)
            {
                await WriteNextOrderAsync(stoppingToken);
            }
        }
    }

    private static string ReadingKey(long sequence) =>
        $"reading/{Sensors[sequence % Sensors.Length]}/{sequence / Sensors.Length:D8}";

    private static byte[] Reading(long sequence)
    {
        var value = new byte[sizeof(double)];
        BinaryPrimitives.WriteDoubleLittleEndian(value, 20.0 + (Math.Sin(sequence / 10.0) * 5.0));
        return value;
    }

    private async Task WriteNextOrderAsync(CancellationToken cancellationToken)
    {
        // Ship the order opened last tick, then open the next one.
        if (_openOrder is { } open)
        {
            if (await WriteAsync(OrdersTree, OrderKey(open), Order(open, Total(open), "shipped"), counter: 1, cancellationToken))
            {
                _openOrder = null;
            }

            return;
        }

        var number = _nextOrder;
        var total = Total(number);
        var counter = 1;
        if ((number - FirstOrder + 1) % _invalidEvery == 0)
        {
            total = -total;
            counter = 2;
        }
        else
        {
            lock (_gate)
            {
                if (_wrongPriceBurst > 0)
                {
                    total *= 100;
                    counter = 3;
                }
            }
        }

        // An invalid order is sent once, as an upstream system sends it; whether it was
        // accepted or diverted is the schema policy's business.
        if (await WriteAsync(OrdersTree, OrderKey(number), Order(number, total, "open"), counter, cancellationToken) || counter == 2)
        {
            _nextOrder++;
            _openOrder = counter == 2 ? null : number;
            if (counter == 3)
            {
                lock (_gate)
                {
                    _wrongPriceBurst--;
                }
            }
        }
    }

    private static string OrderKey(int number) => "order/" + number.ToString("D6", CultureInfo.InvariantCulture);

    private static decimal Total(int number) => 10m + (number % 97);

    private static byte[] Order(int number, decimal total, string status) =>
        Encoding.UTF8.GetBytes(string.Create(
            CultureInfo.InvariantCulture,
            $"{{\"order\":{number},\"total\":{total},\"status\":\"{status}\",\"line\":\"line-{number % 3 + 1}\"}}"));

    private async Task<bool> WriteAsync(string tree, string key, byte[] value, int counter, CancellationToken cancellationToken)
    {
        try
        {
            await _client.SetAsync(new DataSetRequest { TreeId = tree, Key = key, Value = value }, cancellationToken);
            lock (_gate)
            {
                _consecutiveFailures = 0;
                switch (counter)
                {
                    case 0: _telemetry++; break;
                    case 2: _invalid++; break;
                    case 3: _wrongPrices++; _orders++; break;
                    default: _orders++; break;
                }
            }

            return true;
        }
        catch (RpcException ex) when (!cancellationToken.IsCancellationRequested)
        {
            lock (_gate)
            {
                if (counter == 2)
                {
                    _invalid++;
                }

                _consecutiveFailures++;
                _lastError = $"{tree}/{key}: {ex.StatusCode} {ex.Status.Detail}";
            }

            _logger.LogInformation("The line feed's write to {Tree}/{Key} failed: {Status}.", tree, key, ex.StatusCode);
            return false;
        }
    }
}
