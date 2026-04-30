using System.Net.Http.Json;
using Grpc.Core;
using Grpc.Net.Client;
using Grpc.Net.Client.Web;
using Microsoft.Extensions.Logging;
using VehicleFleetSimulator.Api.Grpc;
using VehicleFleetSimulator.Ui.Models;

namespace VehicleFleetSimulator.Ui.Services;

public enum FleetConnectionState
{
    Disconnected,
    Connecting,
    Connected,
    Error,
}

/// <summary>
/// Owns the long-lived gRPC-Web telemetry subscription against the simulator's
/// <c>FleetStream.SubscribeTelemetry</c> RPC. Drives <see cref="FleetState"/> and surfaces
/// connection status to the UI.
/// </summary>
/// <remarks>
/// <para>
/// The supervisor loop fetches the city graph (HTTP), opens the gRPC-Web stream, and reads it
/// to completion. Any failure transitions the state to <see cref="FleetConnectionState.Error"/>,
/// then the loop sleeps a fixed <see cref="ReconnectInterval"/> and retries — forever, until
/// the component disposes us. The 5-second cadence is intentionally constant (no exponential
/// backoff); the simulator and UI are expected to live on the same private network during a
/// benchmark run, so quick recovery beats politeness.
/// </para>
/// <para>
/// gRPC-Web is mandatory: a browser cannot speak HTTP/2 trailers directly, so the
/// <see cref="GrpcWebHandler"/> wraps an <see cref="HttpClient"/> that the matching server-side
/// <c>UseGrpcWeb</c> middleware unwraps.
/// </para>
/// </remarks>
public sealed class FleetStreamClient : IAsyncDisposable
{
    public static readonly TimeSpan ReconnectInterval = TimeSpan.FromSeconds(5);

    private readonly Uri _baseAddress;
    private readonly FleetState _state;
    private readonly ILogger<FleetStreamClient> _logger;
    private readonly CancellationTokenSource _cts = new();
    private Task? _supervisor;

    public FleetStreamClient(Uri baseAddress, FleetState state, ILogger<FleetStreamClient> logger)
    {
        _baseAddress = baseAddress;
        _state = state;
        _logger = logger;
    }

    public FleetConnectionState ConnectionState { get; private set; } = FleetConnectionState.Disconnected;
    public string? LastError { get; private set; }
    public CityGraphDto? CityGraph { get; private set; }

    public event Action? StateChanged;
    public event Action? CityGraphLoaded;

    public void Start()
    {
        _supervisor ??= Task.Run(() => SuperviseAsync(_cts.Token));
    }

    private async Task SuperviseAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            SetState(FleetConnectionState.Connecting, error: null);
            try
            {
                using var http = new HttpClient(new GrpcWebHandler(GrpcWebMode.GrpcWeb, new HttpClientHandler()))
                {
                    BaseAddress = _baseAddress,
                };

                if (CityGraph is null)
                {
                    using var bareHttp = new HttpClient { BaseAddress = _baseAddress };
                    var doc = await bareHttp.GetFromJsonAsync<CitiesResponse>("/api/cities", ct).ConfigureAwait(false);
                    if (doc is not null)
                    {
                        CityGraph = new CityGraphDto(
                            doc.Cities ?? [],
                            doc.Edges ?? []);
                        CityGraphLoaded?.Invoke();
                    }
                }

                using var channel = GrpcChannel.ForAddress(_baseAddress, new GrpcChannelOptions
                {
                    HttpClient = http,
                    DisposeHttpClient = false,
                });
                var client = new FleetStream.FleetStreamClient(channel);
                using var call = client.SubscribeTelemetry(new TelemetryFilter(), cancellationToken: ct);
                SetState(FleetConnectionState.Connected, error: null);
                await foreach (var batch in call.ResponseStream.ReadAllAsync(ct).ConfigureAwait(false))
                {
                    // Server coalesces ~50 ms of telemetry into each batch; iterate inline so
                    // FleetState.Apply still sees one message at a time but we pay protobuf
                    // decode cost once per frame rather than once per vehicle tick.
                    var messages = batch.Messages;
                    for (int i = 0; i < messages.Count; i++)
                    {
                        _state.Apply(messages[i]);
                    }
                }
                // Server closed the stream cleanly; treat it as a transient and retry.
                SetState(FleetConnectionState.Disconnected, error: "Stream closed by server.");
            }
            catch (OperationCanceledException) when (ct.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "Telemetry stream failed, retrying in {Interval}.", ReconnectInterval);
                SetState(FleetConnectionState.Error, ex.Message);
            }

            try
            {
                await Task.Delay(ReconnectInterval, ct).ConfigureAwait(false);
            }
            catch (OperationCanceledException) { return; }
        }
    }

    private void SetState(FleetConnectionState newState, string? error)
    {
        ConnectionState = newState;
        LastError = error;
        StateChanged?.Invoke();
    }

    public async ValueTask DisposeAsync()
    {
        _cts.Cancel();
        if (_supervisor is not null)
        {
            try { await _supervisor.ConfigureAwait(false); } catch { /* ignore */ }
        }
        _cts.Dispose();
    }

    private sealed record CitiesResponse(
        List<CityDto>? Cities,
        List<EdgeDto>? Edges,
        Dictionary<string, CityPositionDto>? PositionOverrides);
}
