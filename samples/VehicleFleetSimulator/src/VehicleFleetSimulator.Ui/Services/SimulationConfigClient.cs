using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace VehicleFleetSimulator.Ui.Services;

/// <summary>
/// Thin HTTP client over <c>/api/config/simulation</c>. Exposes only the fields the UI needs to
/// read and write — the full simulator config surface is intentionally not modelled here so the
/// abstractions package doesn't have to be referenced by the WASM project.
/// </summary>
public sealed class SimulationConfigClient
{
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    private readonly Uri _baseAddress;

    public SimulationConfigClient(Uri baseAddress)
    {
        _baseAddress = baseAddress;
    }

    public async Task<double?> GetTimeScaleAsync(CancellationToken ct = default)
    {
        try
        {
            using var http = new HttpClient { BaseAddress = _baseAddress };
            var doc = await http.GetFromJsonAsync<SimulationConfigDto>("/api/config/simulation", JsonOpts, ct)
                .ConfigureAwait(false);
            return doc?.TimeScale;
        }
        catch
        {
            // The slider falls back to its default if the API can't be reached on first load.
            return null;
        }
    }

    public async Task SetTimeScaleAsync(double value, CancellationToken ct = default)
    {
        using var http = new HttpClient { BaseAddress = _baseAddress };
        var resp = await http.PutAsJsonAsync(
            "/api/config/simulation",
            new SimulationConfigPatchDto(TimeScale: value),
            JsonOpts,
            ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
    }

    private sealed record SimulationConfigDto(
        [property: JsonPropertyName("timeScale")] double TimeScale);

    private sealed record SimulationConfigPatchDto(
        [property: JsonPropertyName("timeScale")] double? TimeScale);
}
