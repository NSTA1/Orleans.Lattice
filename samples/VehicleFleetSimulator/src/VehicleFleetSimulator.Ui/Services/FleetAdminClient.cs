using System.Net.Http.Json;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace VehicleFleetSimulator.Ui.Services;

/// <summary>
/// HTTP entry-point for the small set of <c>/api/*</c> control-plane operations surfaced by
/// the Control flyout: spawn, reset, list scenario presets, and apply a preset. Other admin
/// endpoints exist on the API surface but aren't currently consumed by the UI; they can be
/// re-added here when (if) a future UI feature needs them.
/// </summary>
/// <remarks>
/// We intentionally don't reference <c>VehicleFleetSimulator.Abstractions</c> from the WASM
/// project; instead, we mirror the request/response shapes as small DTOs here so this client
/// is the only spot that has to know the wire format. The shapes are checked at runtime via
/// JSON; build errors here mean a real divergence has happened.
/// </remarks>
public sealed class FleetAdminClient
{
    private static readonly JsonSerializerOptions JsonOpts = new(JsonSerializerDefaults.Web);

    private readonly Uri _baseAddress;
    // Reusing a single HttpClient for the entire UI session avoids socket-exhaustion warnings
    // and lets keep-alive amortise TLS handshake costs across the dozens of small admin calls a
    // user typically makes per session.
    private readonly HttpClient _http;

    public FleetAdminClient(Uri baseAddress)
    {
        _baseAddress = baseAddress;
        _http = new HttpClient { BaseAddress = baseAddress };
    }

    public Uri BaseAddress => _baseAddress;

    // ─── Spawn / reset ───────────────────────────────────────────────────────

    public async Task<int> SpawnAsync(int count, string? startCityId, CancellationToken ct = default)
    {
        if (count <= 0) return 0;
        var specs = new VehicleSpecDto[count];
        for (int i = 0; i < count; i++) specs[i] = new VehicleSpecDto(null, startCityId);
        var resp = await _http.PostAsJsonAsync("/api/vehicles/batch", specs, JsonOpts, ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadFromJsonAsync<BatchSpawnResponse>(JsonOpts, ct).ConfigureAwait(false);
        return body?.Count ?? 0;
    }

    public async Task<ResetSummary> ResetAsync(CancellationToken ct = default)
    {
        var resp = await _http.DeleteAsync("/api/vehicles", ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        return await resp.Content.ReadFromJsonAsync<ResetSummary>(JsonOpts, ct).ConfigureAwait(false)
            ?? new ResetSummary(0, 0);
    }

    // ─── Scenarios ───────────────────────────────────────────────────────────

    public async Task<IReadOnlyList<ScenarioPresetDto>> ListScenariosAsync(CancellationToken ct = default)
    {
        try
        {
            var doc = await _http.GetFromJsonAsync<ScenariosResponse>("/api/scenarios", JsonOpts, ct).ConfigureAwait(false);
            return (IReadOnlyList<ScenarioPresetDto>?)doc?.Scenarios ?? Array.Empty<ScenarioPresetDto>();
        }
        catch { return Array.Empty<ScenarioPresetDto>(); }
    }

    public async Task<int> ApplyScenarioAsync(string name, CancellationToken ct = default)
    {
        var resp = await _http.PostAsync($"/api/scenarios/{Uri.EscapeDataString(name)}", content: null, ct).ConfigureAwait(false);
        resp.EnsureSuccessStatusCode();
        var body = await resp.Content.ReadFromJsonAsync<CountResponse>(JsonOpts, ct).ConfigureAwait(false);
        return body?.Count ?? 0;
    }

    // ─── DTOs (local, mirroring the API JSON shapes) ─────────────────────────

    private sealed record VehicleSpecDto(
        [property: JsonPropertyName("vehicleId")] Guid? VehicleId,
        [property: JsonPropertyName("startCityId")] string? StartCityId);

    private sealed record BatchSpawnResponse(
        [property: JsonPropertyName("count")] int Count);

    public sealed record ResetSummary(
        [property: JsonPropertyName("removed")] int Removed,
        [property: JsonPropertyName("stopped")] int Stopped);

    private sealed record CountResponse(
        [property: JsonPropertyName("count")] int? Count);

    public sealed record ScenarioPresetDto(
        [property: JsonPropertyName("name")] string Name,
        [property: JsonPropertyName("displayName")] string DisplayName,
        [property: JsonPropertyName("description")] string Description,
        [property: JsonPropertyName("vehicleCount")] int VehicleCount,
        [property: JsonPropertyName("startCityId")] string? StartCityId,
        [property: JsonPropertyName("resetFleetFirst")] bool ResetFleetFirst);

    private sealed record ScenariosResponse(
        [property: JsonPropertyName("scenarios")] List<ScenarioPresetDto>? Scenarios);
}
