using System.Text.Json;
using System.Text.Json.Serialization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Scaling;

/// <summary>
/// Endpoint-routing extensions that expose the
/// <see cref="ILatticeScalingSignal"/> facade over HTTP for an external
/// autoscaler to scrape.
/// </summary>
/// <remarks>
/// The endpoint serves the current cluster-aggregate <see cref="ScalingSignal"/>
/// as JSON with a stable, documented, camelCase shape whose top-level
/// <c>scaleValue</c> property is the scalar a KEDA <c>metrics-api</c> scaler (or
/// an Azure Container Apps <c>custom</c> metrics-api scale rule) reads via
/// <c>valueLocation: "scaleValue"</c>. The JSON contract is produced with an
/// explicit, statically cached <see cref="JsonSerializerOptions"/> so the
/// property names are independent of any ambient host JSON configuration.
/// <para>
/// The endpoint is unauthenticated by default because it is a scrape target,
/// but it composes with the host pipeline (authorization, rate limiting, and
/// so on) like any other mapped endpoint.
/// </para>
/// </remarks>
public static class LatticeScalingEndpointRouteBuilderExtensions
{
    /// <summary>
    /// Serializer options for the scrape response: camelCase property names and
    /// string-valued enums, cached once so no per-request options allocation
    /// occurs. Enums are emitted as strings so the compute WAL-saturation
    /// classification is human-legible in the scraped body; the numeric
    /// <c>scaleValue</c> the autoscaler reads is unaffected.
    /// </summary>
    private static readonly JsonSerializerOptions ResponseJsonOptions = new(JsonSerializerDefaults.Web)
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        Converters = { new JsonStringEnumConverter() },
    };

    /// <summary>
    /// Maps a <c>GET</c> endpoint that returns the current
    /// <see cref="ScalingSignal"/> as JSON. The route is
    /// <paramref name="path"/> when supplied, otherwise the configured
    /// <see cref="LatticeScalingSignalOptions.EndpointPath"/> (default
    /// <see cref="LatticeScalingSignalOptions.DefaultEndpointPath"/>).
    /// </summary>
    /// <param name="endpoints">The endpoint route builder to map onto.</param>
    /// <param name="path">
    /// Optional explicit route to serve from. When <see langword="null"/> the
    /// path is resolved from the bound
    /// <see cref="LatticeScalingSignalOptions"/>, falling back to
    /// <see cref="LatticeScalingSignalOptions.DefaultEndpointPath"/> when the
    /// options are not registered.
    /// </param>
    /// <returns>
    /// The <see cref="IEndpointConventionBuilder"/> for the mapped endpoint so
    /// callers can chain conventions (authorization, tags, and so on).
    /// </returns>
    /// <exception cref="ArgumentNullException">
    /// <paramref name="endpoints"/> is <see langword="null"/>.
    /// </exception>
    public static IEndpointConventionBuilder MapLatticeScalingSignal(
        this IEndpointRouteBuilder endpoints,
        string? path = null)
    {
        ArgumentNullException.ThrowIfNull(endpoints);

        var route = path
            ?? endpoints.ServiceProvider
                .GetService<IOptions<LatticeScalingSignalOptions>>()?.Value.EndpointPath
            ?? LatticeScalingSignalOptions.DefaultEndpointPath;

        return endpoints.MapGet(route, static async (
            HttpContext httpContext,
            ILatticeScalingSignal signal,
            CancellationToken cancellationToken) =>
        {
            var snapshot = await signal.GetScalingSignalAsync(cancellationToken).ConfigureAwait(false);
            await httpContext.Response
                .WriteAsJsonAsync(snapshot, ResponseJsonOptions, cancellationToken)
                .ConfigureAwait(false);
        });
    }
}
