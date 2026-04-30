using Microsoft.AspNetCore.Http;

namespace VehicleFleetSimulator.Api.Services;

/// <summary>
/// Endpoint filter mirror of <see cref="ApiKeyInterceptor"/> for the REST surface. Looks for
/// <c>x-api-key</c> in the request headers and rejects with 401 when it doesn't match the
/// configured <c>Auth:ApiKey</c>. When no key is configured (typical in dev) the filter is a
/// pass-through, matching the gRPC interceptor's behaviour exactly.
/// </summary>
public sealed class ApiKeyEndpointFilter : IEndpointFilter
{
    public const string HeaderName = "x-api-key";

    private readonly string? _expectedKey;

    public ApiKeyEndpointFilter(IConfiguration configuration)
    {
        _expectedKey = configuration["Auth:ApiKey"];
    }

    public ValueTask<object?> InvokeAsync(EndpointFilterInvocationContext context, EndpointFilterDelegate next)
    {
        if (string.IsNullOrEmpty(_expectedKey))
            return next(context); // disabled when unconfigured (parity with ApiKeyInterceptor)

        // StringValues compares case-insensitive on header names but the value is exact. We do
        // an ordinal compare of the value to refuse a key that only differs in case (api keys
        // are credentials, not identifiers).
        var supplied = context.HttpContext.Request.Headers[HeaderName];
        if (supplied.Count == 0 || !string.Equals(supplied.ToString(), _expectedKey, StringComparison.Ordinal))
        {
            return new ValueTask<object?>(Results.Problem(
                title: "Unauthorized",
                detail: "Missing or invalid x-api-key header.",
                statusCode: StatusCodes.Status401Unauthorized));
        }

        return next(context);
    }
}
