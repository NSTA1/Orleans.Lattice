using Grpc.Core;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Data.Grpc;

/// <summary>
/// Default <see cref="ILatticeDataApiActiveTenantBridge"/> that lifts a single
/// configurable request header into a <see cref="TenantId"/>. Reads the header
/// named by <see cref="LatticeDataApiGrpcOptions.ActiveTenantHeaderName"/>
/// (default <c>lattice-active-tenant</c>) and parses its value against the tenant
/// id grammar, returning the parsed tenant so the data-API service can stamp it as
/// the call's ambient active tenant.
/// </summary>
/// <remarks>
/// The asserted tenant is validated against the caller's subject membership by the
/// tenancy add-on downstream; this bridge performs no authorization and only
/// shuttles a syntactically valid tenant assertion onto the ambient scope. An
/// absent, empty, whitespace, or syntactically invalid header yields
/// <see langword="null"/> (no active tenant asserted), which the resolver treats
/// fail-closed.
/// </remarks>
internal sealed class HeaderLatticeDataApiActiveTenantBridge : ILatticeDataApiActiveTenantBridge
{
    private readonly IOptions<LatticeDataApiGrpcOptions> _options;

    /// <summary>
    /// Initialises the bridge with the resolved gRPC binding options.
    /// </summary>
    public HeaderLatticeDataApiActiveTenantBridge(IOptions<LatticeDataApiGrpcOptions> options)
    {
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    /// <inheritdoc />
    public TenantId? Resolve(ServerCallContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        var headerName = _options.Value.ActiveTenantHeaderName;
        if (string.IsNullOrEmpty(headerName))
        {
            return null;
        }

        // gRPC metadata keys are stored lower-cased; normalise the lookup so a
        // configured header name with any casing matches the inbound entry.
        var raw = context.RequestHeaders?.GetValue(headerName.ToLowerInvariant());
        if (string.IsNullOrWhiteSpace(raw))
        {
            return null;
        }

        // Fail-closed: a header that is not a syntactically valid tenant id is not
        // an assertion we honour - the call proceeds with no active tenant.
        return TenantId.TryParse(raw.Trim(), out var tenant) ? tenant : null;
    }
}
