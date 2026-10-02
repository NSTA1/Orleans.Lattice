using Grpc.Core;

namespace Orleans.Lattice.Api.Apps.Grpc;

/// <summary>An operation classified locally from the bound RPC, with a caller-asserted app slug.</summary>
/// <param name="Call">Inbound context containing credentials and cancellation.</param>
/// <param name="Operation">The server-classified operation, never supplied by the payload.</param>
/// <param name="AppSlug">Asserted target slug, or null for the listing and capability calls, which name no app.</param>
public readonly record struct LatticeAppsApiAuthorizationContext(
    ServerCallContext Call, LatticeAppsApiOperation Operation, string? AppSlug);
