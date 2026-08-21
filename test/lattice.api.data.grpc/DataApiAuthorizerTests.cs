using Grpc.Core;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Unit coverage for the two built-in coarse authorizers that decide whether an
/// inbound data-API call may reach the data plane at all. The default-deny
/// authorizer keeps the write-capable surface closed; the opt-in allow-all
/// authorizer defers every decision to the per-tree / per-key access gate.
/// </summary>
[TestFixture]
public sealed class DataApiAuthorizerTests
{
    private static LatticeDataApiAuthorizationContext Context() =>
        new(
            new StubServerCallContext(),
            LatticeDataApiOperation.SetPoint,
            "tree-a");

    [Test]
    public async Task AllowAll_authorizes_every_call()
    {
        var authorizer = new AllowAllDataApiAuthorizer();

        var authorized = await authorizer.IsAuthorizedAsync(Context(), CancellationToken.None);

        Assert.That(authorized, Is.True);
    }

    [Test]
    public async Task DenyAll_rejects_every_call()
    {
        var authorizer = new DenyAllDataApiAuthorizer();

        var authorized = await authorizer.IsAuthorizedAsync(Context(), CancellationToken.None);

        Assert.That(authorized, Is.False);
    }
}
