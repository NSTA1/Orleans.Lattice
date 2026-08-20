using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Data.Grpc.Tests;

/// <summary>
/// Additional unit coverage for <see cref="HeaderLatticeDataApiCredentialBridge"/>
/// focused on the short-circuit when the configured credential header name has been
/// blanked out - the bridge resolves no credential without ever inspecting the
/// inbound metadata.
/// </summary>
[TestFixture]
public sealed class HeaderLatticeDataApiCredentialBridgeEmptyHeaderTests
{
    [Test]
    public void Resolve_returns_null_when_the_credential_header_name_is_empty()
    {
        var options = Options.Create(new LatticeDataApiGrpcOptions { CredentialHeaderName = string.Empty });
        var bridge = new HeaderLatticeDataApiCredentialBridge(options);

        var credential = bridge.Resolve(new StubServerCallContext("/orleans.lattice.api.data/Get"));

        Assert.That(credential, Is.Null);
    }
}
