using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Fast unit coverage for the harness's auth-posture wiring: the stub credential
/// bridge and stub permission resolver that make a posture deterministic, and the
/// options defaults. These run in the unit tier without a live server; the full
/// posture behaviour end to end is proven by
/// <see cref="RepoContextMcpHarnessSmokeTests"/>.
/// </summary>
[TestFixture]
public sealed class RepoContextMcpHarnessPostureTests
{
    [Test]
    public void Stub_credential_bridge_returns_the_configured_credential()
    {
        var credential = new LatticeCredential("reader");
        var bridge = new RepoContextMcpStubCredentialBridge(credential);

        var resolved = bridge.Resolve(new Microsoft.AspNetCore.Http.DefaultHttpContext());
        Assert.That(resolved, Is.EqualTo(credential));
    }

    [Test]
    public void Stub_credential_bridge_returns_null_for_the_anonymous_posture()
    {
        var bridge = new RepoContextMcpStubCredentialBridge(null);
        Assert.That(bridge.Resolve(new Microsoft.AspNetCore.Http.DefaultHttpContext()), Is.Null);
    }

    [Test]
    public async Task Stub_permission_resolver_returns_the_configured_access_set()
    {
        var access = LatticeApiMcpAccessSet.None.With(LatticeApiMcpGroup.RepoContext);
        var resolver = new RepoContextMcpStubPermissionResolver(access);

        var resolved = await resolver.ResolveAsync(
            new LatticeCredential("writer"), TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(resolved.Contains(LatticeApiMcpGroup.RepoContext), Is.True);
            Assert.That(resolved.Contains(LatticeApiMcpGroup.Data), Is.False);
        });
    }

    [Test]
    public void Options_default_to_the_writer_posture()
        => Assert.That(new RepoContextMcpHarnessOptions().Posture,
            Is.EqualTo(RepoContextMcpAuthPosture.Writer));
}
