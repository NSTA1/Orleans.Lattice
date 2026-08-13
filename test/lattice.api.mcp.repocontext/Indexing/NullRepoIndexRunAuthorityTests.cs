namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Indexing;

/// <summary>
/// Unit tests for the default <see cref="IRepoIndexRunAuthority"/> the package
/// registers. The default resolves no credential, so a host that registers no
/// authority runs indexing under whatever ambient credential the enqueue captured
/// - the pre-existing behaviour for an in-process host whose access gate is not
/// enabled. A host that enforces a fail-closed gate replaces it with an authority
/// that resolves a fixed run credential (proven by the container host's
/// LocalTrustedRunAuthority tests).
/// </summary>
[TestFixture]
public sealed class NullRepoIndexRunAuthorityTests
{
    [Test]
    public void Default_authority_resolves_no_credential()
        => Assert.That(new NullRepoIndexRunAuthority().Resolve(), Is.Null);
}
