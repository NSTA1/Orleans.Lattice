using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Graph;

/// <summary>
/// Unit tests for the argument validation the three graph tool handlers perform before
/// they touch the request context or the store. A missing repository id or path must
/// fault fast with an <see cref="McpException"/>, which is safe to assert with a null
/// context because validation runs first.
/// </summary>
[TestFixture]
public sealed class RepoContextGraphHandlerValidationTests
{
    [TestCase("", "src/A.cs")]
    [TestCase("  ", "src/A.cs")]
    [TestCase("acme", "")]
    [TestCase("acme", "   ")]
    public void OutlineAsync_missing_repoId_or_path_throws(string repoId, string path)
        => Assert.That(
            () => RepoContextToolHandlers.OutlineAsync(null!, repoId, path),
            Throws.InstanceOf<McpException>());

    [TestCase("", "src/A.cs")]
    [TestCase("  ", "src/A.cs")]
    [TestCase("acme", "")]
    [TestCase("acme", "   ")]
    public void RelatedAsync_missing_repoId_or_path_throws(string repoId, string path)
        => Assert.That(
            () => RepoContextToolHandlers.RelatedAsync(null!, repoId, path),
            Throws.InstanceOf<McpException>());

    [TestCase("", "workspace")]
    [TestCase("  ", "workspace")]
    [TestCase("acme", "")]
    [TestCase("acme", "   ")]
    public void ChangedAsync_missing_repoId_or_path_throws(string repoId, string path)
        => Assert.That(
            async () => await RepoContextToolHandlers.ChangedAsync(null!, repoId, path),
            Throws.InstanceOf<McpException>());
}
