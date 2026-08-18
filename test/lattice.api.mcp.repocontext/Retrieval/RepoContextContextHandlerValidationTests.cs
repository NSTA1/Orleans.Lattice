using ModelContextProtocol;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Retrieval;

/// <summary>
/// Unit tests for the argument validation <see cref="RepoContextToolHandlers.ContextAsync"/>
/// performs before it touches the request context or resolves any service. A missing
/// repository id or task must fault fast with an <see cref="McpException"/>, which is
/// safe to assert with a null context because validation runs first.
/// </summary>
[TestFixture]
public sealed class RepoContextContextHandlerValidationTests
{
    [TestCase("", "add a widget")]
    [TestCase("   ", "add a widget")]
    [TestCase("acme", "")]
    [TestCase("acme", "   ")]
    public void ContextAsync_missing_repoId_or_task_throws(string repoId, string task)
        => Assert.That(
            () => RepoContextToolHandlers.ContextAsync(null!, repoId, task),
            Throws.InstanceOf<McpException>());
}
