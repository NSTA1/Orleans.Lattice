using ModelContextProtocol;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// The harness's own smoke test (issue #1441): proves the
/// <see cref="RepoContextMcpHarness"/> can bring up the repository-context MCP
/// server on a co-hosted in-memory Lattice cluster, that a granted caller
/// discovers and calls the read-only <c>repocontext_health</c> tool over the real
/// MCP protocol and gets the expected structured result, and that an
/// unauthenticated caller is default-denied (offered nothing and unable to invoke
/// the tool). If this fixture is green the tool sub-issues can build on the
/// harness rather than re-implementing bring-up.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it co-hosts a real Orleans silo and an in-process
/// ASP.NET Core test server and drives the full MCP streamable-HTTP handshake, so
/// it is excluded from the fast unit dev loop (matching the existing
/// <c>Orleans.Lattice.Api.Mcp</c> live-server tests). The harness's building
/// blocks - the posture presets, the client ergonomics, and the registration
/// wiring - are covered by fast unit fixtures elsewhere in this project.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextMcpHarnessSmokeTests
{
    [Test]
    public async Task Writer_discovers_and_calls_the_health_tool_over_mcp()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Writer },
            TestContext.CurrentContext.CancellationToken);
        await using var client = await harness.ConnectAsync(TestContext.CurrentContext.CancellationToken);

        var toolNames = await client.ListToolNamesAsync(TestContext.CurrentContext.CancellationToken);
        Assert.That(toolNames, Does.Contain("repocontext_health"),
            "A granted caller must be offered the repository-context health probe.");

        var result = await client.CallToolAsync(
            "repocontext_health",
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        var json = result.RequireStructuredContent();
        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            Assert.That(json.GetProperty("available").GetBoolean(), Is.True);
            Assert.That(json.GetProperty("group").GetString(), Is.EqualTo("repocontext"));
            Assert.That(json.GetProperty("status").GetString(), Is.Not.Null.And.Not.Empty);
        });
    }

    [Test]
    public async Task Harness_co_hosts_a_working_in_memory_lattice_cluster()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        // The co-hosted cluster is real: a tree round-trips a write through the
        // harness's grain factory, off the MCP path, so tool sub-issues can arrange
        // and assert tree state directly.
        var tree = harness.GrainFactory.GetGrain<ILattice>("repocontext-mcp-harness/facts");
        await tree.SetAsync("key", new byte[] { 1, 2, 3 });
        var value = await tree.GetAsync("key");

        Assert.That(value, Is.EqualTo(new byte[] { 1, 2, 3 }));
    }

    [Test]
    public async Task Unauthenticated_caller_is_offered_nothing_and_cannot_call_the_health_tool()
    {
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions { Posture = RepoContextMcpAuthPosture.Unauthenticated },
            TestContext.CurrentContext.CancellationToken);
        await using var client = await harness.ConnectAsync(TestContext.CurrentContext.CancellationToken);

        var toolNames = await client.ListToolNamesAsync(TestContext.CurrentContext.CancellationToken);
        Assert.That(toolNames, Is.Empty,
            "An unauthenticated session is fail-closed: it is offered no tools at all.");

        // The tool is not registered in this session, so a direct invocation fails
        // at the protocol layer (unknown tool) before it can reach any handler -
        // the default-denied end-to-end outcome.
        Assert.That(
            () => client.CallToolAsync(
                "repocontext_health",
                cancellationToken: TestContext.CurrentContext.CancellationToken).AsTask(),
            Throws.InstanceOf<McpException>());
    }
}
