using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Client;
using Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Proves the <see cref="RepoContextToolInvocationLogger"/> decorator brackets a
/// real tool call end to end: driving <c>repocontext_health</c> over the MCP
/// protocol emits a start ("invoked") and a completion ("completed") line naming
/// the tool, under the decorator's dedicated category, captured through a logger
/// provider injected into the harness. The host adds the wall-clock timestamp
/// prefix to each console line separately (the Simple console formatter), so this
/// fixture asserts the message content the decorator is responsible for.
/// </summary>
/// <remarks>
/// Marked <c>Integration</c>: it co-hosts a real Orleans silo and an in-process
/// ASP.NET Core test server and drives the full MCP streamable-HTTP handshake, so
/// it is excluded from the fast unit dev loop. The decorator's wiring into the
/// group is covered by fast unit assertions in
/// <see cref="RepoContextToolGroupTests"/>.
/// </remarks>
[TestFixture]
[Category("Integration")]
public sealed class RepoContextToolInvocationLoggingTests
{
    [Test]
    public async Task Tool_call_logs_an_invoked_and_a_completed_line_for_the_tool()
    {
        var capturing = new CapturingLoggerProvider();
        await using var harness = await RepoContextMcpHarness.StartAsync(
            new RepoContextMcpHarnessOptions
            {
                Posture = RepoContextMcpAuthPosture.Writer,
                ConfigureServices = services =>
                    services.AddSingleton<ILoggerProvider>(capturing),
            },
            TestContext.CurrentContext.CancellationToken);
        await using var client = await harness.ConnectAsync(TestContext.CurrentContext.CancellationToken);

        _ = await client.CallToolAsync(
            "repocontext_health",
            cancellationToken: TestContext.CurrentContext.CancellationToken);

        var lines = capturing.Entries
            .Where(e => e.Category == RepoContextToolInvocationLogger.LogCategory)
            .Select(e => e.Message)
            .ToArray();

        Assert.Multiple(() =>
        {
            Assert.That(
                lines.Any(l => l.Contains("repocontext_health") && l.Contains("invoked")),
                Is.True,
                "Expected an 'invoked' line naming the tool.");
            Assert.That(
                lines.Any(l => l.Contains("repocontext_health") && l.Contains("completed")),
                Is.True,
                "Expected a 'completed' line naming the tool.");
        });
    }
}
