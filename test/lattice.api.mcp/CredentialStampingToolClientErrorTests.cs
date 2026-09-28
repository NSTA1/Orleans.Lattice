using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;
using Orleans.Lattice.Testing;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for the client-error path of <see cref="CredentialStampingTool"/>:
/// a call rejected as the caller's mistake is answered with an error result,
/// logged at Debug without a stack, and counted - never thrown, because the
/// ModelContextProtocol SDK logs every thrown tool exception at Error with its
/// stack (issue #3761). Server faults and authorization denials must still throw.
/// </summary>
[TestFixture]
public sealed class CredentialStampingToolClientErrorTests
{
    [Test]
    public async Task A_marked_inner_fault_is_answered_as_an_error_result_not_thrown()
    {
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => throw McpToolClientErrors.NotFound($"No record exists at '{key}'."));
        var logs = new CapturingLoggerProvider();
        await using var services = Services(logs);
        using var counter = new ClientErrorCounter(name);

        var result = await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args(("key", "k1")));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.True);
            Assert.That(ErrorText(result), Is.EqualTo($"An error occurred invoking '{name}': No record exists at 'k1'."),
                "The caller must see exactly the text the SDK builds for a thrown McpException.");
            Assert.That(counter.Reasons, Is.EqualTo(new[] { LatticeApiMcpMetrics.ReasonNotFound }));
        });
        AssertLoggedAtDebugOnly(logs, name, LatticeApiMcpMetrics.ReasonNotFound);
    }

    [Test]
    public async Task A_missing_required_argument_is_answered_as_an_invalid_argument()
    {
        // The SDK's argument binder raises the fault before the tool body runs;
        // this pins that it is recognised by the assembly that actually raises it.
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => key);
        var logs = new CapturingLoggerProvider();
        await using var services = Services(logs);
        using var counter = new ClientErrorCounter(name);

        var result = await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args());

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.True);
            Assert.That(ErrorText(result), Does.Contain("could not bind its arguments").And.Contain("'key'"));
            Assert.That(counter.Reasons, Is.EqualTo(new[] { LatticeApiMcpMetrics.ReasonInvalidArgument }));
        });
        AssertLoggedAtDebugOnly(logs, name, LatticeApiMcpMetrics.ReasonInvalidArgument);
    }

    [Test]
    public async Task An_unknown_argument_is_answered_as_an_unknown_argument()
    {
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => key);
        var logs = new CapturingLoggerProvider();
        await using var services = Services(logs);
        using var counter = new ClientErrorCounter(name);

        var result = await McpToolInvocation.CallAsync(
            tool, services, McpToolInvocation.Args(("key", "k"), ("keyy", "k")));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.True);
            Assert.That(ErrorText(result), Does.Contain("does not accept the argument(s): 'keyy'"));
            Assert.That(counter.Reasons, Is.EqualTo(new[] { LatticeApiMcpMetrics.ReasonUnknownArgument }));
        });
        AssertLoggedAtDebugOnly(logs, name, LatticeApiMcpMetrics.ReasonUnknownArgument);
    }

    [Test]
    public async Task An_unmarked_McpException_still_throws()
    {
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => throw new McpException("server fault"));
        await using var services = Services(new CapturingLoggerProvider());
        using var counter = new ClientErrorCounter(name);

        var ex = Assert.ThrowsAsync<McpException>(async () =>
            await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args(("key", "k"))));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Is.EqualTo("server fault"));
            Assert.That(counter.Reasons, Is.Empty, "A server fault must not be counted as a client error.");
        });
    }

    [Test]
    public async Task An_ArgumentException_raised_by_the_tool_itself_still_throws()
    {
        // Only the binder's ArgumentException is a caller mistake; one from the
        // tool's own call chain is a server defect and must keep failing loudly.
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => throw new ArgumentException("tool defect"));
        await using var services = Services(new CapturingLoggerProvider());
        using var counter = new ClientErrorCounter(name);

        var ex = Assert.ThrowsAsync<McpException>(async () =>
            await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args(("key", "k"))));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain(nameof(ArgumentException)).And.Contain("tool defect"));
            Assert.That(McpToolClientErrors.TryGetReason(ex, out _), Is.False);
            Assert.That(counter.Reasons, Is.Empty);
        });
    }

    [Test]
    public async Task An_authorization_denial_still_throws_and_is_never_counted_as_a_client_error()
    {
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => key);
        await using var services = Services(new CapturingLoggerProvider(), new DenyAllMcpAuthorizer());
        using var counter = new ClientErrorCounter(name);

        var ex = Assert.ThrowsAsync<McpException>(async () =>
            await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args(("keyy", "k"))));

        Assert.Multiple(() =>
        {
            Assert.That(ex!.Message, Does.Contain("not authorized"),
                "A denial must be surfaced as a denial even when the call is also malformed.");
            Assert.That(counter.Reasons, Is.Empty);
        });
    }

    [Test]
    public async Task A_client_error_is_answered_when_no_logger_factory_is_registered()
    {
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => throw McpToolClientErrors.InvalidArgument("bad key"));
        await using var services = Services(logs: null);

        var result = await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args(("key", "k")));

        Assert.That(result.IsError, Is.True);
    }

    [Test]
    public async Task A_successful_call_is_neither_counted_nor_logged()
    {
        var name = UniqueToolName();
        var tool = Wrap(name, (string key) => key);
        var logs = new CapturingLoggerProvider();
        await using var services = Services(logs);
        using var counter = new ClientErrorCounter(name);

        var result = await McpToolInvocation.CallAsync(tool, services, McpToolInvocation.Args(("key", "k")));

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.Not.True);
            Assert.That(counter.Reasons, Is.Empty);
            Assert.That(logs.Entries.Where(e => e.EventId.Name == "McpToolClientError"), Is.Empty);
        });
    }

    [Test]
    public void ReportClientError_builds_the_sdk_error_result_shape()
    {
        var name = UniqueToolName();
        var logs = new CapturingLoggerProvider();
        using var services = Services(logs);
        using var counter = new ClientErrorCounter(name);

        var result = CredentialStampingTool.ReportClientError(
            services, name, "refused", McpToolClientErrorReason.RejectedContent);

        Assert.Multiple(() =>
        {
            Assert.That(result.IsError, Is.True);
            Assert.That(result.Content, Has.Count.EqualTo(1));
            Assert.That(ErrorText(result), Is.EqualTo($"An error occurred invoking '{name}': refused"));
            Assert.That(counter.Reasons, Is.EqualTo(new[] { LatticeApiMcpMetrics.ReasonRejectedContent }));
        });
        AssertLoggedAtDebugOnly(logs, name, LatticeApiMcpMetrics.ReasonRejectedContent);
    }

    private static string UniqueToolName() => "client_error_" + Guid.NewGuid().ToString("N")[..12];

    private static CredentialStampingTool Wrap(string name, Func<string, string> body)
        => new(McpServerTool.Create(body, new McpServerToolCreateOptions { Name = name }), LatticeApiMcpGroup.Data);

    private static ServiceProvider Services(CapturingLoggerProvider? logs, ILatticeApiMcpAuthorizer? authorizer = null)
    {
        var services = new ServiceCollection();
        services.AddSingleton<ILatticeApiMcpAuthorizer>(authorizer ?? new AllowAllMcpAuthorizer());
        services.AddSingleton<IHttpContextAccessor>(new HttpContextAccessor { HttpContext = new DefaultHttpContext() });
        if (logs is not null)
        {
            services.AddSingleton<ILoggerFactory>(logs);
        }

        return services.BuildServiceProvider();
    }

    private static string ErrorText(CallToolResult result)
        => result.Content.OfType<TextContentBlock>().Single().Text;

    private static void AssertLoggedAtDebugOnly(CapturingLoggerProvider logs, string toolName, string reason)
    {
        var entries = logs.Entries.Where(e => e.Message.Contains(toolName, StringComparison.Ordinal)).ToList();
        Assert.Multiple(() =>
        {
            Assert.That(entries, Has.Count.EqualTo(1), "The client error must be logged exactly once.");
            Assert.That(entries[0].Level, Is.EqualTo(LogLevel.Debug));
            Assert.That(entries[0].EventId.Name, Is.EqualTo("McpToolClientError"));
            Assert.That(entries[0].Exception, Is.Null, "A client error must not be logged with a stack.");
            Assert.That(entries[0].Category, Is.EqualTo(typeof(CredentialStampingTool).FullName));
            Assert.That(entries[0].Message, Does.Contain($"({reason})"));
        });
    }

    private sealed class ClientErrorCounter : IDisposable
    {
        private readonly List<string> _reasons = [];
        private readonly System.Diagnostics.Metrics.MeterListener _listener;

        public ClientErrorCounter(string toolName)
        {
            _listener = MeterListening.StartForInstrument(LatticeApiMcpMetrics.ToolClientErrors, l =>
                l.SetMeasurementEventCallback<long>((_, _, tags, _) =>
                {
                    string? tool = null;
                    string? reason = null;
                    foreach (var tag in tags)
                    {
                        if (tag.Key == LatticeApiMcpMetrics.TagTool)
                        {
                            tool = tag.Value as string;
                        }
                        else if (tag.Key == LatticeApiMcpMetrics.TagReason)
                        {
                            reason = tag.Value as string;
                        }
                    }

                    if (tool == toolName && reason is not null)
                    {
                        lock (_reasons)
                        {
                            _reasons.Add(reason);
                        }
                    }
                }));
        }

        public IReadOnlyList<string> Reasons
        {
            get
            {
                lock (_reasons)
                {
                    return _reasons.ToArray();
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
