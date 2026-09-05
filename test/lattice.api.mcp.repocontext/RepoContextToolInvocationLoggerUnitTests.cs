using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using ModelContextProtocol.Protocol;
using ModelContextProtocol.Server;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests;

/// <summary>
/// Unit coverage for the three arms of <see cref="RepoContextToolInvocationLogger"/>
/// that the end-to-end fixture cannot reach: the delegate-only path taken when no
/// logger factory can be resolved from the request scope, and the cancellation and
/// failure arms that must log and then rethrow.
/// <para>
/// The decorator is deliberately transparent, so the guarantee worth pinning is
/// that it never changes what the caller sees - a fault or a cancellation still
/// propagates unchanged, with the same exception instance - while still leaving a
/// timed line behind.
/// </para>
/// </summary>
[TestFixture]
public sealed class RepoContextToolInvocationLoggerUnitTests
{
    /// <summary>
    /// A minimal tool whose invocation the test drives: it returns, cancels, or
    /// throws on demand, and counts how many times it was actually called so the
    /// decorator can be proven to delegate exactly once.
    /// </summary>
    private sealed class StubTool(Func<CallToolResult> invoke) : McpServerTool
    {
        public int Calls { get; private set; }

        public override Tool ProtocolTool { get; } = new() { Name = "repocontext_probe" };

        /// <inheritdoc />
        public override IReadOnlyList<object> Metadata { get; } = [];

        public override ValueTask<CallToolResult> InvokeAsync(
            RequestContext<CallToolRequestParams> request,
            CancellationToken cancellationToken = default)
        {
            Calls++;
            return new ValueTask<CallToolResult>(invoke());
        }
    }

    private sealed class CapturingProvider : ILoggerProvider
    {
        public List<(string Category, LogLevel Level, string Message, Exception? Exception)> Entries { get; } = [];

        public ILogger CreateLogger(string categoryName) => new Sink(this, categoryName);

        public void Dispose()
        {
        }

        private sealed class Sink(CapturingProvider owner, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => true;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter)
            {
                lock (owner.Entries)
                {
                    owner.Entries.Add((category, logLevel, formatter(state, exception), exception));
                }
            }
        }
    }

    private static readonly CallToolResult Ok = new() { Content = [] };

    private static Task<RequestContext<CallToolRequestParams>> ContextWithLogging(CapturingProvider provider)
    {
        var services = new ServiceCollection();
        services.AddLogging(b => b.AddProvider(provider).SetMinimumLevel(LogLevel.Trace));
        return RepoContextRequestContexts.CreateAsync(services.BuildServiceProvider());
    }

    private static IReadOnlyList<string> LinesFor(CapturingProvider provider)
    {
        lock (provider.Entries)
        {
            return provider.Entries
                .Where(e => e.Category == RepoContextToolInvocationLogger.LogCategory)
                .Select(e => e.Message)
                .ToArray();
        }
    }

    [Test]
    public async Task Without_a_logger_factory_the_decorator_just_delegates()
    {
        // A request scope with no logging registered must not fault the call: the
        // decorator adds nothing and the inner tool's result passes through.
        var inner = new StubTool(() => Ok);
        var decorated = new RepoContextToolInvocationLogger(inner);
        var context = await RepoContextRequestContexts.CreateAsync(
            new ServiceCollection().BuildServiceProvider());

        var result = await decorated.InvokeAsync(context, TestContext.CurrentContext.CancellationToken);

        Assert.Multiple(() =>
        {
            Assert.That(result, Is.SameAs(Ok));
            Assert.That(inner.Calls, Is.EqualTo(1), "the inner tool must still be invoked exactly once");
        });
    }

    [Test]
    public async Task A_null_request_service_provider_is_tolerated()
    {
        var inner = new StubTool(() => Ok);
        var decorated = new RepoContextToolInvocationLogger(inner);
        var context = await RepoContextRequestContexts.CreateAsync(services: null);

        var result = await decorated.InvokeAsync(context, TestContext.CurrentContext.CancellationToken);

        Assert.That(result, Is.SameAs(Ok));
    }

    [Test]
    public void A_null_request_is_rejected()
    {
        var decorated = new RepoContextToolInvocationLogger(new StubTool(() => Ok));

        Assert.That(
            async () => await decorated.InvokeAsync(null!, CancellationToken.None),
            Throws.ArgumentNullException);
    }

    [Test]
    public async Task A_successful_call_is_bracketed_by_an_invoked_and_a_completed_line()
    {
        var provider = new CapturingProvider();
        var decorated = new RepoContextToolInvocationLogger(new StubTool(() => Ok));
        var context = await ContextWithLogging(provider);

        await decorated.InvokeAsync(context, TestContext.CurrentContext.CancellationToken);

        var lines = LinesFor(provider);
        Assert.Multiple(() =>
        {
            Assert.That(lines.Any(l => l.Contains("repocontext_probe") && l.Contains("invoked")), Is.True);
            Assert.That(lines.Any(l => l.Contains("repocontext_probe") && l.Contains("completed")), Is.True);
        });
    }

    [Test]
    public async Task A_cancelled_call_logs_a_cancelled_line_and_rethrows()
    {
        // Cancellation is not a failure, so it must be logged at information
        // level and not as a warning with an exception attached.
        var provider = new CapturingProvider();
        var cancellation = new OperationCanceledException("client went away");
        var decorated = new RepoContextToolInvocationLogger(new StubTool(() => throw cancellation));
        var context = await ContextWithLogging(provider);

        var thrown = Assert.ThrowsAsync<OperationCanceledException>(
            async () => await decorated.InvokeAsync(context, CancellationToken.None));

        var entries = provider.Entries
            .Where(e => e.Category == RepoContextToolInvocationLogger.LogCategory)
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(cancellation), "the original cancellation must propagate unchanged");
            Assert.That(
                entries.Any(e => e.Message.Contains("repocontext_probe") && e.Message.Contains("cancelled")),
                Is.True);
            Assert.That(
                entries.Any(e => e.Message.Contains("completed")), Is.False,
                "a cancelled call must not also be reported as completed");
            Assert.That(entries.Any(e => e.Level >= LogLevel.Warning), Is.False,
                "cancellation is an ordinary outcome, not a fault");
        });
    }

    [Test]
    public async Task A_failed_call_logs_a_warning_with_the_exception_and_rethrows()
    {
        var provider = new CapturingProvider();
        var failure = new InvalidOperationException("the store is unreachable");
        var decorated = new RepoContextToolInvocationLogger(new StubTool(() => throw failure));
        var context = await ContextWithLogging(provider);

        var thrown = Assert.ThrowsAsync<InvalidOperationException>(
            async () => await decorated.InvokeAsync(context, TestContext.CurrentContext.CancellationToken));

        var warnings = provider.Entries
            .Where(e => e.Category == RepoContextToolInvocationLogger.LogCategory && e.Level >= LogLevel.Warning)
            .ToArray();
        Assert.Multiple(() =>
        {
            Assert.That(thrown, Is.SameAs(failure), "the original fault must propagate unchanged");
            Assert.That(warnings, Has.Length.EqualTo(1));
            Assert.That(warnings[0].Message, Does.Contain("repocontext_probe").And.Contain("failed"));
            Assert.That(warnings[0].Exception, Is.SameAs(failure),
                "the fault must be attached to the record, not just formatted into the message");
        });
    }

    [Test]
    public async Task The_decorator_advertises_the_inner_tool_verbatim()
    {
        // The group's discovery and schema tests must not be able to tell the
        // decorator apart from the tool it wraps.
        var inner = new StubTool(() => Ok);
        var decorated = new RepoContextToolInvocationLogger(inner);
        var provider = new CapturingProvider();
        var context = await ContextWithLogging(provider);

        await decorated.InvokeAsync(context, TestContext.CurrentContext.CancellationToken);

        Assert.That(decorated.ProtocolTool.Name, Is.EqualTo(inner.ProtocolTool.Name));
    }
}
