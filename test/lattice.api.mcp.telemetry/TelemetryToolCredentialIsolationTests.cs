using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Tests;

/// <summary>
/// Credential-isolation tests for the telemetry tool group's error path.
/// <para>
/// The telemetry backend credential is deliberately isolated: it is minted for the
/// backend only and is never the caller's own credential, precisely so a remote MCP
/// caller can query telemetry without ever being able to obtain it. An exception
/// message is an uncontrolled channel - the proxy does not own every
/// <see cref="HttpMessageHandler"/> in its own pipeline - so interpolating a caught
/// exception's text into a tool result would hand that credential back to the
/// caller. Every tool in the group is exercised here with a handler that throws
/// with the observed <c>Authorization</c> value, under both static auth modes.
/// </para>
/// </summary>
[TestFixture]
public sealed class TelemetryToolCredentialIsolationTests
{
    private const string BackendBase = "https://prometheus.internal:9090/";
    private const string SecretToken = "SECRET-TOKEN-VALUE";
    private const string SecretUser = "SECRET-USER";
    private const string SecretPassword = "SECRET-PASSWORD";

    private static PrometheusQueryClient BearerClient(out AuthorizationEchoingHttpMessageHandler handler)
        => Client(
            out handler,
            new LatticeApiMcpTelemetryOptions
            {
                AuthMode = LatticeTelemetryBackendAuthMode.Bearer,
                Credential = new LatticeTelemetryBackendCredential { BearerToken = SecretToken },
            });

    private static PrometheusQueryClient BasicClient(out AuthorizationEchoingHttpMessageHandler handler)
        => Client(
            out handler,
            new LatticeApiMcpTelemetryOptions
            {
                AuthMode = LatticeTelemetryBackendAuthMode.Basic,
                Credential = new LatticeTelemetryBackendCredential
                {
                    BasicUsername = SecretUser,
                    BasicPassword = SecretPassword,
                },
            });

    private static PrometheusQueryClient Client(
        out AuthorizationEchoingHttpMessageHandler handler,
        LatticeApiMcpTelemetryOptions options)
    {
        handler = new AuthorizationEchoingHttpMessageHandler();
        var http = new HttpClient(handler) { BaseAddress = new Uri(BackendBase) };
        return new PrometheusQueryClient(http, Options.Create(options));
    }

    private static TelemetryMetricAccessPolicy ReadAll()
        => new(new LatticeApiMcpTelemetryOptions());

    private static IOptions<LatticeApiMcpTelemetryOptions> Guardrails()
        => Options.Create(new LatticeApiMcpTelemetryOptions());

    /// <summary>
    /// Asserts the tool result is a clean failure that carries no fragment of the
    /// backend credential, and that the adversarial handler really did observe one
    /// (so a vacuous pass is impossible).
    /// </summary>
    private static void AssertNoCredentialLeak(
        bool success,
        string? error,
        AuthorizationEchoingHttpMessageHandler handler,
        params string[] secrets)
    {
        Assert.Multiple(() =>
        {
            Assert.That(handler.ObservedAuthorization, Is.Not.Null.And.Not.EqualTo("<none>"),
                "The handler must have seen a credential, or the test proves nothing.");
            Assert.That(success, Is.False);
            Assert.That(error, Is.Not.Null.And.Not.Empty,
                "A backend fault must still be distinguishable from success.");

            foreach (var secret in secrets)
            {
                Assert.That(error, Does.Not.Contain(secret),
                    "The backend credential must never cross back to the MCP caller.");
            }

            Assert.That(error, Does.Not.Contain(handler.ObservedAuthorization!));
            Assert.That(error, Does.Not.Contain("Authorization"));
            Assert.That(error, Does.Not.Contain("boom"),
                "No part of the backend exception's free text may be propagated.");
        });
    }

    [Test]
    public async Task Query_does_not_leak_the_bearer_credential_into_the_tool_result()
    {
        var client = BearerClient(out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        AssertNoCredentialLeak(result.Success, result.Error, handler, SecretToken);
    }

    [Test]
    public async Task QueryRange_does_not_leak_the_bearer_credential_into_the_tool_result()
    {
        var client = BearerClient(out var handler);
        var end = DateTimeOffset.UtcNow;

        var result = await TelemetryToolHandlers.QueryRangeAsync(
            client, ReadAll(), Guardrails(), CancellationToken.None,
            "up", end.AddMinutes(-10), end, TimeSpan.FromMinutes(1));

        AssertNoCredentialLeak(result.Success, result.Error, handler, SecretToken);
    }

    [Test]
    public async Task ListMetrics_does_not_leak_the_bearer_credential_into_the_tool_result()
    {
        var client = BearerClient(out var handler);

        var result = await TelemetryToolHandlers.ListMetricsAsync(client, ReadAll(), CancellationToken.None);

        AssertNoCredentialLeak(result.Success, result.Error, handler, SecretToken);
    }

    [Test]
    public async Task MetricMetadata_does_not_leak_the_bearer_credential_into_the_tool_result()
    {
        var client = BearerClient(out var handler);

        var result = await TelemetryToolHandlers.MetricMetadataAsync(
            client, ReadAll(), CancellationToken.None, "up");

        AssertNoCredentialLeak(result.Success, result.Error, handler, SecretToken);
    }

    [Test]
    public async Task Query_does_not_leak_the_basic_credential_into_the_tool_result()
    {
        var client = BasicClient(out var handler);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        AssertNoCredentialLeak(result.Success, result.Error, handler, SecretUser, SecretPassword);
    }

    [Test]
    public async Task A_backend_fault_stays_distinguishable_from_a_metric_access_denial()
    {
        var client = BearerClient(out _);
        var denyAllOptions = new LatticeApiMcpTelemetryOptions
        {
            MetricAccess = LatticeTelemetryMetricAccessMode.DenyAllExceptAllowed,
        };
        var denied = await TelemetryToolHandlers.QueryAsync(
            client, new TelemetryMetricAccessPolicy(denyAllOptions), CancellationToken.None, "up");

        var faulted = await TelemetryToolHandlers.QueryAsync(
            client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(denied.Success, Is.False);
            Assert.That(faulted.Success, Is.False);
            Assert.That(denied.Error, Is.Not.EqualTo(faulted.Error),
                "Callers act on the difference between 'denied by policy' and 'backend fault', "
                + "so flattening the free-text detail must not flatten that distinction.");
            Assert.That(denied.Error, Does.Contain("allow-list"));
        });
    }

    [Test]
    public async Task The_backend_fault_detail_is_retained_server_side_for_the_operator()
    {
        // Withholding the detail from the caller must not destroy it: the proxy is
        // the trusted side of the boundary and keeps the full exception, so a
        // backend outage stays diagnosable.
        var handler = new AuthorizationEchoingHttpMessageHandler();
        var http = new HttpClient(handler) { BaseAddress = new Uri(BackendBase) };
        var logger = new CapturingLogger<PrometheusQueryClient>();
        var client = new PrometheusQueryClient(
            http,
            Options.Create(new LatticeApiMcpTelemetryOptions
            {
                AuthMode = LatticeTelemetryBackendAuthMode.Bearer,
                Credential = new LatticeTelemetryBackendCredential { BearerToken = SecretToken },
            }),
            tokenProvider: null,
            logger);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(result.Success, Is.False);
            Assert.That(result.Error, Does.Not.Contain(SecretToken));
            Assert.That(logger.Entries, Is.Not.Empty,
                "The proxy must log the fault it refuses to describe to the caller.");
            Assert.That(logger.Entries[0].Exception, Is.Not.Null);
            Assert.That(logger.Entries[0].Message, Does.Not.Contain("?"),
                "The log line names the backend endpoint without echoing the caller's PromQL.");
        });
    }

    [Test]
    public async Task A_credential_stamping_fault_is_also_retained_server_side()
    {
        // A misconfigured dynamic-bearer mode throws while stamping, before any
        // request reaches the wire. That is the purest configuration fault there
        // is, and the caller-facing message now points the operator at these
        // logs - so it must land here rather than vanish on both sides.
        var handler = new AuthorizationEchoingHttpMessageHandler();
        var http = new HttpClient(handler) { BaseAddress = new Uri(BackendBase) };
        var logger = new CapturingLogger<PrometheusQueryClient>();
        var client = new PrometheusQueryClient(
            http,
            Options.Create(new LatticeApiMcpTelemetryOptions
            {
                AuthMode = LatticeTelemetryBackendAuthMode.DynamicBearer,
            }),
            tokenProvider: null,
            logger);

        var result = await TelemetryToolHandlers.QueryAsync(client, ReadAll(), CancellationToken.None, "up");

        Assert.Multiple(() =>
        {
            Assert.That(handler.ObservedAuthorization, Is.Null,
                "Fail-closed: no request may reach the backend when stamping fails.");
            Assert.That(result.Success, Is.False);
            Assert.That(logger.Entries, Is.Not.Empty,
                "A stamping fault must be logged, not swallowed on both sides of the boundary.");
            Assert.That(
                logger.Entries[0].Exception!.Message,
                Does.Contain(nameof(ITelemetryBackendTokenProvider)));
        });
    }

    /// <summary>A minimal <see cref="ILogger{TCategoryName}"/> that records what it was asked to write.</summary>
    private sealed class CapturingLogger<T> : ILogger<T>
    {
        public List<(LogLevel Level, string Message, Exception? Exception)> Entries { get; } = [];

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
            => Entries.Add((logLevel, formatter(state, exception), exception));
    }
}
