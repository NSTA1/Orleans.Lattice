using System.Net;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Telemetry.Tests;

/// <summary>
/// Backend-fault and credential-edge tests for <see cref="PrometheusQueryClient"/>.
/// The client is the only place a backend-request fault detail is retained, and it
/// must retain it without echoing the caller's PromQL expression: the log line
/// names the backend endpoint path with the query string dropped. These tests also
/// pin the credential arms that stamp nothing - a mode whose credential material is
/// absent must send no <c>Authorization</c> header rather than an empty one.
/// </summary>
public sealed partial class PrometheusQueryClientTests
{
    private readonly List<HttpClient> _clients = [];

    private HttpClient Track(HttpClient client)
    {
        _clients.Add(client);
        return client;
    }

    [TearDown]
    public void DisposeTrackedClients()
    {
        foreach (var client in _clients)
        {
            client.Dispose();
        }

        _clients.Clear();
    }

    private PrometheusQueryClient CreateLoggingClient(
        LatticeTelemetryOptions options,
        RecordingLogger logger,
        HttpStatusCode statusCode = HttpStatusCode.InternalServerError,
        string responseJson = "{\"status\":\"success\",\"data\":{}}")
    {
        var handler = new CapturingHttpMessageHandler(responseJson, statusCode);
        var http = Track(new HttpClient(handler) { BaseAddress = new Uri(BackendBase) });
        return new PrometheusQueryClient(http, Options.Create(options), tokenProvider: null, logger);
    }

    // ---- Fault logging keeps the PromQL expression off the log line ----

    [Test]
    public void A_backend_fault_logs_the_endpoint_path_without_the_query_string()
    {
        var logger = new RecordingLogger();
        var client = CreateLoggingClient(new LatticeTelemetryOptions(), logger);

        Assert.ThrowsAsync<HttpRequestException>(
            () => client.InstantQueryAsync("secret_metric{tenant=\"acme\"}", time: null, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(logger.Entries, Has.Count.EqualTo(1));
            Assert.That(logger.Entries[0].Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(logger.Entries[0].Message, Does.Contain("api/v1/query"));
            Assert.That(
                logger.Entries[0].Message,
                Does.Not.Contain("secret_metric"),
                "The log line must name the endpoint only; the caller's PromQL expression must not be echoed.");
            Assert.That(logger.Entries[0].Message, Does.Not.Contain("acme"));
        });
    }

    [Test]
    public void A_backend_fault_on_a_query_less_endpoint_logs_the_path_verbatim()
    {
        // The label-values endpoint carries no query string, so the path is used as
        // it stands rather than being truncated at a '?' that is not there.
        var logger = new RecordingLogger();
        var client = CreateLoggingClient(new LatticeTelemetryOptions(), logger);

        Assert.ThrowsAsync<HttpRequestException>(
            () => client.ListMetricNamesAsync(CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(logger.Entries, Has.Count.EqualTo(1));
            Assert.That(logger.Entries[0].Message, Does.Contain("api/v1/label/__name__/values"));
        });
    }

    [Test]
    public void A_backend_404_is_logged_at_debug_rather_than_warning()
    {
        // A 404 metadata endpoint is a degradable condition, not an operator-facing
        // fault, so it must not raise a warning in a healthy deployment's logs.
        var logger = new RecordingLogger();
        var client = CreateLoggingClient(
            new LatticeTelemetryOptions(), logger, HttpStatusCode.NotFound);

        Assert.ThrowsAsync<HttpRequestException>(
            () => client.MetricMetadataAsync("up", CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(logger.Entries, Has.Count.EqualTo(1));
            Assert.That(logger.Entries[0].Level, Is.EqualTo(LogLevel.Debug));
        });
    }

    [Test]
    public void A_backend_fault_is_not_formatted_when_the_level_is_disabled()
    {
        var logger = new RecordingLogger { Enabled = false };
        var client = CreateLoggingClient(new LatticeTelemetryOptions(), logger);

        Assert.ThrowsAsync<HttpRequestException>(
            () => client.InstantQueryAsync("up", time: null, CancellationToken.None));

        Assert.That(logger.Entries, Is.Empty);
    }

    [Test]
    public void A_backend_fault_without_a_logger_still_propagates()
    {
        var handler = new CapturingHttpMessageHandler(
            "{\"status\":\"success\",\"data\":{}}", HttpStatusCode.InternalServerError);
        using var http = new HttpClient(handler) { BaseAddress = new Uri(BackendBase) };
        var client = new PrometheusQueryClient(http, Options.Create(new LatticeTelemetryOptions()));

        Assert.ThrowsAsync<HttpRequestException>(
            () => client.InstantQueryAsync("up", time: null, CancellationToken.None));
    }

    [Test]
    public void A_cancelled_request_propagates_without_being_logged_as_a_fault()
    {
        // A caller cancellation is not a backend fault: it is rethrown by the
        // cancellation filter ahead of the logging catch, so it leaves no log entry.
        var logger = new RecordingLogger();
        var client = CreateLoggingClient(
            new LatticeTelemetryOptions(), logger, HttpStatusCode.OK);
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.CatchAsync<OperationCanceledException>(
            () => client.InstantQueryAsync("up", time: null, cts.Token));

        Assert.That(logger.Entries, Is.Empty);
    }

    // ---- Credential arms that deliberately stamp nothing ----
    //
    // Each configuration below is rejected by LatticeTelemetryOptionsValidator
    // at host start (see LatticeTelemetryOptionsValidatorTests), so a validated
    // host never reaches these arms. They are pinned here because the client's own
    // guard is the defence-in-depth layer behind that gate, and its behaviour should
    // change deliberately rather than by accident: today an absent static credential
    // stamps no header, which is NOT a supported posture, merely the client's current
    // fallback. Contrast DynamicBearer, which throws rather than send an
    // unauthenticated request - see the fail-closed tests in the sibling file.

    [Test]
    public async Task Bearer_mode_with_no_credential_object_sends_no_authorization_header()
    {
        var options = new LatticeTelemetryOptions
        {
            AuthMode = LatticeTelemetryBackendAuthMode.Bearer,
        };
        var client = CreateClient(options, out var handler);

        await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.That(handler.LastRequest!.Headers.Authorization, Is.Null);
    }

    [Test]
    public async Task Bearer_mode_with_an_empty_token_sends_no_authorization_header()
    {
        var options = new LatticeTelemetryOptions
        {
            AuthMode = LatticeTelemetryBackendAuthMode.Bearer,
            Credential = new LatticeTelemetryBackendCredential { BearerToken = string.Empty },
        };
        var client = CreateClient(options, out var handler);

        await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.That(handler.LastRequest!.Headers.Authorization, Is.Null);
    }

    [Test]
    public async Task Basic_mode_with_no_credential_object_sends_no_authorization_header()
    {
        var options = new LatticeTelemetryOptions
        {
            AuthMode = LatticeTelemetryBackendAuthMode.Basic,
        };
        var client = CreateClient(options, out var handler);

        await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.That(handler.LastRequest!.Headers.Authorization, Is.Null);
    }

    [Test]
    public async Task Basic_mode_with_an_empty_username_sends_no_authorization_header()
    {
        var options = new LatticeTelemetryOptions
        {
            AuthMode = LatticeTelemetryBackendAuthMode.Basic,
            Credential = new LatticeTelemetryBackendCredential
            {
                BasicUsername = string.Empty,
                BasicPassword = "secret",
            },
        };
        var client = CreateClient(options, out var handler);

        await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.That(handler.LastRequest!.Headers.Authorization, Is.Null);
    }

    // ---- Envelope parsing edges ----

    [Test]
    public async Task InstantQuery_appends_the_evaluation_timestamp_when_one_is_supplied()
    {
        var client = CreateClient(new LatticeTelemetryOptions(), out var handler);

        await client.InstantQueryAsync(
            "up", DateTimeOffset.FromUnixTimeMilliseconds(1435781451781), CancellationToken.None);

        Assert.That(handler.LastRequest!.RequestUri!.Query, Does.Contain("time=1435781451.781"));
    }

    [Test]
    public async Task ListMetricNames_returns_an_empty_list_when_the_data_member_is_not_an_array()
    {
        var client = CreateClient(
            new LatticeTelemetryOptions(), out _, "{\"status\":\"success\",\"data\":{}}");

        var names = await client.ListMetricNamesAsync(CancellationToken.None);

        Assert.That(names, Is.Empty);
    }

    [Test]
    public async Task ListMetricNames_returns_an_empty_list_when_there_is_no_data_member()
    {
        var client = CreateClient(new LatticeTelemetryOptions(), out _, "{\"status\":\"success\"}");

        var names = await client.ListMetricNamesAsync(CancellationToken.None);

        Assert.That(names, Is.Empty);
    }

    [Test]
    public async Task A_response_with_no_status_member_reads_as_an_empty_status()
    {
        var client = CreateClient(
            new LatticeTelemetryOptions(), out _, "{\"data\":{\"resultType\":\"vector\"}}");

        var response = await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.That(response.Status, Is.Empty);
    }

    [Test]
    public async Task A_response_with_no_data_member_reads_as_an_undefined_element()
    {
        var client = CreateClient(new LatticeTelemetryOptions(), out _, "{\"status\":\"success\"}");

        var response = await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(response.Status, Is.EqualTo("success"));
            Assert.That(response.Data.ValueKind, Is.EqualTo(System.Text.Json.JsonValueKind.Undefined));
        });
    }

    [Test]
    public async Task MetricMetadata_without_a_metric_name_targets_the_unfiltered_endpoint()
    {
        var client = CreateClient(new LatticeTelemetryOptions(), out var handler);

        await client.MetricMetadataAsync(metric: null, CancellationToken.None);

        Assert.That(
            handler.LastRequest!.RequestUri!.ToString(),
            Is.EqualTo($"{BackendBase}api/v1/metadata"));
    }

    [Test]
    public void Null_range_query_is_rejected()
    {
        var client = CreateClient(new LatticeTelemetryOptions(), out _);
        Assert.ThrowsAsync<ArgumentNullException>(
            () => client.RangeQueryAsync(
                query: null!,
                DateTimeOffset.UnixEpoch,
                DateTimeOffset.UnixEpoch.AddMinutes(1),
                TimeSpan.FromSeconds(30),
                CancellationToken.None));
    }

    [Test]
    public void A_null_http_client_is_rejected()
        => Assert.Throws<ArgumentNullException>(
            () => _ = new PrometheusQueryClient(
                http: null!, Options.Create(new LatticeTelemetryOptions())));

    [Test]
    public void Null_options_are_rejected()
    {
        using var http = new HttpClient(new CapturingHttpMessageHandler());
        Assert.Throws<ArgumentNullException>(() => _ = new PrometheusQueryClient(http, options: null!));
    }

    [Test]
    public void A_backend_timeout_is_logged_as_a_fault_rather_than_treated_as_a_caller_cancellation()
    {
        // HttpClient surfaces its own timeout as a TaskCanceledException even though
        // the caller's token was never cancelled. The cancellation filter must not
        // swallow it: it is a backend fault the operator needs in the logs.
        var logger = new RecordingLogger();
        var handler = new ThrowingHttpMessageHandler(
            new TaskCanceledException("The request was canceled due to the configured HttpClient.Timeout."));
        using var http = new HttpClient(handler) { BaseAddress = new Uri(BackendBase) };
        var client = new PrometheusQueryClient(
            http, Options.Create(new LatticeTelemetryOptions()), tokenProvider: null, logger);

        Assert.CatchAsync<TaskCanceledException>(
            () => client.InstantQueryAsync("up", time: null, CancellationToken.None));

        Assert.Multiple(() =>
        {
            Assert.That(logger.Entries, Has.Count.EqualTo(1));
            Assert.That(logger.Entries[0].Level, Is.EqualTo(LogLevel.Warning));
            Assert.That(logger.Entries[0].Message, Does.Contain("api/v1/query"));
        });
    }

    [Test]
    public async Task A_json_null_status_reads_as_an_empty_status()
    {
        var client = CreateClient(
            new LatticeTelemetryOptions(), out _, "{\"status\":null,\"data\":{}}");

        var response = await client.InstantQueryAsync("up", time: null, CancellationToken.None);

        Assert.That(response.Status, Is.Empty);
    }

    [Test]
    public async Task A_json_null_metadata_status_reads_as_an_empty_status()
    {
        var client = CreateClient(
            new LatticeTelemetryOptions(), out _, "{\"status\":null,\"data\":{}}");

        var response = await client.MetricMetadataAsync("up", CancellationToken.None);

        Assert.That(response.Status, Is.Empty);
    }

    /// <summary>
    /// A minimal <see cref="ILogger{TCategoryName}"/> that records the level and
    /// formatted message of every entry, so a test can assert both what was logged
    /// and - just as importantly - what was kept out of it.
    /// </summary>
    private sealed class RecordingLogger : ILogger<PrometheusQueryClient>
    {
        public List<(LogLevel Level, string Message)> Entries { get; } = [];

        public bool Enabled { get; init; } = true;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => Enabled;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter)
        {
            ArgumentNullException.ThrowIfNull(formatter);
            Entries.Add((logLevel, formatter(state, exception)));
        }
    }
}
