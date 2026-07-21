using Azure.Core;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Telemetry.Azure.Tests;

/// <summary>
/// Tests for <see cref="AzureTelemetryBackendTokenProvider"/>: it acquires a
/// token from the configured credential for the configured scope, serves a cached
/// token until it nears expiry, refreshes past the skew, coalesces concurrent
/// callers into a single acquisition, and fails clearly without a credential.
/// </summary>
[TestFixture]
public sealed class AzureTelemetryBackendTokenProviderTests
{
    private static readonly DateTimeOffset Start = DateTimeOffset.UnixEpoch;

    private static AzureTelemetryBackendTokenProvider CreateProvider(
        TokenCredential credential,
        MutableTimeProvider clock,
        TimeSpan? refreshSkew = null,
        string? scope = null)
    {
        var options = Options.Create(new AzureTelemetryBackendTokenOptions
        {
            Credential = credential,
            Scope = scope ?? AzureTelemetryBackendTokenOptions.ManagedPrometheusScope,
            RefreshSkew = refreshSkew ?? TimeSpan.FromMinutes(5),
        });
        return new AzureTelemetryBackendTokenProvider(options, clock);
    }

    private static AccessToken TokenValidFor(TimeSpan lifetime, string value)
        => new(value, Start + lifetime);

    [Test]
    public async Task Acquires_and_returns_a_token_for_the_configured_scope()
    {
        var clock = new MutableTimeProvider(Start);
        var credential = new FakeTokenCredential(_ => TokenValidFor(TimeSpan.FromHours(1), "tok-0"));
        var provider = CreateProvider(credential, clock, scope: "https://prometheus.monitor.azure.com/.default");

        var token = await provider.GetAccessTokenAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(token, Is.EqualTo("tok-0"));
            Assert.That(credential.CallCount, Is.EqualTo(1));
            Assert.That(credential.LastScopes, Is.EqualTo(new[] { "https://prometheus.monitor.azure.com/.default" }));
        });
    }

    [Test]
    public async Task A_cached_token_is_reused_without_reacquiring()
    {
        var clock = new MutableTimeProvider(Start);
        var credential = new FakeTokenCredential(call => TokenValidFor(TimeSpan.FromHours(1), $"tok-{call}"));
        var provider = CreateProvider(credential, clock);

        var first = await provider.GetAccessTokenAsync(CancellationToken.None);
        clock.Advance(TimeSpan.FromMinutes(30));
        var second = await provider.GetAccessTokenAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("tok-0"));
            Assert.That(second, Is.EqualTo("tok-0"));
            Assert.That(credential.CallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public async Task A_token_within_the_refresh_skew_of_expiry_is_reacquired()
    {
        var clock = new MutableTimeProvider(Start);
        var credential = new FakeTokenCredential(call => TokenValidFor(TimeSpan.FromHours(1), $"tok-{call}"));
        var provider = CreateProvider(credential, clock, refreshSkew: TimeSpan.FromMinutes(5));

        var first = await provider.GetAccessTokenAsync(CancellationToken.None);
        // 56 min in: only 4 min left, inside the 5 min skew -> refresh.
        clock.Advance(TimeSpan.FromMinutes(56));
        var second = await provider.GetAccessTokenAsync(CancellationToken.None);

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.EqualTo("tok-0"));
            Assert.That(second, Is.EqualTo("tok-1"));
            Assert.That(credential.CallCount, Is.EqualTo(2));
        });
    }

    [Test]
    public async Task Concurrent_callers_share_a_single_acquisition()
    {
        var clock = new MutableTimeProvider(Start);
        var gate = new TaskCompletionSource();
        var credential = new FakeTokenCredential(call => TokenValidFor(TimeSpan.FromHours(1), $"tok-{call}"), gate);
        var provider = CreateProvider(credential, clock);

        var first = provider.GetAccessTokenAsync(CancellationToken.None);
        var second = provider.GetAccessTokenAsync(CancellationToken.None);
        gate.SetResult();
        var results = await Task.WhenAll(first.AsTask(), second.AsTask());

        Assert.Multiple(() =>
        {
            Assert.That(results[0], Is.EqualTo("tok-0"));
            Assert.That(results[1], Is.EqualTo("tok-0"));
            Assert.That(credential.CallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_missing_credential_fails_with_a_clear_message()
    {
        var clock = new MutableTimeProvider(Start);
        var options = Options.Create(new AzureTelemetryBackendTokenOptions { Credential = null });
        var provider = new AzureTelemetryBackendTokenProvider(options, clock);

        var ex = Assert.ThrowsAsync<InvalidOperationException>(
            () => provider.GetAccessTokenAsync(CancellationToken.None).AsTask());
        Assert.That(ex!.Message, Does.Contain(nameof(AzureTelemetryBackendTokenOptions.Credential)));
    }

    [Test]
    public void The_constructor_rejects_null_options()
        => Assert.Throws<ArgumentNullException>(
            () => new AzureTelemetryBackendTokenProvider(options: null!));
}
