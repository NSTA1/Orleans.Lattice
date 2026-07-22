using Azure.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.Tests;

/// <summary>
/// Unit tests for <see cref="ManagedIdentityAdministratorCredentialSource"/>, the
/// self-refreshing administrator credential source. Proves it acquires a token for
/// the configured scope, serves a cached token until it nears expiry, refreshes
/// past the skew, coalesces concurrent callers into a single acquisition, and
/// fails closed (returns null) when no credential is configured or acquisition
/// throws. Deterministic - a mutable clock, no real waiting.
/// </summary>
[TestFixture]
public sealed class ManagedIdentityAdministratorCredentialSourceTests
{
    private const string SiloScope = "api://silo-app-id/.default";
    private static readonly DateTimeOffset Start = DateTimeOffset.UnixEpoch;

    private static ManagedIdentityAdministratorCredentialSource CreateSource(
        TokenCredential? credential,
        MutableTimeProvider clock,
        TimeSpan? refreshSkew = null,
        string? scope = SiloScope)
    {
        var options = Options.Create(new LatticeApiMcpManagedIdentityAdministratorOptions
        {
            Credential = credential,
            Scope = scope ?? string.Empty,
            RefreshSkew = refreshSkew ?? TimeSpan.FromMinutes(5),
        });
        return new ManagedIdentityAdministratorCredentialSource(
            options,
            NullLogger<ManagedIdentityAdministratorCredentialSource>.Instance,
            clock);
    }

    private static AccessToken TokenValidFor(TimeSpan lifetime, string value)
        => new(value, Start + lifetime);

    [Test]
    public void Acquires_a_token_for_the_configured_scope()
    {
        var clock = new MutableTimeProvider(Start);
        var credential = new FakeTokenCredential(_ => TokenValidFor(TimeSpan.FromHours(1), "tok-0"));
        var source = CreateSource(credential, clock);

        var resolved = source.Resolve();

        Assert.Multiple(() =>
        {
            Assert.That(resolved, Is.Not.Null);
            Assert.That(resolved!.Value.Token, Is.EqualTo("tok-0"));
            Assert.That(credential.CallCount, Is.EqualTo(1));
            Assert.That(credential.LastScopes, Is.EqualTo(new[] { SiloScope }));
        });
    }

    [Test]
    public void A_cached_token_is_reused_without_reacquiring()
    {
        var clock = new MutableTimeProvider(Start);
        var credential = new FakeTokenCredential(call => TokenValidFor(TimeSpan.FromHours(1), $"tok-{call}"));
        var source = CreateSource(credential, clock);

        var first = source.Resolve();
        clock.Advance(TimeSpan.FromMinutes(30));
        var second = source.Resolve();

        Assert.Multiple(() =>
        {
            Assert.That(first!.Value.Token, Is.EqualTo("tok-0"));
            Assert.That(second!.Value.Token, Is.EqualTo("tok-0"));
            Assert.That(credential.CallCount, Is.EqualTo(1));
        });
    }

    [Test]
    public void A_token_within_the_refresh_skew_of_expiry_is_reacquired()
    {
        var clock = new MutableTimeProvider(Start);
        var credential = new FakeTokenCredential(call => TokenValidFor(TimeSpan.FromHours(1), $"tok-{call}"));
        var source = CreateSource(credential, clock, refreshSkew: TimeSpan.FromMinutes(5));

        var first = source.Resolve();
        // 56 min in: only 4 min left, inside the 5 min skew -> refresh.
        clock.Advance(TimeSpan.FromMinutes(56));
        var second = source.Resolve();

        Assert.Multiple(() =>
        {
            Assert.That(first!.Value.Token, Is.EqualTo("tok-0"));
            Assert.That(second!.Value.Token, Is.EqualTo("tok-1"));
            Assert.That(credential.CallCount, Is.EqualTo(2));
        });
    }

    [Test]
    public void Concurrent_callers_share_a_single_acquisition()
    {
        var clock = new MutableTimeProvider(Start);
        var credential = new FakeTokenCredential(call => TokenValidFor(TimeSpan.FromHours(1), $"tok-{call}"));
        var source = CreateSource(credential, clock);

        var results = new LatticeCredential?[16];
        Parallel.For(0, results.Length, i => results[i] = source.Resolve());

        Assert.Multiple(() =>
        {
            Assert.That(credential.CallCount, Is.EqualTo(1),
                "A cold rotation must coalesce into a single acquisition under the gate.");
            foreach (var result in results)
            {
                Assert.That(result!.Value.Token, Is.EqualTo("tok-0"));
            }
        });
    }

    [Test]
    public void A_missing_credential_fails_closed()
    {
        var clock = new MutableTimeProvider(Start);
        var source = CreateSource(credential: null, clock);

        Assert.That(source.Resolve(), Is.Null,
            "With no credential configured the source must fail closed (null), not throw.");
    }

    [Test]
    public void An_acquisition_failure_fails_closed()
    {
        var clock = new MutableTimeProvider(Start);
        var credential = new FakeTokenCredential(_ => TokenValidFor(TimeSpan.FromHours(1), "tok"), throwOnAcquire: true);
        var source = CreateSource(credential, clock);

        Assert.That(source.Resolve(), Is.Null,
            "A failed acquisition must fail closed (null) rather than surface the exception.");
    }

    [Test]
    public void Constructor_rejects_null_dependencies()
    {
        var options = Options.Create(new LatticeApiMcpManagedIdentityAdministratorOptions());

        Assert.Multiple(() =>
        {
            Assert.Throws<ArgumentNullException>(() => new ManagedIdentityAdministratorCredentialSource(
                null!, NullLogger<ManagedIdentityAdministratorCredentialSource>.Instance));
            Assert.Throws<ArgumentNullException>(() => new ManagedIdentityAdministratorCredentialSource(
                options, null!));
        });
    }
}
