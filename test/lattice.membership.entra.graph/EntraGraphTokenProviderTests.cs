namespace Orleans.Lattice.Membership.Entra.Graph.Tests;

/// <summary>
/// Unit tests for <see cref="EntraGraphTokenProvider"/>: caching, single-flight
/// acquisition under concurrency, and transparent re-acquisition after expiry.
/// No live MSAL / Azure AD call is made.
/// </summary>
public class EntraGraphTokenProviderTests
{
    private static readonly DateTimeOffset Start = new(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static EntraGraphTokenProvider CreateProvider(
        out ManualTimeProvider clock,
        out FakeGraphTokenAcquirer acquirer,
        TimeSpan? lifetime = null,
        TimeSpan? skew = null)
    {
        clock = new ManualTimeProvider(Start);
        acquirer = new FakeGraphTokenAcquirer(clock, lifetime ?? TimeSpan.FromHours(1));
        return new EntraGraphTokenProvider(acquirer, clock, skew ?? TimeSpan.FromMinutes(5));
    }

    [Test]
    public void Constructor_null_acquirer_throws()
    {
        Assert.That(
            () => new EntraGraphTokenProvider(null!, TimeProvider.System, TimeSpan.Zero),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_negative_skew_throws()
    {
        var acquirer = new FakeGraphTokenAcquirer(TimeProvider.System, TimeSpan.FromHours(1));
        Assert.That(
            () => new EntraGraphTokenProvider(acquirer, TimeProvider.System, TimeSpan.FromSeconds(-1)),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }

    [Test]
    public async Task GetAccessTokenAsync_first_call_acquires_once()
    {
        var provider = CreateProvider(out _, out var acquirer);

        var token = await provider.GetAccessTokenAsync();

        Assert.That(token, Is.EqualTo("token-1"));
        Assert.That(acquirer.CallCount, Is.EqualTo(1));
    }

    [Test]
    public async Task GetAccessTokenAsync_within_lifetime_reuses_cached_token()
    {
        var provider = CreateProvider(out var clock, out var acquirer);

        var first = await provider.GetAccessTokenAsync();
        clock.Advance(TimeSpan.FromMinutes(1));
        var second = await provider.GetAccessTokenAsync();

        Assert.That(second, Is.EqualTo(first));
        Assert.That(acquirer.CallCount, Is.EqualTo(1));
    }

    [Test]
    public async Task GetAccessTokenAsync_concurrent_cold_callers_share_one_acquisition()
    {
        var provider = CreateProvider(out _, out var acquirer);
        acquirer.BlockNext();

        var calls = Enumerable
            .Range(0, 16)
            .Select(_ => provider.GetAccessTokenAsync().AsTask())
            .ToArray();

        // Let every caller reach the single-flight gate before releasing.
        await Task.Delay(100);
        acquirer.Release();
        var tokens = await Task.WhenAll(calls);

        Assert.That(acquirer.CallCount, Is.EqualTo(1));
        Assert.That(tokens, Is.All.EqualTo("token-1"));
    }

    [Test]
    public async Task GetAccessTokenAsync_after_expiry_reacquires_transparently()
    {
        var provider = CreateProvider(out var clock, out var acquirer, lifetime: TimeSpan.FromHours(1));

        var first = await provider.GetAccessTokenAsync();
        clock.Advance(TimeSpan.FromHours(2));
        var second = await provider.GetAccessTokenAsync();

        Assert.That(first, Is.EqualTo("token-1"));
        Assert.That(second, Is.EqualTo("token-2"));
        Assert.That(acquirer.CallCount, Is.EqualTo(2));
    }

    [Test]
    public async Task GetAccessTokenAsync_within_refresh_skew_reacquires()
    {
        var provider = CreateProvider(
            out var clock,
            out var acquirer,
            lifetime: TimeSpan.FromHours(1),
            skew: TimeSpan.FromMinutes(5));

        await provider.GetAccessTokenAsync();

        // 56 minutes in: within the 5-minute skew of the 60-minute expiry.
        clock.Advance(TimeSpan.FromMinutes(56));
        await provider.GetAccessTokenAsync();

        Assert.That(acquirer.CallCount, Is.EqualTo(2));
    }
}
