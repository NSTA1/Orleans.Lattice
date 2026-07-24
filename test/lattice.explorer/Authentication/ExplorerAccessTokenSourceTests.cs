using Orleans.Lattice.Explorer.Core.Authentication;

namespace Orleans.Lattice.Explorer.Tests.Authentication;

[TestFixture]
public class ExplorerAccessTokenSourceTests
{
    private static readonly DateTimeOffset Origin = new(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private sealed class MutableTimeProvider(DateTimeOffset start) : TimeProvider
    {
        private long _ticks = start.UtcTicks;

        public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);

        public void Advance(TimeSpan delta) => Interlocked.Add(ref _ticks, delta.Ticks);
    }

    private static ExplorerAccessToken Token(DateTimeOffset expiresOn, string value = "token")
        => new() { Token = value, ExpiresOn = expiresOn };

    [Test]
    public async Task GetAuthorizationHeaderAsync_freshToken_returnsHeader_withoutAcquiring()
    {
        var time = new MutableTimeProvider(Origin);
        var acquireCount = 0;
        var source = new ExplorerAccessTokenSource(
            Token(Origin.AddMinutes(10), "first"),
            _ => { acquireCount++; return new ValueTask<ExplorerAccessToken?>(Token(Origin.AddMinutes(20), "renewed")); },
            time);

        var header = await source.GetAuthorizationHeaderAsync();

        Assert.That(header, Is.EqualTo("Bearer first"));
        Assert.That(acquireCount, Is.EqualTo(0), "a still-valid token is served without renewal");
    }

    [Test]
    public async Task GetAuthorizationHeaderAsync_expiringToken_refreshesProactively()
    {
        var time = new MutableTimeProvider(Origin);
        var acquireCount = 0;
        var source = new ExplorerAccessTokenSource(
            Token(Origin.AddMinutes(10), "first"),
            _ =>
            {
                acquireCount++;
                return new ValueTask<ExplorerAccessToken?>(Token(time.GetUtcNow().AddMinutes(10), "renewed"));
            },
            time);

        // Default margin is 2 minutes: at 9 minutes the token is within the margin
        // of its 10-minute expiry, so it must be renewed before it is used.
        time.Advance(TimeSpan.FromMinutes(9));
        var header = await source.GetAuthorizationHeaderAsync();

        Assert.That(header, Is.EqualTo("Bearer renewed"));
        Assert.That(acquireCount, Is.EqualTo(1));
    }

    [Test]
    public async Task GetAuthorizationHeaderAsync_concurrentExpiring_acquiresExactlyOnce()
    {
        var time = new MutableTimeProvider(Origin);
        var acquireCount = 0;
        var release = new TaskCompletionSource();
        var source = new ExplorerAccessTokenSource(
            Token(Origin.AddMinutes(10), "first"),
            async _ =>
            {
                Interlocked.Increment(ref acquireCount);
                await release.Task;
                return Token(time.GetUtcNow().AddMinutes(10), "renewed");
            },
            time);

        time.Advance(TimeSpan.FromMinutes(9));
        var callers = Enumerable.Range(0, 16).Select(_ => source.GetAuthorizationHeaderAsync().AsTask()).ToArray();
        await Task.Delay(100);
        release.SetResult();
        var headers = await Task.WhenAll(callers);

        Assert.That(acquireCount, Is.EqualTo(1), "a burst of expiring callers shares one in-flight refresh");
        Assert.That(headers, Is.All.EqualTo("Bearer renewed"));
    }

    [Test]
    public async Task RefreshAsync_concurrentForced_acquiresExactlyOnce()
    {
        var time = new MutableTimeProvider(Origin);
        var acquireCount = 0;
        var release = new TaskCompletionSource();
        var source = new ExplorerAccessTokenSource(
            Token(Origin.AddMinutes(10), "first"),
            async _ =>
            {
                Interlocked.Increment(ref acquireCount);
                await release.Task;
                return Token(time.GetUtcNow().AddMinutes(10), "renewed");
            },
            time);

        var callers = Enumerable.Range(0, 16).Select(_ => source.RefreshAsync().AsTask()).ToArray();
        await Task.Delay(100);
        release.SetResult();
        var results = await Task.WhenAll(callers);

        Assert.That(acquireCount, Is.EqualTo(1), "a burst of forced refreshes (a 401 storm) shares one in-flight refresh");
        Assert.That(results, Is.All.True);
    }

    [Test]
    public async Task RefreshAsync_forced_acquiresFreshToken()
    {
        var time = new MutableTimeProvider(Origin);
        var source = new ExplorerAccessTokenSource(
            Token(Origin.AddMinutes(10), "first"),
            _ => new ValueTask<ExplorerAccessToken?>(Token(Origin.AddMinutes(30), "renewed")),
            time);

        var refreshed = await source.RefreshAsync();
        var header = await source.GetAuthorizationHeaderAsync();

        Assert.That(refreshed, Is.True);
        Assert.That(header, Is.EqualTo("Bearer renewed"));
    }

    [Test]
    public async Task Acquire_returnsNull_latchesRevoked_soCallerReChallenges()
    {
        var time = new MutableTimeProvider(Origin);
        var acquireCount = 0;
        var source = new ExplorerAccessTokenSource(
            Token(Origin.AddMinutes(10), "first"),
            _ => { acquireCount++; return new ValueTask<ExplorerAccessToken?>((ExplorerAccessToken?)null); },
            time);

        time.Advance(TimeSpan.FromMinutes(9));
        var header = await source.GetAuthorizationHeaderAsync();
        var refreshed = await source.RefreshAsync();
        var headerAgain = await source.GetAuthorizationHeaderAsync();

        Assert.That(header, Is.Null, "an unrenewable token yields no header");
        Assert.That(refreshed, Is.False, "a revoked source cannot refresh");
        Assert.That(headerAgain, Is.Null);
        Assert.That(acquireCount, Is.EqualTo(1), "once revoked the source stops calling the acquire delegate");
    }

    [Test]
    public async Task Acquire_returnsNull_raisesReauthRequired_exactlyOnce()
    {
        var time = new MutableTimeProvider(Origin);
        var source = new ExplorerAccessTokenSource(
            Token(Origin.AddMinutes(10), "first"),
            _ => new ValueTask<ExplorerAccessToken?>((ExplorerAccessToken?)null),
            time);

        var reauthCount = 0;
        source.ReauthRequired += () => Interlocked.Increment(ref reauthCount);

        // First forced refresh latches the revoked state and fires the event.
        var first = await source.RefreshAsync();
        // Later calls stay revoked but must not re-raise the event.
        var second = await source.RefreshAsync();
        _ = await source.GetAuthorizationHeaderAsync();

        Assert.Multiple(() =>
        {
            Assert.That(first, Is.False);
            Assert.That(second, Is.False);
            Assert.That(reauthCount, Is.EqualTo(1), "the revoked transition is edge-triggered - raised once, never again");
        });
    }

    [Test]
    public async Task Renewal_success_neverRaisesReauthRequired()
    {
        var time = new MutableTimeProvider(Origin);
        var source = new ExplorerAccessTokenSource(
            Token(Origin.AddMinutes(10), "first"),
            _ => new ValueTask<ExplorerAccessToken?>(Token(Origin.AddMinutes(30), "renewed")),
            time);

        var raised = false;
        source.ReauthRequired += () => raised = true;

        var refreshed = await source.RefreshAsync();

        Assert.Multiple(() =>
        {
            Assert.That(refreshed, Is.True);
            Assert.That(raised, Is.False, "a source that can still renew never asks for re-authentication");
        });
    }

    [Test]
    public void Constructor_nullAcquire_throws()
    {
        Assert.That(
            () => new ExplorerAccessTokenSource(Token(Origin), null!, new MutableTimeProvider(Origin)),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_nullTimeProvider_throws()
    {
        Assert.That(
            () => new ExplorerAccessTokenSource(Token(Origin), _ => new ValueTask<ExplorerAccessToken?>((ExplorerAccessToken?)null), null!),
            Throws.ArgumentNullException);
    }

    [Test]
    public void Constructor_negativeMargin_throws()
    {
        Assert.That(
            () => new ExplorerAccessTokenSource(
                Token(Origin),
                _ => new ValueTask<ExplorerAccessToken?>((ExplorerAccessToken?)null),
                new MutableTimeProvider(Origin),
                TimeSpan.FromSeconds(-1)),
            Throws.TypeOf<ArgumentOutOfRangeException>());
    }
}
