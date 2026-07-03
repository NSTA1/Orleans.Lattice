using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Explorer.Entra.Tests;

/// <summary>A minimal <see cref="IOptionsMonitor{T}"/> over a fixed value for tests.</summary>
internal sealed class StaticOptionsMonitor<T>(T value) : IOptionsMonitor<T>
{
    public T CurrentValue { get; } = value;

    public T Get(string? name) => CurrentValue;

    public IDisposable? OnChange(Action<T, string?> listener) => null;
}

/// <summary>A manually advanced clock for deterministic token-expiry tests.</summary>
internal sealed class MutableTimeProvider(DateTimeOffset start) : TimeProvider
{
    private long _ticks = start.UtcTicks;

    public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _ticks), TimeSpan.Zero);

    public void Advance(TimeSpan delta) => Interlocked.Add(ref _ticks, delta.Ticks);
}

/// <summary>
/// A fake <see cref="IEntraInteractiveTokenAcquirer"/> with a controllable clock,
/// so the refresh and re-challenge behaviour is verified without any MSAL,
/// network, or Azure dependency.
/// </summary>
internal sealed class FakeEntraAcquirer(TimeProvider time) : IEntraInteractiveTokenAcquirer
{
    public int InteractiveCount { get; private set; }

    public int SilentCount { get; private set; }

    public EntraTokenResult? SilentResult { get; set; }

    public EntraTokenRequest? LastRequest { get; private set; }

    public Task<EntraTokenResult> AcquireInteractiveAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)
    {
        InteractiveCount++;
        LastRequest = request;
        return Task.FromResult(new EntraTokenResult
        {
            AccessToken = "access-1",
            ExpiresOn = time.GetUtcNow().AddMinutes(10),
            Username = "user@contoso.com",
        });
    }

    public Task<EntraTokenResult?> AcquireSilentAsync(EntraTokenRequest request, CancellationToken cancellationToken = default)
    {
        SilentCount++;
        LastRequest = request;
        return Task.FromResult(SilentResult);
    }
}
