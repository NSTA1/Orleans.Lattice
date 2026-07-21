namespace Orleans.Lattice.Explorer.Entra.Web.Tests;

/// <summary>
/// A controllable <see cref="IExplorerWebTokenAcquirer"/> for tests: it records
/// the scopes it was asked for and returns a queued sequence of tokens, or throws
/// a queued exception, so the auth method's challenge, silent renewal, and
/// re-challenge behaviour can be verified without Microsoft.Identity.Web.
/// </summary>
internal sealed class FakeWebTokenAcquirer : IExplorerWebTokenAcquirer
{
    private readonly Queue<Func<ExplorerWebToken>> _responses = new();

    /// <summary>The scopes passed to the most recent call.</summary>
    public IReadOnlyList<string>? LastScopes { get; private set; }

    /// <summary>The number of times <see cref="AcquireTokenAsync"/> was called.</summary>
    public int CallCount { get; private set; }

    /// <summary>Queues a successful token result.</summary>
    public FakeWebTokenAcquirer EnqueueToken(ExplorerWebToken token)
    {
        _responses.Enqueue(() => token);
        return this;
    }

    /// <summary>Queues an exception to throw on the next call.</summary>
    public FakeWebTokenAcquirer EnqueueThrow(Exception exception)
    {
        _responses.Enqueue(() => throw exception);
        return this;
    }

    public Task<ExplorerWebToken> AcquireTokenAsync(IReadOnlyList<string> scopes, CancellationToken cancellationToken = default)
    {
        CallCount++;
        LastScopes = scopes;
        if (_responses.Count == 0)
        {
            throw new InvalidOperationException("No response queued for AcquireTokenAsync.");
        }

        return Task.FromResult(_responses.Dequeue()());
    }
}
