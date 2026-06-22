namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// An in-memory <see cref="ICredentialStore"/> that holds the credential only for
/// the lifetime of the process. It is the safe default used by tests and by any
/// head that has not registered a platform-backed store; nothing is written to
/// disk, so no secret rests on the machine.
/// </summary>
public sealed class InMemoryCredentialStore : ICredentialStore
{
    private readonly object _gate = new();
    private StoredCredential? _credential;

    /// <inheritdoc />
    public Task<StoredCredential?> GetAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            return Task.FromResult(_credential);
        }
    }

    /// <inheritdoc />
    public Task SetAsync(StoredCredential credential, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(credential);
        lock (_gate)
        {
            _credential = credential;
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task ClearAsync(CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            _credential = null;
        }

        return Task.CompletedTask;
    }
}
