namespace Orleans.Lattice.Explorer.Core.Authentication;

/// <summary>
/// An in-memory <see cref="ICredentialStore"/> that holds a single credential in
/// a field. Nothing is written to disk, so no secret rests on the machine.
/// <para>
/// <b>It holds one credential, so its lifetime is load-bearing.</b> Registered as
/// a singleton it is a process-global sign-in, and in a multi-user head every
/// Blazor circuit would read whatever the last operator to sign in wrote.
/// <see cref="ExplorerAuthServiceCollectionExtensions.AddExplorerAuth"/>
/// therefore registers it <b>scoped</b>, so the default is per-circuit and one
/// operator's credential can never be served to another. A head that registers it
/// itself must keep that lifetime, or register a per-user platform store (DPAPI on
/// desktop, the encrypted server cookie on web) instead.
/// </para>
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
