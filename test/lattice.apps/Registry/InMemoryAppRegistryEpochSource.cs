namespace Orleans.Lattice.Apps.Tests;

/// <summary>
/// An in-memory <see cref="IAppRegistryEpochSource"/> several maintainers share, standing in for the single
/// cluster-wide <see cref="IAppRegistryEpochGrain"/> activation.
/// </summary>
internal sealed class InMemoryAppRegistryEpochSource : IAppRegistryEpochSource
{
    private readonly object _gate = new();
    private AppRegistryEpoch _current = new(Guid.NewGuid(), 0);

    /// <summary>The current epoch.</summary>
    public AppRegistryEpoch Current
    {
        get
        {
            lock (_gate)
            {
                return _current;
            }
        }
    }

    /// <summary>When set, every read and advance throws, as an unreachable epoch grain does.</summary>
    public bool Fail { get; set; }

    /// <summary>The number of advances still to fail before one lands.</summary>
    public int FailAdvances { get; set; }

    /// <summary>Simulates a reactivation of the epoch grain: a fresh incarnation at version zero.</summary>
    public void Restart()
    {
        lock (_gate)
        {
            _current = new AppRegistryEpoch(Guid.NewGuid(), 0);
        }
    }

    /// <inheritdoc />
    public Task<AppRegistryEpoch> GetAsync(CancellationToken cancellationToken) =>
        Fail ? Task.FromException<AppRegistryEpoch>(new InvalidOperationException("epoch unreachable")) : Task.FromResult(Current);

    /// <inheritdoc />
    public Task<AppRegistryEpoch> AdvanceAsync(CancellationToken cancellationToken)
    {
        lock (_gate)
        {
            if (Fail)
            {
                return Task.FromException<AppRegistryEpoch>(new InvalidOperationException("epoch unreachable"));
            }

            if (FailAdvances > 0)
            {
                FailAdvances--;
                return Task.FromException<AppRegistryEpoch>(new InvalidOperationException("advance failed"));
            }

            _current = _current with { Version = _current.Version + 1 };
            return Task.FromResult(_current);
        }
    }
}
