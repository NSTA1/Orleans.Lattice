using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// A directly driven <see cref="IExplorerSession"/> that records what it was
/// asked to apply and announces the change exactly as Core's session does.
/// </summary>
internal sealed class FakeExplorerSession : IExplorerSession
{
    /// <summary>Creates an unconfigured session over <paramref name="connection"/>.</summary>
    /// <param name="connection">The connection it exposes.</param>
    public FakeExplorerSession(FakeStateConnection connection) => Connection = connection.Connection;

    /// <inheritdoc />
    public ILatticeStateConnection Connection { get; }

    /// <inheritdoc />
    public bool IsConfigured { get; private set; }

    /// <inheritdoc />
    public ExplorerConfiguration? Current { get; private set; }

    /// <summary>Every configuration passed to <see cref="ApplyAsync"/>.</summary>
    public List<ExplorerConfiguration> Applied { get; } = [];

    /// <summary>When set, <see cref="ApplyAsync"/> throws it.</summary>
    public Exception? ApplyFailure { get; set; }

    /// <summary>How many times <see cref="InitializeAsync"/> ran.</summary>
    public int Initializations { get; private set; }

    /// <summary>How many handlers are subscribed to <see cref="ConfigurationChanged"/>.</summary>
    public int ConfigurationSubscribers => ConfigurationChanged?.GetInvocationList().Length ?? 0;

    /// <inheritdoc />
    public event Action? ConfigurationChanged;

    /// <summary>Starts configured with <paramref name="configuration"/>, as a loaded store does.</summary>
    /// <param name="configuration">The persisted configuration.</param>
    /// <returns>This session.</returns>
    public FakeExplorerSession Configured(ExplorerConfiguration configuration)
    {
        Current = configuration;
        IsConfigured = true;
        return this;
    }

    /// <inheritdoc />
    public Task<bool> InitializeAsync(CancellationToken cancellationToken = default)
    {
        Initializations++;
        return Task.FromResult(IsConfigured);
    }

    /// <inheritdoc />
    public Task ApplyAsync(ExplorerConfiguration configuration, CancellationToken cancellationToken = default)
    {
        if (ApplyFailure is not null)
        {
            return Task.FromException(ApplyFailure);
        }

        Applied.Add(configuration);
        Current = configuration;
        IsConfigured = true;
        ConfigurationChanged?.Invoke();
        return Task.CompletedTask;
    }
}
