using NSubstitute;
using Orleans.Lattice.Explorer.Core.Connection;

namespace Orleans.Lattice.Explorer.Tests.Shell.Session;

/// <summary>
/// A substitute <see cref="ILatticeStateConnection"/> whose status a test sets
/// and announces explicitly.
/// </summary>
internal sealed class FakeStateConnection
{
    private LatticeConnectionStatus _status = LatticeConnectionStatus.Disconnected;

    /// <summary>Creates the substitute.</summary>
    public FakeStateConnection()
    {
        Connection = Substitute.For<ILatticeStateConnection>();
        Connection.Status.Returns(_ => _status);
        Connection.ReconnectAsync(Arg.Any<CancellationToken>()).Returns(Task.FromResult(true));
    }

    /// <summary>The connection the explorer session exposes.</summary>
    public ILatticeStateConnection Connection { get; }

    /// <summary>Moves to <paramref name="status"/> and raises <see cref="ILatticeStateConnection.StatusChanged"/>.</summary>
    /// <param name="status">The new status.</param>
    public void Move(LatticeConnectionStatus status)
    {
        _status = status;
        Connection.StatusChanged += Raise.Event<Action<LatticeConnectionStatus>>(status);
    }

    /// <summary>Sets the status without announcing it, as the state before a component subscribes.</summary>
    /// <param name="status">The status.</param>
    public void Seed(LatticeConnectionStatus status) => _status = status;
}
