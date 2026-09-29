using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Design.Components;

namespace Orleans.Lattice.Explorer.UI.Session;

/// <summary>
/// Announces a lost connection once per circuit: one toast when the connection
/// goes from usable to faulted, and that toast withdrawn the moment it recovers.
/// </summary>
/// <remarks>
/// <para>
/// It is scoped, so each circuit has exactly one, however many connection
/// indicators render (the header's, and the compact menu's while it is open).
/// Announcing from the indicator instead raised one toast per mounted indicator
/// and never withdrew them.
/// </para>
/// <para>
/// Core raises status changes on a thread-pool thread; the toast queue is
/// thread-safe and the region marshals its own rendering, so nothing here needs
/// the renderer. A repeated faulted report (a health check that fails again) is
/// the same outage and raises nothing.
/// </para>
/// </remarks>
internal sealed class SessionConnectionAnnouncer : IDisposable
{
    private readonly IExplorerSession _session;
    private readonly LtToastService _toasts;
    private readonly Lock _gate = new();
    private bool _started;
    private bool _faulted;
    private long? _toast;

    /// <summary>Creates the circuit's announcer.</summary>
    /// <param name="session">The circuit's connection session.</param>
    /// <param name="toasts">The circuit's toast queue.</param>
    /// <exception cref="ArgumentNullException">Either argument is <see langword="null"/>.</exception>
    public SessionConnectionAnnouncer(IExplorerSession session, LtToastService toasts)
    {
        ArgumentNullException.ThrowIfNull(session);
        ArgumentNullException.ThrowIfNull(toasts);
        _session = session;
        _toasts = toasts;
    }

    /// <summary>The toast currently announcing an outage, or <see langword="null"/> when none is.</summary>
    internal long? CurrentToast
    {
        get
        {
            lock (_gate)
            {
                return _toast;
            }
        }
    }

    /// <summary>Starts listening. Calling it again does nothing.</summary>
    public void Start()
    {
        lock (_gate)
        {
            if (_started)
            {
                return;
            }

            _started = true;
            _faulted = _session.Connection.Status.State == LatticeConnectionState.Faulted;
        }

        _session.Connection.StatusChanged += OnStatusChanged;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            if (!_started)
            {
                return;
            }

            _started = false;
        }

        _session.Connection.StatusChanged -= OnStatusChanged;
    }

    private void OnStatusChanged(LatticeConnectionStatus status)
    {
        lock (_gate)
        {
            if (status.State == LatticeConnectionState.Faulted)
            {
                if (_faulted)
                {
                    return;
                }

                _faulted = true;
                var where = status.Endpoint is { Length: > 0 } endpoint ? $" from {endpoint}" : string.Empty;
                var why = status.Message is { Length: > 0 } message ? $": {message}" : ".";
                _toast = _toasts.Show($"Disconnected{where}{why}", LtToastTone.Danger).Id;
                return;
            }

            if (status.State == LatticeConnectionState.Connected)
            {
                _faulted = false;
                if (_toast is { } id)
                {
                    _toasts.Dismiss(id);
                    _toast = null;
                }
            }
        }
    }
}
