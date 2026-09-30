using Orleans.Lattice.Api.Telemetry;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>
/// The circuit's telemetry catalogue, read once and shared by the area's
/// availability probe, its Home status, its completions and its pages, so a
/// navigation never costs a catalogue round trip.
/// </summary>
/// <remarks>
/// The answer is kept whatever it was - a catalogue or a fault - until something
/// that could change it happens: a sign-in or sign-out, a new connection, a
/// different asserted tenant, or an explicit refresh. A caller's cancellation only abandons that caller's wait; the
/// shared read carries on for the next one.
/// </remarks>
internal sealed class TelemetryCatalogCache : IDisposable
{
    private readonly ILatticeTelemetry _telemetry;
    private readonly IExplorerAuthSession? _auth;
    private readonly IExplorerSession? _session;
    private readonly ShellCaller _caller;
    private readonly bool _ownsCaller;
    private readonly object _gate = new();
    private Task<TelemetryQueryCatalog>? _read;
    private ShellCallerKey _readCaller;

    /// <summary>Creates the cache over the circuit's telemetry facade.</summary>
    /// <param name="telemetry">The telemetry facade.</param>
    /// <param name="auth">The circuit's sign-in session, whose changes invalidate the cache.</param>
    /// <param name="session">The circuit's connection session, whose changes invalidate the cache.</param>
    /// <param name="tenant">The circuit's asserted tenant, read when no <paramref name="caller"/> is given.</param>
    /// <param name="caller">
    /// The circuit's caller, which keys the cached answer; when <see langword="null"/>,
    /// a caller over <paramref name="auth"/>, <paramref name="session"/> and <paramref name="tenant"/>.
    /// </param>
    public TelemetryCatalogCache(
        [FromKeyedServices(ShellFacades.Key)] ILatticeTelemetry telemetry,
        IExplorerAuthSession? auth = null,
        IExplorerSession? session = null,
        ShellAssertedTenant? tenant = null,
        ShellCaller? caller = null)
    {
        ArgumentNullException.ThrowIfNull(telemetry);
        _telemetry = telemetry;
        _auth = auth;
        _session = session;
        _ownsCaller = caller is null;
        _caller = caller ?? new ShellCaller(auth, session, tenant);

        if (_auth is not null)
        {
            _auth.AuthenticationChanged += Invalidate;
        }

        if (_session is not null)
        {
            _session.ConfigurationChanged += Invalidate;
        }
    }

    /// <summary>Raised when the cached answer is dropped, so a page re-reads it.</summary>
    public event Action? Changed;

    /// <summary>The catalogue, when it has been read successfully for the caller now; otherwise <see langword="null"/>.</summary>
    public TelemetryQueryCatalog? Current
    {
        get
        {
            var caller = _caller.Current;
            lock (_gate)
            {
                return _read is { IsCompletedSuccessfully: true } read && _readCaller == caller ? read.Result : null;
            }
        }
    }

    /// <summary>Reads the catalogue, sharing one read across callers.</summary>
    /// <param name="cancellationToken">Abandons this caller's wait.</param>
    /// <returns>The catalogue.</returns>
    public Task<TelemetryQueryCatalog> GetAsync(CancellationToken cancellationToken = default)
    {
        var caller = _caller.Current;
        Task<TelemetryQueryCatalog> read;
        lock (_gate)
        {
            if (_read is null || _read.IsCanceled || _readCaller != caller)
            {
                _read = ReadAsync();
                _readCaller = caller;
            }

            read = _read;
        }

        return read.WaitAsync(cancellationToken);
    }

    /// <summary>Drops the cached answer, so the next read asks the cluster again.</summary>
    public void Invalidate()
    {
        lock (_gate)
        {
            _read = null;
        }

        Changed?.Invoke();
    }

    /// <inheritdoc />
    public void Dispose()
    {
        if (_auth is not null)
        {
            _auth.AuthenticationChanged -= Invalidate;
        }

        if (_session is not null)
        {
            _session.ConfigurationChanged -= Invalidate;
        }

        if (_ownsCaller)
        {
            _caller.Dispose();
        }
    }

    // Async so a transport that refuses synchronously (no connection configured)
    // is remembered as a faulted read like any other fault.
    private async Task<TelemetryQueryCatalog> ReadAsync() =>
        await _telemetry.GetCatalogAsync(CancellationToken.None).ConfigureAwait(false);
}
