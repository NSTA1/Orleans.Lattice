using System.Buffers.Text;
using System.Security.Cryptography;
using Orleans.Lattice.Explorer.Core.Authentication;
using Orleans.Lattice.Explorer.Core.Configuration;
using Orleans.Lattice.Explorer.Core.Connection;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Downloads;

/// <summary>
/// A circuit's side of the download seam: it mints the circuit's download tickets,
/// captures the connection a download runs over, and withdraws every ticket it minted
/// when the circuit's sign-in or endpoint changes or the circuit ends.
/// </summary>
/// <remarks>
/// <para>
/// <b>Scoped per circuit.</b> It reads the circuit's own session, sign-in and asserted
/// tenant, and holds the circuit's <see cref="Binding"/>, so it must never be captured
/// by a singleton. What it hands the process-wide <see cref="ExplorerDownloadTicketStore"/>
/// is a ticket carrying values, never this service or the circuit's other scoped services.
/// </para>
/// <para>
/// <b>Runs as the circuit's user.</b> <see cref="CaptureConnection"/> snapshots the
/// endpoint, the sign-in attached to that endpoint, and the tenant asserted right now,
/// so a download reads exactly what this user may read, through the facades, which
/// authorize every call. The credential stays in server memory: the browser carries only
/// the opaque token and the binding, in a form post, never in a URL.
/// </para>
/// <para>
/// <b>Bound to the credential.</b> A sign-in, a sign-out or a new endpoint withdraws every
/// ticket the circuit minted: a waiting one can no longer be redeemed and a running one
/// stops, its file ending with a trailer that says so. Ending the circuit does the same,
/// because the credential a download captured belongs to the circuit.
/// </para>
/// </remarks>
internal sealed class ExplorerDownloads : IDisposable
{
    /// <summary>The download endpoint's address, relative to the Explorer's base href.</summary>
    public const string FormAction = "downloads";

    /// <summary>The form field carrying the ticket's token.</summary>
    public const string TokenField = "token";

    /// <summary>The form field carrying the circuit's binding.</summary>
    public const string BindingField = "binding";

    /// <summary>The longest token or binding the endpoint will consider.</summary>
    public const int MaxFieldLength = 128;

    private readonly ExplorerDownloadTicketStore _store;
    private readonly IExplorerSession? _session;
    private readonly IExplorerAuthSession? _auth;
    private readonly ShellAssertedTenant? _tenant;
    private readonly List<ExplorerDownloadTicket> _minted = [];
    private readonly Lock _gate = new();
    private bool _disposed;

    /// <summary>Creates the circuit's downloads.</summary>
    /// <param name="store">The process-wide ticket store.</param>
    /// <param name="session">The circuit's session, which owns the endpoint; <see langword="null"/> when the head registers none.</param>
    /// <param name="auth">The circuit's sign-in; <see langword="null"/> when the head registers none.</param>
    /// <param name="tenant">The circuit's asserted tenant; <see langword="null"/> asserts none.</param>
    public ExplorerDownloads(
        ExplorerDownloadTicketStore store,
        IExplorerSession? session = null,
        IExplorerAuthSession? auth = null,
        ShellAssertedTenant? tenant = null)
    {
        ArgumentNullException.ThrowIfNull(store);
        _store = store;
        _session = session;
        _auth = auth;
        _tenant = tenant;
        Binding = NewBinding();

        if (_session is not null)
        {
            _session.ConfigurationChanged += WithdrawAll;
        }

        if (_auth is not null)
        {
            _auth.AuthenticationChanged += WithdrawAll;
        }
    }

    /// <summary>
    /// The circuit's binding: a random value only this circuit renders, presented with a
    /// token to prove the token is being redeemed by the circuit that minted it.
    /// </summary>
    public string Binding { get; }

    /// <summary>
    /// Snapshots the connection a download runs over: the endpoint, the sign-in attached
    /// to that endpoint and the tenant asserted now. <see langword="null"/> when no
    /// endpoint is configured.
    /// </summary>
    /// <returns>The settings, or <see langword="null"/>.</returns>
    public LatticeConnectionSettings? CaptureConnection()
    {
        if (_session?.Current is not { } configuration)
        {
            return null;
        }

        return configuration.ToConnectionSettings() with
        {
            Authentication = _auth?.GetAuthenticationFor(configuration.Endpoint),
            ActiveTenantProvider = new FixedTenant(_tenant?.AssertedTenant),
        };
    }

    /// <summary>Mints a ticket for <paramref name="download"/>, bound to this circuit.</summary>
    /// <param name="download">The download.</param>
    /// <returns>The ticket, whose token the circuit renders into the download form.</returns>
    /// <exception cref="ObjectDisposedException">The circuit has ended.</exception>
    /// <exception cref="InvalidOperationException">Too many downloads are waiting across the process.</exception>
    public ExplorerDownloadTicket Mint(ExplorerDownload download)
    {
        ArgumentNullException.ThrowIfNull(download);
        lock (_gate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _minted.RemoveAll(static ticket => ticket.State is not (ExplorerDownloadState.Ready or ExplorerDownloadState.Running));
            var ticket = _store.Mint(download, Binding);
            _minted.Add(ticket);
            return ticket;
        }
    }

    /// <summary>Withdraws <paramref name="ticket"/>, which this circuit minted.</summary>
    /// <param name="ticket">The ticket.</param>
    public void Withdraw(ExplorerDownloadTicket ticket)
    {
        ArgumentNullException.ThrowIfNull(ticket);
        lock (_gate)
        {
            if (!_minted.Remove(ticket))
            {
                return;
            }
        }

        _store.Revoke(ticket);
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            _disposed = true;
        }

        if (_session is not null)
        {
            _session.ConfigurationChanged -= WithdrawAll;
        }

        if (_auth is not null)
        {
            _auth.AuthenticationChanged -= WithdrawAll;
        }

        WithdrawAll();
    }

    private void WithdrawAll()
    {
        ExplorerDownloadTicket[] minted;
        lock (_gate)
        {
            minted = [.. _minted];
            _minted.Clear();
        }

        foreach (var ticket in minted)
        {
            _store.Revoke(ticket);
        }
    }

    private static string NewBinding()
    {
        Span<byte> bytes = stackalloc byte[32];
        RandomNumberGenerator.Fill(bytes);
        return Base64Url.EncodeToString(bytes);
    }

    /// <summary>A tenant fixed when a download was prepared, so switching tenant mid-download changes nothing.</summary>
    private sealed class FixedTenant(string? tenant) : ILatticeActiveTenantProvider
    {
        public string? AssertedTenant { get; } = tenant;
    }
}
