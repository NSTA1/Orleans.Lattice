using System.Buffers.Text;
using System.Collections.Concurrent;
using System.Security.Cryptography;

namespace Orleans.Lattice.Explorer.UI.Downloads;

/// <summary>
/// The process-wide register of outstanding download tickets, which the download
/// endpoint redeems against. It is the one singleton the download seam needs, because
/// a download request is not part of any circuit.
/// </summary>
/// <remarks>
/// <para>
/// <b>Single use.</b> Presenting a token removes it, whatever the outcome, so a token
/// can never be presented twice: a second presentation finds nothing.
/// </para>
/// <para>
/// <b>Short-lived and bounded.</b> A ticket is redeemable for
/// <see cref="DefaultLifetime"/>, and at most <see cref="MaxOutstanding"/> wait at once;
/// expired tickets are swept on every mint, and a mint beyond the bound is refused rather
/// than evicting a live ticket.
/// </para>
/// <para>
/// <b>Bound to a circuit.</b> Redemption requires the minting circuit's binding as well
/// as the token, and a token presented with any other binding is burned.
/// </para>
/// </remarks>
internal sealed class ExplorerDownloadTicketStore
{
    /// <summary>How long a ticket stays redeemable after it is minted.</summary>
    public static readonly TimeSpan DefaultLifetime = TimeSpan.FromMinutes(1);

    /// <summary>The most tickets that may wait to be presented at once, across every circuit.</summary>
    public const int MaxOutstanding = 512;

    private const int TokenBytes = 32;

    private readonly ConcurrentDictionary<string, ExplorerDownloadTicket> _tickets = new(StringComparer.Ordinal);
    private readonly TimeProvider _time;

    /// <summary>Creates the store.</summary>
    /// <param name="time">The clock, or <see langword="null"/> for the system clock.</param>
    public ExplorerDownloadTicketStore(TimeProvider? time = null) => _time = time ?? TimeProvider.System;

    /// <summary>How many tickets are waiting to be presented.</summary>
    public int Outstanding => _tickets.Count;

    /// <summary>Mints a ticket for <paramref name="download"/>, bound to <paramref name="binding"/>.</summary>
    /// <param name="download">The download.</param>
    /// <param name="binding">The minting circuit's binding.</param>
    /// <param name="lifetime">How long it stays redeemable, or <see langword="null"/> for <see cref="DefaultLifetime"/>.</param>
    /// <returns>The ticket.</returns>
    /// <exception cref="InvalidOperationException">Too many tickets are already waiting.</exception>
    public ExplorerDownloadTicket Mint(ExplorerDownload download, string binding, TimeSpan? lifetime = null)
    {
        ArgumentNullException.ThrowIfNull(download);
        ArgumentException.ThrowIfNullOrEmpty(binding);

        var now = _time.GetUtcNow();
        Sweep(now);
        if (_tickets.Count >= MaxOutstanding)
        {
            throw new InvalidOperationException("Too many downloads are waiting to start. Try again in a minute.");
        }

        var ticket = new ExplorerDownloadTicket(NewToken(), binding, download, now + (lifetime ?? DefaultLifetime));
        _tickets[ticket.Token] = ticket;
        return ticket;
    }

    /// <summary>
    /// Presents <paramref name="token"/> with <paramref name="binding"/>. Whatever the
    /// outcome, the token is spent.
    /// </summary>
    /// <param name="token">The presented token.</param>
    /// <param name="binding">The presented circuit binding.</param>
    /// <param name="ticket">The redeemed ticket, now running, when the outcome is <see cref="ExplorerDownloadRedemption.Redeemed"/>.</param>
    /// <returns>The outcome.</returns>
    public ExplorerDownloadRedemption Redeem(string? token, string? binding, out ExplorerDownloadTicket? ticket)
    {
        ticket = null;
        if (string.IsNullOrEmpty(token)
            || token.Length > ExplorerDownloads.MaxFieldLength
            || !_tickets.TryRemove(token, out var found))
        {
            return ExplorerDownloadRedemption.Unknown;
        }

        if (_time.GetUtcNow() >= found.ExpiresAt)
        {
            found.TryLeaveReady(ExplorerDownloadState.Expired);
            return ExplorerDownloadRedemption.Expired;
        }

        if (!found.IsBoundTo(binding))
        {
            found.TryLeaveReady(ExplorerDownloadState.Refused);
            return ExplorerDownloadRedemption.WrongCircuit;
        }

        if (!found.TryLeaveReady(ExplorerDownloadState.Running))
        {
            // Withdrawn by its circuit between the lookup and now.
            return ExplorerDownloadRedemption.Unknown;
        }

        ticket = found;
        return ExplorerDownloadRedemption.Redeemed;
    }

    /// <summary>Withdraws <paramref name="ticket"/>: it can no longer be redeemed, and a running download is stopped.</summary>
    /// <param name="ticket">The ticket.</param>
    public void Revoke(ExplorerDownloadTicket ticket)
    {
        ArgumentNullException.ThrowIfNull(ticket);
        _tickets.TryRemove(new KeyValuePair<string, ExplorerDownloadTicket>(ticket.Token, ticket));
        ticket.Withdraw();
    }

    private void Sweep(DateTimeOffset now)
    {
        foreach (var (token, ticket) in _tickets)
        {
            if (now >= ticket.ExpiresAt && _tickets.TryRemove(new KeyValuePair<string, ExplorerDownloadTicket>(token, ticket)))
            {
                ticket.TryLeaveReady(ExplorerDownloadState.Expired);
            }
        }
    }

    private static string NewToken()
    {
        Span<byte> bytes = stackalloc byte[TokenBytes];
        RandomNumberGenerator.Fill(bytes);
        return Base64Url.EncodeToString(bytes);
    }
}
