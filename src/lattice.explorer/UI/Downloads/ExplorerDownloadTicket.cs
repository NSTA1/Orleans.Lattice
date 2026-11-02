using System.Security.Cryptography;
using System.Text;

namespace Orleans.Lattice.Explorer.UI.Downloads;

/// <summary>
/// A single-use, short-lived permission to stream one <see cref="ExplorerDownload"/>,
/// minted by a circuit and bound to it. The browser presents the
/// <see cref="Token"/> with the circuit's binding to the download endpoint; the circuit
/// keeps the ticket and watches it for progress and the outcome.
/// </summary>
/// <remarks>
/// <para>
/// The ticket is the only object the circuit and the endpoint share, and it holds no
/// services: the download it carries captured its own connection snapshot when the
/// circuit built it. Its <see cref="Changed"/> event fires on whatever thread the
/// download runs on, so a component that listens must marshal to its renderer.
/// </para>
/// <para>
/// Only a hash of the circuit's binding is kept, compared in constant time, so the
/// binding itself never sits in the process-wide store.
/// </para>
/// </remarks>
internal sealed class ExplorerDownloadTicket
{
    private readonly byte[] _bindingHash;
    private readonly CancellationTokenSource _withdrawn = new();
    private readonly Lock _gate = new();
    private ExplorerDownloadState _state = ExplorerDownloadState.Ready;
    private long _items;
    private long _bytes;
    private ExplorerDownloadTrailer? _trailer;

    /// <summary>Creates a ticket. Only <see cref="ExplorerDownloadTicketStore"/> mints them.</summary>
    /// <param name="token">The opaque, unguessable token the browser presents.</param>
    /// <param name="binding">The minting circuit's binding.</param>
    /// <param name="download">The download the ticket streams.</param>
    /// <param name="expiresAt">When the ticket stops being redeemable.</param>
    internal ExplorerDownloadTicket(string token, string binding, ExplorerDownload download, DateTimeOffset expiresAt)
    {
        ArgumentException.ThrowIfNullOrEmpty(token);
        ArgumentException.ThrowIfNullOrEmpty(binding);
        ArgumentNullException.ThrowIfNull(download);
        Token = token;
        Download = download;
        ExpiresAt = expiresAt;
        _bindingHash = Hash(binding);
    }

    /// <summary>The opaque token the browser presents. It authorizes nothing without the binding.</summary>
    public string Token { get; }

    /// <summary>The download the ticket streams.</summary>
    public ExplorerDownload Download { get; }

    /// <summary>When the ticket stops being redeemable.</summary>
    public DateTimeOffset ExpiresAt { get; }

    /// <summary>Where the ticket is in its life.</summary>
    public ExplorerDownloadState State
    {
        get
        {
            lock (_gate)
            {
                return _state;
            }
        }
    }

    /// <summary>How many items the download has written so far.</summary>
    public long Items => Interlocked.Read(ref _items);

    /// <summary>How many body bytes the download has written so far.</summary>
    public long Bytes => Interlocked.Read(ref _bytes);

    /// <summary>The trailer the file ended with, once <see cref="State"/> is <see cref="ExplorerDownloadState.Finished"/>.</summary>
    public ExplorerDownloadTrailer? Trailer
    {
        get
        {
            lock (_gate)
            {
                return _trailer;
            }
        }
    }

    /// <summary>Raised, on any thread, when the state, the progress or the trailer changes.</summary>
    public event Action? Changed;

    /// <summary>Signalled when the circuit withdraws a running download.</summary>
    internal CancellationToken Withdrawn => _withdrawn.Token;

    /// <summary>Records progress: the totals written so far.</summary>
    /// <param name="items">Items written so far.</param>
    /// <param name="bytes">Body bytes written so far.</param>
    public void Report(long items, long bytes)
    {
        Interlocked.Exchange(ref _items, items);
        Interlocked.Exchange(ref _bytes, bytes);
        Raise();
    }

    /// <summary>Whether <paramref name="binding"/> is the binding the ticket was minted for, compared in constant time.</summary>
    /// <param name="binding">The presented binding.</param>
    internal bool IsBoundTo(string? binding) =>
        !string.IsNullOrEmpty(binding)
        && binding.Length <= ExplorerDownloads.MaxFieldLength
        && CryptographicOperations.FixedTimeEquals(Hash(binding), _bindingHash);

    /// <summary>Moves a ready ticket to <paramref name="next"/>; false when it is no longer ready.</summary>
    /// <param name="next">The state to move to.</param>
    internal bool TryLeaveReady(ExplorerDownloadState next)
    {
        lock (_gate)
        {
            if (_state != ExplorerDownloadState.Ready)
            {
                return false;
            }

            _state = next;
        }

        Raise();
        return true;
    }

    /// <summary>Records the trailer the file ended with.</summary>
    /// <param name="trailer">The trailer.</param>
    internal void Finish(ExplorerDownloadTrailer trailer)
    {
        ArgumentNullException.ThrowIfNull(trailer);
        lock (_gate)
        {
            _trailer = trailer;
            _state = ExplorerDownloadState.Finished;
        }

        Raise();
    }

    /// <summary>
    /// Withdraws the ticket: a ready ticket becomes <see cref="ExplorerDownloadState.Revoked"/>,
    /// and a running download is cancelled, ending with a trailer that says so.
    /// </summary>
    internal void Withdraw()
    {
        if (!TryLeaveReady(ExplorerDownloadState.Revoked))
        {
            _withdrawn.Cancel();
        }
    }

    private void Raise()
    {
        try
        {
            Changed?.Invoke();
        }
        catch (Exception)
        {
            // A listener's fault must never stop the download it is watching.
        }
    }

    private static byte[] Hash(string value) => SHA256.HashData(Encoding.UTF8.GetBytes(value));
}
