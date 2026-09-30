namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// One circuit's queue of notifications, rendered by <see cref="LtToastRegion"/>.
/// Registered scoped, so each signed-in browser tab has its own.
/// </summary>
/// <remarks>
/// <para>
/// A toast stays until it is dismissed or pushed out by newer ones: nothing
/// times out, so a reader who looks away never misses a message (WCAG 2.2.1),
/// and nothing here depends on a timer. The queue keeps at most
/// <see cref="Capacity"/> toasts, dropping the oldest first.
/// </para>
/// <para>
/// News the page already shows - a tenant switch, which the address and the
/// header name at once - is an <see cref="Announce">announcement</see> instead:
/// read out politely by a screen reader, never drawn over the page, and so
/// never waiting to be dismissed.
/// </para>
/// </remarks>
internal sealed class LtToastService
{
    /// <summary>The most toasts the queue holds at once.</summary>
    public const int Capacity = 5;

    private readonly Lock _gate = new();
    private readonly List<LtToast> _toasts = [];
    private long _nextId;
    private LtToast? _announcement;

    /// <summary>Raised after the queue or the announcement changes.</summary>
    public event Action? Changed;

    /// <summary>
    /// The latest announcement, or <see langword="null"/> when there has been none.
    /// Each has its own id, so the same words announced twice are read twice.
    /// </summary>
    public LtToast? Announcement
    {
        get
        {
            lock (_gate)
            {
                return _announcement;
            }
        }
    }

    /// <summary>
    /// Announces news the page already shows, to screen readers only: it is not
    /// drawn, covers nothing, and replaces the previous announcement.
    /// </summary>
    /// <param name="message">The message, as plain text. Must not be empty.</param>
    /// <returns>The announcement.</returns>
    /// <exception cref="ArgumentException"><paramref name="message"/> is null, empty or white space.</exception>
    public LtToast Announce(string message)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);

        LtToast announcement;
        lock (_gate)
        {
            announcement = new LtToast(++_nextId, LtToastTone.Info, message);
            _announcement = announcement;
        }

        Changed?.Invoke();
        return announcement;
    }

    /// <summary>The toasts now showing, oldest first.</summary>
    public IReadOnlyList<LtToast> Toasts
    {
        get
        {
            lock (_gate)
            {
                return _toasts.ToArray();
            }
        }
    }

    /// <summary>Adds a toast to the queue.</summary>
    /// <param name="message">The message, as plain text. Must not be empty.</param>
    /// <param name="tone">What kind of news it carries.</param>
    /// <returns>The toast that was added.</returns>
    /// <exception cref="ArgumentException"><paramref name="message"/> is null, empty or white space.</exception>
    public LtToast Show(string message, LtToastTone tone = LtToastTone.Info)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(message);

        LtToast toast;
        lock (_gate)
        {
            toast = new LtToast(++_nextId, tone, message);
            _toasts.Add(toast);
            if (_toasts.Count > Capacity)
            {
                _toasts.RemoveAt(0);
            }
        }

        Changed?.Invoke();
        return toast;
    }

    /// <summary>Removes a toast from the queue.</summary>
    /// <param name="id">The toast's id.</param>
    /// <returns>Whether a toast was removed.</returns>
    public bool Dismiss(long id)
    {
        bool removed;
        lock (_gate)
        {
            removed = _toasts.RemoveAll(toast => toast.Id == id) > 0;
        }

        if (removed)
        {
            Changed?.Invoke();
        }

        return removed;
    }
}
