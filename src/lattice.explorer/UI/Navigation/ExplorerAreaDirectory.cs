using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Orleans.Lattice.Explorer.UI.Navigation.Address;
using Orleans.Lattice.Explorer.UI.Transport;

namespace Orleans.Lattice.Explorer.UI.Navigation;

/// <summary>
/// The directory of native areas: every registered <see cref="IExplorerArea"/>,
/// validated once and held in spine order, and the fail-closed questions the
/// chrome asks of them.
/// </summary>
/// <remarks>
/// <para>
/// Keys are validated when the directory is built, so a misdeclared area fails
/// the circuit loudly rather than producing an unreachable stop: every key must be
/// a lower-case keyword, unique, and not one of the <see cref="ReservedKeys"/>.
/// </para>
/// <para>
/// The chrome asks again on navigation, because availability can change with
/// sign-in or a grant, and an area memoizes its own capability probe per circuit.
/// <see cref="Invalidate"/> raises <see cref="Changed"/> for a change the chrome
/// cannot see, such as a sign-in.
/// </para>
/// <para>
/// A probe that meets a transient fault (<see cref="AreaProbeFaults"/>) or no
/// answer in time is not an answer, so it neither hides the area nor admits the
/// caller (issue #5420). The directory shows the last answer this caller was given
/// for the area, marked stale, when that answer showed it; it keeps a hidden area
/// hidden; and with no earlier answer it shows the area as
/// <see cref="AreaAvailabilityKind.Unchecked"/>, which cannot be opened. It then
/// checks again on its own after each of <see cref="ExplorerChromeOptions.RecheckDelays"/>,
/// and <see cref="Recheck"/> checks again at once. Every other fault is a definite
/// answer and hides the area, as before. What it remembers belongs to the caller
/// it was read for, and is forgotten when the caller changes.
/// </para>
/// </remarks>
internal sealed class ExplorerAreaDirectory : IDisposable
{
    private readonly IExplorerArea[] _areas;
    private readonly Dictionary<string, IExplorerArea> _byKey;
    private readonly ExplorerChromeOptions _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly ShellCaller? _caller;
    private readonly object _gate = new();
    private readonly Dictionary<string, (AreaAvailability Availability, string? Badge)> _knownAvailability = new(StringComparer.Ordinal);
    private readonly Dictionary<string, string?> _knownStatus = new(StringComparer.Ordinal);
    private readonly HashSet<string> _uncheckedAvailability = new(StringComparer.Ordinal);
    private readonly HashSet<string> _uncheckedStatus = new(StringComparer.Ordinal);
    private ShellCallerKey _memoCaller;
    private ITimer? _recheck;
    private int _recheckAttempt;
    private int _recheckStateChanged;
    private bool _disposed;

    /// <summary>Builds the directory over every registered area.</summary>
    /// <param name="areas">The registered areas.</param>
    /// <param name="options">The chrome's time bounds.</param>
    /// <param name="time">The clock the bounds are measured on.</param>
    /// <param name="logger">Where a failing area is reported.</param>
    /// <param name="caller">The circuit's caller, which keys every answer remembered; <see langword="null"/> keys them all alike.</param>
    /// <exception cref="InvalidOperationException">An area's key is invalid, reserved or duplicated.</exception>
    public ExplorerAreaDirectory(
        IEnumerable<IExplorerArea> areas,
        ExplorerChromeOptions options,
        TimeProvider time,
        ILogger<ExplorerAreaDirectory>? logger = null,
        ShellCaller? caller = null)
    {
        ArgumentNullException.ThrowIfNull(areas);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(time);

        _options = options;
        _time = time;
        _logger = logger ?? NullLogger<ExplorerAreaDirectory>.Instance;
        _caller = caller;
        _memoCaller = CallerNow;
        _byKey = new Dictionary<string, IExplorerArea>(StringComparer.Ordinal);

        foreach (var area in areas)
        {
            var key = area.Key;
            if (!ExplorerAddressEncoding.IsKeyword(key) || ReservedKeys.Contains(key))
            {
                throw new InvalidOperationException(
                    $"The area {area.GetType().Name} declares the key '{key}', which is not a lower-case area key or is reserved ({string.Join(", ", ReservedKeys)}).");
            }

            if (!_byKey.TryAdd(key, area))
            {
                throw new InvalidOperationException(
                    $"The areas {_byKey[key].GetType().Name} and {area.GetType().Name} both declare the key '{key}'.");
            }
        }

        _areas = [.. _byKey.Values.OrderBy(area => area.DirectoryOrder).ThenBy(area => area.Key, StringComparer.Ordinal)];
    }

    /// <summary>Keys no area may take, because the grammar or a Shell page owns them.</summary>
    public static IReadOnlySet<string> ReservedKeys { get; } = new HashSet<string>(StringComparer.Ordinal)
    {
        ExplorerAddress.TenantSegment,
        ExplorerRoutes.NotFoundSegment,
    };

    /// <summary>
    /// Raised when something the chrome cannot observe may have changed which areas
    /// are shown, including when an automatic recheck falls due. It may be raised
    /// off the renderer's thread.
    /// </summary>
    public event Action? Changed;

    /// <summary>
    /// Raised when <see cref="IsRecheckScheduled"/> may have changed: a recheck was
    /// scheduled, fell due, was asked for, or everything settled. It may be raised
    /// off the renderer's thread.
    /// </summary>
    public event Action? RecheckStateChanged;

    /// <summary>Every registered area, in spine order, whatever its availability.</summary>
    public IReadOnlyList<IExplorerArea> Areas => _areas;

    /// <summary>
    /// Whether the directory will still check again on its own: something could not
    /// be checked, and a recheck is waiting or the attempts in
    /// <see cref="ExplorerChromeOptions.RecheckDelays"/> are not yet spent.
    /// </summary>
    public bool IsRecheckScheduled
    {
        get
        {
            lock (_gate)
            {
                return !_disposed
                    && (_uncheckedAvailability.Count > 0 || _uncheckedStatus.Count > 0)
                    && (_recheck is not null || _recheckAttempt < _options.RecheckDelays.Count);
            }
        }
    }

    /// <summary>The area with <paramref name="key"/>, or <see langword="null"/>.</summary>
    /// <param name="key">The area key.</param>
    public IExplorerArea? Find(string? key) =>
        key is not null && _byKey.TryGetValue(key, out var area) ? area : null;

    /// <summary>Announces that availability may have changed, so the chrome asks again.</summary>
    public void Invalidate() => Changed?.Invoke();

    /// <summary>
    /// Checks again now: the manual retry. It restarts the automatic rechecks'
    /// backoff and raises <see cref="Changed"/>, so the chrome asks every area again.
    /// </summary>
    public void Recheck()
    {
        lock (_gate)
        {
            CancelRecheck();
            _recheckAttempt = 0;
            MarkRecheckStateChanged();
        }

        NotifyRecheckState();
        Changed?.Invoke();
    }

    /// <summary>
    /// The shown stops, in spine order: every area that answered
    /// <see cref="AreaAvailabilityKind.Visible"/> or
    /// <see cref="AreaAvailabilityKind.Unavailable"/> in time, and every area that
    /// could not be checked, as its last shown answer marked stale or as
    /// <see cref="AreaAvailabilityKind.Unchecked"/>. All areas are asked in parallel.
    /// </summary>
    /// <param name="cancellationToken">Cancels the whole request.</param>
    public async Task<IReadOnlyList<ExplorerAreaEntry>> GetEntriesAsync(CancellationToken cancellationToken = default)
    {
        var probes = new Task<(AreaAvailability Availability, string? Badge, bool IsStale)>[_areas.Length];
        for (var i = 0; i < _areas.Length; i++)
        {
            probes[i] = CheckAsync(_areas[i], cancellationToken);
        }

        var answers = await Task.WhenAll(probes).ConfigureAwait(false);

        var entries = new List<ExplorerAreaEntry>(_areas.Length);
        for (var i = 0; i < _areas.Length; i++)
        {
            if (answers[i].Availability.IsShown)
            {
                entries.Add(new ExplorerAreaEntry(_areas[i], answers[i].Availability, answers[i].Badge) { IsStale = answers[i].IsStale });
            }
        }

        return entries;
    }

    /// <summary>
    /// Asks one area for its spine badge, failing closed to <see langword="null"/>
    /// on a fault or after <see cref="ExplorerChromeOptions.AvailabilityTimeout"/>.
    /// </summary>
    /// <param name="area">The area.</param>
    /// <param name="cancellationToken">The caller's token; its cancellation propagates.</param>
    public async Task<string?> GetDirectoryBadgeAsync(IExplorerArea area, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(area);

        var (outcome, value, error) = await TimeBoxed
            .RunAsync(area.GetDirectoryBadgeAsync, _options.AvailabilityTimeout, _time, cancellationToken)
            .ConfigureAwait(false);

        if (outcome == TimeBoxed.Outcome.Completed)
        {
            return string.IsNullOrWhiteSpace(value) ? null : value;
        }

        _logger.LogInformation(error, "The {Area} area did not report its directory badge ({Outcome}).", area.Key, outcome);
        return null;
    }

    /// <summary>
    /// Asks one area whether it may be seen, failing closed: a definite fault or a
    /// cancellation is <see cref="AreaAvailability.Hidden"/>; a transient fault or no
    /// answer within <see cref="ExplorerChromeOptions.AvailabilityTimeout"/> is the
    /// last shown answer for this caller, or <see cref="AreaAvailability.Unchecked"/>.
    /// Neither ever admits a caller the area did not admit.
    /// </summary>
    /// <param name="area">The area.</param>
    /// <param name="cancellationToken">The caller's token; its cancellation propagates.</param>
    public async Task<AreaAvailability> GetAvailabilityAsync(IExplorerArea area, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(area);
        return (await CheckAsync(area, cancellationToken).ConfigureAwait(false)).Availability;
    }

    /// <summary>
    /// Asks one area for its Home status line: <see langword="null"/> (no line) when
    /// it has none or on a definite fault; on a transient fault or after
    /// <see cref="ExplorerChromeOptions.HomeStatusTimeout"/>, the last line read for
    /// this caller marked stale, or <see cref="ExplorerAreaStatus.Unchecked"/>.
    /// </summary>
    /// <param name="area">The area.</param>
    /// <param name="cancellationToken">The caller's token; its cancellation propagates.</param>
    public async Task<ExplorerAreaStatus?> GetHomeStatusAsync(IExplorerArea area, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(area);

        var caller = CallerNow;
        var (outcome, value, error) = await TimeBoxed
            .RunAsync(area.GetHomeStatusAsync, _options.HomeStatusTimeout, _time, cancellationToken)
            .ConfigureAwait(false);

        if (outcome == TimeBoxed.Outcome.Completed)
        {
            var text = string.IsNullOrWhiteSpace(value) ? null : value;
            SettleStatus(area.Key, caller, text);
            return text is null ? null : new ExplorerAreaStatus { Text = text };
        }

        if (outcome == TimeBoxed.Outcome.Faulted && !AreaProbeFaults.IsTransient(error))
        {
            _logger.LogWarning(error, "The {Area} area did not report its Home status ({Outcome}).", area.Key, outcome);
            SettleStatus(area.Key, caller, null);
            return null;
        }

        _logger.LogWarning(error, "The {Area} area's Home status could not be checked ({Outcome}); it is checked again.", area.Key, outcome);
        ExplorerAreaStatus status;
        lock (_gate)
        {
            if (!StillFor(caller))
            {
                status = ExplorerAreaStatus.Unchecked;
            }
            else
            {
                if (_uncheckedStatus.Add(area.Key))
                {
                    MarkRecheckStateChanged();
                }

                ScheduleRecheck();
                status = _knownStatus.TryGetValue(area.Key, out var known) && known is not null
                    ? new ExplorerAreaStatus { Text = known, IsStale = true }
                    : ExplorerAreaStatus.Unchecked;
            }
        }

        NotifyRecheckState();
        return status;
    }

    /// <inheritdoc />
    public void Dispose()
    {
        lock (_gate)
        {
            _disposed = true;
            CancelRecheck();
        }
    }

    private ShellCallerKey CallerNow => _caller?.Current ?? default;

    private async Task<(AreaAvailability Availability, string? Badge, bool IsStale)> CheckAsync(IExplorerArea area, CancellationToken cancellationToken)
    {
        var caller = CallerNow;
        var (outcome, value, error) = await TimeBoxed
            .RunAsync(area.GetAvailabilityAsync, _options.AvailabilityTimeout, _time, cancellationToken)
            .ConfigureAwait(false);

        if (outcome == TimeBoxed.Outcome.Completed && value.Kind != AreaAvailabilityKind.Unchecked)
        {
            var badge = value.Kind == AreaAvailabilityKind.Visible
                ? await GetDirectoryBadgeAsync(area, cancellationToken).ConfigureAwait(false)
                : null;
            SettleAvailability(area.Key, caller, value, badge);
            return (value, badge, false);
        }

        if (outcome == TimeBoxed.Outcome.Faulted && !AreaProbeFaults.IsTransient(error))
        {
            _logger.LogWarning(
                error,
                "The {Area} area did not report its availability ({Outcome}); it is hidden.",
                area.Key,
                outcome);
            SettleAvailability(area.Key, caller, AreaAvailability.Hidden, null);
            return (AreaAvailability.Hidden, null, false);
        }

        // No answer: a transient fault, no answer in time, or the area itself
        // reporting that it could not check. Never an answer that admits.
        _logger.LogWarning(
            error,
            "The {Area} area's availability could not be checked ({Outcome}); it is shown as not checked and checked again.",
            area.Key,
            outcome == TimeBoxed.Outcome.Completed ? "Unchecked" : outcome.ToString());
        (AreaAvailability Availability, string? Badge, bool IsStale) answer;
        lock (_gate)
        {
            if (!StillFor(caller))
            {
                answer = (AreaAvailability.Unchecked, null, false);
            }
            else
            {
                if (_uncheckedAvailability.Add(area.Key))
                {
                    MarkRecheckStateChanged();
                }

                ScheduleRecheck();

                // A hidden answer stays hidden: a fault never reveals an area this
                // caller was refused.
                answer = _knownAvailability.TryGetValue(area.Key, out var known)
                    ? known.Availability.IsShown
                        ? (known.Availability, known.Badge, true)
                        : (AreaAvailability.Hidden, null, false)
                    : (AreaAvailability.Unchecked, null, false);
            }
        }

        NotifyRecheckState();
        return answer;
    }

    private void SettleAvailability(string key, ShellCallerKey caller, AreaAvailability availability, string? badge)
    {
        lock (_gate)
        {
            if (!StillFor(caller))
            {
                return;
            }

            _knownAvailability[key] = (availability, badge);
            var settled = _uncheckedAvailability.Remove(key);

            // Home asks only a visible area for its status, so a status still
            // waiting to be checked would otherwise wait forever.
            settled |= availability.Kind != AreaAvailabilityKind.Visible && _uncheckedStatus.Remove(key);
            if (settled)
            {
                MarkRecheckStateChanged();
                ResetRecheckWhenSettled();
            }
        }

        NotifyRecheckState();
    }

    private void SettleStatus(string key, ShellCallerKey caller, string? text)
    {
        lock (_gate)
        {
            if (!StillFor(caller))
            {
                return;
            }

            _knownStatus[key] = text;
            if (_uncheckedStatus.Remove(key))
            {
                MarkRecheckStateChanged();
                ResetRecheckWhenSettled();
            }
        }

        NotifyRecheckState();
    }

    /// <summary>
    /// Whether <paramref name="caller"/>, the caller an answer was read for, is still
    /// the caller, forgetting everything read for an earlier one. Call under the gate.
    /// </summary>
    private bool StillFor(ShellCallerKey caller)
    {
        var now = CallerNow;
        if (_memoCaller != now)
        {
            _memoCaller = now;
            _knownAvailability.Clear();
            _knownStatus.Clear();
            _uncheckedAvailability.Clear();
            _uncheckedStatus.Clear();
            _recheckAttempt = 0;
            CancelRecheck();
            MarkRecheckStateChanged();
        }

        return now == caller;
    }

    /// <summary>Starts the next automatic recheck unless one is waiting or the attempts are spent. Call under the gate.</summary>
    private void ScheduleRecheck()
    {
        var delays = _options.RecheckDelays;
        if (_disposed || _recheck is not null || _recheckAttempt >= delays.Count)
        {
            return;
        }

        var delay = delays[_recheckAttempt++];
        _recheck = _time.CreateTimer(
            static state => ((ExplorerAreaDirectory)state!).OnRecheckDue(),
            this,
            delay,
            Timeout.InfiniteTimeSpan);
    }

    private void OnRecheckDue()
    {
        lock (_gate)
        {
            if (_disposed)
            {
                return;
            }

            CancelRecheck();
            MarkRecheckStateChanged();
        }

        NotifyRecheckState();
        Changed?.Invoke();
    }

    /// <summary>Once nothing is waiting to be checked, stops the rechecks and restarts their backoff. Call under the gate.</summary>
    private void ResetRecheckWhenSettled()
    {
        if (_uncheckedAvailability.Count == 0 && _uncheckedStatus.Count == 0)
        {
            _recheckAttempt = 0;
            CancelRecheck();
        }
    }

    /// <summary>Records that <see cref="IsRecheckScheduled"/> may have changed. Call under the gate.</summary>
    private void MarkRecheckStateChanged() => Volatile.Write(ref _recheckStateChanged, 1);

    /// <summary>Raises <see cref="RecheckStateChanged"/> once for every change recorded. Call outside the gate.</summary>
    private void NotifyRecheckState()
    {
        if (Interlocked.Exchange(ref _recheckStateChanged, 0) == 1)
        {
            RecheckStateChanged?.Invoke();
        }
    }

    /// <summary>Call under the gate.</summary>
    private void CancelRecheck()
    {
        _recheck?.Dispose();
        _recheck = null;
    }
}
