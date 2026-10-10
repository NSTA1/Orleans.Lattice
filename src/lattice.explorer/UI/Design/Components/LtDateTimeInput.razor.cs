using System.Globalization;
using Microsoft.AspNetCore.Components;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.JSInterop;
using Orleans.Lattice.Explorer.UI.Design.Tokens;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A labelled point in time: a calendar and time picker over a typed ISO 8601 entry,
/// in UTC with the zone always shown, the reader's own local time beside it as
/// secondary text, quick picks, a minimum and a maximum, and an optional empty state.
/// </summary>
/// <remarks>
/// <para>
/// The time zone is explicit and never converted silently. Every value is held, typed and
/// shown in UTC, and the box says so. A typed time that names another offset is read as the
/// instant it names, and the box then rewrites it in UTC. The reader's local time is
/// secondary text, read from the browser once the page is interactive. Values are to the
/// second.
/// </para>
/// <para>
/// The field is a label row over one control box, like every other field primitive, so
/// it lines up in a toolbar. Its picker opens below the box, and on a phone it opens in
/// the flow of the page. Everything in the picker works from the keyboard: the arrow keys
/// move by day and week, Page Up and Page Down by month (with Shift, by year), Home and End
/// to the ends of the week, and Escape closes the picker and returns focus to its button.
/// Navigation stops at the first and last representable dates.
/// </para>
/// <para>
/// A value is raised through <see cref="ValueChanged"/> as soon as what is typed reads as
/// an instant in range. Call <see cref="ConfirmAsync"/> before acting on a submit, so a
/// value that does not read, or is out of range, shows its message and is not acted on.
/// </para>
/// </remarks>
public partial class LtDateTimeInput : IAsyncDisposable
{
    private readonly string _id = LtIds.Next("lt-datetime");
    private ElementReference _root;
    private ElementReference _input;
    private ElementReference _toggle;
    private ElementReference _activeDay;
    private IJSObjectReference? _behaviour;
    private bool _disposed;
    private bool _seen;
    private DateTimeOffset? _lastValue;
    private string _text = string.Empty;
    private string? _ownError;
    private bool _open;
    private DateOnly _month;
    private DateOnly _active;
    private bool _focusActiveDay;
    private bool _focusToggle;
    private TimeZoneInfo? _localZone;
    private TimeProvider? _time;

    /// <summary>The visible label.</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The chosen instant, in UTC, or <see langword="null"/> when the field is empty.</summary>
    [Parameter]
    public DateTimeOffset? Value { get; set; }

    /// <summary>Raised with each new instant the field reads, or <see langword="null"/> when it is emptied.</summary>
    [Parameter]
    public EventCallback<DateTimeOffset?> ValueChanged { get; set; }

    /// <summary>
    /// What an empty field means, such as <c>Latest</c>. When set the field may be left
    /// empty, the text shows while it is, and the picker offers to clear it. When
    /// <see langword="null"/> a value is required.
    /// </summary>
    [Parameter]
    public string? EmptyText { get; set; }

    /// <summary>A short explanation shown under the box and announced with it.</summary>
    [Parameter]
    public string? Hint { get; set; }

    /// <summary>
    /// A validation message from the page. It is shown in place of the field's own, marks the
    /// field invalid, and is announced with it.
    /// </summary>
    [Parameter]
    public string? Error { get; set; }

    /// <summary>The earliest instant the field accepts, or <see langword="null"/> for none.</summary>
    [Parameter]
    public DateTimeOffset? Min { get; set; }

    /// <summary>The latest instant the field accepts, or <see langword="null"/> for none.</summary>
    [Parameter]
    public DateTimeOffset? Max { get; set; }

    /// <summary>Whether an instant later than now is accepted. Defaults to <see langword="true"/>.</summary>
    [Parameter]
    public bool AllowFuture { get; set; } = true;

    /// <summary>Whether the picker offers quick picks: now, an hour ago, a day ago and a week ago.</summary>
    [Parameter]
    public bool QuickPicks { get; set; } = true;

    /// <summary>Whether the field is disabled.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    [Inject]
    internal IJSRuntime JS { get; set; } = default!;

    [Inject]
    internal IServiceProvider Services { get; set; } = default!;

    [CascadingParameter(Name = LtBreakpointCascade.Name)]
    private LtBreakpoint? Breakpoint { get; set; }

    /// <summary>The input element's id, for a caller that needs to point at it.</summary>
    public string InputId => _id;

    /// <summary>The button that opens the picker, where focus returns when it closes.</summary>
    internal ElementReference ToggleReference => _toggle;

    /// <summary>The calendar's one focusable day, where focus goes as the keyboard moves it.</summary>
    internal ElementReference ActiveDayReference => _activeDay;

    private string ZoneId => _id + "-zone";

    private string LocalId => _id + "-local";

    private string HintId => _id + "-hint";

    private string ErrorId => _id + "-error";

    private string PickerId => _id + "-picker";

    private string MonthId => _id + "-month";

    private string TimeId => _id + "-time";

    private string? EffectiveError => Error ?? _ownError;

    private string? LocalText => Value is { } value && _localZone is { } zone
        ? "Your local time: " + LtTimeText.InZone(value, zone)
        : null;

    private string DescribedBy
    {
        get
        {
            var ids = ZoneId;
            if (Value is not null && _localZone is not null)
            {
                ids += " " + LocalId;
            }

            if (Hint is not null)
            {
                ids += " " + HintId;
            }

            return EffectiveError is null ? ids : ids + " " + ErrorId;
        }
    }

    private string ControlClass => EffectiveError is null ? "lt-datetime__control" : "lt-datetime__control lt-datetime__control--invalid";

    private string PickerClass => Breakpoint == LtBreakpoint.Compact ? "lt-datetime__picker lt-datetime__picker--inline" : "lt-datetime__picker";

    private DateTimeOffset Now => LtTimeText.ToSecond((_time ??= Services.GetService<TimeProvider>() ?? TimeProvider.System).GetUtcNow());

    /// <summary>The latest instant accepted now: <see cref="Max"/>, or now when the future is refused, whichever is earlier.</summary>
    private DateTimeOffset? EffectiveMax
    {
        get
        {
            var now = AllowFuture ? (DateTimeOffset?)null : Now;
            return (Max, now) switch
            {
                (null, null) => null,
                ({ } max, null) => max,
                (null, { } latest) => latest,
                ({ } max, { } latest) => max < latest ? max : latest,
            };
        }
    }

    /// <summary>
    /// Reads what is typed, shows the field's own message when it does not read or is out of
    /// range, and otherwise raises the instant: call it before acting on a submit.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the field holds an instant in range, or is empty and may be;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public async Task<bool> ConfirmAsync()
    {
        var accepted = await CommitAsync().ConfigureAwait(true);
        StateHasChanged();
        return accepted;
    }

    /// <summary>Moves keyboard focus to the typed entry.</summary>
    /// <returns>A task that completes when the focus request has been answered or refused.</returns>
    /// <remarks>A request whose input a later render removed is a focus not taken, never a fault.</remarks>
    public ValueTask FocusAsync() => _input.FocusSafelyAsync();

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        _disposed = true;
        if (_behaviour is { } behaviour)
        {
            _behaviour = null;
            try
            {
                await behaviour.InvokeVoidAsync("dispose").ConfigureAwait(false);
                await behaviour.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException or ObjectDisposedException)
            {
                // Best effort: the page or the circuit has gone.
            }
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>The message for what is typed, or <see langword="null"/> when it is acceptable.</summary>
    /// <param name="text">The typed text.</param>
    /// <param name="value">The instant read, when there is one.</param>
    internal string? Judge(string text, out DateTimeOffset? value)
    {
        value = null;
        if (string.IsNullOrWhiteSpace(text))
        {
            return EmptyText is null ? "Choose a date and time." : null;
        }

        if (!LtTimeText.TryParseInstant(text, out var instant))
        {
            return "Write a time such as " + LtTimeText.Iso(new DateTimeOffset(Now.Year, Now.Month, Now.Day, Now.Hour, 0, 0, TimeSpan.Zero)) + ".";
        }

        if (Min is { } min && instant < LtTimeText.ToSecond(min))
        {
            return "Choose a time no earlier than " + LtTimeText.Readable(min) + ".";
        }

        if (EffectiveMax is { } max && instant > max)
        {
            return Max is { } stated && stated <= max
                ? "Choose a time no later than " + LtTimeText.Readable(stated) + "."
                : "Choose a time that is not in the future.";
        }

        value = instant;
        return null;
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (!_seen || Value != _lastValue)
        {
            _seen = true;
            _lastValue = Value;
            _text = Value is { } value ? LtTimeText.Iso(value) : string.Empty;
            _ownError = null;
        }
    }

    /// <inheritdoc />
    protected override async Task OnAfterRenderAsync(bool firstRender)
    {
        if (_focusActiveDay)
        {
            _focusActiveDay = false;
            await _activeDay.FocusSafelyAsync().ConfigureAwait(true);
        }

        if (_focusToggle)
        {
            _focusToggle = false;
            await _toggle.FocusSafelyAsync().ConfigureAwait(true);
        }

        if (!firstRender)
        {
            return;
        }

        try
        {
            var module = await JS.InvokeAsync<IJSObjectReference?>("import", ShellDesignAssets.DateTimeModuleSpecifier).ConfigureAwait(true);
            if (module is null || _disposed)
            {
                return;
            }

            _behaviour = await module.InvokeAsync<IJSObjectReference?>("attach", _root).ConfigureAwait(true);
            var zone = await module.InvokeAsync<LtBrowserZone?>("zone").ConfigureAwait(true);
            if (zone is not null && !_disposed && Resolve(zone) is { } resolved)
            {
                _localZone = resolved;
                StateHasChanged();
            }
        }
        catch (Exception exception) when (exception is JSException or JSDisconnectedException or TaskCanceledException or InvalidOperationException)
        {
            // Script is an enhancement: without it the local time is not shown and the
            // arrow keys may also scroll the page; nothing else changes.
        }
    }

    /// <summary>The browser's zone as .NET knows it: by its IANA id, else as a fixed offset.</summary>
    /// <param name="zone">What the browser reported.</param>
    internal static TimeZoneInfo? Resolve(LtBrowserZone zone)
    {
        if (!string.IsNullOrWhiteSpace(zone.Id) && TimeZoneInfo.TryFindSystemTimeZoneById(zone.Id, out var found))
        {
            return found;
        }

        if (zone.OffsetMinutes is >= -14 * 60 and <= 14 * 60 and { } minutes)
        {
            var offset = TimeSpan.FromMinutes(minutes);
            var name = LtTimeText.Offset(offset);
            return TimeZoneInfo.CreateCustomTimeZone(name, offset, name, name);
        }

        return null;
    }

    private async Task<bool> CommitAsync()
    {
        _ownError = Judge(_text, out var value);
        if (_ownError is not null)
        {
            return false;
        }

        _text = value is { } instant ? LtTimeText.Iso(instant) : string.Empty;
        await RaiseAsync(value).ConfigureAwait(true);
        return true;
    }

    private Task RaiseAsync(DateTimeOffset? value)
    {
        if (value == _lastValue)
        {
            return Task.CompletedTask;
        }

        // Recorded first, so the new parameter that comes back does not rewrite the text.
        _lastValue = value;
        Value = value;
        return ValueChanged.InvokeAsync(value);
    }

    private Task OnTextInputAsync(ChangeEventArgs args)
    {
        _text = args.Value as string ?? string.Empty;
        _ownError = null;
        return Judge(_text, out var value) is null ? RaiseAsync(value) : Task.CompletedTask;
    }

    private Task OnTextChangeAsync(ChangeEventArgs args)
    {
        _text = args.Value as string ?? string.Empty;
        return CommitAsync();
    }

    private void Toggle()
    {
        if (_open)
        {
            _open = false;
            return;
        }

        var anchor = Value ?? Clamp(Now);
        _active = DateOnly.FromDateTime(anchor.UtcDateTime);
        _month = new DateOnly(_active.Year, _active.Month, 1);
        _open = true;
        _focusActiveDay = true;
    }

    private void Close()
    {
        _open = false;
        _focusToggle = true;
    }

    private void OnPickerKeyDown(KeyboardEventArgs args)
    {
        if (args.Key == "Escape")
        {
            Close();
        }
    }

    private void OnDayKeyDown(KeyboardEventArgs args)
    {
        DateOnly? next = args.Key switch
        {
            "ArrowLeft" => ShiftDay(_active, -1),
            "ArrowRight" => ShiftDay(_active, 1),
            "ArrowUp" => ShiftDay(_active, -7),
            "ArrowDown" => ShiftDay(_active, 7),
            "PageUp" => ShiftMonth(_active, args.ShiftKey ? -12 : -1),
            "PageDown" => ShiftMonth(_active, args.ShiftKey ? 12 : 1),
            "Home" => ShiftDay(_active, -MondayIndex(_active)),
            "End" => ShiftDay(_active, 6 - MondayIndex(_active)),
            _ => null,
        };

        if (next is { } day)
        {
            MoveTo(day);
        }
    }

    private void MoveTo(DateOnly day)
    {
        _active = day;
        _month = new DateOnly(day.Year, day.Month, 1);
        _focusActiveDay = true;
    }

    private void ShowMonth(int months)
    {
        _month = ShiftMonth(_month, months);
        var day = Math.Min(_active.Day, DateTime.DaysInMonth(_month.Year, _month.Month));
        _active = new DateOnly(_month.Year, _month.Month, day);
    }

    private static DateOnly ShiftDay(DateOnly day, int days) =>
        DateOnly.FromDayNumber(Math.Clamp(day.DayNumber + days, DateOnly.MinValue.DayNumber, DateOnly.MaxValue.DayNumber));

    private static DateOnly ShiftMonth(DateOnly day, int months)
    {
        var current = (day.Year - 1) * 12 + day.Month - 1;
        if (current + months is < 0 or >= 9999 * 12)
        {
            return day;
        }

        return day.AddMonths(months);
    }

    private Task PickDayAsync(DateOnly day)
    {
        _active = day;
        if (!DayInRange(day))
        {
            return Task.CompletedTask;
        }

        var time = Value is { } value ? TimeOnly.FromDateTime(value.UtcDateTime) : TimeOnly.MinValue;
        return PickAsync(new DateTimeOffset(day.ToDateTime(time), TimeSpan.Zero));
    }

    private Task OnTimeChangeAsync(ChangeEventArgs args)
    {
        if (!TimeOnly.TryParse(args.Value as string, CultureInfo.InvariantCulture, DateTimeStyles.None, out var time))
        {
            return Task.CompletedTask;
        }

        var day = Value is { } value ? DateOnly.FromDateTime(value.UtcDateTime) : _active;
        return PickAsync(new DateTimeOffset(day.ToDateTime(time), TimeSpan.Zero));
    }

    private async Task QuickPickAsync(TimeSpan ago)
    {
        await PickAsync(Now - ago).ConfigureAwait(true);
        Close();
    }

    private async Task ClearAsync()
    {
        _text = string.Empty;
        _ownError = null;
        await RaiseAsync(null).ConfigureAwait(true);
        Close();
    }

    private Task PickAsync(DateTimeOffset instant)
    {
        var value = Clamp(LtTimeText.ToSecond(instant));
        _text = LtTimeText.Iso(value);
        _ownError = null;
        return RaiseAsync(value);
    }

    private DateTimeOffset Clamp(DateTimeOffset instant)
    {
        if (Min is { } min && instant < min)
        {
            instant = LtTimeText.ToSecond(min);
        }

        if (EffectiveMax is { } max && instant > max)
        {
            instant = LtTimeText.ToSecond(max);
        }

        return instant;
    }

    private bool InRange(DateTimeOffset instant) =>
        (Min is not { } min || instant >= LtTimeText.ToSecond(min)) && (EffectiveMax is not { } max || instant <= max);

    private bool DayInRange(DateOnly day)
    {
        var start = new DateTimeOffset(day.ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
        var end = day == DateOnly.MaxValue ? LtTimeText.ToSecond(DateTimeOffset.MaxValue) : start.AddDays(1).AddSeconds(-1);
        return (Min is not { } min || end >= min) && (EffectiveMax is not { } max || start <= max);
    }

    private IReadOnlyList<DateOnly?[]> Weeks()
    {
        var first = _month.DayNumber - MondayIndex(_month);
        var last = _month.DayNumber + DateTime.DaysInMonth(_month.Year, _month.Month) - 1;
        var weeks = new List<DateOnly?[]>(6);
        for (var start = first; start <= last; start += 7)
        {
            var week = new DateOnly?[7];
            for (var i = 0; i < 7; i++)
            {
                var number = start + i;
                if (number >= DateOnly.MinValue.DayNumber && number <= DateOnly.MaxValue.DayNumber)
                {
                    week[i] = DateOnly.FromDayNumber(number);
                }
            }

            weeks.Add(week);
        }

        return weeks;
    }

    private DateOnly? SelectedDay => Value is { } value ? DateOnly.FromDateTime(value.UtcDateTime) : null;

    private DateOnly Today => DateOnly.FromDateTime(Now.UtcDateTime);

    private string TimeValue => Value is { } value ? value.UtcDateTime.ToString("HH:mm:ss", CultureInfo.InvariantCulture) : string.Empty;

    private string MonthText => _month.ToString("MMMM yyyy", CultureInfo.InvariantCulture);

    private static int MondayIndex(DateOnly day) => ((int)day.DayOfWeek + 6) % 7;

    private static string DayName(DateOnly day) => day.ToString("dddd d MMMM yyyy", CultureInfo.InvariantCulture);

    private static string DayNumber(DateOnly day) => day.Day.ToString(CultureInfo.InvariantCulture);

    private string DayClass(DateOnly day) => day.Month == _month.Month ? "lt-datetime__day" : "lt-datetime__day lt-datetime__day--outside";

    /// <summary>The quick picks, each with how long ago it is.</summary>
    private static readonly (string Text, TimeSpan Ago)[] Picks =
    [
        ("Now", TimeSpan.Zero),
        ("1 hour ago", TimeSpan.FromHours(1)),
        ("24 hours ago", TimeSpan.FromHours(24)),
        ("7 days ago", TimeSpan.FromDays(7)),
    ];

    /// <summary>The weekday headings, Monday first.</summary>
    private static readonly (string Short, string Long)[] Weekdays =
    [
        ("Mo", "Monday"),
        ("Tu", "Tuesday"),
        ("We", "Wednesday"),
        ("Th", "Thursday"),
        ("Fr", "Friday"),
        ("Sa", "Saturday"),
        ("Su", "Sunday"),
    ];
}
