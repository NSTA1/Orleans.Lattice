using System.Globalization;
using Microsoft.AspNetCore.Components;

namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// A labelled duration: one whole-number box per unit - days, hours, minutes or seconds -
/// inside a single control box, each named by its unit, with a minimum, a maximum and an
/// optional empty state.
/// </summary>
/// <remarks>
/// <para>
/// A duration is never free text: each box takes a whole number of its unit, and the unit
/// is written beside it. A value is shown in the units the field offers, the largest one
/// taking whatever no larger unit is offered for (36 hours in a field of hours and minutes
/// is 36 h, not 1 d 12 h); a remainder smaller than the smallest unit offered is not shown.
/// </para>
/// <para>
/// A value is raised through <see cref="ValueChanged"/> as soon as the boxes read as a
/// duration in range. Call <see cref="ConfirmAsync"/> before acting on a submit, so a
/// duration that does not read, or is out of range, shows its message and is not acted on.
/// </para>
/// </remarks>
public partial class LtDurationInput
{
    private static readonly (LtDurationUnits Unit, string Short, string Long, TimeSpan Size)[] AllUnits =
    [
        (LtDurationUnits.Days, "d", "days", TimeSpan.FromDays(1)),
        (LtDurationUnits.Hours, "h", "hours", TimeSpan.FromHours(1)),
        (LtDurationUnits.Minutes, "min", "minutes", TimeSpan.FromMinutes(1)),
        (LtDurationUnits.Seconds, "s", "seconds", TimeSpan.FromSeconds(1)),
    ];

    private readonly string _id = LtIds.Next("lt-duration");
    private (LtDurationUnits Unit, string Short, string Long, TimeSpan Size)[] _units = [];
    private string[] _texts = [];
    private bool _seen;
    private TimeSpan? _lastValue;
    private LtDurationUnits _lastUnits;
    private string? _ownError;

    /// <summary>The visible label.</summary>
    [Parameter, EditorRequired]
    public string Label { get; set; } = string.Empty;

    /// <summary>The duration, or <see langword="null"/> when the field is empty.</summary>
    [Parameter]
    public TimeSpan? Value { get; set; }

    /// <summary>Raised with each new duration the field reads, or <see langword="null"/> when it is emptied.</summary>
    [Parameter]
    public EventCallback<TimeSpan?> ValueChanged { get; set; }

    /// <summary>The units offered, one box each, largest first. Defaults to hours and minutes.</summary>
    [Parameter]
    public LtDurationUnits Units { get; set; } = LtDurationUnits.Hours | LtDurationUnits.Minutes;

    /// <summary>Whether the field may be left empty. When <see langword="false"/>, a duration is required.</summary>
    [Parameter]
    public bool Optional { get; set; }

    /// <summary>The shortest duration accepted, or <see langword="null"/> for none.</summary>
    [Parameter]
    public TimeSpan? Min { get; set; }

    /// <summary>The longest duration accepted, or <see langword="null"/> for none.</summary>
    [Parameter]
    public TimeSpan? Max { get; set; }

    /// <summary>A short explanation shown under the box and announced with it.</summary>
    [Parameter]
    public string? Hint { get; set; }

    /// <summary>A validation message from the page, shown in place of the field's own.</summary>
    [Parameter]
    public string? Error { get; set; }

    /// <summary>Whether the field is disabled.</summary>
    [Parameter]
    public bool Disabled { get; set; }

    /// <summary>The first box's id, for a caller that needs to point at the field.</summary>
    public string InputId => UnitId(0);

    private string LabelId => _id + "-label";

    private string HintId => _id + "-hint";

    private string ErrorId => _id + "-error";

    private string? EffectiveError => Error ?? _ownError;

    private string? DescribedBy => (Hint, EffectiveError) switch
    {
        (null, null) => null,
        (not null, null) => HintId,
        (null, not null) => ErrorId,
        _ => HintId + " " + ErrorId,
    };

    private string ControlClass => EffectiveError is null ? "lt-duration__control" : "lt-duration__control lt-duration__control--invalid";

    private string UnitList => _units.Length switch
    {
        0 => string.Empty,
        1 => _units[0].Long,
        _ => string.Join(", ", _units[..^1].Select(unit => unit.Long)) + " and " + _units[^1].Long,
    };

    /// <summary>
    /// Reads the boxes, shows the field's own message when they do not read or are out of
    /// range, and otherwise raises the duration: call it before acting on a submit.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the field holds a duration in range, or is empty and may be;
    /// otherwise <see langword="false"/>.
    /// </returns>
    public async Task<bool> ConfirmAsync()
    {
        _ownError = Judge(_texts, out var value);
        if (_ownError is null)
        {
            await RaiseAsync(value).ConfigureAwait(true);
        }

        StateHasChanged();
        return _ownError is null;
    }

    /// <summary>The message for what the boxes hold, or <see langword="null"/> when it is acceptable.</summary>
    /// <param name="texts">The text of each box, largest unit first.</param>
    /// <param name="value">The duration read, when there is one.</param>
    internal string? Judge(IReadOnlyList<string> texts, out TimeSpan? value)
    {
        value = null;
        if (texts.All(string.IsNullOrWhiteSpace))
        {
            return Optional ? null : $"Give a duration in {UnitList}.";
        }

        var ticks = 0L;
        for (var i = 0; i < texts.Count && i < _units.Length; i++)
        {
            var text = string.IsNullOrWhiteSpace(texts[i]) ? "0" : texts[i].Trim();
            if (!long.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var count)
                || count > TimeSpan.MaxValue.Ticks / _units[i].Size.Ticks
                || ticks > TimeSpan.MaxValue.Ticks - (count * _units[i].Size.Ticks))
            {
                return $"Give a whole number of {UnitList}.";
            }

            ticks += count * _units[i].Size.Ticks;
        }

        var duration = TimeSpan.FromTicks(ticks);
        if (Min is { } min && duration < min)
        {
            return "Give at least " + LtTimeText.Duration(min) + ".";
        }

        if (Max is { } max && duration > max)
        {
            return "Give at most " + LtTimeText.Duration(max) + ".";
        }

        value = duration;
        return null;
    }

    /// <inheritdoc />
    protected override void OnParametersSet()
    {
        if (Units != _lastUnits || _units.Length == 0)
        {
            _lastUnits = Units;
            _units = [.. AllUnits.Where(unit => Units.HasFlag(unit.Unit))];
            if (_units.Length == 0)
            {
                throw new InvalidOperationException($"{nameof(LtDurationInput)} '{Label}' offers no unit; set {nameof(Units)}.");
            }

            _seen = false;
        }

        if (!_seen || Value != _lastValue)
        {
            _seen = true;
            _lastValue = Value;
            _texts = Split(Value);
            _ownError = null;
        }
    }

    private string[] Split(TimeSpan? value)
    {
        var texts = new string[_units.Length];
        if (value is not { } duration)
        {
            Array.Fill(texts, string.Empty);
            return texts;
        }

        var remaining = Math.Max(0, duration.Ticks);
        for (var i = 0; i < _units.Length; i++)
        {
            var count = remaining / _units[i].Size.Ticks;
            remaining -= count * _units[i].Size.Ticks;
            texts[i] = count.ToString(CultureInfo.InvariantCulture);
        }

        return texts;
    }

    private string UnitId(int index) => _id + "-" + index.ToString(CultureInfo.InvariantCulture);

    private string UnitLabel(int index) => Label + ", " + _units[index].Long;

    private Task OnUnitInputAsync(int index, ChangeEventArgs args)
    {
        _texts[index] = args.Value as string ?? string.Empty;
        _ownError = null;
        return Judge(_texts, out var value) is null ? RaiseAsync(value) : Task.CompletedTask;
    }

    private async Task OnUnitChangeAsync(int index, ChangeEventArgs args)
    {
        _texts[index] = args.Value as string ?? string.Empty;
        _ownError = Judge(_texts, out var value);
        if (_ownError is null)
        {
            await RaiseAsync(value).ConfigureAwait(true);
        }
    }

    private Task RaiseAsync(TimeSpan? value)
    {
        if (value == _lastValue)
        {
            return Task.CompletedTask;
        }

        // Recorded first, so the new parameter that comes back does not rewrite the boxes.
        _lastValue = value;
        Value = value;
        return ValueChanged.InvokeAsync(value);
    }
}
