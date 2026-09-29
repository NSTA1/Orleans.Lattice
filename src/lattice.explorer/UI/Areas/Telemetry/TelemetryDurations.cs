using System.Globalization;
using System.Text.RegularExpressions;

namespace Orleans.Lattice.Explorer.UI.Areas.Telemetry;

/// <summary>
/// The durations the Telemetry area writes into and reads from the address
/// (<c>15m</c>, <c>1h</c>, <c>7d</c>), the range and step ladders it offers, and
/// how it says a duration in prose.
/// </summary>
internal static partial class TelemetryDurations
{
    /// <summary>The longest duration the address may name: a year and a bit.</summary>
    public static readonly TimeSpan Longest = TimeSpan.FromDays(400);

    /// <summary>The time ranges the toolbar offers, shortest first.</summary>
    public static IReadOnlyList<TimeSpan> Ranges { get; } =
    [
        TimeSpan.FromMinutes(15),
        TimeSpan.FromHours(1),
        TimeSpan.FromHours(6),
        TimeSpan.FromHours(24),
        TimeSpan.FromDays(7),
    ];

    /// <summary>The steps the toolbar offers, finest first.</summary>
    public static IReadOnlyList<TimeSpan> Steps { get; } =
    [
        TimeSpan.FromSeconds(15),
        TimeSpan.FromMinutes(1),
        TimeSpan.FromMinutes(5),
        TimeSpan.FromMinutes(15),
        TimeSpan.FromHours(1),
    ];

    /// <summary>The ladder an automatic step is chosen from, finest first.</summary>
    public static IReadOnlyList<TimeSpan> AutomaticSteps { get; } =
    [
        TimeSpan.FromSeconds(15),
        TimeSpan.FromMinutes(1),
        TimeSpan.FromMinutes(5),
        TimeSpan.FromMinutes(15),
        TimeSpan.FromHours(1),
        TimeSpan.FromHours(6),
        TimeSpan.FromDays(1),
    ];

    /// <summary>Parses an address token such as <c>15m</c>.</summary>
    /// <param name="text">The token.</param>
    /// <param name="duration">The duration it names.</param>
    /// <returns>Whether the token names a positive duration no longer than <see cref="Longest"/>.</returns>
    public static bool TryParse(string? text, out TimeSpan duration)
    {
        duration = default;
        if (string.IsNullOrEmpty(text))
        {
            return false;
        }

        var match = Token().Match(text);
        if (!match.Success
            || !long.TryParse(match.Groups["n"].Value, NumberStyles.None, CultureInfo.InvariantCulture, out var count)
            || count <= 0
            || count > 100_000_000)
        {
            return false;
        }

        duration = match.Groups["u"].Value switch
        {
            "s" => TimeSpan.FromSeconds(count),
            "m" => TimeSpan.FromMinutes(count),
            "h" => TimeSpan.FromHours(count),
            _ => TimeSpan.FromDays(count),
        };

        return duration <= Longest;
    }

    /// <summary>Formats a duration as the shortest exact address token.</summary>
    /// <param name="duration">A positive duration.</param>
    /// <returns>The token, such as <c>1h</c> or <c>90s</c>.</returns>
    public static string Format(TimeSpan duration)
    {
        if (duration.Ticks % TimeSpan.TicksPerDay == 0)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{duration.Ticks / TimeSpan.TicksPerDay}d");
        }

        if (duration.Ticks % TimeSpan.TicksPerHour == 0)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{duration.Ticks / TimeSpan.TicksPerHour}h");
        }

        if (duration.Ticks % TimeSpan.TicksPerMinute == 0)
        {
            return string.Create(CultureInfo.InvariantCulture, $"{duration.Ticks / TimeSpan.TicksPerMinute}m");
        }

        return string.Create(CultureInfo.InvariantCulture, $"{Math.Max(1, duration.Ticks / TimeSpan.TicksPerSecond)}s");
    }

    /// <summary>A short label for a toolbar choice, such as <c>15 min</c> or <c>7 d</c>.</summary>
    /// <param name="duration">The duration.</param>
    /// <returns>The label.</returns>
    public static string Label(TimeSpan duration)
    {
        var token = Format(duration);
        var unit = token[^1] switch
        {
            's' => "s",
            'm' => "min",
            'h' => "h",
            _ => "d",
        };

        return token[..^1] + " " + unit;
    }

    /// <summary>A duration in prose, such as <c>24 hours</c> or <c>1 minute</c>.</summary>
    /// <param name="duration">The duration.</param>
    /// <returns>The prose.</returns>
    public static string Describe(TimeSpan duration)
    {
        var token = Format(duration);
        var count = long.Parse(token[..^1], CultureInfo.InvariantCulture);
        var unit = token[^1] switch
        {
            's' => "second",
            'm' => "minute",
            'h' => "hour",
            _ => "day",
        };

        return string.Create(CultureInfo.InvariantCulture, $"{count} {unit}{(count == 1 ? string.Empty : "s")}");
    }

    [GeneratedRegex("^(?<n>[0-9]{1,9})(?<u>[smhd])$", RegexOptions.CultureInvariant)]
    private static partial Regex Token();
}
