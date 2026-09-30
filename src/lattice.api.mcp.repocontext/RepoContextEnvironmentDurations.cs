using System.Globalization;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Parses the seconds-valued <c>LATTICE_*</c> environment variables the
/// repository-context host reads its cadences and budgets from, so every reader
/// applies the same definition of an unusable value.
/// </summary>
/// <remarks>
/// A reader falls back to its default rather than failing the host, and "unusable"
/// has to include a number <see cref="TimeSpan"/> cannot hold: <c>Infinity</c>, or
/// a finite value past <see cref="TimeSpan.MaxValue"/> such as <c>1e20</c>.
/// <see cref="TimeSpan.FromSeconds(double)"/> throws
/// <see cref="OverflowException"/> for both, and the readers run while the host's
/// services are registered, so a single mistyped exponent used to stop the host
/// from starting at all.
/// </remarks>
internal static class RepoContextEnvironmentDurations
{
    /// <summary>
    /// Parses <paramref name="raw"/> as a number of seconds in the invariant
    /// culture. Succeeds only for a finite value that <see cref="TimeSpan"/> can
    /// represent and that is positive - or zero, when
    /// <paramref name="allowZero"/> is set.
    /// </summary>
    /// <param name="raw">The raw variable value; <see langword="null"/> or blank fails.</param>
    /// <param name="allowZero">Whether zero seconds is an accepted value.</param>
    /// <param name="value">The parsed duration when parsing succeeds; otherwise <see cref="TimeSpan.Zero"/>.</param>
    /// <returns><see langword="true"/> when <paramref name="raw"/> is a usable duration.</returns>
    internal static bool TryParseSeconds(string? raw, bool allowZero, out TimeSpan value)
    {
        value = TimeSpan.Zero;
        if (string.IsNullOrWhiteSpace(raw)
            || !double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var seconds)
            || !double.IsFinite(seconds)
            || seconds < 0
            || (seconds == 0 && !allowZero)
            || seconds >= TimeSpan.MaxValue.TotalSeconds)
        {
            return false;
        }

        try
        {
            value = TimeSpan.FromSeconds(seconds);
        }
        catch (OverflowException)
        {
            // Defensive: the bound above is compared in seconds, and the conversion
            // to ticks can round a value just below it onto the boundary.
            return false;
        }

        return true;
    }

    /// <summary>
    /// Reads the environment variable <paramref name="key"/> as a number of seconds,
    /// returning <paramref name="fallback"/> when it is absent or not a usable
    /// duration (see <see cref="TryParseSeconds"/>).
    /// </summary>
    /// <param name="key">The environment variable name.</param>
    /// <param name="fallback">The value to use when the variable is absent or unusable.</param>
    /// <param name="allowZero">Whether zero seconds is an accepted value.</param>
    /// <returns>The configured duration, or <paramref name="fallback"/>.</returns>
    internal static TimeSpan ReadSeconds(string key, TimeSpan fallback, bool allowZero)
        => TryParseSeconds(Environment.GetEnvironmentVariable(key), allowZero, out var value)
            ? value
            : fallback;
}
