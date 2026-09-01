namespace Orleans.Lattice.Explorer.Core.Session;

/// <summary>
/// The outcome of restoring one remembered preference: the value to use, whether
/// it came from memory, and - when a remembered value had to be abandoned - a
/// sentence explaining that to the user.
/// </summary>
/// <remarks>
/// <para>
/// This is the shell's shared answer to "the remembered value no longer
/// resolves". Before it, each caller invented its own handling, which is how a
/// deleted tree could leave a surface pointed at nothing. Restoring through
/// <see cref="IExplorerShellPreferences.Resolve{T, TState}"/> means every caller
/// falls back the same way and explains itself the same way.
/// </para>
/// <para>
/// <see cref="Explanation"/> is populated only for
/// <see cref="ExplorerPreferenceFallbackReason.NotResolvable"/>. Nothing being
/// remembered is not worth a message; a remembered thing having vanished is,
/// because otherwise the user sees the shell land somewhere they did not leave it
/// and has no way to know why.
/// </para>
/// </remarks>
/// <typeparam name="T">The preference's value type.</typeparam>
/// <param name="Value">The value to use: the remembered one, or the caller's fallback.</param>
/// <param name="Reason">Why the value is what it is.</param>
/// <param name="Explanation">
/// A user-facing sentence, or <see langword="null"/> when there is nothing worth
/// saying.
/// </param>
public readonly record struct ExplorerPreferenceResolution<T>(
    T Value,
    ExplorerPreferenceFallbackReason Reason,
    string? Explanation)
{
    /// <summary>Whether <see cref="Value"/> is the remembered value rather than the fallback.</summary>
    public bool IsRestored => Reason == ExplorerPreferenceFallbackReason.None;

    /// <summary>
    /// Whether a remembered value was found but had to be abandoned, which is the
    /// case worth surfacing to the user.
    /// </summary>
    public bool WasAbandoned => Reason == ExplorerPreferenceFallbackReason.NotResolvable;

    /// <summary>
    /// A resolution that restored <paramref name="value"/> from memory.
    /// </summary>
    /// <param name="value">The remembered value.</param>
    public static ExplorerPreferenceResolution<T> Restored(T value) =>
        new(value, ExplorerPreferenceFallbackReason.None, Explanation: null);

    /// <summary>
    /// A resolution that fell back to <paramref name="fallback"/> for a reason the
    /// user does not need explaining.
    /// </summary>
    /// <param name="fallback">The value to use instead.</param>
    /// <param name="reason">The reason, either not stored or not yet loaded.</param>
    public static ExplorerPreferenceResolution<T> FellBack(T fallback, ExplorerPreferenceFallbackReason reason) =>
        new(fallback, reason, Explanation: null);

    /// <summary>
    /// A resolution that abandoned a remembered value which no longer resolves,
    /// carrying the sentence to show the user.
    /// </summary>
    /// <param name="fallback">The value to use instead.</param>
    /// <param name="explanation">The user-facing explanation.</param>
    public static ExplorerPreferenceResolution<T> Abandoned(T fallback, string explanation) =>
        new(fallback, ExplorerPreferenceFallbackReason.NotResolvable, explanation);
}
