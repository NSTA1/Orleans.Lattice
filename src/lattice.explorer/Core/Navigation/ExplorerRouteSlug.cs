namespace Orleans.Lattice.Explorer.Core.Navigation;

/// <summary>
/// The canonical form of every route segment and query key the Explorer shell
/// puts in a URL: lower case, ASCII, hyphen separated.
/// </summary>
/// <remarks>
/// <para>
/// This is a single, deliberate epic-wide decision, not a style preference. A URL
/// is shared, bookmarked, typed and logged, and a shell that answers
/// <c>/explore/trees</c> but not <c>/Explore/Trees</c> is a shell whose links
/// break depending on who typed them. Rather than case-fold on the way in and
/// hope every future author remembers, the shell declares exactly one canonical
/// spelling and refuses to emit anything else.
/// </para>
/// <para>
/// Enforcement is layered so drift cannot survive: <see cref="EnsureCanonical"/>
/// is called by every seam that accepts a caller-supplied segment or key
/// (<see cref="ExplorerRoute"/>, <see cref="ExplorerRouteParameter"/>, and the
/// preference-key registry), and a repository hygiene test scans the declared
/// <c>@page</c> routes and the constants on <see cref="ExplorerRouteSegments"/>
/// so an upper-case literal fails the build rather than shipping.
/// </para>
/// <para>
/// Parsing is deliberately more forgiving than emission: an inbound URL is
/// normalised with <see cref="Normalize"/> so a hand-typed
/// <c>/Explore/Trees</c> still lands somewhere sensible instead of erroring. The
/// shell then rewrites the address bar to the canonical spelling.
/// </para>
/// </remarks>
public static class ExplorerRouteSlug
{
    /// <summary>
    /// Whether <paramref name="value"/> is already in canonical form: non-empty,
    /// and containing only lower-case ASCII letters, digits, hyphen, underscore
    /// and dot. An empty or <see langword="null"/> value is <em>not</em>
    /// canonical; callers that allow an absent segment test for emptiness first.
    /// </summary>
    /// <param name="value">The candidate segment or query key.</param>
    public static bool IsCanonical(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return false;
        }

        // Indexed over the string rather than a LINQ predicate: this runs on the
        // navigation path for every segment of every route the shell emits.
        for (var i = 0; i < value.Length; i++)
        {
            if (!IsCanonicalChar(value[i]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>
    /// Returns <paramref name="value"/> in canonical form, lower-casing ASCII
    /// letters and replacing every other non-canonical character with a hyphen.
    /// Returns <see cref="string.Empty"/> for a <see langword="null"/> or empty
    /// input. Used on the inbound (parsing) path, where tolerance beats
    /// rejection.
    /// </summary>
    /// <param name="value">The segment or key to normalise.</param>
    public static string Normalize(string? value)
    {
        if (string.IsNullOrEmpty(value))
        {
            return string.Empty;
        }

        if (IsCanonical(value))
        {
            // The overwhelmingly common case: a link the shell itself emitted.
            // Returning the original instance keeps the parse path allocation
            // free for every canonical segment.
            return value;
        }

        return string.Create(value.Length, value, static (destination, source) =>
        {
            for (var i = 0; i < source.Length; i++)
            {
                var c = source[i];
                destination[i] = c is >= 'A' and <= 'Z'
                    ? (char)(c + 32)
                    : IsCanonicalChar(c) ? c : '-';
            }
        });
    }

    /// <summary>
    /// Throws when <paramref name="value"/> is not already in canonical form.
    /// The guard behind every seam that stores or emits a segment or key, so a
    /// non-canonical spelling fails at the point it is introduced rather than
    /// surfacing later as a URL nobody else can reproduce.
    /// </summary>
    /// <param name="value">The segment or key to check.</param>
    /// <param name="paramName">The caller's parameter name, supplied automatically.</param>
    /// <exception cref="ArgumentException">
    /// <paramref name="value"/> is <see langword="null"/>, empty, or contains a
    /// character outside the canonical set (an upper-case letter, a space, or a
    /// path separator, for example).
    /// </exception>
    public static void EnsureCanonical(
        string? value,
        [System.Runtime.CompilerServices.CallerArgumentExpression(nameof(value))] string? paramName = null)
    {
        if (!IsCanonical(value))
        {
            throw new ArgumentException(
                $"Explorer route segments and query keys must be lower-case ASCII (letters, digits, '-', '_' or '.'); '{value}' is not.",
                paramName);
        }
    }

    /// <summary>
    /// Derives a route slug from a dotted identifier, taking the last dotted
    /// segment and normalising it - so the plugin id
    /// <c>orleans.lattice.data</c> addresses as <c>data</c>.
    /// </summary>
    /// <remarks>
    /// The default derivation the shell and its consumers share, so a surface's
    /// URL spelling is predictable rather than separately invented at each call
    /// site. It is deliberately syntactic: two plugins whose ids end in the same
    /// segment derive the same slug, and disambiguating them is the resolving
    /// caller's business, not this helper's.
    /// </remarks>
    /// <param name="identifier">The dotted identifier, typically a plugin id.</param>
    /// <returns>The derived slug, or <see cref="string.Empty"/> when nothing usable remains.</returns>
    public static string FromIdentifier(string? identifier)
    {
        if (string.IsNullOrEmpty(identifier))
        {
            return string.Empty;
        }

        var lastDot = identifier.LastIndexOf('.');
        var tail = lastDot >= 0 && lastDot < identifier.Length - 1
            ? identifier[(lastDot + 1)..]
            : identifier;

        return Normalize(tail);
    }

    private static bool IsCanonicalChar(char c) =>
        c is (>= 'a' and <= 'z') or (>= '0' and <= '9') or '-' or '_' or '.';
}
