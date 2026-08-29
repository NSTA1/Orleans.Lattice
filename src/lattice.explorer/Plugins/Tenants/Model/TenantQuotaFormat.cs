using System.Globalization;
using Orleans.Lattice.Explorer.Tenancy;

namespace Orleans.Lattice.Explorer.Tenants;

/// <summary>
/// The display vocabulary for tenant quota figures: the labels, the byte and
/// count formatters, and the words the surface uses for the two absences a quota
/// reading can carry.
/// <para>
/// Every method is pure and culture-invariant, so a rendered figure never
/// depends on the host's locale and a test asserting one is deterministic.
/// </para>
/// </summary>
public static class TenantQuotaFormat
{
    /// <summary>The word used for a dimension with no ceiling at all.</summary>
    public const string UnlimitedText = "Unlimited";

    /// <summary>The words used for a reading that carried no consumption figure.</summary>
    public const string NotMeasuredText = "Not measured";

    /// <summary>The caption for a reading that is a converged cross-cluster total.</summary>
    public const string GlobalScopeCaption =
        "Figures are a converged total across every cluster the tenant runs on.";

    /// <summary>
    /// The caption for a reading taken from this cluster only, which is
    /// deliberately not presented as a global total.
    /// </summary>
    public const string PerClusterScopeCaption =
        "Figures are this cluster's local view only. The tenant may be consuming more elsewhere.";

    /// <summary>
    /// The caption for a registered tenant whose warm reading has not compiled
    /// yet, whose ceilings are still authoritative.
    /// </summary>
    public const string NoUsageCaption =
        "No consumption reading has compiled for this tenant yet. The ceilings below are "
        + "authoritative; the usage figures are absent rather than zero.";

    private static readonly string[] ByteUnits = ["B", "KB", "MB", "GB", "TB", "PB"];

    /// <summary>
    /// The display label for <paramref name="kind"/>.
    /// </summary>
    /// <param name="kind">The dimension to label.</param>
    /// <returns>The dimension's display label.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="kind"/> is not a defined dimension.</exception>
    public static string Label(ExplorerTenantQuotaDimensionKind kind) => kind switch
    {
        ExplorerTenantQuotaDimensionKind.Bytes => "Stored bytes",
        ExplorerTenantQuotaDimensionKind.Keys => "Live keys",
        ExplorerTenantQuotaDimensionKind.MemoryBytes => "Resident memory",
        ExplorerTenantQuotaDimensionKind.TreeCount => "Owned trees",
        ExplorerTenantQuotaDimensionKind.OpsPerSecond => "Operations per second",
        _ => throw new ArgumentOutOfRangeException(nameof(kind)),
    };

    /// <summary>
    /// Whether <paramref name="kind"/>'s figures are byte counts, so they render
    /// in binary units rather than as plain counts.
    /// </summary>
    /// <param name="kind">The dimension to classify.</param>
    /// <returns><see langword="true"/> for a byte-valued dimension.</returns>
    public static bool IsByteValued(ExplorerTenantQuotaDimensionKind kind) =>
        kind is ExplorerTenantQuotaDimensionKind.Bytes or ExplorerTenantQuotaDimensionKind.MemoryBytes;

    /// <summary>
    /// Formats <paramref name="value"/> for <paramref name="kind"/>: binary units
    /// for a byte-valued dimension, a grouped count otherwise.
    /// </summary>
    /// <param name="kind">The dimension the value belongs to.</param>
    /// <param name="value">The value to format.</param>
    /// <returns>The formatted value.</returns>
    public static string Value(ExplorerTenantQuotaDimensionKind kind, long value) =>
        IsByteValued(kind) ? Bytes(value) : Count(value);

    /// <summary>
    /// Formats <paramref name="value"/> as a grouped count, invariantly.
    /// </summary>
    /// <param name="value">The value to format.</param>
    /// <returns>The grouped count.</returns>
    public static string Count(long value) => value.ToString("N0", CultureInfo.InvariantCulture);

    /// <summary>
    /// Formats <paramref name="value"/> as a byte figure in binary units, to one
    /// decimal place above a kilobyte. Negative input is not expected from the
    /// control API and formats as a plain byte count rather than being clamped,
    /// so a surprising figure stays visible instead of being disguised.
    /// </summary>
    /// <param name="value">The byte count to format.</param>
    /// <returns>The formatted byte figure.</returns>
    public static string Bytes(long value)
    {
        if (value < 1024)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"{value} {ByteUnits[0]}");
        }

        double scaled = value;
        var unit = 0;
        while (scaled >= 1024 && unit < ByteUnits.Length - 1)
        {
            scaled /= 1024;
            unit++;
        }

        return string.Create(CultureInfo.InvariantCulture, $"{scaled:0.#} {ByteUnits[unit]}");
    }

    /// <summary>
    /// The caption for the scope a reading was taken and is enforced under. A
    /// per-cluster reading is genuinely not a global total, so the two are never
    /// captioned the same way.
    /// </summary>
    /// <param name="scope">The enforcement scope of the reading.</param>
    /// <returns>The caption to render beside the figures.</returns>
    public static string ScopeCaption(ExplorerTenantQuotaEnforcement scope) =>
        scope == ExplorerTenantQuotaEnforcement.PerCluster ? PerClusterScopeCaption : GlobalScopeCaption;

    /// <summary>
    /// Parses an operator-typed ceiling: blank means unbounded (and so
    /// <see langword="null"/>), and a value must be a non-negative integer.
    /// <c>0</c> parses to a real ceiling of zero, which permits nothing and is
    /// deliberately not the same as blank.
    /// </summary>
    /// <param name="text">The raw input, which may be <see langword="null"/> or blank.</param>
    /// <param name="limit">The parsed ceiling, or <see langword="null"/> for unbounded.</param>
    /// <returns><see langword="true"/> when the input was blank or a valid non-negative integer.</returns>
    public static bool TryParseLimit(string? text, out long? limit)
    {
        limit = null;
        if (string.IsNullOrWhiteSpace(text))
        {
            return true;
        }

        if (!long.TryParse(text.Trim(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            || parsed < 0)
        {
            return false;
        }

        limit = parsed;
        return true;
    }

    /// <summary>
    /// Renders a ceiling back into the editor's text form: blank for unbounded,
    /// so a round trip through the editor cannot turn "no ceiling" into "a
    /// ceiling of zero".
    /// </summary>
    /// <param name="limit">The ceiling, or <see langword="null"/> for unbounded.</param>
    /// <returns>The editor text.</returns>
    public static string ToEditorText(long? limit) =>
        limit is { } value ? value.ToString(CultureInfo.InvariantCulture) : string.Empty;
}
