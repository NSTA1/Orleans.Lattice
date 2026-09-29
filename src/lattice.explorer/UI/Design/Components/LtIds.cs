namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>
/// Issues element ids for the ARIA relationships a primitive draws between its
/// own elements (a label and its input, a tab and its panel), so two instances
/// on one page never collide.
/// </summary>
internal static class LtIds
{
    private static long _next;

    /// <summary>A fresh, process-unique id starting with <paramref name="prefix"/>.</summary>
    /// <param name="prefix">A short, lower-case stem naming the primitive, such as <c>lt-input</c>.</param>
    public static string Next(string prefix) =>
        prefix + "-" + Interlocked.Increment(ref _next).ToString(System.Globalization.CultureInfo.InvariantCulture);
}
