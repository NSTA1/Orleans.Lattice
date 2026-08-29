namespace Orleans.Lattice.Explorer.MyTenant;

/// <summary>
/// The inline width declarations a quota bar's fill uses, pre-composed once for
/// every whole percentage.
/// <para>
/// A bar's fill is the one attribute on the quota surface whose value genuinely
/// changes with the data, so composing it per render would allocate two strings
/// per dimension per poll - the integer's own text and the concatenation - on a
/// surface that re-renders on every usage refresh. There are only 101 possible
/// values, so they are built once and handed out thereafter.
/// </para>
/// </summary>
public static class TenantQuotaBarStyle
{
    private static readonly string[] Widths = Build();

    /// <summary>
    /// The inline style for a fill of <paramref name="percent"/>, clamped to
    /// <c>[0, 100]</c>. Returns a cached instance, so this allocates nothing.
    /// </summary>
    /// <param name="percent">The fill percentage.</param>
    /// <returns>The inline width declaration.</returns>
    public static string Width(int percent) => Widths[Math.Clamp(percent, 0, 100)];

    private static string[] Build()
    {
        var widths = new string[101];
        for (var i = 0; i < widths.Length; i++)
        {
            widths[i] = string.Concat("width:", i.ToString(System.Globalization.CultureInfo.InvariantCulture), "%");
        }

        return widths;
    }
}
