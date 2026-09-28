namespace Orleans.Lattice.Api.Apps;

/// <summary>
/// Semantic Version 2.0 precedence over version text the app engine has already validated: major, minor and
/// patch compare numerically, a release outranks any of its prereleases, prerelease identifiers compare
/// numerically when both are numeric and ordinally otherwise (a numeric identifier ranking lower), and
/// build metadata is ignored.
/// </summary>
internal static class AppVersionPrecedence
{
    /// <summary>Compares two semantic versions by precedence.</summary>
    /// <param name="left">The first version text.</param>
    /// <param name="right">The second version text.</param>
    /// <returns>A negative value when <paramref name="left"/> has lower precedence, zero when equal, positive when higher.</returns>
    public static int Compare(string left, string right)
    {
        ArgumentNullException.ThrowIfNull(left);
        ArgumentNullException.ThrowIfNull(right);
        var a = StripBuild(left.AsSpan());
        var b = StripBuild(right.AsSpan());
        SplitPrerelease(a, out var coreA, out var preA);
        SplitPrerelease(b, out var coreB, out var preB);

        for (var i = 0; i < 3; i++)
        {
            var order = CompareNumeric(NextSegment(ref coreA, '.'), NextSegment(ref coreB, '.'));
            if (order != 0)
                return order;
        }

        if (preA.IsEmpty || preB.IsEmpty)
            return preA.IsEmpty == preB.IsEmpty ? 0 : preA.IsEmpty ? 1 : -1;

        while (!preA.IsEmpty && !preB.IsEmpty)
        {
            var x = NextSegment(ref preA, '.');
            var y = NextSegment(ref preB, '.');
            var xNumeric = IsNumeric(x);
            var yNumeric = IsNumeric(y);
            var order = (xNumeric, yNumeric) switch
            {
                (true, true) => CompareNumeric(x, y),
                (true, false) => -1,
                (false, true) => 1,
                _ => Math.Sign(x.SequenceCompareTo(y)),
            };
            if (order != 0)
                return order;
        }

        return preA.IsEmpty == preB.IsEmpty ? 0 : preA.IsEmpty ? -1 : 1;
    }

    private static ReadOnlySpan<char> StripBuild(ReadOnlySpan<char> version)
    {
        var plus = version.IndexOf('+');
        return plus < 0 ? version : version[..plus];
    }

    private static void SplitPrerelease(ReadOnlySpan<char> version, out ReadOnlySpan<char> core, out ReadOnlySpan<char> prerelease)
    {
        var dash = version.IndexOf('-');
        core = dash < 0 ? version : version[..dash];
        prerelease = dash < 0 ? default : version[(dash + 1)..];
    }

    private static ReadOnlySpan<char> NextSegment(ref ReadOnlySpan<char> rest, char separator)
    {
        var index = rest.IndexOf(separator);
        ReadOnlySpan<char> segment;
        if (index < 0)
        {
            segment = rest;
            rest = default;
        }
        else
        {
            segment = rest[..index];
            rest = rest[(index + 1)..];
        }

        return segment;
    }

    private static bool IsNumeric(ReadOnlySpan<char> value)
    {
        if (value.IsEmpty)
            return false;
        foreach (var c in value)
        {
            if (c is < '0' or > '9')
                return false;
        }

        return true;
    }

    private static int CompareNumeric(ReadOnlySpan<char> x, ReadOnlySpan<char> y)
    {
        x = x.TrimStart('0');
        y = y.TrimStart('0');
        if (x.Length != y.Length)
            return x.Length < y.Length ? -1 : 1;
        return Math.Sign(x.SequenceCompareTo(y));
    }
}
