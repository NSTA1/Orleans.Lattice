namespace Orleans.Lattice.Explorer.Core.Data;

/// <summary>
/// The Data tab's page-size choices: increments of 25 up to a maximum of 150,
/// with a default of 25.
/// </summary>
public static class DataPaging
{
    /// <summary>The default page size.</summary>
    public const int DefaultPageSize = 25;

    /// <summary>The maximum selectable page size.</summary>
    public const int MaxPageSize = 150;

    /// <summary>The page-size increment.</summary>
    public const int Increment = 25;

    /// <summary>The selectable page sizes: 25, 50, 75, 100, 125, 150.</summary>
    public static IReadOnlyList<int> PageSizes { get; } = BuildPageSizes();

    /// <summary>
    /// Clamps an arbitrary requested size to the nearest valid page size,
    /// falling back to <see cref="DefaultPageSize"/> for non-positive input.
    /// </summary>
    public static int Normalize(int requested)
    {
        if (requested <= 0)
        {
            return DefaultPageSize;
        }

        if (requested >= MaxPageSize)
        {
            return MaxPageSize;
        }

        var rounded = (int)Math.Round((double)requested / Increment, MidpointRounding.AwayFromZero) * Increment;
        return Math.Clamp(rounded, Increment, MaxPageSize);
    }

    private static int[] BuildPageSizes()
    {
        var count = MaxPageSize / Increment;
        var sizes = new int[count];
        for (var i = 0; i < count; i++)
        {
            sizes[i] = (i + 1) * Increment;
        }

        return sizes;
    }
}
