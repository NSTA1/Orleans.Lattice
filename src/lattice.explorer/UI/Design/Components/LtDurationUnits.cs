namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>The units an <see cref="LtDurationInput"/> offers, one box each, largest first.</summary>
[Flags]
public enum LtDurationUnits
{
    /// <summary>No unit.</summary>
    None = 0,

    /// <summary>Whole days.</summary>
    Days = 1,

    /// <summary>Whole hours.</summary>
    Hours = 2,

    /// <summary>Whole minutes.</summary>
    Minutes = 4,

    /// <summary>Whole seconds.</summary>
    Seconds = 8,
}
