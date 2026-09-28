namespace Orleans.Lattice.Api.Apps;

/// <summary>Which available apps a catalogue listing returns, relative to the active tenant's installations.</summary>
public enum AvailableAppFilter
{
    /// <summary>Every app the selected sources offer.</summary>
    All = 0,
    /// <summary>Only apps installed in the active tenant.</summary>
    Installed = 1,
    /// <summary>Only apps not installed in the active tenant.</summary>
    Available = 2,
    /// <summary>Only installed apps for which the same source offers a newer version.</summary>
    Updates = 3,
}
