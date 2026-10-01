namespace Orleans.Lattice.Explorer.UI.Design.Components;

/// <summary>The reader's time zone as the browser reports it: its IANA id and its current offset from UTC.</summary>
internal sealed class LtBrowserZone
{
    /// <summary>The IANA zone id, such as <c>Europe/London</c>, or <see langword="null"/> when the browser names none.</summary>
    public string? Id { get; set; }

    /// <summary>The zone's offset from UTC now, in minutes east of Greenwich.</summary>
    public int? OffsetMinutes { get; set; }
}
