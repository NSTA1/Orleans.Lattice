namespace Orleans.Lattice.Explorer.UiTests;

/// <summary>
/// One appearance the Explorer can be put in: its material (Paper or Board), its
/// contrast (standard or more) and its density (comfortable or compact).
/// </summary>
/// <param name="Board">Board rather than Paper.</param>
/// <param name="More">The high-contrast overlay rather than standard contrast.</param>
/// <param name="Compact">The compact density rather than comfortable.</param>
internal sealed record ShellAppearanceChoice(bool Board, bool More, bool Compact)
{
    /// <summary>Every combination: two materials, two contrasts, two densities.</summary>
    public static IReadOnlyList<ShellAppearanceChoice> All { get; } =
    [
        .. from board in new[] { false, true }
           from more in new[] { false, true }
           from compact in new[] { false, true }
           select new ShellAppearanceChoice(board, more, compact),
    ];

    /// <summary>Paper, standard contrast, comfortable: the Explorer's default look.</summary>
    public static ShellAppearanceChoice Default { get; } = new(Board: false, More: false, Compact: false);

    /// <summary>The <c>data-lt-command</c> of each appearance control that makes this choice.</summary>
    public IReadOnlyList<string> Commands =>
    [
        Board ? "appearance.theme.board" : "appearance.theme.paper",
        More ? "appearance.contrast.more" : "appearance.contrast.standard",
        Compact ? "appearance.density.compact" : "appearance.density.comfortable",
    ];

    /// <summary>What the document's <c>data-bs-theme</c> carries.</summary>
    public string ThemeAttribute => Board ? "dark" : "light";

    /// <summary>What the document's <c>data-lt-contrast</c> carries.</summary>
    public string ContrastAttribute => More ? "more" : "standard";

    /// <inheritdoc />
    public override string ToString() =>
        $"{(Board ? "Board" : "Paper")}, {(More ? "more contrast" : "standard contrast")}, {(Compact ? "compact" : "comfortable")}";
}
