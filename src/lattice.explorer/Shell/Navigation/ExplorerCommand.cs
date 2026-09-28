using Orleans.Lattice.Explorer.Shell.Navigation.Address;

namespace Orleans.Lattice.Explorer.Shell.Navigation;

/// <summary>
/// A palette command: a named action that is also, always, a visible control
/// somewhere in the Explorer. Nothing is reachable only through the palette.
/// </summary>
/// <remarks>
/// The visible control that performs the same action carries
/// <c>data-lt-command="{Id}"</c>, on the page at <see cref="Target"/> or in the
/// chrome. Running a command navigates to <see cref="Target"/> when it is set, then
/// runs <see cref="InvokeAsync"/> when it is set.
/// </remarks>
internal sealed record ExplorerCommand
{
    /// <summary>The attribute the visible control for a command carries, with the command's <see cref="Id"/>.</summary>
    public const string ControlAttribute = "data-lt-command";

    /// <summary>Declares a command.</summary>
    /// <param name="id">A lower-case dotted id prefixed by its owner, such as <c>data.create-tree</c>.</param>
    /// <param name="title">The command's name in the palette, such as "Create a tree".</param>
    /// <exception cref="ArgumentException"><paramref name="id"/> or <paramref name="title"/> is not valid.</exception>
    public ExplorerCommand(string id, string title)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(title);
        if (!IsValidId(id))
        {
            throw new ArgumentException(
                $"'{id}' is not a command id: use lower-case dotted words, such as data.create-tree.",
                nameof(id));
        }

        Id = id;
        Title = title;
    }

    /// <summary>The command's stable id, and the value of its control's <see cref="ControlAttribute"/>.</summary>
    public string Id { get; }

    /// <summary>The command's name in the palette.</summary>
    public string Title { get; }

    /// <summary>An optional plain description shown beside the title.</summary>
    public string? Detail { get; init; }

    /// <summary>Where running the command navigates first, or <see langword="null"/> to stay.</summary>
    public ExplorerAddress? Target { get; init; }

    /// <summary>The action to run, after any navigation, or <see langword="null"/> when navigating is the whole command.</summary>
    public Func<CancellationToken, ValueTask>? InvokeAsync { get; init; }

    /// <summary>Whether <paramref name="id"/> is a valid command id: lower-case keywords separated by dots.</summary>
    /// <param name="id">The candidate id.</param>
    public static bool IsValidId(string? id) =>
        !string.IsNullOrEmpty(id)
        && id.Split('.').All(ExplorerAddressEncoding.IsKeyword);
}
