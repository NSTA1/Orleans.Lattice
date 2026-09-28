using Orleans.Lattice.Explorer.Shell.Design.Tokens;
using Orleans.Lattice.Explorer.Shell.Layout.Appearance;

namespace Orleans.Lattice.Explorer.Tests.Shell.Navigation;

/// <summary>An appearance applier that records what it was asked to apply.</summary>
internal sealed class RecordingAppearanceApplier : IShellAppearanceApplier
{
    /// <summary>Every appearance applied, in order.</summary>
    public List<(ShellTheme Theme, ShellContrast Contrast, LtDensity Density)> Applied { get; } = [];

    /// <inheritdoc />
    public ValueTask ApplyAsync(ShellTheme theme, ShellContrast contrast, LtDensity density, CancellationToken cancellationToken = default)
    {
        Applied.Add((theme, contrast, density));
        return ValueTask.CompletedTask;
    }
}
