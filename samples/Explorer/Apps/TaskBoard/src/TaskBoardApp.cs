using System.Reflection;

namespace Orleans.Lattice.Samples.Explorer.TaskBoard;

/// <summary>
/// Where the task-board sample app lives in this assembly. A silo registers it with the in-image
/// app source through the three-argument <c>AddLatticeApp</c> overload:
/// <c>siloBuilder.AddLatticeApp(TaskBoardApp.Slug, TaskBoardApp.Assembly, TaskBoardApp.ManifestResourceName)</c>.
/// </summary>
public static class TaskBoardApp
{
    /// <summary>The app slug the manifest declares.</summary>
    public const string Slug = "task-board";

    /// <summary>The manifest's embedded-resource name.</summary>
    public const string ManifestResourceName = "Orleans.Lattice.Samples.Explorer.TaskBoard.manifest.json";

    /// <summary>
    /// The embedded-resource name prefix of the UI bundle assets; it is the in-image source's default
    /// for <see cref="ManifestResourceName"/>, so no explicit prefix need be passed.
    /// </summary>
    public const string AssetResourcePrefix = "Orleans.Lattice.Samples.Explorer.TaskBoard.ui.";

    /// <summary>The assembly that carries the manifest and the bundle.</summary>
    public static Assembly Assembly => typeof(TaskBoardApp).Assembly;
}
