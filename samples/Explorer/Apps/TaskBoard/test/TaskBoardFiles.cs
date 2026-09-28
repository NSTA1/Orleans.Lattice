using System.Reflection;
using Orleans.Lattice.Apps;

namespace Orleans.Lattice.Samples.Explorer.TaskBoard.Tests;

/// <summary>Reads the task board's manifest and bundle, from disk and from the embedded resources.</summary>
internal static class TaskBoardFiles
{
    /// <summary>The bundle assets the manifest lists, in manifest order.</summary>
    public static readonly string[] AssetPaths = ["index.html", "app.css", "app.mjs", "icon.svg"];

    /// <summary>The app library's source directory, stamped into this assembly at build time.</summary>
    public static string SourceDirectory { get; } = typeof(TaskBoardFiles).Assembly
        .GetCustomAttributes<AssemblyMetadataAttribute>()
        .Single(a => a.Key == "TaskBoardSourceDirectory").Value!;

    /// <summary>Reads the bytes of a file under the app's source directory.</summary>
    public static byte[] ReadBytes(string relative) =>
        File.ReadAllBytes(Path.Combine(SourceDirectory, relative.Replace('/', Path.DirectorySeparatorChar)));

    /// <summary>Reads a file under the app's source directory as text.</summary>
    public static string ReadText(string relative) =>
        File.ReadAllText(Path.Combine(SourceDirectory, relative.Replace('/', Path.DirectorySeparatorChar)));

    /// <summary>Reads a bundle asset's bytes from disk.</summary>
    public static byte[] ReadAsset(string path) => ReadBytes("ui/" + path);

    /// <summary>Reads an embedded resource of the app assembly.</summary>
    public static byte[] ReadEmbedded(string resourceName)
    {
        using var stream = TaskBoardApp.Assembly.GetManifestResourceStream(resourceName)
            ?? throw new AssertionException($"The app assembly has no embedded resource '{resourceName}'.");
        using var buffer = new MemoryStream();
        stream.CopyTo(buffer);
        return buffer.ToArray();
    }

    /// <summary>Parses the embedded manifest, failing with every diagnostic when it does not validate.</summary>
    public static AppManifest Manifest()
    {
        using var stream = new MemoryStream(ReadEmbedded(TaskBoardApp.ManifestResourceName));
        var result = AppManifestParser.Parse(stream);
        Assert.That(result.Errors, Is.Empty, string.Join("; ", result.Errors.Select(e => e.Path + ": " + e.Message)));
        return result.Manifest!;
    }
}
