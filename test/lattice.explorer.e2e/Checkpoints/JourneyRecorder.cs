using System.Collections.Concurrent;
using System.Diagnostics;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Sessions;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Checkpoints;

/// <summary>
/// Files one journey's checkpoint screenshots, in order, under
/// <c>test/lattice.explorer.e2e/artifacts/screenshots/{jid}/</c> as
/// <c>{nnn}-{step}-{substep}.png</c>, and keeps its <see cref="JourneyManifest"/> beside them.
/// </summary>
/// <remarks>
/// One recorder per journey id per test run, shared by every session of the journey, so the
/// checkpoints of several personas interleave in the order they were taken. The directory is
/// emptied when the recorder is first made, so the manifest always lists exactly the files
/// beside it. The manifest is rewritten after every checkpoint, so a journey that fails part
/// way still leaves a consistent manifest of what it captured.
/// </remarks>
public sealed class JourneyRecorder
{
    /// <summary>Overrides the commit the manifest records (CI sets it to the pull request's head commit).</summary>
    public const string CommitVariable = "LATTICE_E2E_COMMIT";

    private static readonly ConcurrentDictionary<string, JourneyRecorder> Recorders = new(StringComparer.Ordinal);
    private static readonly Lazy<string?> RunCommit = new(ResolveCommit);

    private readonly List<JourneyManifestEntry> _entries = [];
    private readonly Lock _gate = new();
    private int _next;

    private JourneyRecorder(string journeyId, JourneyViewport viewport)
    {
        JourneyId = journeyId;
        Viewport = viewport;
        Directory = Path.Combine(Root, journeyId);
        if (System.IO.Directory.Exists(Directory))
        {
            System.IO.Directory.Delete(Directory, recursive: true);
        }

        System.IO.Directory.CreateDirectory(Directory);
        WriteManifest();
    }

    /// <summary>Where every journey's screenshot directory is: <c>test/lattice.explorer.e2e/artifacts/screenshots</c>.</summary>
    public static string Root => Path.Combine(JourneyPaths.ProjectDirectory, "artifacts", "screenshots");

    /// <summary>The journey id.</summary>
    public string JourneyId { get; }

    /// <summary>The viewport the journey's screenshots are taken at.</summary>
    public JourneyViewport Viewport { get; }

    /// <summary>The journey's screenshot directory.</summary>
    public string Directory { get; }

    /// <summary>The journey's manifest file.</summary>
    public string ManifestFile => Path.Combine(Directory, JourneyManifest.FileName);

    /// <summary>The checkpoints recorded so far, in order.</summary>
    public IReadOnlyList<JourneyManifestEntry> Entries
    {
        get
        {
            lock (_gate)
            {
                return [.. _entries];
            }
        }
    }

    /// <summary>The recorder of journey <paramref name="journeyId"/>, made on first use at the run's viewport.</summary>
    /// <param name="journeyId">The journey id, lower-case kebab-case.</param>
    public static JourneyRecorder For(string journeyId) =>
        Recorders.GetOrAdd(JourneyNames.RequireSlug(journeyId, nameof(journeyId)), id => new JourneyRecorder(id, JourneyViewport.Run));

    /// <summary>
    /// The recorder of the running test's journey: the id its fixture carries through an
    /// <see cref="IJourneyIdentity"/> attribute such as <see cref="JourneyAttribute"/>.
    /// </summary>
    /// <exception cref="InvalidOperationException">The fixture carries no journey id.</exception>
    public static JourneyRecorder ForCurrentTest() =>
        For(JourneyAttribute.Current() ?? throw new InvalidOperationException(
            $"The fixture {TestContext.CurrentContext.Test.ClassName} names no journey: give it [Journey(\"<jid>\")] (or a chapter attribute implementing {nameof(IJourneyIdentity)})."));

    /// <summary>The manifest as it stands.</summary>
    public JourneyManifest Manifest()
    {
        lock (_gate)
        {
            return Snapshot();
        }
    }

    private JourneyManifest Snapshot() => new()
    {
        Journey = JourneyId,
        Commit = RunCommit.Value,
        RunId = Environment.GetEnvironmentVariable("GITHUB_RUN_ID"),
        RunAttempt = Environment.GetEnvironmentVariable("GITHUB_RUN_ATTEMPT"),
        Viewport = Viewport.Name,
        Width = Viewport.Width,
        Height = Viewport.Height,
        Entries = [.. _entries],
    };

    /// <summary>Takes the next place in the journey, and the path its screenshot is written to.</summary>
    /// <param name="step">The step id.</param>
    /// <param name="substep">The substep id.</param>
    internal (int Index, string File, string Path) Reserve(string step, string substep)
    {
        lock (_gate)
        {
            var index = ++_next;
            var file = $"{index:D3}-{step}-{substep}.png";
            return (index, file, System.IO.Path.Combine(Directory, file));
        }
    }

    /// <summary>Adds a captured checkpoint and rewrites the manifest.</summary>
    /// <param name="entry">The checkpoint.</param>
    internal void Record(JourneyManifestEntry entry)
    {
        lock (_gate)
        {
            _entries.Add(entry);
            _entries.Sort((left, right) => left.Index.CompareTo(right.Index));
            WriteManifest();
        }
    }

    private void WriteManifest()
    {
        var manifest = Snapshot();
        manifest.Write(ManifestFile);
        File.WriteAllText(Path.Combine(Directory, "manifest.md"), manifest.ToMarkdown());
    }

    private static string? ResolveCommit()
    {
        foreach (var variable in new[] { CommitVariable, "GITHUB_SHA" })
        {
            if (Environment.GetEnvironmentVariable(variable) is { Length: > 0 } value)
            {
                return value;
            }
        }

        try
        {
            using var git = Process.Start(new ProcessStartInfo("git", "rev-parse HEAD")
            {
                WorkingDirectory = JourneyPaths.RepositoryRoot,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                UseShellExecute = false,
            });
            if (git is null)
            {
                return null;
            }

            if (!git.WaitForExit(5_000))
            {
                git.Kill(entireProcessTree: true);
                throw new TimeoutException(
                    $"'git rev-parse HEAD' in {JourneyPaths.RepositoryRoot} did not exit within 5 seconds, so the screenshot manifest cannot name its commit. Set {CommitVariable} instead.");
            }

            var output = git.StandardOutput.ReadToEnd().Trim();
            return git.ExitCode == 0 && output.Length > 0 ? output : null;
        }
        catch (Exception ex) when (ex is System.ComponentModel.Win32Exception or InvalidOperationException)
        {
            return null;
        }
    }
}
