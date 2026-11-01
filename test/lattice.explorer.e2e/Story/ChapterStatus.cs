namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>How a chapter of a story run ended.</summary>
public enum ChapterStatus
{
    /// <summary>Its UI steps ran and its contract verified what they produced.</summary>
    Passed,

    /// <summary>
    /// Its journey does not exist yet, so its F6 contract established and verified its produces
    /// in its place. Not a pass: I1 (#5400) requires a story run with none.
    /// </summary>
    StoodIn,

    /// <summary>It ran and failed.</summary>
    Failed,

    /// <summary>A chapter it consumes failed or was blocked, so it never ran. Never a pass.</summary>
    Blocked,
}
