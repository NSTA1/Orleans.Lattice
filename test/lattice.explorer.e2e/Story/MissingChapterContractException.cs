namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

/// <summary>A chapter has no <see cref="IChapterContract"/>: F6 (#5364) has not implemented it.</summary>
public sealed class MissingChapterContractException : Exception
{
    /// <summary>Creates the exception for <paramref name="chapter"/>.</summary>
    /// <param name="chapter">The chapter id.</param>
    public MissingChapterContractException(string chapter)
        : base($"No IChapterContract exists for chapter {chapter}. F6 (#5364) implements a contract for every chapter; until it does, {chapter} can be neither established nor verified.")
    {
        Chapter = chapter;
    }

    /// <summary>The chapter without a contract.</summary>
    public string Chapter { get; }
}
