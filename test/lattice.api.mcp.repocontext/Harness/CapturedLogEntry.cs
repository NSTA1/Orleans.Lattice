using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext.Tests.Harness;

/// <summary>
/// One log entry recorded by a <see cref="CapturingLoggerProvider"/>: the category
/// it was written under, its level, the formatted message, and any exception.
/// </summary>
/// <param name="Category">The logger category the entry was written under.</param>
/// <param name="Level">The severity the entry was logged at.</param>
/// <param name="Message">The fully formatted message text.</param>
/// <param name="Exception">The exception attached to the entry, if any.</param>
public readonly record struct CapturedLogEntry(
    string Category,
    LogLevel Level,
    string Message,
    Exception? Exception);
