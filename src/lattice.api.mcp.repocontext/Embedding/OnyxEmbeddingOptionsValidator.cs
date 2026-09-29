using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Validates <see cref="OnyxEmbeddingOptions"/> when the options are first resolved:
/// a configured <see cref="OnyxEmbeddingOptions.RequestTimeout"/> must be a value the
/// embedding <see cref="HttpClient"/> accepts - strictly positive and no longer than
/// <see cref="int.MaxValue"/> milliseconds, or <see cref="Timeout.InfiniteTimeSpan"/>
/// for no timeout.
/// </summary>
/// <remarks>
/// <see cref="OnyxEmbeddingProvider"/> assigns the timeout to
/// <see cref="HttpClient.Timeout"/> on every call, and that setter throws
/// <see cref="ArgumentOutOfRangeException"/> for any other value. That exception is
/// not one of the transport faults the provider maps to a fail-closed result, so
/// without this check a misconfigured timeout made every health probe and every
/// embed call throw instead of degrading, and nothing reported the misconfiguration
/// until the first call. Validating it fails at resolution instead.
/// </remarks>
internal sealed class OnyxEmbeddingOptionsValidator : IValidateOptions<OnyxEmbeddingOptions>
{
    /// <summary>
    /// The longest finite timeout <see cref="HttpClient.Timeout"/> accepts
    /// (<see cref="int.MaxValue"/> milliseconds, about 24.8 days).
    /// </summary>
    internal static readonly TimeSpan MaxRequestTimeout = TimeSpan.FromMilliseconds(int.MaxValue);

    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, OnyxEmbeddingOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.RequestTimeout is not { } timeout || timeout == Timeout.InfiniteTimeSpan)
        {
            return ValidateOptionsResult.Success;
        }

        if (timeout <= TimeSpan.Zero)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(OnyxEmbeddingOptions.RequestTimeout)} must be strictly positive when set (was {timeout}); "
                + "use Timeout.InfiniteTimeSpan for no timeout, or leave it null for the HttpClient default.");
        }

        if (timeout > MaxRequestTimeout)
        {
            return ValidateOptionsResult.Fail(
                $"{nameof(OnyxEmbeddingOptions.RequestTimeout)} must be at most {MaxRequestTimeout} "
                + $"(int.MaxValue milliseconds), the longest finite timeout the embedding HttpClient accepts "
                + $"(was {timeout}); use Timeout.InfiniteTimeSpan for no timeout.");
        }

        return ValidateOptionsResult.Success;
    }
}
