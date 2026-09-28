using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Validates <see cref="RepoContextTtlOptions"/> at first resolve for every named
/// (per-repository) and the default instance. A configured
/// <see cref="RepoContextTtlOptions.DefaultMemoryTtl"/> must be strictly positive.
/// Memory entries are written through the multi-value-register accessor
/// (<see cref="MvRegisterAccessor{T}"/>), which does not reject a non-positive TTL
/// but writes the entry with no TTL at all, so without this check such a default
/// would silently leave every new memory entry durable; validating it catches the
/// misconfiguration when the options value is resolved rather than never. Mirrors
/// how the view and replication options are validated.
/// </summary>
internal sealed class RepoContextTtlOptionsValidator : IValidateOptions<RepoContextTtlOptions>
{
    /// <inheritdoc />
    public ValidateOptionsResult Validate(string? name, RepoContextTtlOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);

        var failures = new List<string>();

        if (options.DefaultMemoryTtl is { } ttl && ttl <= TimeSpan.Zero)
        {
            failures.Add(
                $"{nameof(RepoContextTtlOptions.DefaultMemoryTtl)} must be a positive, finite duration when set " +
                $"(was {ttl}); leave it null to keep memory entries durable by default.");
        }

        return failures.Count > 0 ? ValidateOptionsResult.Fail(failures) : ValidateOptionsResult.Success;
    }
}
