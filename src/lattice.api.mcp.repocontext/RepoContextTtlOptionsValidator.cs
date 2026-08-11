using Microsoft.Extensions.Options;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Validates <see cref="RepoContextTtlOptions"/> at first resolve for every named
/// (per-repository) and the default instance. A configured
/// <see cref="RepoContextTtlOptions.DefaultMemoryTtl"/> must be strictly positive
/// and finite, because the core write path
/// (<see cref="ILattice.SetAsync(string, byte[], System.TimeSpan, System.Threading.CancellationToken)"/>)
/// rejects a non-positive TTL - catching a misconfiguration at startup rather
/// than on the first ephemeral write. Mirrors how the view and replication
/// options are validated.
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
