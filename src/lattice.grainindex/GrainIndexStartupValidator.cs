using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace Orleans.Lattice.GrainIndex;

/// <summary>
/// Forces every declared grain index's options to resolve while the host is
/// starting, so an invalid declaration fails the host rather than surfacing on
/// the first write to the index.
/// <para>
/// Named options are built lazily, so without this the per-index validator would
/// not run until something asked for that index by name. Resolving them here
/// turns a latent configuration error into a startup failure with the index name
/// in the message.
/// </para>
/// </summary>
internal sealed class GrainIndexStartupValidator(
    IOptions<GrainIndexDeclarationOptions> declarations,
    IOptionsMonitor<GrainIndexOptions> indexOptions) : IHostedService
{
    /// <inheritdoc />
    public Task StartAsync(CancellationToken cancellationToken)
    {
        // Resolving .Value runs the declaration-set validator (duplicate names,
        // empty projection sets); resolving each named instance runs the
        // per-index validator (tree name, backfill knobs).
        var definitions = declarations.Value.Definitions;
        for (var i = 0; i < definitions.Count; i++)
        {
            _ = indexOptions.Get(definitions[i].Name);
        }

        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
