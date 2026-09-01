using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Orleans.Lattice.Api.Mcp.RepoContext;

/// <summary>
/// Onboards every configured git-sourced repository once the host is up, by arming
/// its self-index grain exactly as <c>add_repo</c> arms a mounted one. A git source
/// is declared in configuration rather than registered by a tool call, so without
/// this service nothing would ever start its reminder-driven refresh loop.
/// <para>
/// The service is registered only when at least one git source is configured, and
/// it stands down immediately on a spoke - a spoke receives the index over the
/// replication plane and must never fetch source content. Arming is idempotent, so
/// a restart re-arms the same repositories without duplicating work, and a
/// repository whose first arm fails (the silo is still starting, or the git host is
/// unreachable) is retried with a bounded backoff rather than abandoned.
/// </para>
/// </summary>
internal sealed class RepoContextGitSourceArmingService(
    IGrainFactory grainFactory,
    RepoContextIndexSourceGate sourceGate,
    RepoContextIndexingOptions indexingOptions,
    IRepoIndexRunAuthority runAuthority,
    TimeProvider timeProvider,
    ILogger<RepoContextGitSourceArmingService> logger) : BackgroundService
{
    /// <summary>
    /// The delay before the first arming attempt, so the silo is active before the
    /// first grain call. Awaited through the injected <see cref="TimeProvider"/>.
    /// </summary>
    private static readonly TimeSpan InitialDelay = TimeSpan.FromSeconds(5);

    /// <summary>The backoff between arming attempts, doubled per failed pass up to <see cref="MaximumBackoff"/>.</summary>
    private static readonly TimeSpan InitialBackoff = TimeSpan.FromSeconds(10);

    /// <summary>The ceiling the retry backoff is clamped to.</summary>
    private static readonly TimeSpan MaximumBackoff = TimeSpan.FromMinutes(5);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!indexingOptions.IndexingEnabled)
        {
            logger.LogInformation(
                "Git-sourced repositories are inert on a {Role} node; no source fetch is armed here.",
                indexingOptions.Role);
            return;
        }

        var pending = new HashSet<string>(
            sourceGate.GitSources.Select(source => source.RepoId), StringComparer.Ordinal);
        if (pending.Count == 0)
        {
            return;
        }

        try
        {
            await Task.Delay(InitialDelay, timeProvider, stoppingToken).ConfigureAwait(false);

            var backoff = InitialBackoff;
            while (pending.Count > 0 && !stoppingToken.IsCancellationRequested)
            {
                foreach (var repoId in pending.ToArray())
                {
                    if (await TryArmAsync(repoId).ConfigureAwait(false))
                    {
                        pending.Remove(repoId);
                    }
                }

                if (pending.Count == 0)
                {
                    break;
                }

                await Task.Delay(backoff, timeProvider, stoppingToken).ConfigureAwait(false);
                backoff = backoff < MaximumBackoff
                    ? TimeSpan.FromTicks(Math.Min(backoff.Ticks * 2, MaximumBackoff.Ticks))
                    : MaximumBackoff;
            }
        }
        catch (OperationCanceledException)
        {
            // The host is stopping; the next start re-arms from configuration.
        }
    }

    /// <summary>
    /// Arms one repository's self-index grain under the same fixed run credential the
    /// background indexer uses, so the arming call carries a subject the access gate
    /// can authorize rather than whatever ambient credential the host thread held.
    /// </summary>
    /// <param name="repoId">The git-sourced repository identity to arm.</param>
    /// <returns><see langword="true"/> when the grain was armed; otherwise <see langword="false"/>.</returns>
    private async Task<bool> TryArmAsync(string repoId)
    {
        var request = sourceGate.SeedRequestFor(repoId);
        if (request is null)
        {
            // The repository stopped being git-sourced between enumeration and arming.
            return true;
        }

        try
        {
            var credential = runAuthority.Resolve();
            if (credential is null)
            {
                await ArmCoreAsync(repoId, request).ConfigureAwait(false);
            }
            else
            {
                using var scope = LatticeCredentialContext.With(credential);
                await ArmCoreAsync(repoId, request).ConfigureAwait(false);
            }

            logger.LogInformation(
                "Repo {RepoId}: git source armed; its refresh loop now tracks the configured ref.", repoId);
            return true;
        }
        catch (Exception ex)
        {
            // Never surface a token: the message is redacted before it is logged.
            logger.LogWarning(
                "Repo {RepoId}: arming the git source failed ({Reason}); it will be retried.",
                repoId, RepoContextSecretRedactor.RedactUrls(ex.Message));
            return false;
        }
    }

    /// <summary>Arms one repository's self-index grain.</summary>
    /// <param name="repoId">The repository identity to arm.</param>
    /// <param name="request">The seed job request the git source rewrites at fetch time.</param>
    private Task ArmCoreAsync(string repoId, RepoIndexJobRequest request) =>
        grainFactory.GetGrain<IRepoContextSelfIndexGrain>(repoId).EnsureRunningAsync(request);
}
