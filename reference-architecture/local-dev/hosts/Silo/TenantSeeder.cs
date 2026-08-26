using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Tenancy;

namespace Orleans.Lattice.ReferenceArchitecture.Silo;

/// <summary>
/// Seeds the demo tenants declared in the mounted local-dev identity model (the
/// <c>tenants</c> section of <c>identities.json</c>) into the durable tenant
/// registry at silo startup, so the harness has differentiated, introspectable
/// tenants without hand-authoring registry state. Each declared tenant becomes an
/// active, unbounded, shared-placement <see cref="TenantRecord"/> whose declared
/// admin subjects are its tenant administrators - which is exactly what the
/// tenant-admin self-service surface resolves from a caller's credential, so
/// <c>lattice_tenant_list</c> / <c>lattice_tenant_get</c> return each identity's
/// own tenants and fail closed on the rest.
/// </summary>
/// <remarks>
/// <para>
/// Registered only when tenancy is enabled (<c>Tenancy:Enabled=true</c>). The
/// registry persists the reserved <c>sys-tenant-*</c> trees under system-origin,
/// so no ambient administrator credential is required. Every write is a
/// last-writer-wins merge on a deterministic tenant key (registers converge on the
/// same value, and admin subjects are an add-wins set), so re-running on every silo
/// / region start is idempotent and both regions converge on the same records. The
/// batch is retried with a fixed backoff because grain calls fail until the silo is
/// active, mirroring <see cref="AdministratorAccessSeeder"/> and
/// <see cref="LocalDevIdentitySeeder"/>.
/// </para>
/// <para>
/// The writer id stamped on every field is this cluster's
/// <see cref="ClusterOptions.ClusterId"/>, so a concurrent seed from the peer
/// region tie-breaks deterministically rather than clobbering.
/// </para>
/// </remarks>
internal sealed class TenantSeeder(
    IServiceProvider services,
    LocalDevIdentityModel model,
    IOptions<ClusterOptions> clusterOptions,
    ILogger<TenantSeeder> logger) : BackgroundService
{
    private const int MaxAttempts = 12;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(5);

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (model.Tenants.Count == 0)
        {
            return;
        }

        var registry = services.GetService<ITenantRegistry>();
        if (registry is null)
        {
            logger.LogWarning(
                "No ITenantRegistry is registered; skipping demo tenant seeding. "
                + "The lattice_tenant_* tools will resolve no demo tenants.");
            return;
        }

        var writerId = clusterOptions.Value.ClusterId;
        for (var attempt = 1; attempt <= MaxAttempts; attempt++)
        {
            try
            {
                await SeedAsync(registry, writerId, stoppingToken).ConfigureAwait(false);
                logger.LogInformation(
                    "Seeded {TenantCount} demo tenant(s) into the tenant registry.",
                    model.Tenants.Count);
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex) when (attempt < MaxAttempts)
            {
                // The silo may not yet be active (grain calls fail until it is), so
                // retry the whole idempotent batch with a fixed backoff.
                logger.LogDebug(
                    ex,
                    "Attempt {Attempt}/{MaxAttempts} to seed the demo tenants failed; retrying.",
                    attempt,
                    MaxAttempts);
                try
                {
                    await Task.Delay(RetryDelay, stoppingToken).ConfigureAwait(false);
                }
                catch (OperationCanceledException)
                {
                    return;
                }
            }
            catch (Exception ex)
            {
                logger.LogWarning(
                    ex,
                    "Failed to seed the demo tenants after {MaxAttempts} attempts; "
                    + "the lattice_tenant_* tools may resolve no demo tenants until the model is seeded.",
                    MaxAttempts);
                return;
            }
        }
    }

    private async Task SeedAsync(ITenantRegistry registry, string writerId, CancellationToken cancellationToken)
    {
        // A single running clock across the whole batch keeps every stamp strictly
        // monotonic; the values are identical on every run, so the last-writer-wins
        // merge converges rather than accumulating change.
        var clock = HybridLogicalClock.Zero;
        foreach (var tenant in model.Tenants)
        {
            if (!TenantId.TryParse(tenant.Id, out var tenantId))
            {
                logger.LogWarning(
                    "Skipping demo tenant '{TenantId}': not a valid tenant id "
                    + "(lower-case alphanumeric and hyphens, 1-63 characters).",
                    tenant.Id);
                continue;
            }

            clock = HybridLogicalClock.Tick(clock);
            var record = TenantRecord.Create(
                tenantId,
                TenantStatus.Active,
                TenantQuotas.Unbounded,
                TenantPlacement.Shared,
                clock,
                writerId);

            foreach (var subjectId in tenant.AdminSubjects)
            {
                clock = HybridLogicalClock.Tick(clock);
                record.AddAdminSubject(subjectId, clock, writerId);
            }

            await registry.PutAsync(record, cancellationToken).ConfigureAwait(false);
            logger.LogDebug(
                "Seeded demo tenant '{TenantId}' with {AdminCount} admin subject(s).",
                tenantId.Value,
                tenant.AdminSubjects.Count);
        }
    }
}
