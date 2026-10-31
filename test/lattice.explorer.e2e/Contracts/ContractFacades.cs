using Grpc.Core;
using Orleans.Lattice.Api.Apps.Grpc;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.Replication.Grpc;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Api.TenantAdmin.Grpc;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Estate;
using Orleans.Lattice.Explorer.EndToEnd.Tests.Story;

namespace Orleans.Lattice.Explorer.EndToEnd.Tests.Contracts;

/// <summary>
/// The facades a contract calls, through the estate's gateway, as one user in one region: the
/// same gRPC surface the Explorer drives on that user's behalf. A contract never reaches past
/// them into a grain; every state it establishes is state an API caller could establish.
/// </summary>
public sealed class ContractFacades
{
    private readonly CallInvoker _invoker;

    private ContractFacades(JourneyEstate estate, string user, string region, string? tenant)
    {
        Estate = estate;
        User = user;
        Region = region;
        _invoker = estate.Invoker(user, region: region, tenant: tenant);
    }

    /// <summary>The estate.</summary>
    public JourneyEstate Estate { get; }

    /// <summary>The user the calls are made as.</summary>
    public string User { get; }

    /// <summary>The region the calls reach.</summary>
    public string Region { get; }

    /// <summary>The facades of <paramref name="estate"/> as <paramref name="user"/> in <paramref name="region"/>.</summary>
    /// <param name="estate">The estate.</param>
    /// <param name="user">The user; the platform operator by default.</param>
    /// <param name="region">The region; west by default.</param>
    /// <param name="tenant">The tenant to assert, or <see langword="null"/>.</param>
    public static ContractFacades Of(JourneyEstate estate, string user = JourneyEstateOptions.Administrator, string region = JourneyEstate.West, string? tenant = null)
    {
        ArgumentNullException.ThrowIfNull(estate);
        return new ContractFacades(estate, user, region, tenant);
    }

    /// <summary>The facades of a chapter's estate.</summary>
    /// <param name="context">The chapter's context.</param>
    /// <param name="user">The user; the platform operator by default.</param>
    /// <param name="region">The region; west by default.</param>
    /// <param name="tenant">The tenant to assert, or <see langword="null"/>.</param>
    public static ContractFacades Of(ChapterContext context, string user = JourneyEstateOptions.Administrator, string region = JourneyEstate.West, string? tenant = null)
    {
        ArgumentNullException.ThrowIfNull(context);
        return Of(context.Estate, user, region, tenant);
    }

    /// <summary>Groups, members, rules and explain.</summary>
    public LatticeAuthApiGrpcClient Auth => LatticeAuthApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>Trees, aliases, views, tag indexes, resize, reshard, snapshot, WAL and restore.</summary>
    public LatticeTreeAdminApiGrpcClient TreeAdmin => LatticeTreeAdminApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>Key reads and writes.</summary>
    public LatticeDataApiGrpcClient Data => LatticeDataApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>Policies, versioning, compliance, remediation and migration.</summary>
    public LatticeSchemaApiGrpcClient Schema => LatticeSchemaApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>Captures, restores, schedules, health and the catalogue.</summary>
    public LatticeBackupApiGrpcClient Backup => LatticeBackupApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>Runtime replication enrolment.</summary>
    public LatticeReplicationApiGrpcClient Replication => LatticeReplicationApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>App install, lifecycle, consent and role bindings.</summary>
    public LatticeAppsApiGrpcClient Apps => LatticeAppsApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>The app catalogue's sources.</summary>
    public LatticeAppCatalogApiGrpcClient AppCatalog => LatticeAppCatalogApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>Tenants, quotas, admins, regions and cross-tenant grants.</summary>
    public LatticeTenantAdminApiGrpcClient TenantAdmin => LatticeTenantAdminApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>A caller's own tenants.</summary>
    public LatticeTenantSelfServiceApiGrpcClient TenantSelfService => LatticeTenantSelfServiceApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);

    /// <summary>The read-only state surface: catalogues, scans and dead letters.</summary>
    public LatticeStateApiGrpcClient State => LatticeStateApiGrpcClient.Create(_invoker, JourneyEstate.Serialization);
}
