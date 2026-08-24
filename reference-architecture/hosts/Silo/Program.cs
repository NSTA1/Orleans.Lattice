using System.Net;
using Azure.Identity;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using OpenTelemetry.Metrics;
using Orleans.Configuration;
using Orleans.Lattice;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Backup;
using Orleans.Lattice.Api.Backup.Grpc;
using Orleans.Lattice.Api.Data;
using Orleans.Lattice.Api.Data.Grpc;
using Orleans.Lattice.Api.Replication;
using Orleans.Lattice.Api.Replication.Grpc;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Api.TreeAdmin;
using Orleans.Lattice.Api.TreeAdmin.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Backup;
using Orleans.Lattice.Backup.AzureBlob;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Membership.Entra;
using Orleans.Lattice.Membership.Entra.Graph;
using Orleans.Lattice.ReferenceArchitecture.Hosting;
using Orleans.Lattice.ReferenceArchitecture.Silo;
using Orleans.Lattice.Replication;
using Orleans.Lattice.Replication.Grpc;
using Orleans.Lattice.Scaling;
using Orleans.Lattice.Schema;
using Orleans.Lattice.Storage.AzureTable;

// ---------------------------------------------------------------------------
// Reference-architecture Silo host.
//
// A production-shaped Orleans silo hosting Lattice for the active-active,
// cross-region estate described in reference-architecture.md. It is the only
// always-on head in each region and hosts:
//
//   * Azure Table Orleans clustering + durable Azure Table WAL.
//   * The cross-region replication shipper + receiver over gRPC, with
//     receiver-enrollment gating (Replication:Peers) and an explicit per-tree
//     wire merge mode (Replication:Trees), set symmetrically per region.
//   * The Azure Blob backup sink, with a Backup:Primary flag selecting whether
//     this region runs the scheduler (primary) or is DR standby (scheduler off).
//   * The read-only State API, the read-write Data API, the auth-admin control
//     plane and the backup control plane over gRPC.
//   * The lattice.scaling compute-axis signal endpoint for KEDA.
//   * Entra-backed authentication for its exposed facades.
//
// Every external input (connection targets, tenant / client ids, the
// replication key, the peer list, merge modes, the backup-primary flag) comes
// from environment variables / IConfiguration. No secret is ever hardcoded: the
// only secret, the per-cluster replication key, is read from the environment
// (LATTICE_REPLICATION_SECRET, injected from Key Vault at deploy time).
// ---------------------------------------------------------------------------

var builder = WebApplication.CreateBuilder(args);
var config = builder.Configuration;

// Front Door (og-state) and Container Apps both probe this host's /health path
// continuously - AFD once per edge PoP, ACA on its liveness cadence. Separately,
// the cross-region replication engine transport fires silo-to-silo on a fixed
// cadence regardless of activity: the anti-entropy digest probe, peer high-water-
// mark polling, and Merkle walk all hit the orleans.lattice.replication.* gRPC
// services, each successful call emitting a burst of framework "Request starting"/
// "Request finished"/routing chatter. Drop that sub-Warning noise for the health
// path and the engine-transport services so it does not dominate log storage; a
// probe that draws a non-success response, every real (non-probe) request, and any
// warning/error still surface. The engine-transport service ids mirror the Front
// Door origin-lock exemption list below (the same internal silo-to-silo surface).
builder.Logging.SuppressProbeRequestLogs(
    FrontDoorOriginLockApplicationBuilderExtensions.HealthPath,
    "/orleans.lattice.replication.LatticeReplication",
    "/orleans.lattice.replication.LatticeRemoteSnapshot",
    "/orleans.lattice.replication.LatticeSaga");

var storage = AzureStorageIdentity.FromConfiguration(config);

// The Blob backup sink resolves its own storage identity so durable backups can
// live in a storage account isolated from the primary cluster storage (in the
// local harness, the dedicated azurite-backup-sink container). Falls back to the
// primary storage identity when no backup-specific storage is configured.
var backupStorage = AzureStorageIdentity.ForBackupSink(config, storage);

var clusterId = config["Cluster:Id"] ?? "lattice";
var serviceId = config["Cluster:ServiceId"] ?? clusterId;
var replicationClusterId = config["Replication:ClusterId"] ?? clusterId;

var httpPort = config.GetValue("Silo:HttpPort", 8080);
var grpcPort = config.GetValue("Silo:GrpcPort", 8081);
var siloPort = config.GetValue("Silo:SiloPort", 11111);
var gatewayPort = config.GetValue("Silo:GatewayPort", 30000);
var advertisedIp = config["Silo:AdvertisedIp"];

var backupPrimary = config.GetValue("Backup:Primary", false);
var entraEnabled = config.GetValue("Entra:Enabled", false);
var requireApiAuthorization = config.GetValue("StateApi:RequireAuthorization", false);

// Read-write Data API surface. Enabled by default: the write-capable
// Orleans.Lattice.Api.Data gRPC binding is co-hosted on the same silo gRPC port
// as the read-only State API, so writes ride the same Entra-authenticated,
// origin-locked front-door endpoint. It is safe on by default because the real
// enforcement is the deny-by-default per-tree/per-key access gate keyed on the
// caller's Entra-resolved subject; the coarse transport gate is opened with the
// AllowAllDataApiAuthorizer. Set DataApi:Enabled=false to withhold the write
// surface entirely (the binding is not mapped and the facade is not exposed).
var dataApiEnabled = config.GetValue("DataApi:Enabled", true);

// Global-ingress origin lock: when set, every client-facing request on the
// external gRPC port must carry an X-Azure-FDID header matching this id. Empty
// (dev/compose, and the first deploy pass before Front Door exists) leaves the
// head unlocked. Threaded from the compute Bicep as LATTICE_FRONT_DOOR_ID.
var frontDoorId = config["LATTICE_FRONT_DOOR_ID"];

var replicationPeers = ReplicationTopology.ParsePeers(config);
var replicatedTrees = ReplicationTopology.ParseTrees(config);
var allowPlaintextReplication = config.GetValue("Replication:AllowPlaintext", false);

// Runtime per-tree replication configuration (the 8.0.6 control plane). When on,
// the silo enrols the reserved sys-replication-config CRDT tree, installs the
// dynamic snapshot-backed replicated-tree membership / merge-mode resolver seeded
// by the static Replication:Trees map, and co-hosts the ILatticeReplicationControl
// facade so an operator can enable/disable a tree's replication at runtime (via
// the replication control-API gRPC binding and the MCP lattice_replication_* tool
// group) without a redeploy. Secure default OFF: a bare host carries no
// replication control surface; the deployed estate opts in via
// enableReplicationControl (still fail-closed behind the deny-by-default
// LatticeOperation.Replication gate). Set Replication:EnableRuntimeConfig=true to
// enable it (the local compose harness does).
var enableRuntimeReplicationConfig = config.GetValue("Replication:EnableRuntimeConfig", false);

// Cross-cluster anti-entropy (the digest probe + Merkle-walk drift localisation +
// bounded auto-remediation). Off by default: a healthy estate converges via the
// forward change feed, so this periodic reconciliation is a fallback that heals
// divergence introduced out-of-band - rows written before a tree was brought into
// replication at runtime, or a peer that was offline past its WAL retention. The
// repair re-ships only the localised divergent key ranges under a strict traffic
// budget (with a scoped bootstrap-snapshot fallback when the WAL has rolled off),
// so enabling it is safe on a live cluster. Set symmetrically per region via
// Replication:EnableDigestAntiEntropy=true; Replication:DigestProbeIntervalSeconds
// optionally shortens the default probe cadence for a faster reconciliation.
var enableDigestAntiEntropy = config.GetValue("Replication:EnableDigestAntiEntropy", false);
var digestProbeIntervalSeconds = config.GetValue("Replication:DigestProbeIntervalSeconds", 0);

// Kestrel exposes two ports: an HTTP/1 port for health probes and the scaling
// signal (both plain REST, so ACA can TCP/HTTP-probe without a shell), and an
// HTTP/2 port for the gRPC surfaces (state, auth, replication).
builder.WebHost.ConfigureKestrel(kestrel =>
{
    kestrel.ListenAnyIP(httpPort, listen => listen.Protocols = HttpProtocols.Http1);
    kestrel.ListenAnyIP(grpcPort, listen => listen.Protocols = HttpProtocols.Http2);
});

builder.Host.UseOrleans(silo =>
{
    silo.Configure<ClusterOptions>(options =>
    {
        options.ClusterId = clusterId;
        options.ServiceId = serviceId;
    });

    // ACA runs the silo as a single container app whose replicas form the
    // Orleans cluster. Same-revision replica-to-replica connectivity carries
    // the silo-to-silo and gateway traffic; the advertised IP is the replica's
    // own address (supplied by the platform via Silo:AdvertisedIp when the
    // default NIC probe is not appropriate).
    if (!string.IsNullOrWhiteSpace(advertisedIp))
    {
        silo.ConfigureEndpoints(IPAddress.Parse(advertisedIp), siloPort, gatewayPort, listenOnAnyHostAddress: true);
    }
    else
    {
        silo.ConfigureEndpoints(siloPort, gatewayPort, listenOnAnyHostAddress: true);
    }

    // Azure Table clustering (Orleans membership).
    silo.UseAzureStorageClustering(options =>
        storage.ConfigureTable(options, config["Clustering:TableName"] ?? "OrleansLatticeClustering"));

    // Azure Table reminders (used by replication maintenance and backup sweeps).
    silo.UseAzureTableReminderService(options =>
        storage.ConfigureTable(options, config["Reminders:TableName"] ?? "OrleansLatticeReminders"));

    // Durable grain storage on Azure Table for Lattice grain state. The core
    // takes a per-named-store factory so every Lattice grain-state store is
    // durable across a replica restart, with the WAL as the mutation-durability
    // boundary underneath.
    var grainTableName = config["GrainStorage:TableName"] ?? "OrleansLatticeGrains";
    silo.AddAzureTableGrainStorageAsDefault(options => storage.ConfigureTable(options, grainTableName));
    silo.AddLattice((services, storeName) =>
        services.AddAzureTableGrainStorage(storeName, options => storage.ConfigureTable(options, grainTableName)));

    // Durable Azure Table WAL: the region's mutation-durability boundary.
    silo.AddAzureTableWalStorage(options =>
        storage.ConfigureWal(options, config["Wal:TableName"] ?? "OrleansLatticeWal"));

    // -- Cross-region replication (shipper + receiver) --------------------
    // ReplicatedTrees is the per-tree wire merge mode; ReplicationPeers is the
    // receiver-enrollment gate. Both must be set symmetrically across regions.
    silo.AddLatticeReplication(options =>
    {
        options.ClusterId = replicationClusterId;
        if (replicatedTrees.Count > 0)
        {
            options.ReplicatedTrees = replicatedTrees;
        }

        if (replicationPeers.Count > 0)
        {
            options.ReplicationPeers = replicationPeers.Keys.ToArray();
        }

        // Automatic cross-cluster anti-entropy: detect drift via the digest
        // probe, localise it with the read-only Merkle walk, then auto-remediate
        // by re-shipping the divergent ranges to the lagging peer. Each stage is
        // an independent opt-in that defaults off, so the whole chain is turned
        // on explicitly here: the master gate (AutoRemediateOnDigestMismatch)
        // permits repair, and the two repair executors (LeafReReplayEnabled for
        // the retained-WAL path and BootstrapFallbackEnabled for the scoped
        // snapshot re-seed when the WAL was trimmed past the divergence point)
        // actually ship the missing entries. Without the executors the probe
        // detects and localises drift forever but never repairs it.
        if (enableDigestAntiEntropy)
        {
            options.DigestProbeEnabled = true;
            options.MerkleWalkEnabled = true;
            options.AutoRemediateOnDigestMismatch = true;
            options.LeafReReplayEnabled = true;
            options.BootstrapFallbackEnabled = true;
            if (digestProbeIntervalSeconds > 0)
            {
                options.DigestProbeInterval = TimeSpan.FromSeconds(digestProbeIntervalSeconds);
            }
        }
    }, enableRuntimeConfig: enableRuntimeReplicationConfig);

    // Single administrative plane across sites: enrol the reserved Membership and
    // Auth policy system trees into replication so an authorization grant (or a
    // membership edit) authored on ANY cluster converges to every peer over the
    // same engine. Without this, the auth policy tree lives only in the cluster it
    // was written to, so an operator would have to re-author every grant on each
    // site. Enrolment is LWW with eventual (divergence-window) convergence; it is
    // a no-op when no ReplicationPeers are configured (single-region deployment).
    silo.ReplicateLatticeSystemTrees();

    // The gRPC replication transport dials the enrolled peer endpoints. The
    // per-cluster replication key is read from the environment by the default
    // EnvironmentVariableSecretSource (LATTICE_REPLICATION_SECRET), never from
    // source or the image. AllowPlaintextEndpoints is a local-only escape hatch
    // for the http:// compose harness; Azure uses server TLS via the ACA FQDN.
    silo.Services.AddLatticeReplicationGrpc(grpc =>
    {
        grpc.LocalClusterId = replicationClusterId;
        grpc.AllowPlaintextEndpoints = allowPlaintextReplication;
        foreach (var (peerClusterId, endpoint) in replicationPeers)
        {
            grpc.Peers[peerClusterId] = endpoint;
        }
    });

    // The replication control facade (ILatticeReplicationControl) over the runtime
    // config authority: the enable / disable / status control plane the MCP
    // lattice_replication_* tools drive. Registered only when the runtime config
    // authority exists (enableRuntimeConfig above); its gRPC binding is mapped and
    // coarse-gated below exactly like State / Data / Auth / Backup.
    if (enableRuntimeReplicationConfig)
    {
        silo.AddLatticeReplicationApi();
    }

    // -- Backup sink + primary/standby scheduler --------------------------
    silo.AddLatticeBackup();
    silo.AddLatticeBackupAzureBlob(options =>
        backupStorage.ConfigureBackupSink(options, config["Backup:ContainerName"] ?? LatticeBackupAzureBlobOptions.DefaultContainerName));

    // The backup control facade over the engine: the read/capture/restore/delete
    // control plane the Explorer's Backups area (and the MCP backup tools) drive.
    // Must be registered after AddLatticeBackup(); its gRPC binding is mapped and
    // coarse-gated below exactly like State / Data / Auth.
    silo.AddLatticeBackupApi();

    // Exactly one region is the designated backup-primary and owns the schedule;
    // every other region is DR standby with the scheduler off, so there are no
    // competing backup chains writing the shared sink.
    if (backupPrimary)
    {
        silo.ConfigureLatticeBackupSchedule(schedule =>
        {
            schedule.FullBackupScheduleEnabled = true;
            schedule.FullBackupInterval = TimeSpan.FromHours(config.GetValue("Backup:FullIntervalHours", 24));
            schedule.IncrementalBackupScheduleEnabled = true;
            schedule.IncrementalBackupInterval = TimeSpan.FromMinutes(config.GetValue("Backup:IncrementalIntervalMinutes", 60));
            schedule.RetentionEnabled = true;
            schedule.RetentionKeepLast = config.GetValue("Backup:RetentionKeepLast", 7);
        });
    }

    // -- Scaling signal (compute axis, for the KEDA bridge) ---------------
    silo.AddLatticeScalingSignal(options =>
        options.MinReplicas = config.GetValue("Scaling:MinReplicas", 1));

    // -- State API + membership + authorization + auth-admin API ----------
    silo.AddLatticeStateApi();
    if (dataApiEnabled)
    {
        // Co-host the write-capable data-API facade so its gRPC binding (mapped
        // below) can serve mutations from the same silo gRPC endpoint.
        silo.AddLatticeDataApi();
    }
    silo.AddLatticeMembership();
    silo.AddLatticeAuth(options =>
    {
        // Deny-by-default: a subject with no matching rule is refused, and the
        // read-visibility filter only surfaces trees a caller may read. The
        // effect is configurable purely so the local compose harness can run a
        // fully-open dev cluster (Auth:DefaultEffect=Allow); every deployed
        // region leaves it at the secure Deny default.
        options.DefaultEffect = string.Equals(config["Auth:DefaultEffect"], "Allow", StringComparison.OrdinalIgnoreCase)
            ? LatticeEffect.Allow
            : LatticeEffect.Deny;
        // The AdministratorAccessSeeder grants each bootstrap administrator a single
        // cluster-wide (all-trees, Tree:*) full-access rule so every MCP facade group
        // is advertised to them at discovery. Authoring a Tree:*-scoped data-plane rule
        // requires the all-trees grant tier to be enabled; with it off (the default)
        // PutRuleAsync rejects the rule and the security administrator is left with no
        // MCP tools until a grant is authored by hand. Enable the tier so the seed is
        // authorable. The tier never applies to the reserved authorization namespace and
        // preserves the data/telemetry operation-bit separation, so this does not weaken
        // the deny-by-default posture above.
        options.AllTreesGrantsEnabled = true;
        foreach (var administrator in ParseCsv(config["Auth:BootstrapAdministrators"]))
        {
            options.BootstrapAdministrators.Add(administrator);
        }
    });
    silo.AddLatticeAuthApi();

    // -- Schema enforcement + tree-administration control API -------------
    // Co-host the schema-enforcement layer and the schema / tree-administration
    // control facades so the MCP head's remote treeadmin group (which dials the
    // same silo gRPC endpoint as State) can reach them. Schema enforcement is
    // zero-overhead until a per-tree policy is authored - a tree with no policy
    // short-circuits the interceptor - so co-hosting it does not alter existing
    // data-plane writes. Ordering is load-bearing: AddLatticeSchemaApi consumes
    // the ILatticeSchemaAdmin registered by enforcement, and AddLatticeTreeAdminApi
    // consumes the ILatticeSchemaControl registered by the schema API.
    silo.AddLatticeSchemaEnforcement();
    silo.AddLatticeSchemaApi();
    silo.AddLatticeTreeAdminApi();

    // -- Entra-backed authentication for the exposed facades --------------
    if (entraEnabled)
    {
        var tenantId = Require(config, "Entra:TenantId");
        var clientId = Require(config, "Entra:ClientId");
        var authority = config["Entra:Authority"] ?? $"https://login.microsoftonline.com/{tenantId}/v2.0";

        silo.AddEntraCredentialAuthenticator(options =>
        {
            options.Authority = authority;
            options.TenantIds.Add(tenantId);
            foreach (var audience in ParseCsv(config["Entra:Audiences"]))
            {
                options.Audiences.Add(audience);
            }

            if (options.Audiences.Count == 0)
            {
                options.Audiences.Add(clientId);
                options.Audiences.Add($"api://{clientId}");
            }

            // Pin the accepted token signature algorithm(s). The authenticator
            // seeds Algorithms with RS256 by default, so this deployment is
            // already hardened; we clear and repopulate explicitly so the
            // allow-list is a first-class, visible configuration point. Pinning
            // the JWT header `alg` closes the algorithm-confusion gap (CWE-347):
            // the validator refuses a token advertising any algorithm outside
            // this set. Override with an Entra:Algorithms CSV; defaults to
            // RS256 (the algorithm Entra issues v2.0 tokens with) when unset.
            options.Algorithms.Clear();
            foreach (var algorithm in ParseCsv(config["Entra:Algorithms"]))
            {
                options.Algorithms.Add(algorithm);
            }

            if (options.Algorithms.Count == 0)
            {
                options.Algorithms.Add(LatticeEntraAuthenticatorOptions.DefaultAlgorithm);
            }
        });

        // App-only Microsoft Graph directory backing (subject / group resolution)
        // is opt-in. Preferred path in Azure: a secret-less managed identity - the
        // region's user-assigned MI (resolved by DefaultAzureCredential via
        // AZURE_CLIENT_ID) authenticates app-only through a federated credential on
        // the silo app registration, so no client secret is stored or rotated.
        // A client secret (injected from Key Vault) is still accepted as a
        // dev / back-compat override and takes precedence when supplied.
        var graphSecret = config["Entra:Graph:ClientSecret"];
        var graphUseManagedIdentity = config.GetValue("Entra:Graph:UseManagedIdentity", false);
        if (!string.IsNullOrWhiteSpace(graphSecret))
        {
            silo.AddEntraGraphGroupResolver(options =>
            {
                options.TenantId = tenantId;
                options.ClientId = clientId;
                options.ClientSecret = graphSecret;
            });
        }
        else if (graphUseManagedIdentity)
        {
            silo.AddEntraGraphGroupResolver(options =>
            {
                options.Credential = new DefaultAzureCredential();
            });
        }
    }
    else if (config.GetValue("Auth:DevAuthenticateForwardedSubject", false))
    {
        // Local dev bypass (Entra off): resolve a forwarded bearer token to its
        // named subject when that id is a configured bootstrap administrator, so
        // the MCP head's permission introspection is served as the administrator
        // and discovery advertises the full tool set. Registered only in the
        // no-Entra harness; see DevBypassCredentialAuthenticator.
        silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DevBypassCredentialAuthenticator>();
    }
});

// The gRPC bindings over the facades. RequireAuthorization is off for the local
// compose harness (a clearly-labelled dev bypass); a deployment sets
// StateApi:RequireAuthorization=true, behind the Entra-authenticated front door.
//
// When enforcement is on the coarse transport gate is wired to match the sign-in
// model:
//
//   * Entra deployment (Entra:Enabled=true): the State and auth-admin coarse
//     gates are OPENED (AllowAll...Authorizer), exactly like the Data API below,
//     because the real enforcement is the deny-by-default per-subject gate applied
//     afterwards on the gated surface using the caller's Entra-resolved subject
//     (the State API's read-visibility filter; the auth-admin facade's bootstrap-
//     administrator check). The State API additionally ADVERTISES the Entra scheme
//     from its unauthenticated GetAuthScheme RPC, so a connecting Explorer offers
//     an interactive Entra sign-in instead of silently falling back to Basic. The
//     advertisement carries only public parameters (authority, tenant, audience);
//     the audience is the silo facade's own client id, which v2 access tokens
//     carry as their aud claim, so the Explorer requests a facade-audience token.
//
//   * Basic-only deployment (no Entra): the turnkey env-var credential authorizer
//     secures the state surface with a shared username/password and advertises the
//     Basic scheme; the auth-admin surface stays fail-closed (no subject resolver
//     to gate on) until an operator wires its authorizer.
builder.Services.AddLatticeStateApiGrpc(options =>
{
    options.RequireAuthorization = requireApiAuthorization;
    if (!requireApiAuthorization)
    {
        return;
    }

    if (entraEnabled)
    {
        var tenantId = config["Entra:TenantId"] ?? string.Empty;
        var authority = config["Entra:Authority"] ?? $"https://login.microsoftonline.com/{tenantId}/v2.0";
        options.AdvertisedAuthSchemes.Add(new Orleans.Lattice.Api.State.Grpc.AuthSchemeDescriptor
        {
            // Well-known parameter keys the Explorer's Entra login method reads
            // (Orleans.Lattice.Explorer.Core.Authentication.ExplorerAuthSchemes):
            // "authority", "tenantId", "audience". The client (application) id is
            // deliberately NOT advertised - the interactive Explorer console uses
            // its own configured public-client id, not the silo facade's.
            SchemeId = "entra",
            DisplayName = "Microsoft Entra ID",
            Parameters = new Dictionary<string, string>(StringComparer.Ordinal)
            {
                ["authority"] = authority,
                ["tenantId"] = tenantId,
                ["audience"] = config["Entra:ClientId"] ?? string.Empty,
            },
        });
    }
    else
    {
        options.AdvertisedAuthSchemes.Add(new Orleans.Lattice.Api.State.Grpc.AuthSchemeDescriptor
        {
            SchemeId = "basic",
            DisplayName = "Username / password",
        });
    }
});
if (requireApiAuthorization)
{
    if (entraEnabled)
    {
        // Coarse gate opened; the per-subject read-visibility filter on the gated
        // ILattice surface is the real enforcement (keyed on the Entra subject).
        builder.Services.AddSingleton<ILatticeStateApiAuthorizer, AllowAllStateApiAuthorizer>();
    }
    else
    {
        builder.Services.AddEnvVarCredentialAuthorizer();
    }
}

builder.Services.AddLatticeAuthApiGrpc(options => options.RequireAuthorization = requireApiAuthorization);
if (requireApiAuthorization && entraEnabled)
{
    // Coarse auth-admin gate opened; the facade default-denies internally on the
    // Entra-resolved subject unless it is a bootstrap administrator, so only the
    // designated security admin(s) can read or mutate the access model.
    builder.Services.AddSingleton<ILatticeAuthApiAuthorizer, AllowAllAuthApiAuthorizer>();
}

// Seed each configured bootstrap administrator with a cluster-wide full-access
// grant at startup so the security administrator can discover and use every MCP
// tool group immediately after deployment. The bootstrap bypass already gives
// them full call-time authority, but MCP discovery advertises tools only against
// authored rules - this closes that gap declaratively. Registered in every mode
// (including the open local compose harness, where the synthetic dev-bypass
// subject is a configured bootstrap administrator) so the head surfaces its tools;
// self-guards when no administrator is configured. The write is idempotent and
// replicated.
builder.Services.AddHostedService<AdministratorAccessSeeder>();

// The write-capable data-API gRPC binding, co-hosted on the same silo gRPC port.
// The coarse transport gate is opened (AllowAllDataApiAuthorizer) because the
// real enforcement is the deny-by-default per-tree/per-key access gate applied
// afterwards on the gated ILattice surface using the caller's Entra-resolved
// subject. RequireAuthorization still fails the binding closed when enforcement
// is on and no authorizer is registered; the local compose harness runs it open.
if (dataApiEnabled)
{
    builder.Services.AddLatticeDataApiGrpc(options => options.RequireAuthorization = requireApiAuthorization);
    if (requireApiAuthorization)
    {
        builder.Services.AddSingleton<ILatticeDataApiAuthorizer, AllowAllDataApiAuthorizer>();
    }
}

// The backup control-API gRPC binding, co-hosted on the same silo gRPC port. The
// coarse transport gate is opened (AllowAllBackupApiAuthorizer) because the real
// enforcement is the deny-by-default per-scope backup access gate applied
// afterwards on the caller's Entra-resolved subject (bootstrap administrators and
// backup-scoped rules). RequireAuthorization still fails the binding closed when
// enforcement is on and no authorizer is registered; the local compose harness
// runs it open. Exposing it lights up the Explorer Backups area, whose capability
// probe otherwise reads the facade as unimplemented and disables the area.
builder.Services.AddLatticeBackupApiGrpc(options => options.RequireAuthorization = requireApiAuthorization);
if (requireApiAuthorization)
{
    builder.Services.AddSingleton<ILatticeBackupApiAuthorizer, AllowAllBackupApiAuthorizer>();
}

// The replication control-API gRPC binding, co-hosted on the same silo gRPC port.
// The coarse transport gate is opened (AllowAllReplicationApiAuthorizer) because
// the real enforcement is the deny-by-default LatticeOperation.Replication access
// gate the facade applies afterwards on the caller's Entra-resolved subject (only
// a subject holding an authored Replication grant may enable / disable / inspect).
// RequireAuthorization still fails the binding closed when enforcement is on and
// no authorizer is registered; the local compose harness runs it open. Exposing it
// lets the MCP head's remote replication group reach the facade over gRPC.
if (enableRuntimeReplicationConfig)
{
    builder.Services.AddLatticeReplicationApiGrpc(options => options.RequireAuthorization = requireApiAuthorization);
    if (requireApiAuthorization)
    {
        builder.Services.AddSingleton<ILatticeReplicationApiAuthorizer, AllowAllReplicationApiAuthorizer>();
    }
}

// The schema-control and tree-administration control-API gRPC bindings,
// co-hosted on the same silo gRPC port. The MCP head's remote treeadmin group
// dials a SINGLE endpoint for both ILatticeTreeAdmin and ILatticeSchemaControl,
// so both services must be mapped here. The coarse transport gate is opened
// (AllowAll*ApiAuthorizer) because the real enforcement is the deny-by-default
// per-tree/per-operation access gate the facades apply afterwards on the caller's
// Entra-resolved subject: tree reads need Read, administration needs Admin,
// irreversible/structural lifecycle ops need TreeLifecycle, bulk-load needs
// BulkLoad, restore needs Restore, and schema mutation needs SchemaAdmin. Only a
// subject holding the authored grant (the seeded bootstrap administrator) passes.
// RequireAuthorization still fails the binding closed when enforcement is on and
// no authorizer is registered; the local compose harness runs it open.
builder.Services.AddLatticeSchemaApiGrpc(options => options.RequireAuthorization = requireApiAuthorization);
if (requireApiAuthorization)
{
    builder.Services.AddSingleton<ILatticeSchemaApiAuthorizer, AllowAllSchemaApiAuthorizer>();
}

builder.Services.AddLatticeTreeAdminApiGrpc(options => options.RequireAuthorization = requireApiAuthorization);
if (requireApiAuthorization)
{
    builder.Services.AddSingleton<ILatticeTreeAdminApiAuthorizer, AllowAllTreeAdminApiAuthorizer>();
}

// Export the whole orleans.lattice meter family over Prometheus at /metrics so a
// scraper (the local compose Prometheus, or Azure Managed Prometheus) can collect
// the cluster telemetry that backs the bundled Grafana dashboards and the MCP
// telemetry tools.
//
// OpenTelemetry's AddMeter matches a meter name EXACTLY - it does not cascade to
// child namespaces - so registering only "orleans.lattice" (LatticeMetrics.MeterName)
// silently dropped every sibling meter from /metrics: orleans.lattice.replication,
// orleans.lattice.replication.grpc, orleans.lattice.membership, orleans.lattice.auth,
// orleans.lattice.backup, and orleans.lattice.scaling never reached the backend, so
// the MCP telemetry tools and Grafana could not see replication apply lag, peer
// entries/bytes-behind, ship duration, membership, auth, backup, or scaling series.
// The "orleans.lattice*" wildcard exports the entire family (and any future lattice
// meter) in one registration.
builder.Services.AddOpenTelemetry()
    .WithMetrics(metrics => metrics
        .AddMeter($"{LatticeMetrics.MeterName}*")
        .AddPrometheusExporter());

var app = builder.Build();

// Enforce the Front Door origin lock before any endpoint runs. The internal
// HTTP/1 port serves /health (platform liveness probe), /metrics (Prometheus
// scrape), and the /lattice/scale signal (KEDA) - all reached directly on the
// internal network without transiting Front Door, so they are exempt. Every
// client-facing gRPC request on the external port is locked.
//
// The replication ENGINE transport services (the live push/probe transport, the
// cross-cluster snapshot transport, and the saga control channel - all under the
// orleans.lattice.replication.Lattice* gRPC service ids) are additionally exempt.
// Cross-cluster peer traffic is silo-to-silo: a peer dials this region's external
// ACA ingress FQDN directly, never transiting Front Door, so it cannot carry the
// X-Azure-FDID header and would be refused with 403 - blocking all cross-cluster
// convergence (config, membership, and auth policy). The core replication push
// transport is infrastructure-agnostic and has no seam to stamp a Front Door
// header. Exempting these paths is safe because the engine transport authenticates
// every inbound RPC with the shared replication secret (LATTICE_REPLICATION_SECRET,
// enforced by the receiver-side interceptor), which is the real gate here - the
// origin lock is redundant defence for an already strongly-authenticated surface.
// The replication CONTROL-API endpoint (orleans.lattice.api.replication, mapped
// below) is deliberately NOT exempt: the MCP head stamps the Front Door header when
// dialling it, so it stays behind the lock.
app.UseFrontDoorOriginLock(
    frontDoorId,
    "/metrics",
    "/lattice/scale",
    "/orleans.lattice.replication.LatticeReplication",
    "/orleans.lattice.replication.LatticeRemoteSnapshot",
    "/orleans.lattice.replication.LatticeSaga");

// The origin lock EXEMPTS /metrics and /lattice/scale (internal scrapers cannot
// stamp the Front Door header), and ASP.NET endpoint routing answers a mapped
// endpoint on BOTH Kestrel listeners - so without this guard those two paths are
// reachable, unauthenticated, on the externally exposed HTTP/2 ingress (grpcPort).
// Confine them to the internal HTTP/1 listener (httpPort): they return 404 on any
// other port. The discriminator is the accepted-socket local port, which Kestrel
// sets from the listener the connection arrived on; unlike the Host / :authority
// header it is not client-supplied, so it cannot be spoofed across the external
// ingress. /health is intentionally NOT confined - Front Door's og-state origin
// group probes it on the HTTP/2 ingress port.
app.UseInternalPortEndpointGuard(httpPort, "/metrics", "/lattice/scale");

app.MapLatticeStateApiGrpc();
app.MapLatticeAuthApiGrpc();
if (dataApiEnabled)
{
    app.MapLatticeDataApiGrpc();
}
app.MapLatticeBackupApiGrpc();
app.MapLatticeReplicationGrpc();
if (enableRuntimeReplicationConfig)
{
    // The replication control-API gRPC endpoint the MCP head's remote replication
    // group dials. Distinct from MapLatticeReplicationGrpc above, which maps the
    // engine's shipper/receiver transport.
    app.MapLatticeReplicationApiGrpc();
}

// The schema-control and tree-administration control-API gRPC endpoints the MCP
// head's remote treeadmin group dials (both off the same silo gRPC endpoint).
app.MapLatticeSchemaApiGrpc();
app.MapLatticeTreeAdminApiGrpc();

// Compute-axis scaling signal for the KEDA Prometheus scaler (default route
// /lattice/scale) and a liveness probe. Both are plain HTTP so ACA can probe
// them without a shell in the final image.
app.MapLatticeScalingSignal();
app.MapPrometheusScrapingEndpoint();

// Liveness probe. Front Door's og-state origin group probes this path with HEAD
// on the gRPC (HTTP/2) ingress port, and Container Apps probes it with GET on the
// internal HTTP/1 port; map both verbs so every probe gets 200 rather than a 404
// (HEAD has no matching endpoint under a GET-only map). Reached directly and
// exempt from the Front Door origin lock above. Its request logs are suppressed
// (see SuppressHealthProbeRequestLogs at startup).
app.MapMethods("/health", ["GET", "HEAD"], () => Results.Ok("healthy"));

app.Run();

static IEnumerable<string> ParseCsv(string? value) =>
    string.IsNullOrWhiteSpace(value)
        ? []
        : value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

static string Require(IConfiguration configuration, string key) =>
    configuration[key] is { Length: > 0 } value
        ? value
        : throw new InvalidOperationException($"Required configuration '{key}' is not set.");
