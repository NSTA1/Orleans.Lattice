using System.Text;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Orleans.Lattice;
using Orleans.Lattice.Api.Auth;
using Orleans.Lattice.Api.Auth.Grpc;
using Orleans.Lattice.Api.Schema;
using Orleans.Lattice.Api.Schema.Grpc;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Explorer.Web;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Membership.Entra;
using Orleans.Lattice.Membership.Entra.Graph;
using Orleans.Lattice.Samples.Explorer;
using Orleans.Lattice.Schema;

// Orleans.Lattice.Explorer sample: co-hosts a single-silo cluster, the state /
// auth / schema gRPC admin surfaces, and the embeddable Explorer web console in
// one process, so you can open the console in a browser and walk a live tree,
// its access-control policy, and its schema governance end to end.
//
// The console is the standalone web head's exact code path: AddLatticeExplorerWeb
// registers it and MapLatticeExplorer mounts it, which is all a consumer needs to
// embed the Explorer in their own ASP.NET app. Here it is pointed at the local
// gRPC endpoint through the launcher-friendly bootstrap environment variables.
//
// Three control planes are co-hosted on the one gRPC endpoint - state (Explore),
// auth (Access), and schema (Schema) - so those three console areas are live. The
// console auto-signs-in as a bootstrap administrator (see below), which is what
// makes the Access and Schema areas, gated on an administrator probe, light up
// without a manual login. (The Backups area stays disabled: this sample does not
// co-host the backup gRPC API.)

const string DemoTree = "factory-floor";
const int GrpcPort = 5199;   // h2c gRPC endpoint the console connects to
const int WebPort = 5080;    // HTTP endpoint you browse the console on

// The console auto-signs-in with these (see the environment variables below).
// The username is registered as a bootstrap administrator on the silo, so the
// admin-gated Access and Schema areas accept it; the password is never checked
// by the sample's trusted-token authenticator.
const string AdminUser = "explorer-admin";
const string AdminPassword = "explorer";

// h2c (HTTP/2 without TLS) keeps the sample dependency-free - no dev cert - and
// matches the insecure-loopback-dev transport the console is seeded with below.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

// -- Identity-directory mode selection --------------------------------------
// The Access area's subject picker and validated create form run against an
// identity directory. This sample offers two config-gated modes, fail-closed by
// default:
//
//   * Static (default, no configuration): a small in-memory roster backs the
//     picker and the create form. An id that is not in the roster fails closed
//     ("No such principal in the directory.") rather than being created as an
//     unvalidated free-text entry. This mode is one-command runnable and is what
//     the sample's own tests exercise.
//
//   * Entra (opt-in): set ALL THREE of LATTICE_ENTRA_TENANT_ID,
//     LATTICE_ENTRA_CLIENT_ID, and LATTICE_ENTRA_CLIENT_SECRET to back the picker
//     with your real Microsoft Entra tenant over Microsoft Graph (app-only). The
//     enablement walkthrough is in this sample's README.
//
// Half-configuring Entra (some but not all three variables) is rejected up front
// with a non-zero exit, so a partial configuration never silently degrades to the
// static directory.
var entraTenantId = Environment.GetEnvironmentVariable("LATTICE_ENTRA_TENANT_ID");
var entraClientId = Environment.GetEnvironmentVariable("LATTICE_ENTRA_CLIENT_ID");
var entraClientSecret = Environment.GetEnvironmentVariable("LATTICE_ENTRA_CLIENT_SECRET");

var entraVarsSet = new[] { entraTenantId, entraClientId, entraClientSecret }
    .Count(v => !string.IsNullOrWhiteSpace(v));
var useEntraDirectory = entraVarsSet == 3;
if (entraVarsSet is > 0 and < 3)
{
    Console.WriteLine("Entra directory mode is half-configured.");
    Console.WriteLine(
        "Set ALL of LATTICE_ENTRA_TENANT_ID, LATTICE_ENTRA_CLIENT_ID, and LATTICE_ENTRA_CLIENT_SECRET");
    Console.WriteLine(
        "to back the Access directory with your real Entra tenant, or unset all three to use the");
    Console.WriteLine("built-in static directory. See samples/Explorer/README.md for the walkthrough.");
    return 2;
}

// The group-merge mode governs whether locally-defined membership contributes to
// authorization. Set LATTICE_MEMBERSHIP_MERGE_MODE to Union (default), TokenOnly,
// or DirectoryOnly to exercise how the Access area reflects it: under TokenOnly
// the group-create and member add/remove controls are disabled (with an
// explanatory banner) but stay read-only viewable, while Policies and Explain stay
// live. An unrecognised value is rejected up front.
var mergeModeVar = Environment.GetEnvironmentVariable("LATTICE_MEMBERSHIP_MERGE_MODE");
var groupMergeMode = SubjectGroupMergeMode.Union;
if (!string.IsNullOrWhiteSpace(mergeModeVar)
    && !Enum.TryParse(mergeModeVar, ignoreCase: true, out groupMergeMode))
{
    Console.WriteLine($"LATTICE_MEMBERSHIP_MERGE_MODE '{mergeModeVar}' is not recognised.");
    Console.WriteLine("Set it to Union, TokenOnly, or DirectoryOnly, or unset it to use Union.");
    return 2;
}


// Seed the console's first-run connection through the bootstrap environment
// variables (read by AddLatticeExplorerWeb's environment bootstrap): point it at
// the co-hosted gRPC endpoint and allow the local h2c dev transport.
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_ENDPOINT", $"http://localhost:{GrpcPort}");
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_INSECURE_DEV", "true");

// Auto-sign-in credential applied in memory for this process. The console picks
// it up on startup and attaches it to every admin call, so the Access and Schema
// areas resolve the caller as the bootstrap administrator with no manual login.
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_USERNAME", AdminUser);
Environment.SetEnvironmentVariable("LATTICE_EXPLORER_PASSWORD", AdminPassword);

// Isolate the console's persisted configuration to a sample-owned file and start
// each run from a clean slate. This keeps the sample off the shared per-user
// Explorer config (%LOCALAPPDATA%\Orleans.Lattice.Explorer\config.json), so a
// previous session's saved endpoint can never hijack the demo, and it lets the
// environment bootstrap above re-seed the co-hosted endpoint and admin sign-in on
// every launch.
var sampleConfigPath = Path.Combine(AppContext.BaseDirectory, "explorer-sample-config.json");
if (File.Exists(sampleConfigPath))
{
    File.Delete(sampleConfigPath);
}

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();

// Serve the Explorer UI's packaged static web assets (its stylesheet, favicon,
// and interop script, shipped as an RCL under _content/Orleans.Lattice.Explorer.UI/)
// no matter which environment the sample runs in. WebApplication only auto-maps
// these in Development, so calling it explicitly keeps `dotnet run` styled even
// under the default Production environment.
builder.WebHost.UseStaticWebAssets();
builder.WebHost.ConfigureKestrel(options =>
{
    // gRPC needs HTTP/2; the Blazor Server console needs HTTP/1.1 for its
    // SignalR circuit, so each gets its own port.
    options.ListenLocalhost(GrpcPort, listen => listen.Protocols = HttpProtocols.Http2);
    options.ListenLocalhost(WebPort, listen => listen.Protocols = HttpProtocols.Http1AndHttp2);
});

builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.AddMemoryGrainStorageAsDefault();
    silo.UseInMemoryReminderService();
    silo.AddLattice((s, name) => s.AddMemoryGrainStorage(name));

    // The read-only state API that backs the console's Explore area.
    silo.AddLatticeStateApi();

    // Membership + authorization give the Access admin area a real control plane
    // to manage and let the fail-closed capability probe succeed. The data-plane
    // default is left permissive so the Explore area works without a sign-in; the
    // reserved control plane (membership + policy) is always governed and only the
    // bootstrap administrator below may manage it.
    silo.AddLatticeMembership(options => options.GroupMergeMode = groupMergeMode);

    // The identity directory the Access area validates and searches against. In
    // the default static mode this is a small in-memory roster, so an unknown
    // principal id fails closed in the create form; in Entra mode it is the real
    // tenant over Microsoft Graph (see the mode selection above).
    if (useEntraDirectory)
    {
        // The Graph identity directory is app-only, but AddEntraGraphGroupResolver
        // requires an Entra authenticator to be registered first, so we add one
        // here. The console itself still signs in as the local bootstrap admin
        // over Basic (DemoBasicAuthenticator, below) - the Entra authenticator only
        // governs bearer-token callers, of which the console is not one.
        silo.AddEntraCredentialAuthenticator(options =>
        {
            options.Authority = $"https://login.microsoftonline.com/{entraTenantId}/v2.0";
            options.TenantIds.Add(entraTenantId!);
            options.Audiences.Add(entraClientId!);
            options.Audiences.Add($"api://{entraClientId}");
        });

        // Backs the Access area's subject picker and validated create with a live
        // Microsoft Graph search/resolve over your tenant (last-wins over the
        // default no-op directory).
        silo.AddEntraGraphGroupResolver(options =>
        {
            options.TenantId = entraTenantId!;
            options.ClientId = entraClientId!;
            options.ClientSecret = entraClientSecret!;
        });
    }
    else
    {
        // A small in-memory roster: the subject picker searches it and the create
        // form validates against it, so an id that is not listed here is blocked
        // ("No such principal in the directory.") instead of being created as an
        // unvalidated free-text id. This is the epic's fail-closed create posture.
        silo.AddStaticIdentityDirectory(roster => roster
            .AddUser(AdminUser, "Explorer Administrator")
            .AddUser("alice", "Alice Ng")
            .AddUser("bob", "Bob Ito")
            .AddUser("carol", "Carol Diaz")
            .AddGroup("operators", "Floor Operators"));
    }
    silo.AddLatticeAuth(options =>
    {
        // Deny-by-default (the framework default): a subject with no matching
        // rule is refused, which is the intuitive fail-closed authorization
        // posture. The demo seeds one illustrative grant after startup (the
        // 'operators' group may Read factory-floor) so the Access > Explain tab
        // shows a real allow-vs-deny split rather than a blanket allow. The
        // bootstrap administrator bypasses the decision engine, so the console's
        // own admin areas keep working regardless of this default.
        options.DefaultEffect = LatticeEffect.Deny;
        options.BootstrapAdministrators.Add(AdminUser);
    });
    silo.AddLatticeAuthApi();

    // Schema enforcement plus its control facade so the Schema admin area is
    // reachable and can govern trees from the console.
    silo.AddLatticeSchemaEnforcement();

    // Per-value versioning, so the Schema area's Versions tab has a live schema
    // registry to target rather than reporting "schema versioning is not
    // registered". A single demo schema (id 1) with two versions and a v1 -> v2
    // upcaster lets an operator set a tree's version config and exercise the
    // advance / migrate actions from the console. (Enforcement is registered
    // first: versioning composes its write interceptor, so the order matters.)
    silo.AddLatticeSchemaVersioning(registry =>
    {
        registry.AddSchema(schemaId: 1, version: 1, name: "machine-status");
        registry.AddSchema(schemaId: 1, version: 2, name: "machine-status");

        // v1 -> v2 adds a default "state": "unknown" member.
        registry.AddUpcaster(
            schemaId: 1,
            fromVersion: 1,
            toVersion: 2,
            transform: LatticeValueTransform.Passthrough(
                LatticeValueTransform.SetMember(
                    "state", LatticeValueTransform.Const(LatticeConstant.Text("unknown")))));
    });

    silo.AddLatticeSchemaApi();

    // Trusts the console's auto-applied Basic sign-in: the auth / schema gRPC
    // bridges hand this authenticator the base64(username:password) token and it
    // resolves the caller subject to "explorer-admin", the bootstrap administrator.
    silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DemoBasicAuthenticator>();
});

// The gRPC binding over the state facade. Authorization is disabled here purely
// to keep the sample one-command runnable; a real deployment registers an
// ILatticeStateApiAuthorizer and leaves RequireAuthorization at its secure
// default. Because the sample co-hosts auth, the state API's read-visibility
// filter is active and fail-closed: it only surfaces trees the resolved caller
// may read. Match the console's "Basic base64(user:pass)" sign-in header so the
// state binding resolves the caller as the bootstrap administrator (the same
// scheme the auth and schema bindings use) - otherwise the catalog stays empty.
builder.Services.AddLatticeStateApiGrpc(o =>
{
    o.RequireAuthorization = false;
    o.CredentialScheme = DemoBasicAuthenticator.Scheme;
});

// The auth and schema control-plane gRPC bindings the Access and Schema areas
// call. Transport authorization is left off (sample-only, so the console needs
// no client certificate), but the silo's own administrator check still runs
// against the resolved caller subject - so the areas only light up because the
// console signs in as the bootstrap administrator. The bridge reads the console's
// "Basic base64(user:pass)" header, hence the Basic credential scheme.
builder.Services.AddLatticeAuthApiGrpc(o =>
{
    o.RequireAuthorization = false;
    o.CredentialScheme = DemoBasicAuthenticator.Scheme;
});
builder.Services.AddLatticeSchemaApiGrpc(o =>
{
    o.RequireAuthorization = false;
    o.CredentialScheme = DemoBasicAuthenticator.Scheme;
});

// The embeddable Explorer web console - the one call a consumer makes to host it.
// The sample pins the console's persisted config to its own isolated file so it
// always connects to the co-hosted endpoint seeded above.
builder.Services.AddLatticeExplorerWeb(o =>
{
    o.ConfigFilePath = sampleConfigPath;

    // The Schema area is withheld from the Explorer's default UI for the initial
    // release, so this sample hides it too - matching the shipped experience. A
    // developer working on the area can bring it back for a run by setting
    // LATTICE_EXPLORER_ENABLE_SCHEMA=true, with no code change.
    o.EnableSchemaArea =
        string.Equals(
            Environment.GetEnvironmentVariable("LATTICE_EXPLORER_ENABLE_SCHEMA"),
            "true",
            StringComparison.OrdinalIgnoreCase);
});

var app = builder.Build();

app.UseAntiforgery();

app.MapLatticeStateApiGrpc();
app.MapLatticeAuthApiGrpc();
app.MapLatticeSchemaApiGrpc();
app.MapLatticeExplorer();

await app.StartAsync();

// Seed demo data and the illustrative authorization policy under a system-origin
// scope so these trusted startup writes bypass the (now deny-by-default) access
// gate - exactly as a co-hosted infrastructure component does. A plain SetAsync
// auto-registers the demo tree, so it surfaces in the console's catalog with no
// extra wiring.
var grainFactory = app.Services.GetRequiredService<IGrainFactory>();
using (LatticeSystemOrigin.Enter())
{
    var tree = grainFactory.GetGrain<ILattice>(DemoTree);
    for (var i = 0; i < 12; i++)
    {
        await tree.SetAsync($"machine-{i:D3}", Encoding.UTF8.GetBytes($"status-{i:D3}"));
    }

    Console.WriteLine($"Seeded '{DemoTree}' with 12 entries.");

    // With deny-by-default authorization, seed one illustrative grant so the Access
    // area shows a real allow-vs-deny split out of the box: the 'operators' group
    // may Read the demo tree, and 'alice' is a member of it. These ids match the
    // static roster seeded above; the Entra mode addresses subjects by real tenant
    // object ids, so there the operator authors rules against their own directory.
    if (!useEntraDirectory)
    {
        var membership = app.Services.GetRequiredService<ILatticeMembershipDirectory>();
        await membership.AddMemberAsync("operators", "alice");

        var policyStore = app.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
        await policyStore.PutRuleAsync(new LatticeAuthorizationRule(
            ruleId: "operators-read-factory-floor",
            subject: LatticeSubjectSelector.Group("operators"),
            scope: LatticeScope.Tree(DemoTree),
            operations: LatticeOperation.Read | LatticeOperation.RangeRead,
            effect: LatticeEffect.Allow));

        Console.WriteLine(
            $"Seeded authorization: deny-by-default, with 'operators' (member 'alice') granted Read on '{DemoTree}'.");
        Console.WriteLine("  In Access > Explain: 'alice' Read -> Allowed (matched rule); 'bob' Read -> Denied (default).");
    }
}
Console.WriteLine($"Silo + state/auth/schema gRPC surface started on http://localhost:{GrpcPort}");
Console.WriteLine($"Explorer console: open http://localhost:{WebPort}/ in a browser.");
Console.WriteLine($"Auto-signed in as bootstrap administrator '{AdminUser}' - the Explore, Access, and Schema areas are all enabled.");
Console.WriteLine(useEntraDirectory
    ? "Identity directory: Microsoft Entra (Graph) - the Access subject picker and validated create run against your real tenant."
    : "Identity directory: static in-memory roster - the Access create form fails closed on any id not in the roster (try 'alice', 'operators', or an unknown id).");
Console.WriteLine(groupMergeMode == SubjectGroupMergeMode.TokenOnly
    ? "Group-merge mode: TokenOnly - locally-defined membership is inert, so the Access group-create and member editing controls render disabled with an explanatory banner (Policies and Explain stay live)."
    : $"Group-merge mode: {groupMergeMode} - locally-defined membership is effective, so group and member editing is enabled. Set LATTICE_MEMBERSHIP_MERGE_MODE=TokenOnly to see the merge-mode-aware gating.");
Console.WriteLine("Press Ctrl+C to stop.");

await app.WaitForShutdownAsync();
return 0;
