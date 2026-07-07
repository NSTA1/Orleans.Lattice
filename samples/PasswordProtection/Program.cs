using System.Text;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Grpc.Net.Client;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.DependencyInjection;
using Orleans.Lattice;
using Orleans.Lattice.Api.State;
using Orleans.Lattice.Api.State.Grpc;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Samples.PasswordProtection;

// ---------------------------------------------------------------------------
// PasswordProtection - the username/password credential mechanism (the reference
// EnvVarCredentialAuthorizer from Orleans.Lattice.Api.State.Grpc) combined with
// per-tree authorization on a single in-process silo.
//
// The demo:
//   1. Creates two operator accounts by hashing their passwords with
//      LatticePasswordHash (salted PBKDF2-SHA256) and publishing each as the
//      LATTICE_STATE_USER_<name> environment variable the authorizer reads.
//   2. Stands up ONE silo that co-hosts the read-only State API gRPC surface,
//      gated by the username/password authorizer at the transport and by the
//      Orleans.Lattice.Auth policy gate on the data path. 'admin' is a bootstrap
//      administrator; 'reader' is granted read-only access to tree 'orders'.
//   3. Connects over real gRPC and proves AUTHENTICATION: the right passwords
//      are accepted, a wrong password and an anonymous call are rejected.
//   4. Proves AUTHORIZATION: the authenticated username drives per-tree reads -
//      admin sees every tree, reader sees only the 'orders' tree it was granted.
//
// The whole thing runs in one process for convenience, but every client call
// crosses a real gRPC channel presenting a Basic credential exactly as an
// external dashboard or CLI would.
// ---------------------------------------------------------------------------

const int Port = 5223;
const string OrdersTree = "orders";   // reader is granted read-only access to this tree
const string LedgerTree = "ledger";   // admin-only; reader is never granted access

const string AdminUser = "admin";
const string AdminPassword = "Adm1n-Passw0rd";
const string ReaderUser = "reader";
const string ReaderPassword = "Read0nly-Passw0rd";

// -- Act 1: create two operator accounts ------------------------------------
// Each credential is a salted PBKDF2-SHA256 hash (never the plaintext), stored
// in the environment variable the authorizer looks up by username. In a real
// deployment an operator produces these with the tools/ helper scripts and sets
// the variables out-of-band; here we mint them in-process for a one-command run.
Console.WriteLine("== Act 1: create two operator accounts ==");
PublishCredential(AdminUser, AdminPassword);
PublishCredential(ReaderUser, ReaderPassword);
Console.WriteLine(
    $"  {AdminUser,-6} -> env LATTICE_STATE_USER_{AdminUser}   (salted pbkdf2-sha256)  role: bootstrap administrator");
Console.WriteLine(
    $"  {ReaderUser,-6} -> env LATTICE_STATE_USER_{ReaderUser}  (salted pbkdf2-sha256)  role: read-only on tree '{OrdersTree}'\n");

// -- Act 2: stand up the single silo + State API gRPC (auth required) -------
// h2c (HTTP/2 without TLS) keeps the sample dependency-free - no dev cert.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

var builder = WebApplication.CreateBuilder(args);
builder.Logging.ClearProviders();
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenLocalhost(Port, listen => listen.Protocols = HttpProtocols.Http2);
});

builder.Host.UseOrleans(silo =>
{
    silo.UseLocalhostClustering();
    silo.AddMemoryGrainStorageAsDefault();
    silo.UseInMemoryReminderService();
    silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

    // Membership resolves the ambient caller credential into a subject; Auth
    // installs the default-deny data-plane gate. 'admin' is a bootstrap
    // administrator so the sample can seed the directory, rules, and trees
    // before any rule exists.
    silo.AddLatticeMembership();
    silo.AddLatticeAuth(options =>
    {
        options.DefaultEffect = LatticeEffect.Deny;
        options.BootstrapAdministrators.Add(AdminUser);
    });

    // Maps the already-authenticated Basic username onto the caller subject id.
    silo.Services.AddSingleton<ILatticeCredentialAuthenticator, PasswordAuthenticator>();

    // The read-only state API facade the gRPC surface binds.
    silo.AddLatticeStateApi();
});

// Register the Basic-aware credential bridge BEFORE AddLatticeStateApiGrpc so
// its TryAdd preserves ours instead of the default bearer bridge.
builder.Services.AddSingleton<ILatticeStateApiCredentialBridge, PasswordCredentialBridge>();

// The gRPC binding (auth required) plus the username/password authorizer that
// validates the inbound Basic header against the LATTICE_STATE_USER_* variables.
builder.Services.AddLatticeStateApiGrpc(o => o.RequireAuthorization = true);
builder.Services.AddEnvVarCredentialAuthorizer();

var app = builder.Build();
app.MapLatticeStateApiGrpc();
await app.StartAsync();

Console.WriteLine("== Act 2: start single silo + State API gRPC (auth required) ==");
Console.WriteLine($"  Silo + state-API gRPC listening on http://localhost:{Port}");

// Seed the two trees as the bootstrap administrator (which bypasses the gate),
// and author the reader's per-tree read rule.
var grainFactory = app.Services.GetRequiredService<IGrainFactory>();
var store = app.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
var directory = app.Services.GetRequiredService<ILatticeMembershipDirectory>();

using (LatticeCredentialContext.Use(AdminUser, scheme: PasswordAuthenticator.Scheme))
{
    await directory.UpsertUserAsync(new MembershipUser(AdminUser, "Administrator"));
    await directory.UpsertUserAsync(new MembershipUser(ReaderUser, "Read-only user"));

    // reader may read (point + range) the 'orders' tree, and nothing else.
    await store.PutRuleAsync(new LatticeAuthorizationRule(
        "reader-read-orders",
        LatticeSubjectSelector.User(ReaderUser),
        LatticeScope.Tree(OrdersTree),
        LatticeOperation.Read | LatticeOperation.RangeRead,
        LatticeEffect.Allow));

    var orders = grainFactory.GetGrain<ILattice>(OrdersTree);
    for (var i = 1; i <= 3; i++)
    {
        await orders.SetAsync($"order-{i:D3}", Encoding.UTF8.GetBytes($"pending-{i}"));
    }

    var ledger = grainFactory.GetGrain<ILattice>(LedgerTree);
    for (var i = 1; i <= 2; i++)
    {
        await ledger.SetAsync($"entry-{i:D3}", Encoding.UTF8.GetBytes($"amount-{i}"));
    }
}

Console.WriteLine($"  Seeded tree '{OrdersTree}' with 3 entries; tree '{LedgerTree}' with 2 entries.\n");

using var channel = GrpcChannel.ForAddress($"http://localhost:{Port}");

// Wait for the compiled policy snapshot to reflect the authored rule (it
// rebuilds off the policy-tree change feed) before asserting enforcement.
await WaitUntilAsync(
    async () => await ScanCountAsync(ClientFor(ReaderUser, ReaderPassword), OrdersTree) == 3,
    TimeSpan.FromSeconds(15));

// -- Act 3: authenticate over gRPC (username/password) ----------------------
Console.WriteLine("== Act 3: authenticate over gRPC (username/password) ==");
Console.WriteLine($"  {AdminUser,-6} correct password -> {await AuthenticationOutcome(ClientFor(AdminUser, AdminPassword))}");
Console.WriteLine($"  {ReaderUser,-6} correct password -> {await AuthenticationOutcome(ClientFor(ReaderUser, ReaderPassword))}");
Console.WriteLine($"  {ReaderUser,-6} WRONG   password -> {await AuthenticationOutcome(ClientFor(ReaderUser, "not-the-password"))}");
Console.WriteLine($"  no credentials          -> {await AuthenticationOutcome(ClientFor(null, null))}\n");

// -- Act 4: authorize per-tree reads (tied to the authenticated user) -------
Console.WriteLine("== Act 4: authorize per-tree reads (tied to the authenticated user) ==");
var adminClient = ClientFor(AdminUser, AdminPassword);
var readerClient = ClientFor(ReaderUser, ReaderPassword);

var adminOrders = await ScanCountAsync(adminClient, OrdersTree);
var adminLedger = await ScanCountAsync(adminClient, LedgerTree);
var readerOrders = await ScanCountAsync(readerClient, OrdersTree);
var readerLedger = await ScanCountAsync(readerClient, LedgerTree);

Console.WriteLine(
    $"  {AdminUser,-6} scan '{OrdersTree}' -> {adminOrders} entries ; scan '{LedgerTree}' -> {adminLedger} entries   (bootstrap admin: sees all)");
Console.WriteLine(
    $"  {ReaderUser,-6} scan '{OrdersTree}' -> {readerOrders} entries ; scan '{LedgerTree}' -> {readerLedger} entries   (granted '{OrdersTree}' read; '{LedgerTree}' hidden)\n");

var success = adminOrders == 3 && adminLedger == 2 && readerOrders == 3 && readerLedger == 0;
Console.WriteLine(success
    ? $"[OK] username/password authenticated both users; per-tree rules limited '{ReaderUser}' to '{OrdersTree}'."
    : "[FAIL] unexpected authentication or authorization outcome.");

await app.StopAsync();
return success ? 0 : 1;

// --- helpers ---------------------------------------------------------------

// Hashes the password with a fresh random salt and publishes it as the
// environment variable the authorizer looks up for this username.
static void PublishCredential(string username, string password) =>
    Environment.SetEnvironmentVariable(
        "LATTICE_STATE_USER_" + username,
        LatticePasswordHash.Hash(password));

// Builds a State API gRPC client that presents the given account's Basic
// credential on every call, or an anonymous client when username is null.
LatticeStateApiGrpcClient ClientFor(string? username, string? password)
{
    var invoker = channel.CreateCallInvoker();
    if (username is not null)
    {
        var header = "Basic " + Convert.ToBase64String(Encoding.UTF8.GetBytes($"{username}:{password}"));
        invoker = invoker.Intercept(metadata =>
        {
            metadata.Add("authorization", header);
            return metadata;
        });
    }

    return LatticeStateApiGrpcClient.Create(invoker, app.Services);
}

// "authenticated" when the credential is accepted at the transport, or
// "rejected (PermissionDenied)" when the username/password gate refuses the call.
static async Task<string> AuthenticationOutcome(LatticeStateApiGrpcClient client)
{
    try
    {
        await client.ListTreesAsync(new CatalogRequest { PageSize = 50 });
        return "authenticated";
    }
    catch (RpcException ex) when (ex.StatusCode == StatusCode.PermissionDenied)
    {
        return "rejected (PermissionDenied)";
    }
}

// Number of entries a client can read from a tree via the snapshot-isolated
// scan cursor. A tree the caller is not permitted to see reads as empty: the
// gate hides it (NotFound / PermissionDenied) rather than disclosing its
// existence or contents.
static async Task<int> ScanCountAsync(LatticeStateApiGrpcClient client, string treeId)
{
    var total = 0;
    string? token = null;
    try
    {
        do
        {
            var page = await client.ScanEntriesAsync(new EntryScanRequest
            {
                TreeId = treeId,
                PageSize = 50,
                ContinuationToken = token,
            });
            total += page.Entries.Count;
            token = page.ContinuationToken;
        }
        while (!string.IsNullOrEmpty(token));
    }
    catch (RpcException ex) when (ex.StatusCode is StatusCode.NotFound or StatusCode.PermissionDenied)
    {
        return 0;
    }

    return total;
}

// Polls the predicate until it is true or the budget elapses.
static async Task<bool> WaitUntilAsync(Func<Task<bool>> predicate, TimeSpan budget)
{
    var deadline = DateTime.UtcNow + budget;
    while (DateTime.UtcNow < deadline)
    {
        if (await predicate())
        {
            return true;
        }

        await Task.Delay(TimeSpan.FromMilliseconds(500));
    }

    return await predicate();
}
