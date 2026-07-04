using System.Text;
using System.Text.Json;
using Azure.Core;
using Azure.Identity;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Membership.Entra;

// ---------------------------------------------------------------------------
// EntraAuthorization - the opt-in authorization layer on a single silo, driven
// by a REAL Microsoft Entra ID (Azure AD) identity, with the signed-in user as
// the tree's owner.
//
// Unlike the Authorization sample (which fakes a token with a demo scheme), this
// sample acquires a genuine Entra access token for the user who is currently
// signed in to the Azure CLI, and makes THAT user the authorization root of
// trust: their Entra object id (oid) is the sole bootstrap administrator. No
// trusted-token shortcut, no seeding identity - the same signed Entra token that
// authenticates the caller also authorizes them as the owner. This is the
// recommended production shape: bind the bootstrap administrator to a real,
// unforgeable identity and never map a plaintext token to an admin id.
//
// It therefore cannot run until you have provisioned an Entra app registration
// and signed in with the Azure CLI. The step-by-step `az` setup is in this
// sample's README and in docs/lattice.membership.entra/entra-setup.md. Without
// that setup this program prints guidance and exits non-zero - it never silently
// no-ops.
//
// The flow is:
//
//   1. Acquire an Entra token for the signed-in az user (AzureCliCredential) and
//      read its oid, so the silo can name that oid the bootstrap administrator.
//   2. Start a single in-process silo: Membership + the Entra authenticator +
//      a default-deny Auth gate whose only bootstrap administrator is the oid.
//   3. Resolve the token to a subject and confirm it is the owner.
//   4. As the Entra user (the owner), write a value to a tree and read it back.
//   5. As an anonymous request (no credential), attempt the same read and write
//      and watch the default-deny gate reject them - fail-closed for everyone
//      who is not the owner.
// ---------------------------------------------------------------------------

const string Tree = "entra-demo";
const string BearerScheme = "Bearer";

// -- 1. Configuration produced by the `az` setup ----------------------------
var tenantId = Environment.GetEnvironmentVariable("LATTICE_ENTRA_TENANT_ID");
var clientId = Environment.GetEnvironmentVariable("LATTICE_ENTRA_CLIENT_ID");
var scope = Environment.GetEnvironmentVariable("LATTICE_ENTRA_SCOPE");

if (string.IsNullOrWhiteSpace(tenantId) || string.IsNullOrWhiteSpace(clientId))
{
    PrintSetupHelp("LATTICE_ENTRA_TENANT_ID and LATTICE_ENTRA_CLIENT_ID must both be set.");
    return 2;
}

// The .default scope on the app's own Application ID URI yields a v2.0 access
// token whose audience is the app. Override with LATTICE_ENTRA_SCOPE if your
// app exposes a specific delegated scope instead.
scope = string.IsNullOrWhiteSpace(scope) ? $"api://{clientId}/.default" : scope;

// -- 1a. Acquire an Entra token for the signed-in az user -------------------
Console.Write($"Acquiring an Entra token for scope '{scope}' via the Azure CLI...");
string entraToken;
try
{
    var credential = new AzureCliCredential(new AzureCliCredentialOptions { TenantId = tenantId });
    var accessToken = await credential.GetTokenAsync(new TokenRequestContext([scope]));
    entraToken = accessToken.Token;
    Console.WriteLine(" done.");
}
catch (Exception ex)
{
    Console.WriteLine(" failed.");
    PrintSetupHelp(
        $"Could not acquire an Entra token via the Azure CLI ({ex.GetType().Name}: {ex.Message}). " +
        "Run 'az login' and complete the app-registration setup first.");
    return 3;
}

// -- 1b. Read the caller's oid so it can be named the bootstrap admin -------
// The oid is a claim inside the token, so it is known before the silo starts.
// This is what lets the signed-in user be configured as the owner up front.
var ownerOid = TryReadOid(entraToken);
if (string.IsNullOrWhiteSpace(ownerOid))
{
    PrintSetupHelp(
        "The acquired token has no 'oid' claim. Confirm the app issues v2.0 tokens " +
        "(entra-setup.md Step 3) so the caller's object id is present.");
    return 4;
}

Console.WriteLine($"Signed-in Entra object id (oid): {ownerOid}");
Console.WriteLine("  -> this oid is the tree owner (sole bootstrap administrator).\n");

// -- 2. Start the silo ------------------------------------------------------
using var host = Host.CreateDefaultBuilder(args)
    .ConfigureLogging(logging =>
    {
        logging.ClearProviders();
        logging.SetMinimumLevel(LogLevel.None);
    })
    .UseOrleans(silo =>
    {
        silo.UseLocalhostClustering();
        silo.AddMemoryGrainStorageAsDefault();
        silo.UseInMemoryReminderService();
        silo.AddLattice((services, name) => services.AddMemoryGrainStorage(name));

        // Membership owns identity resolution and the credential-authenticator seam.
        silo.AddLatticeMembership();

        // The Entra authenticator validates the signed-in user's real bearer token
        // against the tenant allow-list and the app's audience, resolving it to a
        // subject whose id is the caller's Entra object id (oid).
        silo.AddEntraCredentialAuthenticator(options =>
        {
            options.Authority = $"https://login.microsoftonline.com/{tenantId}/v2.0";
            options.TenantIds.Add(tenantId);
            options.Audiences.Add(clientId);
            options.Audiences.Add($"api://{clientId}");
        });

        // Auth installs the fail-closed, default-deny gate. The signed-in user's
        // own Entra oid is the sole bootstrap administrator - the owner - so the
        // same signed token that authenticates the caller also authorizes them.
        // There is no trusted-token authenticator: the root of trust is a real,
        // unforgeable identity. Keep this set to the smallest number of
        // break-glass owner identities in a real deployment.
        silo.AddLatticeAuth(options =>
        {
            options.DefaultEffect = LatticeEffect.Deny;
            options.BootstrapAdministrators.Add(ownerOid);
        });
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.\n");

var membership = host.Services.GetRequiredService<ILatticeMembershipContext>();
var tree = host.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(Tree);

// -- 3. Resolve the Entra token to a subject and confirm the owner ----------
Console.WriteLine("== Resolve the signed-in Entra identity ==");
LatticeSubject subject;
using (LatticeCredentialContext.Use(entraToken, scheme: BearerScheme))
{
    subject = await membership.ResolveCurrentAsync();
}

if (subject.IsAnonymous || !string.Equals(subject.SubjectId, ownerOid, StringComparison.Ordinal))
{
    PrintSetupHelp(
        "The Entra token was not accepted by the authenticator (or resolved to a different " +
        "subject). Check that LATTICE_ENTRA_TENANT_ID matches the token's tenant and that " +
        "LATTICE_ENTRA_CLIENT_ID is the app the token was issued for.");
    await host.StopAsync();
    return 5;
}

Console.WriteLine($"  Resolved subject (oid): {subject.SubjectId}  (owner)\n");

var key = $"greeting/{subject.SubjectId}";
var value = $"Hello from Entra oid {subject.SubjectId} at {DateTimeOffset.UtcNow:u}";

// -- 4. As the Entra user (the owner): write and read -----------------------
Console.WriteLine("== As the signed-in Entra user (owner) ==");
string ownerWrite;
string? ownerRead;
using (LatticeCredentialContext.Use(entraToken, scheme: BearerScheme))
{
    ownerWrite = await WriteOutcome(key, value);
    var stored = await tree.GetAsync(key);
    ownerRead = stored is null ? null : Encoding.UTF8.GetString(stored);
}

Console.WriteLine($"  write {key} -> {ownerWrite}   (owner: allowed)");
Console.WriteLine($"  read  {key} -> {(ownerRead is null ? "(absent)" : $"'{ownerRead}'")}\n");

// -- 5. As an anonymous request (no credential): write and read -------------
// No ambient credential is stamped, so membership resolves the caller to the
// well-known anonymous subject. Under default-deny it is authorized for nothing.
Console.WriteLine("== As an anonymous request (no credential) ==");
var anonWrite = await WriteOutcome(key, "anon-overwrite");
var anonStored = await tree.GetAsync(key);
var anonRead = anonStored is null ? null : Encoding.UTF8.GetString(anonStored);

Console.WriteLine($"  write {key} -> {anonWrite}   (default-deny)");
Console.WriteLine($"  read  {key} -> {(anonRead is null ? "(absent)" : $"'{anonRead}'")}   (soft-denied)\n");

var success =
    ownerWrite == "allowed" &&
    ownerRead == value &&
    anonWrite == "DENIED" &&
    anonRead is null;

Console.WriteLine(success
    ? "[OK] the owner wrote and read a value; the anonymous request was denied."
    : "[FAIL] the expected owner-allowed / anonymous-denied outcome did not hold.");

await host.StopAsync();
return success ? 0 : 1;

// --- helpers ---------------------------------------------------------------

// "allowed" / "DENIED" for a write attempted under the current ambient credential.
async Task<string> WriteOutcome(string k, string v)
{
    try
    {
        await tree.SetAsync(k, Encoding.UTF8.GetBytes(v));
        return "allowed";
    }
    catch (LatticeAuthorizationDeniedException)
    {
        return "DENIED";
    }
}

// Reads the 'oid' claim from a JWT without validating it (validation is the
// authenticator's job inside the silo). Returns null when the token is not a
// readable JWT or carries no oid.
static string? TryReadOid(string jwt)
{
    var parts = jwt.Split('.');
    if (parts.Length < 2)
    {
        return null;
    }

    var payload = parts[1].Replace('-', '+').Replace('_', '/');
    switch (payload.Length % 4)
    {
        case 2: payload += "=="; break;
        case 3: payload += "="; break;
    }

    try
    {
        var json = Encoding.UTF8.GetString(Convert.FromBase64String(payload));
        using var doc = JsonDocument.Parse(json);
        return doc.RootElement.TryGetProperty("oid", out var oid) ? oid.GetString() : null;
    }
    catch
    {
        return null;
    }
}

// Prints actionable setup guidance and where to find the full walkthrough.
static void PrintSetupHelp(string reason)
{
    Console.WriteLine();
    Console.WriteLine("This sample needs a Microsoft Entra app registration and an `az login` session.");
    Console.WriteLine($"  Reason: {reason}");
    Console.WriteLine();
    Console.WriteLine("Quick start:");
    Console.WriteLine("  1. az login");
    Console.WriteLine("  2. Follow docs/lattice.membership.entra/entra-setup.md to create the app");
    Console.WriteLine("     registration and expose an API scope pre-authorized for the Azure CLI.");
    Console.WriteLine("  3. Export the ids the setup prints:");
    Console.WriteLine("       $env:LATTICE_ENTRA_TENANT_ID = '<tenant-guid>'");
    Console.WriteLine("       $env:LATTICE_ENTRA_CLIENT_ID = '<app-client-id>'");
    Console.WriteLine("  4. dotnet run --project samples/EntraAuthorization");
    Console.WriteLine();
    Console.WriteLine("See samples/EntraAuthorization/README.md for the full walkthrough.");
}
