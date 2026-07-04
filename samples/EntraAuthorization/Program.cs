using System.Text;
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
using Orleans.Lattice.Samples.EntraAuthorization;

// ---------------------------------------------------------------------------
// EntraAuthorization - the opt-in authorization layer on a single silo, driven
// by a REAL Microsoft Entra ID (Azure AD) identity.
//
// Unlike the Authorization sample (which fakes a token with a demo scheme), this
// sample acquires a genuine Entra access token for the user who is currently
// signed in to the Azure CLI, resolves that token to a subject through the Entra
// authenticator, and then writes a value to a tree *as that Entra identity*.
//
// It therefore cannot run until you have provisioned an Entra app registration
// and exported its ids as environment variables. The step-by-step `az` setup is
// in this sample's README and in
// docs/lattice.membership.entra/entra-setup.md. Without that setup this program
// prints guidance and exits with a non-zero code - it never silently no-ops.
//
// The flow is:
//
//   1. Acquire an Entra token for the signed-in az user (AzureCliCredential).
//   2. Start a single in-process silo: Membership + the Entra authenticator +
//      a default-deny Auth gate.
//   3. Resolve the token to a subject and print the caller's object id (oid).
//   4. Prove fail-closed: with no rule, the Entra user's write is DENIED.
//   5. As a bootstrap administrator, author an allow rule for that exact oid.
//   6. Write a value to the tree AS THE ENTRA USER, then read it back.
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

// -- 2. Acquire an Entra token for the signed-in az user --------------------
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

// -- 3. Start the silo ------------------------------------------------------
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

        // A tiny trusted-token authenticator used only to seed the first rule as a
        // bootstrap administrator. It handles its own scheme only, so it never
        // shadows the Entra bearer tokens above.
        silo.Services.AddSingleton<ILatticeCredentialAuthenticator, SetupAuthenticator>();

        // Auth installs the fail-closed, default-deny enforcement gate.
        silo.AddLatticeAuth(options =>
        {
            options.DefaultEffect = LatticeEffect.Deny;
            options.BootstrapAdministrators.Add(SetupAuthenticator.SetupAdministrator);
        });
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.\n");

var membership = host.Services.GetRequiredService<ILatticeMembershipContext>();
var store = host.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
var tree = host.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(Tree);

// -- 4. Resolve the Entra token to a subject --------------------------------
Console.WriteLine("== Resolve the signed-in Entra identity ==");
LatticeSubject subject;
using (LatticeCredentialContext.Use(entraToken, scheme: BearerScheme))
{
    subject = await membership.ResolveCurrentAsync();
}

if (subject.IsAnonymous)
{
    PrintSetupHelp(
        "The Entra token was not accepted by the authenticator (resolved to anonymous). " +
        "Check that LATTICE_ENTRA_TENANT_ID matches the token's tenant and that " +
        "LATTICE_ENTRA_CLIENT_ID is the app the token was issued for.");
    await host.StopAsync();
    return 4;
}

Console.WriteLine($"  Resolved subject (oid): {subject.SubjectId}");
Console.WriteLine($"  Groups in token:        {(subject.GroupIds.Count == 0 ? "(none)" : string.Join(", ", subject.GroupIds))}");
Console.WriteLine();

var key = $"greeting/{subject.SubjectId}";
var value = $"Hello from Entra oid {subject.SubjectId} at {DateTimeOffset.UtcNow:u}";

// -- 5. Fail-closed: the Entra user cannot write before a rule exists -------
Console.WriteLine("== Fail-closed: write before any rule is authored ==");
Console.WriteLine($"  write {key} -> {await EntraWriteOutcome(key, value)}   (default-deny)\n");

// -- 6. Author an allow rule for exactly this oid ---------------------------
Console.WriteLine("== Author an allow rule for this oid (as the bootstrap admin) ==");
using (LatticeCredentialContext.Use(SetupAuthenticator.SetupAdministrator, scheme: SetupAuthenticator.Scheme))
{
    await store.PutRuleAsync(new LatticeAuthorizationRule(
        "entra-user-readwrite",
        LatticeSubjectSelector.User(subject.SubjectId),
        LatticeScope.Tree(Tree),
        LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write,
        LatticeEffect.Allow));
}
Console.WriteLine($"  Allowed Read|RangeRead|Write on tree '{Tree}' for user '{subject.SubjectId}'.\n");

// The compiled policy snapshot rebuilds off the policy-tree change feed, so poll
// the actual write until the grant takes effect.
Console.WriteLine("== Write a value to the tree AS THE ENTRA USER ==");
var wrote = await WaitUntilAsync(
    async () => string.Equals(await EntraWriteOutcome(key, value), "allowed", StringComparison.Ordinal),
    TimeSpan.FromSeconds(15));

string? readBack = null;
if (wrote)
{
    using (LatticeCredentialContext.Use(entraToken, scheme: BearerScheme))
    {
        var stored = await tree.GetAsync(key);
        readBack = stored is null ? null : Encoding.UTF8.GetString(stored);
    }
}

Console.WriteLine($"  write {key} -> {(wrote ? "allowed" : "DENIED")}");
Console.WriteLine($"  read  {key} -> {(readBack is null ? "(absent)" : $"'{readBack}'")}");
Console.WriteLine();

var success = wrote && readBack == value;
Console.WriteLine(success
    ? "[OK] wrote and read back a value under the signed-in Entra identity."
    : "[FAIL] the Entra identity could not write and read back its value.");

await host.StopAsync();
return success ? 0 : 1;

// --- helpers ---------------------------------------------------------------

// "allowed" / "DENIED" for a write attempted under the ambient Entra credential.
async Task<string> EntraWriteOutcome(string k, string v)
{
    using (LatticeCredentialContext.Use(entraToken, scheme: BearerScheme))
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
}

// Polls the predicate until it is true or the budget elapses.
async Task<bool> WaitUntilAsync(Func<Task<bool>> predicate, TimeSpan budget)
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
