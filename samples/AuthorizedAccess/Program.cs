using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Samples.AuthorizedAccess;

// ---------------------------------------------------------------------------
// AuthorizedAccess - the opt-in authorization layer end to end.
//
// Two in-process Orleans clusters (site-a, site-b) run the full stack:
// Membership (identity) + Auth (a default-deny enforcement gate), with the
// reserved membership/auth system trees enrolled into cross-cluster
// replication. The sample walks four acts:
//
//   1. Create users and groups in the membership directory.
//   2. Author per-tree, per-key and per-prefix rules for a user and for groups,
//      then show read / write / delete / range enforcement (allow vs deny).
//   3. Show read visibility: a low-privilege caller cannot see (point-read or
//      range-read) entries it lacks read permission for. This is the same
//      per-key read filtering the read-only State API surfaces to the Explorer.
//   4. Converge a revoke: remove a grant on site-a and watch it become enforced
//      on site-b, purely via the system-tree replication special case.
//
// Denied writes throw LatticeAuthorizationDeniedException (fail-closed); denied
// reads return null / an empty range (soft-deny), so a caller sees only what it
// is allowed to see.
// ---------------------------------------------------------------------------

// gRPC over plaintext HTTP/2 (h2c) for the loopback replication transport.
AppContext.SetSwitch("System.Net.Http.SocketsHttpHandler.Http2UnencryptedSupport", true);

const string Tree = SiteFactory.TreeName;
const string Scheme = DemoAuthenticator.Scheme;

// Data keys: three "station/" entries (prefix scope), one "config/" key (key
// scope) and one "secret/" key only auditors may read (tree scope).
string[] stationKeys = ["station/1/status", "station/2/status", "station/3/status"];
const string ConfigKey = "config/threshold";
const string SecretKey = "secret/recipe";

var siteA = new SiteConfig(
    ClusterId: "site-a", SiloPort: 11111, GatewayPort: 30000,
    GrpcPort: 17001, PeerClusterId: "site-b", PeerGrpcPort: 17002);
var siteB = new SiteConfig(
    ClusterId: "site-b", SiloPort: 11112, GatewayPort: 30001,
    GrpcPort: 17002, PeerClusterId: "site-a", PeerGrpcPort: 17001);

var appA = SiteFactory.Build(siteA);
var appB = SiteFactory.Build(siteB);

Console.WriteLine("Starting two Orleans clusters (site-a, site-b) with the auth stack...");
await appA.StartAsync();
await appB.StartAsync();
Console.WriteLine("Both clusters ready and peered over gRPC.\n");

var dirA = appA.Services.GetRequiredService<ILatticeMembershipDirectory>();
var dirB = appB.Services.GetRequiredService<ILatticeMembershipDirectory>();
var storeA = appA.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
var storeB = appB.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
var treeA = appA.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(Tree);
var treeB = appB.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(Tree);

// -- Act 1: identities -------------------------------------------------------
// Seed the same users/groups on both sites directly (the membership trees also
// replicate, but seeding both keeps the non-convergence acts deterministic).
//   alice  -> line-operators   (manages the stations)
//   bob    -> auditors         (reads everything, writes nothing)
//   carol  -> no groups        (low-privilege)
Console.WriteLine("== Act 1: create users and groups ==");
// Seeding writes to the reserved sys-membership-* / sys-auth-policy trees, which
// require Admin. Run it as the bootstrap administrator (declared in SiteFactory),
// which bypasses the gate so the directory and policy can be provisioned before
// any rule exists.
using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    foreach (var dir in new[] { dirA, dirB })
    {
        await dir.UpsertGroupAsync(new MembershipGroup("line-operators", "Line operators"));
        await dir.UpsertGroupAsync(new MembershipGroup("auditors", "Auditors"));
        await dir.UpsertUserAsync(new MembershipUser("alice", "Alice"));
        await dir.UpsertUserAsync(new MembershipUser("bob", "Bob"));
        await dir.UpsertUserAsync(new MembershipUser("carol", "Carol"));
        await dir.AddMemberAsync("line-operators", "alice");
        await dir.AddMemberAsync("auditors", "bob");
    }
}
Console.WriteLine("  alice in line-operators, bob in auditors, carol in no group.\n");

// -- Act 2: rules + enforcement ---------------------------------------------
// Author the base ruleset on both sites (default-deny, so only these grant
// access). Rule ids let us revoke one later.
Console.WriteLine("== Act 2: author per-tree / per-key / per-prefix rules ==");
using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    foreach (var store in new[] { storeA, storeB })
    {
        // Prefix scope: operators read/write/delete/range the "station/" subtree.
        await store.PutRuleAsync(new LatticeAuthorizationRule(
            "operators-stations",
            LatticeSubjectSelector.Group("line-operators"),
            LatticeScope.Prefix(Tree, "station/"),
            LatticeOperation.Read | LatticeOperation.Write | LatticeOperation.Delete | LatticeOperation.RangeRead,
            LatticeEffect.Allow));

        // Key scope: only alice may read/write the single config threshold key.
        await store.PutRuleAsync(new LatticeAuthorizationRule(
            "alice-config",
            LatticeSubjectSelector.User("alice"),
            LatticeScope.Key(Tree, ConfigKey),
            LatticeOperation.Read | LatticeOperation.Write,
            LatticeEffect.Allow));

        // Tree scope: auditors read (and range-read) the whole tree, nothing more.
        await store.PutRuleAsync(new LatticeAuthorizationRule(
            "auditors-readall",
            LatticeSubjectSelector.Group("auditors"),
            LatticeScope.Tree(Tree),
            LatticeOperation.Read | LatticeOperation.RangeRead,
            LatticeEffect.Allow));
    }
}

// Seed the data as the bootstrap admin (which bypasses the gate) so every key
// exists before we demonstrate who can see it.
using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    foreach (var key in stationKeys)
    {
        await treeA.SetAsync(key, Encoding.UTF8.GetBytes("ok"));
    }

    await treeA.SetAsync(ConfigKey, Encoding.UTF8.GetBytes("42"));
    await treeA.SetAsync(SecretKey, Encoding.UTF8.GetBytes("caramel"));
}

// Wait for the compiled policy snapshot to reflect the authored rules (it
// rebuilds off the policy-tree change feed) before asserting enforcement.
await WaitUntilAsync(
    async () => await CanAsync(treeA, "alice", stationKeys[0]),
    TimeSpan.FromSeconds(15));

Console.WriteLine("  As alice (line-operators):");
Console.WriteLine($"    write {stationKeys[0]}  -> {await WriteOutcome(treeA, "alice", stationKeys[0], "running")}");
Console.WriteLine($"    write {ConfigKey}     -> {await WriteOutcome(treeA, "alice", ConfigKey, "50")}");
Console.WriteLine($"    write {SecretKey}      -> {await WriteOutcome(treeA, "alice", SecretKey, "leak")}   (no rule -> deny)");

Console.WriteLine("  As bob (auditors, read-only):");
Console.WriteLine($"    read  {SecretKey}      -> {await ReadOutcome(treeA, "bob", SecretKey)}");
Console.WriteLine($"    write {stationKeys[1]}  -> {await WriteOutcome(treeA, "bob", stationKeys[1], "stop")}   (read-only -> deny)");
Console.WriteLine($"    delete {stationKeys[1]} -> {await DeleteOutcome(treeA, "bob", stationKeys[1])}   (read-only -> deny)");

Console.WriteLine("  As alice (line-operators):");
Console.WriteLine($"    delete {stationKeys[2]} -> {await DeleteOutcome(treeA, "alice", stationKeys[2])}   (prefix grant allows delete)\n");

// -- Act 3: read visibility --------------------------------------------------
Console.WriteLine("== Act 3: read visibility (point + range) ==");
Console.WriteLine("  Point read of the secret recipe:");
Console.WriteLine($"    bob   -> {await ReadOutcome(treeA, "bob", SecretKey)}  (auditor)");
Console.WriteLine($"    carol -> {await ReadOutcome(treeA, "carol", SecretKey)}  (low-privilege: soft-denied)");
Console.WriteLine("  Range read of the whole tree returns only authorized keys:");
Console.WriteLine($"    bob   sees {await RangeCount(treeA, "bob")} keys (auditor: all)");
Console.WriteLine($"    alice sees {await RangeCount(treeA, "alice")} keys (stations + own config key)");
Console.WriteLine($"    carol sees {await RangeCount(treeA, "carol")} keys (nothing)\n");

// -- Act 4: converge a revoke across clusters -------------------------------
// The authorization policy tree is one of the reserved system trees enrolled
// into cross-cluster replication (the "system-tree replication special case").
// A revoke authored on site-a therefore propagates to site-b's policy tree.
// Each site's gate keeps a compiled read-through snapshot of that tree and
// refreshes it when it observes the policy change; here we assert on the
// authoritative convergence signal - the rule vanishing from site-b's tree -
// and additionally report whether site-b's live gate has already picked it up.
Console.WriteLine("== Act 4: a revoke on site-a converges to site-b ==");
Console.WriteLine($"  Before: alice writing {ConfigKey} on site-b -> {await WriteOutcome(treeB, "alice", ConfigKey, "60")}");

// Revoke alice's config-key grant on site-a only.
using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    await storeA.RemoveRuleAsync(Tree, "alice-config");
}
Console.WriteLine("  Revoked 'alice-config' on site-a only. Waiting for site-b to converge...");

// Poll site-b until the revoke has replicated into its policy tree.
var sw = System.Diagnostics.Stopwatch.StartNew();
var timeout = TimeSpan.FromSeconds(60);
bool ruleGoneOnB = false;
bool gateDenies = false;
while (sw.Elapsed < timeout)
{
    ruleGoneOnB = await AsAsync("root-admin", async () =>
        await storeB.GetRuleAsync(Tree, "alice-config") is null);
    gateDenies = !await CanAsync(treeB, "alice", ConfigKey);
    if (ruleGoneOnB)
    {
        break;
    }
    await Task.Delay(TimeSpan.FromSeconds(1));
}
sw.Stop();

Console.WriteLine($"  site-b policy tree caught up: {ruleGoneOnB} (after {sw.Elapsed.TotalSeconds:F0}s)");
Console.WriteLine($"  site-b live gate already denies alice: {gateDenies}");
Console.WriteLine();
Console.WriteLine(ruleGoneOnB
    ? "[OK] the revoke authored on site-a converged onto site-b via system-tree replication."
    : "[FAIL] the revoke did not converge onto site-b within the timeout.");

await appA.StopAsync();
await appB.StopAsync();
return ruleGoneOnB ? 0 : 1;

// --- helpers ---------------------------------------------------------------

// Runs an action under the ambient credential for the given subject. The
// credential flows to the grain on the Orleans request context; the membership
// context resolves it into a subject (with directory-expanded groups).
static async Task<T> AsAsync<T>(string subject, Func<Task<T>> action)
{
    using (LatticeCredentialContext.Use(subject, scheme: Scheme))
    {
        return await action();
    }
}

// True when the subject is currently allowed to write the key (probe, no throw).
static async Task<bool> CanAsync(ILattice tree, string subject, string key) =>
    await AsAsync(subject, async () =>
    {
        try
        {
            await tree.SetAsync(key, Encoding.UTF8.GetBytes("probe"));
            return true;
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return false;
        }
    });

// "allowed" / "DENIED" for a write attempt.
static async Task<string> WriteOutcome(ILattice tree, string subject, string key, string value) =>
    await AsAsync(subject, async () =>
    {
        try
        {
            await tree.SetAsync(key, Encoding.UTF8.GetBytes(value));
            return "allowed";
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return "DENIED";
        }
    });

// "allowed" / "DENIED" for a delete attempt.
static async Task<string> DeleteOutcome(ILattice tree, string subject, string key) =>
    await AsAsync(subject, async () =>
    {
        try
        {
            await tree.DeleteAsync(key);
            return "allowed";
        }
        catch (LatticeAuthorizationDeniedException)
        {
            return "DENIED";
        }
    });

// The stored value for a read, or "(hidden)" when read is soft-denied.
static async Task<string> ReadOutcome(ILattice tree, string subject, string key) =>
    await AsAsync(subject, async () =>
    {
        var value = await tree.GetAsync(key);
        return value is null ? "(hidden)" : $"'{Encoding.UTF8.GetString(value)}'";
    });

// Number of keys a subject can see via an authorized range read.
static async Task<int> RangeCount(ILattice tree, string subject) =>
    await AsAsync(subject, async () =>
    {
        var cursorId = await tree.OpenKeyCursorAsync();
        var count = 0;
        try
        {
            while (true)
            {
                var page = await tree.NextKeysAsync(cursorId, 100);
                count += page.Keys.Count;
                if (!page.HasMore)
                {
                    break;
                }
            }
        }
        finally
        {
            await tree.CloseCursorAsync(cursorId);
        }

        return count;
    });

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
