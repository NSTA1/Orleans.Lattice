using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Samples.Authorization;

// ---------------------------------------------------------------------------
// Authorization - the opt-in authorization layer on a single silo, with a focus
// on group and NESTED-group membership.
//
// One in-process Orleans silo runs the full stack: Membership (identity) + Auth
// (a default-deny enforcement gate). No replication, no gRPC, no web host - just
// the authorization layer you get from AddLatticeMembership + AddLatticeAuth.
//
// The membership graph is deliberately nested:
//
//     staff  (top-level group)
//       |
//       +-- engineering  (a group that is a *member* of staff)
//             |
//             +-- alice   (a user)
//
// Because group membership is transitive, alice belongs to both engineering and
// staff even though she was only ever added to engineering. A rule that grants
// "staff" read therefore reaches alice through the nesting.
//
// The sample walks four acts:
//
//   1. Build the nested membership graph and print each subject's transitive
//      groups, proving alice inherits `staff` via `engineering`.
//   2. Author default-deny rules for the top-level group, the nested group, and
//      a separate flat group, then show read / write / delete enforcement.
//   3. Show read visibility: a range read returns only the keys a subject may
//      read, and a point read of an unauthorized key is soft-denied.
//   4. Grant access dynamically by adding a user to a nested group at runtime and
//      watch the gate begin allowing what it denied a moment earlier.
//
// Denied writes throw LatticeAuthorizationDeniedException (fail-closed); denied
// reads return null / an empty range (soft-deny), so a caller sees only what it
// is allowed to see.
// ---------------------------------------------------------------------------

const string Tree = "catalog";
const string Scheme = DemoAuthenticator.Scheme;

// Data keys: two "svc/" entries (prefix scope), one shared "incident/" key
// (key scope) and one "audit/" key any staff member may read (tree scope).
string[] svcKeys = ["svc/api/status", "svc/db/status"];
const string IncidentKey = "incident/current";
const string AuditKey = "audit/log";

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

        // Membership resolves the ambient caller credential into a subject whose
        // groups are expanded transitively from the directory's user/group edges,
        // so nested-group membership is honoured by the gate.
        silo.AddLatticeMembership();

        // Auth installs the enforcement gate. Default-deny: only explicit allow
        // rules grant access. "root-admin" is a bootstrap administrator so the
        // sample can seed users/groups/rules before any rule exists.
        silo.AddLatticeAuth(options =>
        {
            options.DefaultEffect = LatticeEffect.Deny;
            options.BootstrapAdministrators.Add("root-admin");
        });

        // The trusted-token authenticator that maps the ambient credential's
        // token to the caller subject id (a real deployment uses JWT/Entra).
        silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DemoAuthenticator>();
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.\n");

var directory = host.Services.GetRequiredService<ILatticeMembershipDirectory>();
var store = host.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
var tree = host.Services.GetRequiredService<IGrainFactory>().GetGrain<ILattice>(Tree);

// -- Act 1: nested membership graph -----------------------------------------
// Seeding writes to the reserved sys-membership-* / sys-auth-policy trees, which
// require Admin. Run seeding as the bootstrap administrator (declared above),
// which bypasses the gate so the directory and policy can be provisioned before
// any rule exists.
Console.WriteLine("== Act 1: build a nested membership graph ==");
using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    await directory.UpsertGroupAsync(new MembershipGroup("staff", "All staff"));
    await directory.UpsertGroupAsync(new MembershipGroup("engineering", "Engineering"));
    await directory.UpsertGroupAsync(new MembershipGroup("oncall", "On-call responders"));

    await directory.UpsertUserAsync(new MembershipUser("alice", "Alice"));
    await directory.UpsertUserAsync(new MembershipUser("bob", "Bob"));
    await directory.UpsertUserAsync(new MembershipUser("carol", "Carol"));

    // engineering is a *member* of staff -> a nested group edge.
    await directory.AddMemberAsync("staff", "engineering", MembershipMemberKind.Group);

    // alice is only ever added to engineering; she inherits staff via nesting.
    await directory.AddMemberAsync("engineering", "alice");

    // bob is in a separate, flat group.
    await directory.AddMemberAsync("oncall", "bob");

    // carol starts in no group (she joins one at runtime in Act 4).
}

Console.WriteLine($"  alice's transitive groups: {await GroupsLine("alice")}  (staff inherited via engineering)");
Console.WriteLine($"  bob's   transitive groups: {await GroupsLine("bob")}");
Console.WriteLine($"  carol's transitive groups: {await GroupsLine("carol")}\n");

// -- Act 2: rules + enforcement ---------------------------------------------
Console.WriteLine("== Act 2: author default-deny rules and enforce them ==");
using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    // Tree scope on the TOP-LEVEL group: every staff member (including nested
    // members like alice) may read the whole tree, and nothing more.
    await store.PutRuleAsync(new LatticeAuthorizationRule(
        "staff-read-tree",
        LatticeSubjectSelector.Group("staff"),
        LatticeScope.Tree(Tree),
        LatticeOperation.Read | LatticeOperation.RangeRead,
        LatticeEffect.Allow));

    // Prefix scope on the NESTED group: engineering may write and delete the
    // "svc/" subtree.
    await store.PutRuleAsync(new LatticeAuthorizationRule(
        "engineering-write-svc",
        LatticeSubjectSelector.Group("engineering"),
        LatticeScope.Prefix(Tree, "svc/"),
        LatticeOperation.Write | LatticeOperation.Delete,
        LatticeEffect.Allow));

    // Key scope on a FLAT group: oncall may read and write the single shared
    // incident key (RangeRead too, so it also shows up in an authorized range
    // read - a point Read grant alone does not make a key range-visible).
    await store.PutRuleAsync(new LatticeAuthorizationRule(
        "oncall-incident",
        LatticeSubjectSelector.Group("oncall"),
        LatticeScope.Key(Tree, IncidentKey),
        LatticeOperation.Read | LatticeOperation.RangeRead | LatticeOperation.Write,
        LatticeEffect.Allow));
}

// Seed the data as the bootstrap admin (which bypasses the gate) so every key
// exists before we demonstrate who can see it.
using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    foreach (var key in svcKeys)
    {
        await tree.SetAsync(key, Encoding.UTF8.GetBytes("ok"));
    }

    await tree.SetAsync(IncidentKey, Encoding.UTF8.GetBytes("none"));
    await tree.SetAsync(AuditKey, Encoding.UTF8.GetBytes("seeded"));
}

// Wait for the compiled policy snapshot to reflect the authored rules (it
// rebuilds off the policy-tree change feed) before asserting enforcement.
await WaitUntilAsync(
    async () => await CanReadAsync("alice", AuditKey),
    TimeSpan.FromSeconds(15));

Console.WriteLine("  As alice (engineering -> staff):");
Console.WriteLine($"    read  {AuditKey}       -> {await ReadOutcome("alice", AuditKey)}   (staff tree-read)");
Console.WriteLine($"    write {svcKeys[0]}  -> {await WriteOutcome("alice", svcKeys[0], "running")}   (engineering prefix-write)");
Console.WriteLine($"    write {IncidentKey} -> {await WriteOutcome("alice", IncidentKey, "fire")}   (not oncall -> deny)");

Console.WriteLine("  As bob (oncall):");
Console.WriteLine($"    write {IncidentKey} -> {await WriteOutcome("bob", IncidentKey, "fire")}   (oncall key grant)");
Console.WriteLine($"    write {svcKeys[0]}  -> {await WriteOutcome("bob", svcKeys[0], "stop")}   (not engineering -> deny)");
Console.WriteLine($"    delete {svcKeys[0]} -> {await DeleteOutcome("bob", svcKeys[0])}   (not engineering -> deny)");

Console.WriteLine("  As carol (no groups):");
Console.WriteLine($"    read  {AuditKey}       -> {await ReadOutcome("carol", AuditKey)}   (soft-denied)");
Console.WriteLine($"    write {svcKeys[0]}  -> {await WriteOutcome("carol", svcKeys[0], "x")}   (deny)\n");

// -- Act 3: read visibility --------------------------------------------------
Console.WriteLine("== Act 3: read visibility (point + range) ==");
Console.WriteLine("  Range read of the whole tree returns only authorized keys:");
Console.WriteLine($"    alice sees {await RangeCount("alice")} keys (staff: all)");
Console.WriteLine($"    bob   sees {await RangeCount("bob")} keys (only the incident key it may read)");
Console.WriteLine($"    carol sees {await RangeCount("carol")} keys (nothing)\n");

// -- Act 4: grant access by adding to a nested group at runtime -------------
Console.WriteLine("== Act 4: grant access by joining a nested group at runtime ==");
Console.WriteLine($"  Before: carol range read sees {await RangeCount("carol")} keys; write {svcKeys[0]} -> {await WriteOutcome("carol", svcKeys[0], "y")}");

using (LatticeCredentialContext.Use("root-admin", scheme: Scheme))
{
    // Add carol to engineering. She immediately inherits staff (nested), so she
    // gains both the staff tree-read and the engineering prefix-write.
    await directory.AddMemberAsync("engineering", "carol");
}
Console.WriteLine($"  Added carol to engineering. Her transitive groups: {await GroupsLine("carol")}");

var carolCanRead = await WaitUntilAsync(
    async () => await RangeCount("carol") == 4,
    TimeSpan.FromSeconds(15));

Console.WriteLine($"  After:  carol range read sees {await RangeCount("carol")} keys; write {svcKeys[0]} -> {await WriteOutcome("carol", svcKeys[0], "z")}");
Console.WriteLine();
Console.WriteLine(carolCanRead
    ? "[OK] nested-group membership granted carol read + write with no per-user rule."
    : "[FAIL] carol did not gain access within the timeout.");

await host.StopAsync();
return carolCanRead ? 0 : 1;

// --- helpers ---------------------------------------------------------------

// Runs an action under the ambient credential for the given subject. The
// credential flows to the grain on the Orleans request context; the membership
// context resolves it into a subject with directory-expanded (transitive) groups.
async Task<T> AsAsync<T>(string subject, Func<Task<T>> action)
{
    using (LatticeCredentialContext.Use(subject, scheme: Scheme))
    {
        return await action();
    }
}

// The subject's transitive groups, sorted, as a "{a, b}" string.
async Task<string> GroupsLine(string subject)
{
    var groups = await AsAsync("root-admin", async () => await directory.GroupsOfAsync(subject));
    var ordered = groups.OrderBy(g => g, StringComparer.Ordinal);
    return groups.Count == 0 ? "{}" : "{" + string.Join(", ", ordered) + "}";
}

// True when the subject can currently read the key (probe, no throw).
async Task<bool> CanReadAsync(string subject, string key) =>
    await AsAsync(subject, async () => await tree.GetAsync(key) is not null);

// "allowed" / "DENIED" for a write attempt.
async Task<string> WriteOutcome(string subject, string key, string value) =>
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
async Task<string> DeleteOutcome(string subject, string key) =>
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
async Task<string> ReadOutcome(string subject, string key) =>
    await AsAsync(subject, async () =>
    {
        var value = await tree.GetAsync(key);
        return value is null ? "(hidden)" : $"'{Encoding.UTF8.GetString(value)}'";
    });

// Number of keys a subject can see via an authorized range read.
async Task<int> RangeCount(string subject) =>
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
