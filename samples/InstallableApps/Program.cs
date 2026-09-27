using System.Reflection;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Orleans.Hosting;
using Orleans.Lattice;
using Orleans.Lattice.Api.Apps;
using Orleans.Lattice.Apps;
using Orleans.Lattice.Auth;
using Orleans.Lattice.Membership;
using Orleans.Lattice.Samples.InstallableApps;

// ---------------------------------------------------------------------------
// InstallableApps - declarative app install, consent, activation and teardown.
//
// One in-process Orleans silo runs the control-plane stack: Membership
// (identity) + Auth (a fail-closed gate) + Apps (manifest source, registry and
// activation pipeline) + the Apps API facade. The app is an embedded manifest,
// not executable code loaded from disk, so the operator can inspect requested
// trees, roles and operations before trusting or enabling it.
//
// The sample walks five acts:
//
//   1. Inspect before trust. Load the embedded manifest with
//      AppManifestResources and inspect the same source through DescribeAsync
//      without installing or running app code.
//   2. Install as an operator. A bootstrap administrator binds the manifest role
//      to a membership group and pins a structural capability ceiling; a caller
//      without AppInstall is denied before app metadata is touched.
//   3. Enable. Activation provisions the structural app tree
//      a/{slug}/{tree}, compiles app-owned authorization rules, and lets a group
//      member write/read while a non-member is denied.
//   4. Guard rails. Direct edits to app-owned rule ids are refused, and reducing
//      consent below the manifest request fails activation closed without
//      stopping the silo.
//   5. Disable and uninstall. App-owned rules are removed and the structural
//      tree is soft-deleted, not purged.
// ---------------------------------------------------------------------------

const string Scheme = DemoAuthenticator.Scheme;
const string Operator = "platform-operator";
const string Mallory = "mallory";
const string Member = "alice";
const string NonMember = "bob";
const string Group = "sample-crm-writers";
const string Slug = "sample-crm";
const string Version = "1.0.0";
const string Tree = "records";
const string Key = "contacts/ada";
const string AppTreePrefix = "a/";
const string ManifestResourceName = "Orleans.Lattice.Samples.InstallableApps.AppManifest.json";

var appTreeId = string.Concat(AppTreePrefix, Slug, "/", Tree);
var loaded = AppManifestResources.Load(Assembly.GetExecutingAssembly(), ManifestResourceName);
if (!loaded.IsValid || loaded.Manifest is not { } manifest)
{
    Console.WriteLine("[FAIL] embedded manifest did not validate:");
    foreach (var error in loaded.Errors)
    {
        Console.WriteLine($"  {error.Path}: {error.Message}");
    }

    return 1;
}

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
        silo.AddLatticeMembership();
        silo.AddLatticeAuth(options =>
        {
            options.DefaultEffect = LatticeEffect.Deny;
            options.BootstrapAdministrators.Add(Operator);
        });
        silo.AddLatticeApps()
            .AddLatticeApp(Slug, Assembly.GetExecutingAssembly(), ManifestResourceName)
            .AddLatticeAppsApi();
        silo.Services.AddSingleton<ILatticeCredentialAuthenticator, DemoAuthenticator>();
    })
    .Build();

Console.Write("Silo starting...");
await host.StartAsync();
Console.WriteLine(" ready.\n");

var apps = host.Services.GetRequiredService<ILatticeAppsControl>();
var membership = host.Services.GetRequiredService<ILatticeMembershipDirectory>();
var policy = host.Services.GetRequiredService<ILatticeAuthorizationPolicyStore>();
var grains = host.Services.GetRequiredService<IGrainFactory>();
var appTree = grains.GetGrain<ILattice>(appTreeId);

// -- Act 1: inspect before trust --------------------------------------------
Console.WriteLine("== Act 1: inspect before trust ==");
Console.WriteLine($"  embedded manifest -> {manifest.Identity.Slug} {manifest.Identity.Version}");
foreach (var tree in manifest.Trees)
{
    Console.WriteLine($"  tree '{tree.Name}' -> {AppTreePrefix}{manifest.Identity.Slug}/{tree.Name}; rebuildable: {tree.Rebuildable}");
}

foreach (var role in manifest.Roles)
{
    Console.WriteLine($"  role '{role.Name}' -> {FormatOperations(role.Operations)} on {string.Join(", ", role.Scopes.Select(FormatScope))}");
}

foreach (var tool in manifest.McpTools)
{
    Console.WriteLine($"  mcp tool '{manifest.Identity.Slug}_{tool.Name}' -> role '{tool.Role}'");
}

AppDescriptor describedBeforeInstall;
using (LatticeCredentialContext.Use(Operator, scheme: Scheme))
{
    describedBeforeInstall = await apps.DescribeAsync(Slug, Version)
        ?? throw new InvalidOperationException("The embedded app source did not describe the manifest.");
}

Console.WriteLine($"  DescribeAsync before install -> {describedBeforeInstall.State}; roles: {describedBeforeInstall.Roles.Length}\n");

// -- Act 2: install as an operator ------------------------------------------
Console.WriteLine("== Act 2: install as an operator ==");
using (LatticeCredentialContext.Use(Operator, scheme: Scheme))
{
    await membership.UpsertGroupAsync(new MembershipGroup(Group, "Sample CRM writers"));
    await membership.AddMemberAsync(Group, Member);

    var installed = await apps.InstallAsync(new AppInstallRequest
    {
        Slug = Slug,
        Version = Version,
        RoleBindings = [new AppRoleBindingDescriptor { RoleName = "writer", GroupId = Group }],
        Ceiling = new AppCapabilityCeilingDescriptor
        {
            AllowedOperations = LatticeOperation.Read | LatticeOperation.Write,
        },
    });

    Console.WriteLine($"  InstallAsync as '{Operator}' -> {installed.State} (changed: {installed.Changed})");
}

var deniedInstallRead = await ExpectDeniedAsync(
    $"  DescribeAsync as '{Mallory}' -> denied",
    async () =>
    {
        using var _ = LatticeCredentialContext.Use(Mallory, scheme: Scheme);
        await apps.DescribeAsync(Slug, Version);
    });
Console.WriteLine();

// -- Act 3: enable and use the app tree -------------------------------------
Console.WriteLine("== Act 3: enable and use the app tree ==");
using (LatticeCredentialContext.Use(Operator, scheme: Scheme))
{
    var enabled = await apps.EnableAsync(Slug);
    Console.WriteLine($"  EnableAsync -> {enabled.State} (changed: {enabled.Changed})");
}

var appRules = await AppRulesAsync(policy);
Console.WriteLine($"  app tree -> {appTreeId}");
Console.WriteLine("  compiled rule ids:");
foreach (var rule in appRules)
{
    Console.WriteLine($"    {rule.RuleId} -> {rule.Subject.Kind}:{rule.Subject.Id} {FormatOperations(rule.Operations)} {rule.Scope.TreeId}");
}

using (LatticeCredentialContext.Use(Member, scheme: Scheme))
{
    await appTree.SetAsync(Key, Encoding.UTF8.GetBytes("Ada Lovelace"));
    var value = await appTree.GetAsync(Key);
    Console.WriteLine($"  member '{Member}' write/read -> {Encoding.UTF8.GetString(value ?? [])}");
}

var nonMemberDenied = await ExpectDeniedAsync(
    $"  non-member '{NonMember}' write -> denied",
    async () =>
    {
        using var _ = LatticeCredentialContext.Use(NonMember, scheme: Scheme);
        await appTree.SetAsync(Key, Encoding.UTF8.GetBytes("Mallory"));
    });
Console.WriteLine();

// -- Act 4: guard rails ------------------------------------------------------
Console.WriteLine("== Act 4: guard rails ==");
var ruleEditRejected = false;
using (LatticeCredentialContext.Use(Operator, scheme: Scheme))
{
    try
    {
        var first = appRules.First();
        await policy.PutRuleAsync(first with
        {
            Subject = LatticeSubjectSelector.User(NonMember),
        });
        Console.WriteLine("  overwrite app-owned rule id -> UNEXPECTEDLY allowed");
    }
    catch (LatticeAppOwnedRuleException)
    {
        Console.WriteLine("  overwrite app-owned rule id -> refused (LatticeAppOwnedRuleException)");
        ruleEditRejected = true;
    }
}

var overCeilingFailedClosed = false;
using (LatticeCredentialContext.Use(Operator, scheme: Scheme))
{
    try
    {
        await apps.UpdateConsentAsync(new AppConsentUpdate
        {
            Slug = Slug,
            Version = Version,
            Ceiling = new AppCapabilityCeilingDescriptor { AllowedOperations = LatticeOperation.Read },
        });
        Console.WriteLine("  reduce consent to Read only -> UNEXPECTEDLY allowed");
    }
    catch (InvalidOperationException ex) when (ex.Message.Contains("CeilingExceeded", StringComparison.Ordinal))
    {
        Console.WriteLine("  reduce consent to Read only -> activation failed (CeilingExceeded)");
        overCeilingFailedClosed = true;
    }

    var failedDescriptor = await apps.DescribeAsync(Slug, Version)
        ?? throw new InvalidOperationException("Installed app disappeared after failed activation.");
    var stillServing = await appTree.TreeExistsAsync();
    Console.WriteLine($"  DescribeAsync after failed activation -> {failedDescriptor.State}; silo still serving: {stillServing}");
}

var rulesAfterFailure = await AppRulesAsync(policy);
Console.WriteLine($"  app-owned rules after failed activation -> {rulesAfterFailure.Count}\n");

// -- Act 5: disable and uninstall -------------------------------------------
Console.WriteLine("== Act 5: disable and uninstall ==");
using (LatticeCredentialContext.Use(Operator, scheme: Scheme))
{
    var disabled = await apps.DisableAsync(Slug);
    Console.WriteLine($"  DisableAsync -> {disabled.State} (changed: {disabled.Changed})");

    var uninstalled = await apps.UninstallAsync(Slug);
    Console.WriteLine($"  UninstallAsync -> {uninstalled.State} (changed: {uninstalled.Changed})");
}

var rulesAfterUninstall = await AppRulesAsync(policy);
Console.WriteLine($"  app-owned rules after uninstall -> {rulesAfterUninstall.Count}");

var treeStillRegistered = false;
var treeRejectsReads = false;
using (LatticeCredentialContext.Use(Operator, scheme: Scheme))
{
    treeStillRegistered = await appTree.TreeExistsAsync();
    try
    {
        await appTree.GetAsync(Key);
        Console.WriteLine("  app tree after uninstall -> UNEXPECTEDLY readable");
    }
    catch (InvalidOperationException)
    {
        treeRejectsReads = true;
    }
}

Console.WriteLine($"  app tree after uninstall -> registered: {treeStillRegistered}; soft-deleted: {treeRejectsReads}");
Console.WriteLine();

var ok = deniedInstallRead
    && appRules.Count > 0
    && nonMemberDenied
    && ruleEditRejected
    && overCeilingFailedClosed
    && rulesAfterFailure.Count == 0
    && rulesAfterUninstall.Count == 0
    && treeStillRegistered
    && treeRejectsReads;

Console.WriteLine(ok
    ? "[OK] installable app lifecycle ran end-to-end; consent and app-owned rule guards stayed fail-closed."
    : "[FAIL] an installable app guard did not hold.");

await host.StopAsync();
return ok ? 0 : 1;

static async Task<bool> ExpectDeniedAsync(string line, Func<Task> action)
{
    try
    {
        await action();
        Console.WriteLine(line.Replace("denied", "UNEXPECTEDLY allowed", StringComparison.Ordinal));
        return false;
    }
    catch (LatticeAuthorizationDeniedException)
    {
        Console.WriteLine($"{line} (LatticeAuthorizationDeniedException)");
        return true;
    }
}

static async Task<List<LatticeAuthorizationRule>> AppRulesAsync(ILatticeAuthorizationPolicyStore policy)
{
    var rules = new List<LatticeAuthorizationRule>();
    await foreach (var rule in policy.ListRulesAsync())
    {
        if (LatticeAppRuleIds.IsAppOwned(rule.RuleId))
        {
            rules.Add(rule);
        }
    }

    rules.Sort(static (left, right) => string.CompareOrdinal(left.RuleId, right.RuleId));
    return rules;
}

static string FormatScope(AppScopeTemplate scope)
{
    var app = scope.App is { } other ? other.Value + ":" : string.Empty;
    return scope.Kind switch
    {
        LatticeScopeKind.Tree => app + scope.Tree,
        LatticeScopeKind.Key => app + scope.Tree + "#" + scope.KeyOrPrefix,
        LatticeScopeKind.Prefix => app + scope.Tree + "#" + scope.KeyOrPrefix + "*",
        _ => app + scope.Tree,
    };
}

static string FormatOperations(LatticeOperation operations)
{
    var names = Enum.GetValues<LatticeOperation>()
        .Where(value => value != LatticeOperation.None && operations.HasFlag(value))
        .Select(static value => value.ToString());
    return string.Join("+", names);
}
