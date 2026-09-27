namespace Orleans.Lattice.Api.Apps;

/// <summary>Stable app control DTO aliases; each uses the reserved oia. prefix and at most six characters.</summary>
public static class ApiAppsTypeAliases
{
    /// <summary>The reserved app control alias namespace, distinct from the app engine's oap. namespace.</summary>
    public const string AliasPrefix = "oia.";
    /// <summary>Alias for <see cref="AppInstallRequest"/>.</summary>
    public const string AppInstallRequest = "oia.ir";
    /// <summary>Alias for <see cref="AppRoleBindingDescriptor"/>.</summary>
    public const string AppRoleBindingDescriptor = "oia.rb";
    /// <summary>Alias for <see cref="AppCapabilityCeilingDescriptor"/>.</summary>
    public const string AppCapabilityCeilingDescriptor = "oia.cc";
    /// <summary>Alias for <see cref="AppExceptionScope"/>.</summary>
    public const string AppExceptionScope = "oia.es";
    /// <summary>Alias for <see cref="AppLifecycleResult"/>.</summary>
    public const string AppLifecycleResult = "oia.lr";
    /// <summary>Alias for <see cref="AppConsentUpdate"/>.</summary>
    public const string AppConsentUpdate = "oia.cu";
    /// <summary>Alias for <see cref="AppConsentReport"/>.</summary>
    public const string AppConsentReport = "oia.cr";
    /// <summary>Alias for <see cref="AppSummary"/>.</summary>
    public const string AppSummary = "oia.su";
    /// <summary>Alias for <see cref="AppCatalog"/>.</summary>
    public const string AppCatalog = "oia.ca";
    /// <summary>Alias for <see cref="AppDescriptor"/>.</summary>
    public const string AppDescriptor = "oia.de";
    /// <summary>Alias for <see cref="AppProvenanceDescriptor"/>.</summary>
    public const string AppProvenanceDescriptor = "oia.pr";
    /// <summary>Alias for <see cref="AppTreeDescriptor"/>.</summary>
    public const string AppTreeDescriptor = "oia.tr";
    /// <summary>Alias for <see cref="AppRoleDescriptor"/>.</summary>
    public const string AppRoleDescriptor = "oia.ro";
    /// <summary>Alias for <see cref="AppRoleScope"/>.</summary>
    public const string AppRoleScope = "oia.rs";
    /// <summary>Alias for <see cref="AppSubscriptionDescriptor"/>.</summary>
    public const string AppSubscriptionDescriptor = "oia.sd";
    /// <summary>Alias for <see cref="AppMcpToolDescriptor"/>.</summary>
    public const string AppMcpToolDescriptor = "oia.mt";
    /// <summary>Alias for <see cref="AppReplicationDescriptor"/>.</summary>
    public const string AppReplicationDescriptor = "oia.rp";
    /// <summary>Alias for <see cref="AppSchemaDescriptor"/>.</summary>
    public const string AppSchemaDescriptor = "oia.sc";
    /// <summary>Alias for <see cref="LatticeAppsCapabilities"/>.</summary>
    public const string LatticeAppsCapabilities = "oia.cp";
}
