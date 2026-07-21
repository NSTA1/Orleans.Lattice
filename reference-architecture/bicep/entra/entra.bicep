// =============================================================================
// entra.bicep - Bicep-native Entra (Microsoft Entra ID) provisioning
//               (sub-issue F-192 / #1280)
// -----------------------------------------------------------------------------
// Authors the estate's Entra identity surface declaratively with the Microsoft
// Graph Bicep extension (GA 2025-07-29). It creates, per estate:
//
//   * Three app registrations - one per exposed component:
//       - silo facade  (the read-only State + auth-admin gRPC resource server)
//       - MCP endpoint  (the remote MCP server front door)
//       - Explorer      (the interactive operator console)
//   * A service principal for each app registration.
//   * FEDERATED IDENTITY CREDENTIALS (preferred over client secrets, per the
//     issue) binding EACH region's user-assigned managed identity to the silo
//     and MCP apps. This is what lets a workload obtain an app-only token (for
//     example the silo's Microsoft Graph group resolver) WITHOUT a client secret
//     - the identity is federated, so there is no secret to store, rotate, or
//     leak. Client secrets are deliberately NOT created.
//   * An application app role ("Lattice.Access") on the silo facade app, and
//     app-role assignments granting the MCP and Explorer service principals that
//     role - the app-to-app authorization edge (least privilege: a single,
//     purpose-named role, assigned only to the two SPs that call the silo).
//
// Microsoft Graph directory permissions the silo group resolver needs
// (GroupMember.Read.All, application permission) are DECLARED here as
// requiredResourceAccess AND granted (tenant admin consent) declaratively via a
// Microsoft.Graph/appRoleAssignedTo assignment from the silo service principal to
// the Microsoft Graph service principal's app role - so no imperative
// `az ad app permission admin-consent` step is required. The deploying identity
// still needs a privileged directory role (for example Privileged Role
// Administrator, or the AppRoleAssignment.ReadWrite.All + Application.ReadWrite.All
// application permissions) to create that assignment; that is the sole directory
// privilege the deployer requires and is documented in the deploy README.
//
// NO SECRETS: no passwordCredentials are authored, nothing secret is emitted as
// an output. The app (client) ids emitted below are public identifiers.
//
// This module uses the Microsoft Graph extension, enabled by the bicepconfig.json
// colocated in this folder. It is kept in its own directory precisely so that
// extension/experimental config does NOT apply to main.bicep or the ARM modules,
// which build with stock Bicep defaults.
// =============================================================================

targetScope = 'resourceGroup'

extension microsoftGraphV1

@description('Lowercase base name shared by every region (must equal the value passed to main.bicep). Used to name and uniquely-name the app registrations.')
@minLength(3)
@maxLength(16)
param baseName string

@description('Entra tenant id that owns the app registrations and issues the workload-identity tokens the federated credentials trust.')
param tenantId string

@description('Per-region user-assigned managed identities that federate with the silo and MCP apps (secret-less app-only tokens). Each item: { regionCode: string, principalId: string } where principalId is the identity OBJECT (principal) id. In region-list order.')
param regionManagedIdentities array

@description('Reply (redirect) URIs for the Explorer console web app (the per-region Explorer FQDNs and/or the global Front Door Explorer hostname), each as a full https URL ending in the sign-in callback path. Empty leaves the Explorer app with no web reply URIs (add them on a later pass once the FQDNs are known).')
param explorerRedirectUris array = []

// Microsoft Graph (the resource whose permissions the silo group resolver needs).
var graphAppId = '00000003-0000-0000-c000-000000000000'
// GroupMember.Read.All (application permission) - read group memberships app-only.
var groupMemberReadAllRoleId = '98830695-27a2-44f7-8c18-0c3ebc9698f6'
// Stable id for the custom application role the MCP + Explorer SPs are granted.
var latticeAccessRoleId = guid(baseName, 'Lattice.Access')

// Federated-credential trust anchors. The subject is each managed identity's
// object id; the issuer is this tenant's login authority; the audience is the
// fixed token-exchange audience for workload identity federation.
var ficIssuer = '${environment().authentication.loginEndpoint}${tenantId}/v2.0'
var ficAudiences = [ 'api://AzureADTokenExchange' ]

// =============================================================================
// Silo facade app registration (the protected State + auth-admin resource server)
// =============================================================================

resource siloApp 'Microsoft.Graph/applications@v1.0' = {
  uniqueName: '${baseName}-silo'
  displayName: '${baseName} Lattice silo facade'
  // Single-tenant: only identities from the estate tenant are accepted.
  signInAudience: 'AzureADMyOrg'
  identifierUris: [ 'api://${baseName}-silo' ]
  // The custom application role the MCP and Explorer service principals are
  // granted for app-to-app access to the silo facade.
  appRoles: [
    {
      id: latticeAccessRoleId
      allowedMemberTypes: [ 'Application' ]
      displayName: 'Lattice.Access'
      description: 'Grants a calling application access to the Lattice silo State and auth-admin facades.'
      value: 'Lattice.Access'
      isEnabled: true
    }
  ]
  // App-only Microsoft Graph directory access for the optional group resolver.
  // Application permissions require tenant admin consent (residual manual step).
  requiredResourceAccess: [
    {
      resourceAppId: graphAppId
      resourceAccess: [
        {
          id: groupMemberReadAllRoleId
          type: 'Role'
        }
      ]
    }
  ]
}

resource siloSp 'Microsoft.Graph/servicePrincipals@v1.0' = {
  appId: siloApp.appId
}

// The Microsoft Graph service principal in this tenant (the resource that owns
// the GroupMember.Read.All app role). Referenced as existing so we can grant its
// app role to the silo SP.
resource graphServicePrincipal 'Microsoft.Graph/servicePrincipals@v1.0' existing = {
  appId: graphAppId
}

// Tenant admin consent, expressed declaratively: grant the silo service principal
// the Microsoft Graph GroupMember.Read.All APPLICATION permission. An
// appRoleAssignedTo to the Graph SP is exactly what `az ad app permission
// admin-consent` creates for an application permission, so no imperative consent
// step is needed. Idempotent (keyed by the fixed principal/resource/role triple),
// so re-runs never duplicate the grant.
resource siloGraphGroupMemberConsent 'Microsoft.Graph/appRoleAssignedTo@v1.0' = {
  appRoleId: groupMemberReadAllRoleId
  principalId: siloSp.id
  resourceId: graphServicePrincipal.id
}

// One federated identity credential per region managed identity: the silo app
// trusts tokens the region's workload identity presents, so the silo obtains
// app-only Graph tokens with NO client secret.
resource siloFic 'Microsoft.Graph/applications/federatedIdentityCredentials@v1.0' = [for mi in regionManagedIdentities: {
  name: '${siloApp.uniqueName}/silo-${mi.regionCode}'
  description: 'Workload-identity federation for the ${mi.regionCode} silo managed identity (secret-less app-only access).'
  audiences: ficAudiences
  issuer: ficIssuer
  subject: mi.principalId
}]

// =============================================================================
// MCP endpoint app registration (the remote MCP server front door)
// =============================================================================

resource mcpApp 'Microsoft.Graph/applications@v1.0' = {
  uniqueName: '${baseName}-mcp'
  displayName: '${baseName} Lattice MCP endpoint'
  signInAudience: 'AzureADMyOrg'
  identifierUris: [ 'api://${baseName}-mcp' ]
  // The MCP endpoint calls the silo facade; declare the silo app role it needs.
  requiredResourceAccess: [
    {
      resourceAppId: siloApp.appId
      resourceAccess: [
        {
          id: latticeAccessRoleId
          type: 'Role'
        }
      ]
    }
  ]
}

resource mcpSp 'Microsoft.Graph/servicePrincipals@v1.0' = {
  appId: mcpApp.appId
}

resource mcpFic 'Microsoft.Graph/applications/federatedIdentityCredentials@v1.0' = [for mi in regionManagedIdentities: {
  name: '${mcpApp.uniqueName}/mcp-${mi.regionCode}'
  description: 'Workload-identity federation for the ${mi.regionCode} MCP managed identity (secret-less token acquisition).'
  audiences: ficAudiences
  issuer: ficIssuer
  subject: mi.principalId
}]

// Grant the MCP service principal the silo Lattice.Access app role (app-to-app).
resource mcpToSiloRole 'Microsoft.Graph/appRoleAssignedTo@v1.0' = {
  appRoleId: latticeAccessRoleId
  principalId: mcpSp.id
  resourceId: siloSp.id
}

// =============================================================================
// Explorer console app registration (interactive operator sign-in)
// =============================================================================

resource explorerApp 'Microsoft.Graph/applications@v1.0' = {
  uniqueName: '${baseName}-explorer'
  displayName: '${baseName} Lattice Explorer console'
  signInAudience: 'AzureADMyOrg'
  // Web app (Blazor Server): interactive users sign in and the console calls the
  // silo facade on their behalf. Reply URIs are the Explorer/Front Door hosts.
  web: {
    redirectUris: explorerRedirectUris
    implicitGrantSettings: {
      enableAccessTokenIssuance: false
      enableIdTokenIssuance: false
    }
  }
  requiredResourceAccess: [
    {
      resourceAppId: siloApp.appId
      resourceAccess: [
        {
          id: latticeAccessRoleId
          type: 'Role'
        }
      ]
    }
  ]
}

resource explorerSp 'Microsoft.Graph/servicePrincipals@v1.0' = {
  appId: explorerApp.appId
}

resource explorerFic 'Microsoft.Graph/applications/federatedIdentityCredentials@v1.0' = [for mi in regionManagedIdentities: {
  name: '${explorerApp.uniqueName}/explorer-${mi.regionCode}'
  description: 'Workload-identity federation for the ${mi.regionCode} Explorer managed identity (secret-less token acquisition).'
  audiences: ficAudiences
  issuer: ficIssuer
  subject: mi.principalId
}]

// Grant the Explorer service principal the silo Lattice.Access app role.
resource explorerToSiloRole 'Microsoft.Graph/appRoleAssignedTo@v1.0' = {
  appRoleId: latticeAccessRoleId
  principalId: explorerSp.id
  resourceId: siloSp.id
}

// =============================================================================
// Outputs - public identifiers only (NO secrets)
// =============================================================================

@description('Application (client) id of the silo facade app. Feeds the estate-wide Entra:ClientId / entraClientId on every head (the facades validate tokens for this audience).')
output siloClientId string = siloApp.appId

@description('Object id of the silo facade app registration (for admin-consent targeting).')
output siloAppObjectId string = siloApp.id

@description('Application (client) id of the MCP endpoint app.')
output mcpClientId string = mcpApp.appId

@description('Application (client) id of the Explorer console app.')
output explorerClientId string = explorerApp.appId

@description('The api:// audience the silo facades accept (feeds entraAudiences when a non-default audience is required).')
output siloAudience string = 'api://${baseName}-silo'
