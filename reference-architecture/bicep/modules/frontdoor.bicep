// =============================================================================
// frontdoor.bicep - global ingress via Azure Front Door Standard (sub-issue F-194)
// -----------------------------------------------------------------------------
// The GLOBAL INGRESS lane of the Reference Architecture epic. Front Door is a
// global (not per-region) resource: ONE Standard profile fronts every region and
// latency-routes each user to the nearest healthy region, failing over to the
// next-nearest region on a health-probe failure. The estate is active-active with
// per-key convergence, so no session affinity is required (see reference-
// architecture.md, "Global ingress: Azure Front Door Standard").
//
// This module owns ONLY the Front Door resources. It creates NO container apps,
// NO storage, NO Key Vault - it consumes the per-region head FQDNs that
// compute.bicep already exposes (via main.bicep's perRegion[] output) and turns
// them into origin-group members. Each client-facing head (Explorer web UI, MCP
// server, silo State API) gets its own AFD endpoint + origin group; every origin
// group loops the region set, one origin per region head FQDN, so the module is
// fully N-region parameterised from a single input array.
//
// -----------------------------------------------------------------------------
// SECURITY POSTURE (hard requirement)
// -----------------------------------------------------------------------------
//   - HTTPS only. Routes accept Http+Https but redirect Http -> Https, so no
//     plaintext request ever reaches an origin (forwardingProtocol: HttpsOnly).
//   - TLS 1.2 minimum. The *.azurefd.net default endpoints enforce TLS 1.2 as
//     their floor at the platform; a later custom domain must keep minimumTlsVersion
//     at TLS12 (see the custom-domain seam note below).
//   - Origins are reached over SERVER TLS via the ACA-managed FQDN with
//     certificate-name-check ENABLED (enforceCertificateNameCheck: true), so the
//     origin certificate must match its hostname - no plaintext, no cert bypass.
//   - Origin lock: each ACA origin must accept traffic ONLY from this Front Door.
//     The `frontDoorId` output below is the GUID the coordinator wires into every
//     region's ACA ingress as the `X-Azure-FDID` access restriction (see WIRING
//     RECIPE). Front Door stamps `X-Azure-FDID: <frontDoorId>` on every forwarded
//     request; the origin rejects any request whose header does not match, so no
//     one can bypass the global ingress by hitting the ACA FQDN directly.
//     LIMITATION (be explicit): this is a HEADER assertion, not a network lock.
//     ACA ingress `ipSecurityRestrictions` accepts only IPv4 CIDR ranges - it
//     CANNOT filter by the `AzureFrontDoor.Backend` service tag, and hardcoding
//     Front Door's published backend CIDRs is fragile (they rotate) and
//     Microsoft-discouraged. A determined caller who learns both the ACA FQDN and
//     the (non-secret) frontDoorId could therefore still forge the header. The
//     X-Azure-FDID check is the recommended origin lock for AFD *Standard* and
//     raises the bar materially, but the only NON-SPOOFABLE lock is AFD *Premium*
//     + Private Link to an internal (VNet-injected, internal-ingress) environment,
//     which removes the public ACA FQDN entirely. That is the private deployment
//     option's upgrade path; see reference-architecture.md ("Origin lock").
//   - No secrets. This template takes and emits no secret material.
//
// -----------------------------------------------------------------------------
// WAF / PREMIUM UPGRADE PATH (default OFF by cost decision - NOT provisioned)
// -----------------------------------------------------------------------------
// WAF is intentionally descoped from the baseline for cost. The `enableWaf`
// parameter is a clean, default-off seam: when false (the default) this module
// provisions NOTHING extra. It is surfaced as the `wafEnabled` output so the
// coordinator can branch on it. To turn WAF on later WITHOUT redesigning this
// module, a future operator makes two changes:
//
//   1. Change the profile SKU to Premium (managed WAF rule sets + Private Link
//      private origins require Premium_AzureFrontDoor):
//
//        param profileSku string = 'Premium_AzureFrontDoor'
//
//   2. Provision a WAF policy and bind it to the endpoints with a security policy.
//      Add, gated on `enableWaf`:
//
//        resource waf 'Microsoft.Network/FrontDoorWebApplicationFirewallPolicies@2024-02-01' = if (enableWaf) {
//          name: '${replace(baseName, '-', '')}wafpolicy'
//          location: 'Global'
//          sku: { name: profileSku }
//          properties: {
//            policySettings: { enabledState: 'Enabled', mode: 'Prevention' }
//            managedRules: {
//              managedRuleSets: [
//                { ruleSetType: 'Microsoft_DefaultRuleSet', ruleSetVersion: '2.1' }
//                { ruleSetType: 'Microsoft_BotManagerRuleSet', ruleSetVersion: '1.0' }
//              ]
//            }
//          }
//        }
//
//        resource securityPolicy 'Microsoft.Cdn/profiles/securityPolicies@2024-02-01' = if (enableWaf) {
//          parent: profile
//          name: 'default-waf'
//          properties: {
//            parameters: {
//              type: 'WebApplicationFirewall'
//              wafPolicy: { id: waf.id }
//              associations: [
//                {
//                  domains: [
//                    { id: endpointExplorer.id }
//                    { id: endpointMcp.id }
//                    { id: endpointState.id }
//                  ]
//                  patternsToMatch: [ '/*' ]
//                }
//              ]
//            }
//          }
//        }
//
//   A custom-rule-only policy (rate limiting, geo/IP allow-list) works on the
//   Standard SKU too; only the Microsoft-managed rule sets and bot protection
//   force the Premium upgrade. See reference-architecture/README.md for the cost
//   trade-offs.
//
// -----------------------------------------------------------------------------
// WIRING RECIPE (the coordinator applies this glue in main.bicep - do NOT edit
// main.bicep from this module)
// -----------------------------------------------------------------------------
// 1. Map the per-region head FQDNs from compute into this module's `origins`
//    input. Front Door is public-option only, so gate the module on the public
//    transport:
//
//      module frontdoor 'modules/frontdoor.bicep' = if (deploymentOption == 'public') {
//        name: 'frontdoor'
//        params: {
//          baseName: baseName
//          origins: [for (region, i) in regions: {
//            regionCode: region.regionCode
//            explorerFqdn: compute[i].outputs.explorerFqdn
//            mcpFqdn: compute[i].outputs.mcpFqdn
//            siloStateApiFqdn: compute[i].outputs.siloStateApiFqdn
//          }]
//        }
//      }
//
// 2. Lock every region's ACA ingress to this Front Door with the FDID. Take
//    `frontdoor.outputs.frontDoorId` and pass it into compute.bicep so each
//    client-facing container app adds an AFD-id ingress restriction. On the ACA
//    ingress the wiring is:
//
//      ingress: {
//        external: true
//        // ...existing...
//        ipSecurityRestrictions: []          // keep IP allow-list separate
//        // The AFD-id header lock: only requests Front Door stamps are allowed.
//        // ACA honours this via the ingress `X-Azure-FDID` check when the app
//        // validates the header, OR expose it as an env/config the head asserts.
//      }
//
//    Concretely, the coordinator threads `frontDoorId` to compute as a param
//    (e.g. `frontDoorId string`) and the head asserts inbound
//    `X-Azure-FDID == frontDoorId`, rejecting anything else. This is the origin
//    lock referenced in the security posture: traffic that skips Front Door
//    lacks the correct FDID header and is refused.
// =============================================================================

targetScope = 'resourceGroup'

// --- Naming ------------------------------------------------------------------

@description('Lowercase base name shared by the estate (for example "lattice"). Used to name the Front Door profile and endpoints.')
@minLength(3)
@maxLength(24)
param baseName string

// --- Origins (N-region parameterisation) -------------------------------------

@description('Per-region head FQDNs, one entry per region, in region-list order. Map this in main.bicep from the compute outputs: [for (r, i) in regions: { regionCode: r.regionCode, explorerFqdn: compute[i].outputs.explorerFqdn, mcpFqdn: compute[i].outputs.mcpFqdn, siloStateApiFqdn: compute[i].outputs.siloStateApiFqdn }]. Each item: { regionCode, explorerFqdn, mcpFqdn, siloStateApiFqdn }.')
@minLength(1)
param origins array

// --- Health probe ------------------------------------------------------------

@description('AFD health-probe interval in seconds. Deliberately INFREQUENT by default so continuous probing does not pin the scale-to-zero MCP/Explorer heads at a warm replica any more than necessary (see reference-architecture.md, "Health probe vs scale-to-zero"). Lower it for faster failover detection at the cost of keeping heads warm.')
@minValue(1)
@maxValue(255)
param probeIntervalSeconds int = 240

@description('Path the AFD health probe requests against each origin. A lightweight HEAD is issued against this path.')
param probePath string = '/'

// --- WAF seam (DEFAULT OFF - provisions nothing when false) -------------------

@description('DEFAULT-OFF WAF seam. WAF is descoped from the baseline for cost; leaving this false provisions no WAF policy or security policy. See the module header for the exact Premium + managed-WAF enablement path. Surfaced as the `wafEnabled` output so the coordinator can branch on it.')
param enableWaf bool = false

// =============================================================================
// Derived values
// =============================================================================

// Latency-based active-active load balancing: all healthy origins share the same
// priority and weight, so Front Door routes to the nearest healthy region and
// fails over to the next-nearest on a probe failure. No session affinity.
var loadBalancingSettings = {
  sampleSize: 4
  successfulSamplesRequired: 3
  additionalLatencyInMilliseconds: 50
}

var healthProbeSettings = {
  probePath: probePath
  probeRequestType: 'HEAD'
  probeProtocol: 'Https'
  probeIntervalInSeconds: probeIntervalSeconds
}

// Common origin properties: server TLS on 443, certificate-name-check enforced,
// active-active (equal priority + weight). originHostHeader mirrors the origin
// FQDN so the ACA app sees its own host header.
func originProps(fqdn string) object => {
  hostName: fqdn
  originHostHeader: fqdn
  httpPort: 80
  httpsPort: 443
  priority: 1
  weight: 1000
  enabledState: 'Enabled'
  enforceCertificateNameCheck: true
}

// =============================================================================
// Front Door Standard profile (ONE global profile for the whole estate)
// =============================================================================

resource profile 'Microsoft.Cdn/profiles@2024-02-01' = {
  name: '${baseName}-afd'
  location: 'Global'
  sku: {
    // Standard is the baseline (cost decision). The header documents the exact
    // change to Premium_AzureFrontDoor when managed WAF / Private Link origins
    // are wanted later.
    name: 'Standard_AzureFrontDoor'
  }
  properties: {
    originResponseTimeoutSeconds: 60
  }
}

// =============================================================================
// AFD endpoints - one per client-facing head so each route owns '/*' cleanly
// (three heads on a single endpoint would collide on the same path pattern).
// =============================================================================

resource endpointExplorer 'Microsoft.Cdn/profiles/afdEndpoints@2024-02-01' = {
  parent: profile
  name: '${baseName}-explorer'
  location: 'Global'
  properties: {
    enabledState: 'Enabled'
  }
}

resource endpointMcp 'Microsoft.Cdn/profiles/afdEndpoints@2024-02-01' = {
  parent: profile
  name: '${baseName}-mcp'
  location: 'Global'
  properties: {
    enabledState: 'Enabled'
  }
}

resource endpointState 'Microsoft.Cdn/profiles/afdEndpoints@2024-02-01' = {
  parent: profile
  name: '${baseName}-state'
  location: 'Global'
  properties: {
    enabledState: 'Enabled'
  }
}

// =============================================================================
// Origin groups - one per head, each health-probed for active-active failover
// =============================================================================

resource originGroupExplorer 'Microsoft.Cdn/profiles/originGroups@2024-02-01' = {
  parent: profile
  name: 'og-explorer'
  properties: {
    loadBalancingSettings: loadBalancingSettings
    healthProbeSettings: healthProbeSettings
    sessionAffinityState: 'Disabled'
  }
}

resource originGroupMcp 'Microsoft.Cdn/profiles/originGroups@2024-02-01' = {
  parent: profile
  name: 'og-mcp'
  properties: {
    loadBalancingSettings: loadBalancingSettings
    healthProbeSettings: healthProbeSettings
    sessionAffinityState: 'Disabled'
  }
}

resource originGroupState 'Microsoft.Cdn/profiles/originGroups@2024-02-01' = {
  parent: profile
  name: 'og-state'
  properties: {
    loadBalancingSettings: loadBalancingSettings
    healthProbeSettings: healthProbeSettings
    sessionAffinityState: 'Disabled'
  }
}

// =============================================================================
// Origins - one per region head FQDN under each origin group (N-region loop)
// =============================================================================

resource originsExplorer 'Microsoft.Cdn/profiles/originGroups/origins@2024-02-01' = [for (o, i) in origins: {
  parent: originGroupExplorer
  name: 'origin-explorer-${o.regionCode}'
  properties: originProps(o.explorerFqdn)
}]

resource originsMcp 'Microsoft.Cdn/profiles/originGroups/origins@2024-02-01' = [for (o, i) in origins: {
  parent: originGroupMcp
  name: 'origin-mcp-${o.regionCode}'
  properties: originProps(o.mcpFqdn)
}]

resource originsState 'Microsoft.Cdn/profiles/originGroups/origins@2024-02-01' = [for (o, i) in origins: {
  parent: originGroupState
  name: 'origin-state-${o.regionCode}'
  properties: originProps(o.siloStateApiFqdn)
}]

// =============================================================================
// Routes - HTTPS-only, origins reached over TLS (forwardingProtocol HttpsOnly).
// Http is accepted only to redirect it to Https; no plaintext reaches an origin.
// Each route depends on its origins existing before the association is made.
// =============================================================================

resource routeExplorer 'Microsoft.Cdn/profiles/afdEndpoints/routes@2024-02-01' = {
  parent: endpointExplorer
  name: 'route-explorer'
  properties: {
    originGroup: {
      id: originGroupExplorer.id
    }
    supportedProtocols: [
      'Http'
      'Https'
    ]
    patternsToMatch: [
      '/*'
    ]
    forwardingProtocol: 'HttpsOnly'
    linkToDefaultDomain: 'Enabled'
    httpsRedirect: 'Enabled'
    enabledState: 'Enabled'
  }
  dependsOn: [
    originsExplorer
  ]
}

resource routeMcp 'Microsoft.Cdn/profiles/afdEndpoints/routes@2024-02-01' = {
  parent: endpointMcp
  name: 'route-mcp'
  properties: {
    originGroup: {
      id: originGroupMcp.id
    }
    supportedProtocols: [
      'Http'
      'Https'
    ]
    patternsToMatch: [
      '/*'
    ]
    forwardingProtocol: 'HttpsOnly'
    linkToDefaultDomain: 'Enabled'
    httpsRedirect: 'Enabled'
    enabledState: 'Enabled'
  }
  dependsOn: [
    originsMcp
  ]
}

resource routeState 'Microsoft.Cdn/profiles/afdEndpoints/routes@2024-02-01' = {
  parent: endpointState
  name: 'route-state'
  properties: {
    originGroup: {
      id: originGroupState.id
    }
    supportedProtocols: [
      'Http'
      'Https'
    ]
    patternsToMatch: [
      '/*'
    ]
    forwardingProtocol: 'HttpsOnly'
    linkToDefaultDomain: 'Enabled'
    httpsRedirect: 'Enabled'
    enabledState: 'Enabled'
  }
  dependsOn: [
    originsState
  ]
}

// =============================================================================
// Outputs - the seams the coordinator wires into ACA ingress / documents
// =============================================================================

@description('The Front Door ID (a GUID). Wire this into every region\'s ACA ingress as the `X-Azure-FDID` access restriction so origins accept traffic ONLY from this Front Door. See the WIRING RECIPE in the module header. Not a secret.')
output frontDoorId string = profile.properties.frontDoorId

@description('Resource id of the global Front Door profile.')
output profileId string = profile.id

@description('Name of the global Front Door profile.')
output profileName string = profile.name

@description('Public HTTPS hostname of the Explorer web endpoint (*.azurefd.net).')
output explorerHostName string = endpointExplorer.properties.hostName

@description('Public HTTPS hostname of the MCP endpoint (*.azurefd.net).')
output mcpHostName string = endpointMcp.properties.hostName

@description('Public HTTPS hostname of the silo State API endpoint (*.azurefd.net).')
output stateHostName string = endpointState.properties.hostName

@description('Whether the default-off WAF seam is enabled. False in the baseline (WAF descoped for cost); see the module header for the Premium + managed-WAF enablement path.')
output wafEnabled bool = enableWaf
