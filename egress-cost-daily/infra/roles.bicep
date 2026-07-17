// 在订阅级别为 Function App 托管标识授予所需角色：
//  - Cost Management Reader：查询成本数据
//  - Monitoring Contributor：触发 Action Group 测试通知
// 部署范围：subscription
targetScope = 'subscription'

@description('Function App 系统托管标识的 principalId')
param principalId string

var costReaderRoleId = '72fafb9e-0641-4937-9268-a91bfd8191a3' // Cost Management Reader
var monitoringContributorRoleId = '749f88d5-cbae-40b8-bcfc-e573ddc772fa' // Monitoring Contributor

resource costReader 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(subscription().id, principalId, costReaderRoleId)
  properties: {
    principalId: principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', costReaderRoleId)
  }
}

resource monitoringContributor 'Microsoft.Authorization/roleAssignments@2022-04-01' = {
  name: guid(subscription().id, principalId, monitoringContributorRoleId)
  properties: {
    principalId: principalId
    principalType: 'ServicePrincipal'
    roleDefinitionId: subscriptionResourceId('Microsoft.Authorization/roleDefinitions', monitoringContributorRoleId)
  }
}
