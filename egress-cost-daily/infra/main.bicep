// 部署 Python Function App（消费计划）用于每日出网费用采集。
// 包含：存储账户、Application Insights、Linux 消费计划、Function App（系统托管标识）。
@description('资源部署区域')
param location string = resourceGroup().location

@description('Function App 名称（全局唯一）')
param functionAppName string

@description('目标订阅 ID，用于查询成本与触发 Action Group')
param targetSubscriptionId string = subscription().subscriptionId

@description('Action Group 资源 ID（可选，留空则不走 Action Group）')
param actionGroupId string = ''

@description('计费类别过滤（默认 Bandwidth 捕获出网流量）')
param meterCategory string = 'Bandwidth'

@description('Timer 触发的 CRON（NCRONTAB，默认每天 UTC 02:00）')
param schedule string = '0 0 2 * * *'

@description('ACS 数据存放位置（Email 与 Communication 资源一致）')
param dataLocation string = 'United States'

@description('邮件收件人，多个用逗号分隔')
param emailRecipients string

var storageName = toLower('st${uniqueString(resourceGroup().id, functionAppName)}')

// ---------- Azure Communication Services（直接发邮件）----------
resource emailService 'Microsoft.Communication/emailServices@2023-04-01' = {
  name: '${functionAppName}-email'
  location: 'global'
  properties: {
    dataLocation: dataLocation
  }
}

resource emailDomain 'Microsoft.Communication/emailServices/domains@2023-04-01' = {
  parent: emailService
  name: 'AzureManagedDomain'
  location: 'global'
  properties: {
    domainManagement: 'AzureManaged'
    userEngagementTracking: 'Disabled'
  }
}

resource acs 'Microsoft.Communication/communicationServices@2023-04-01' = {
  name: '${functionAppName}-acs'
  location: 'global'
  properties: {
    dataLocation: dataLocation
    linkedDomains: [
      emailDomain.id
    ]
  }
}

var senderAddress = 'DoNotReply@${emailDomain.properties.fromSenderDomain}'

resource storage 'Microsoft.Storage/storageAccounts@2023-01-01' = {
  name: storageName
  location: location
  sku: {
    name: 'Standard_LRS'
  }
  kind: 'StorageV2'
  properties: {
    minimumTlsVersion: 'TLS1_2'
    allowBlobPublicAccess: false
  }
}

resource appInsights 'Microsoft.Insights/components@2020-02-02' = {
  name: '${functionAppName}-ai'
  location: location
  kind: 'web'
  properties: {
    Application_Type: 'web'
  }
}

resource plan 'Microsoft.Web/serverfarms@2023-12-01' = {
  name: '${functionAppName}-plan'
  location: location
  sku: {
    name: 'Y1'
    tier: 'Dynamic'
  }
  kind: 'linux'
  properties: {
    reserved: true
  }
}

resource functionApp 'Microsoft.Web/sites@2023-12-01' = {
  name: functionAppName
  location: location
  kind: 'functionapp,linux'
  identity: {
    type: 'SystemAssigned'
  }
  properties: {
    serverFarmId: plan.id
    httpsOnly: true
    siteConfig: {
      linuxFxVersion: 'Python|3.11'
      ftpsState: 'Disabled'
      appSettings: [
        {
          name: 'AzureWebJobsStorage'
          value: 'DefaultEndpointsProtocol=https;AccountName=${storage.name};AccountKey=${storage.listKeys().keys[0].value};EndpointSuffix=${environment().suffixes.storage}'
        }
        {
          name: 'FUNCTIONS_EXTENSION_VERSION'
          value: '~4'
        }
        {
          name: 'FUNCTIONS_WORKER_RUNTIME'
          value: 'python'
        }
        {
          name: 'AzureWebJobsFeatureFlags'
          value: 'EnableWorkerIndexing'
        }
        {
          name: 'SCM_DO_BUILD_DURING_DEPLOYMENT'
          value: 'true'
        }
        {
          name: 'ENABLE_ORYX_BUILD'
          value: 'true'
        }
        {
          name: 'APPLICATIONINSIGHTS_CONNECTION_STRING'
          value: appInsights.properties.ConnectionString
        }
        {
          name: 'SUBSCRIPTION_ID'
          value: targetSubscriptionId
        }
        {
          name: 'ACTION_GROUP_ID'
          value: actionGroupId
        }
        {
          name: 'ACS_CONNECTION_STRING'
          value: acs.listKeys().primaryConnectionString
        }
        {
          name: 'EMAIL_SENDER'
          value: senderAddress
        }
        {
          name: 'EMAIL_RECIPIENTS'
          value: emailRecipients
        }
        {
          name: 'METER_CATEGORY'
          value: meterCategory
        }
        {
          name: 'SCHEDULE'
          value: schedule
        }
      ]
    }
  }
}

output functionAppName string = functionApp.name
output principalId string = functionApp.identity.principalId
output emailSender string = senderAddress
