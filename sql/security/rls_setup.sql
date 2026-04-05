-- ============================================================================
-- Row-Level Security Setup
-- Functions + User Access Control Table
-- ============================================================================

-- User Access Control Table
CREATE TABLE community_health_intelligence.gold.user_access_control (
  user_email STRING NOT NULL COMMENT 'Databricks user email (matches CURRENT_USER())',
  access_level STRING NOT NULL COMMENT 'ADMIN, COUNTY, or SUBCOUNTY',
  county_name STRING COMMENT 'Assigned county (NULL for ADMIN)',
  sub_county_name STRING COMMENT 'Assigned sub-county (NULL for ADMIN/COUNTY level)',
  created_at TIMESTAMP,
  updated_at TIMESTAMP)
USING delta
COMMENT 'Maps users to their geographic data access scope for row-level security'
COLLATION 'UTF8_BINARY'
TBLPROPERTIES (
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.appendOnly' = 'supported',
  'delta.feature.deletionVectors' = 'supported',
  'delta.feature.domainMetadata' = 'supported',
  'delta.feature.invariants' = 'supported',
  'delta.feature.rowTracking' = 'supported',
  'delta.minReaderVersion' = '3',
  'delta.minWriterVersion' = '7',
  'delta.parquet.compression.codec' = 'zstd',
  'purpose' = 'row-level-security',
  'quality' = 'gold')
;

-- Function: county_access_filter
-- Function:      community_health_intelligence.gold.county_access_filter
-- Type:          SCALAR
-- Input:         county STRING
-- Returns:       BOOLEAN
-- Collation:     UTF8_BINARY
-- Deterministic: true
-- Data Access:   READS SQL DATA
-- Configs:       spark.connect.session.connectML.enabled=true
--                spark.connect.session.connectML.mlCache.memoryControl.maxModelSize=268435456
--                spark.connect.session.planCompression.threshold=10485760
--                spark.databricks.ai.adaptiveBatch.initialSize=4
--                spark.databricks.ai.adaptiveBatch.maxRetries=100
--                spark.databricks.ai.adaptiveBatch.partitionJitterEnabled=true
--                spark.databricks.ai.adaptiveBatch.retriableInvokeStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED
--                spark.databricks.ai.adaptiveBatch.retryMaxDelayMs=120000
--                spark.databricks.ai.grpc.sync.retriableStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED
--                spark.databricks.docingest.batchEval.size=8
--                spark.databricks.docingest.contentElementTypes.excluded=
--                spark.databricks.docingest.maxDocumentPages=2000
--                spark.databricks.docingest.pagination.concurrency=1
--                spark.databricks.docingest.stage2.descriptionModel=databricks-gemma-3-12b
--                spark.databricks.docingest.version.default=2.0
--                spark.databricks.docingest.version.supported=2.0
--                spark.databricks.sql.ai.partitionProcessor.aimdLimiter.enabled=true
--                spark.databricks.sql.ai.partitionProcessor.aimdLimiter.max=200
--                spark.databricks.sql.ai.partitionProcessor.globalMaxInFlightElements=10240
--                spark.databricks.sql.ai.partitionProcessor.maxBufferedResults=512
--                spark.databricks.sql.ai.partitionProcessor.maxInFlightElements=200
--                spark.databricks.sql.ai.partitionProcessor.maxWaitTimeSeconds=172800
--                spark.databricks.sql.ai.remoteFunction.grpc.initialRetryInterval=100
--                spark.databricks.sql.ai.remoteFunction.grpc.maxRetryInterval=10000
--                spark.databricks.sql.ai.remoteFunction.grpc.retryIntervalJitterFactor=1.0
--                spark.databricks.sql.expression.aiFunctions.repartition=0
--                spark.databricks.sql.functions.aiForecast.enabled=false
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.clusterSizeBasedGlobalParallelism.scaleFactor=512.0
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.debugLogEnabled=true
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.dynamicPoolSizeEnabled=false
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.enabled=true
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize=2048
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize.aiParseDocument=64
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdCurrentQpsIncreaseRatio=0.0
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdSuccessRatio=0.95
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdTotalQpsIncreaseRatio=0.0
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.taskWaitTimeInSeconds=1000
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.threadKeepAliveTimeInSeconds=600
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.useDynamicTaskQueueExecutor=false
--                spark.databricks.sql.functions.aiFunctions.batch.aiQuery.embedding.request.size=40
--                spark.databricks.sql.functions.aiFunctions.batch.execution.size=2048
--                spark.databricks.sql.functions.aiFunctions.batchInferenceApi.enabled=true
--                spark.databricks.sql.functions.aiFunctions.batchSession.enabled=true
--                spark.databricks.sql.functions.aiFunctions.batchSession.terminate.enabled=true
--                spark.databricks.sql.functions.aiFunctions.createBatchSessionOnExecutor=false
--                spark.databricks.sql.functions.aiFunctions.decimal.dataType.enabled=true
--                spark.databricks.sql.functions.aiFunctions.embeddingsEndpointName=databricks-gte-large-en
--                spark.databricks.sql.functions.aiFunctions.extract.tileMetadata.enabled=true
--                spark.databricks.sql.functions.aiFunctions.grpc.enabled=true
--                spark.databricks.sql.functions.aiFunctions.hermesEnabledFunctions=ai_extract,ai_classify
--                spark.databricks.sql.functions.aiFunctions.includeWorkspaceUrl=false
--                spark.databricks.sql.functions.aiFunctions.model.parameters.enabled=true
--                spark.databricks.sql.functions.aiFunctions.modelEndpointTypeParsing.enabled=true
--                spark.databricks.sql.functions.aiFunctions.multiModality.model.list=databricks-llama-4-maverick,databricks-claude-sonnet-4,databricks-claude-3-7-sonnet,databricks-gemma-3-12b,databricks-gpt-5,databricks-gpt-5-mini,databricks-gpt-5-nano,databricks-gpt-5-1,databricks-gpt-5-2,databricks-gemini-2-5-flash,databricks-gemini-2-5-pro,databricks-gemini-3-flash,databricks-gemini-3-pro,databricks-claude-haiku-4-5
--                spark.databricks.sql.functions.aiFunctions.nullInputJsonOutput={"response":null,"error_message":null}
--                spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.batch.execution.enabled=true
--                spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.translate.instruction=### Instruction

Translate the provided text to %s, and output only the translated text
in the target language in this format: <DBSQLAI>translated text</DBSQLAI>.

If the text is already in the target language, output the provided text.

### Text

%s
--                spark.databricks.sql.functions.aiFunctions.registry.enabled=false
--                spark.databricks.sql.functions.aiFunctions.remoteFunction.grpc.batch.maxConcurrentRequests=2048
--                spark.databricks.sql.functions.aiFunctions.remoteHttpClient.maxConnections=2048
--                spark.databricks.sql.functions.aiFunctions.remoteHttpClient.timeoutInSeconds=360
--                spark.databricks.sql.functions.aiFunctions.safe.inference.enabled=true
--                spark.databricks.sql.functions.aiFunctions.useDedicatedHttpClient=true
--                spark.databricks.sql.functions.aiFunctions.useGrpcForSessionManagement=true
--                spark.databricks.sql.functions.aiFunctions.vectorSearch.initialRetryIntervalMillis=1000
--                spark.databricks.sql.functions.aiFunctions.vectorSearch.maxRetryInterval=120000
--                spark.databricks.sql.functions.aiFunctions.vectorSearch.retryIntervalMultiplierFactor=2.0
--                spark.databricks.sql.functions.aiGen.endpointName=databricks-meta-llama-3-3-70b-instruct
--                spark.databricks.sql.functions.aiQuery.hybrid.reasoning.foundation.model.list=databricks-claude-3-7-sonnet,databricks-gemini-2-5-pro,databricks-gemini-2-5-flash,databricks-gemini-3-flash
--                spark.databricks.sql.functions.aiQuery.openAI.oSeries.model.list=o1-mini,o1,o3-mini,o3,o4-mini,gpt-5,gpt-5-mini,gpt-5-nano,GPT-5,GPT-5 Mini,GPT-5 Nano
--                spark.databricks.sql.functions.aiQuery.temperature.whitelistMode.enabled=true
--                spark.databricks.sql.functions.vectorSearch.enabled=true
--                spark.databricks.sql.functions.vectorSearch.use.indexIdentifierOnly=true
--                spark.databricks.sql.rowColumnAccess.useEffectivePolicies.enabled=true
--                spark.sql.analyzer.dontDeduplicateExpressionIfExprIdInOutput=false
--                spark.sql.ansi.enabled=true
--                spark.sql.artifact.isolation.enabled=true
--                spark.sql.copyInto.timeout.threadDump.enabled=false
--                spark.sql.cte.recursion.enabled=true
--                spark.sql.defaultStorage.useDefaultStorageForCreateCatalogWithoutLocation.enabled=true
--                spark.sql.excel.streaming.enabled=true
--                spark.sql.functions.remoteHttpClient.retryOn400TimeoutError=true
--                spark.sql.functions.remoteHttpClient.retryOnSocketTimeoutException=true
--                spark.sql.hive.convertCTAS=true
--                spark.sql.insertIntoReplaceUsing.disallowMisalignedColumns.enabled=true
--                spark.sql.insertIntoReplaceUsing.oldDPOCodePath.enabled=true
--                spark.sql.legacy.codingErrorAction=true
--                spark.sql.legacy.createHiveTableByDefault=false
--                spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled=false
--                spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled=false
--                spark.sql.legacy.useLegacyXMLParser.migrationValidation.enabled=false
--                spark.sql.orc.compression.codec=snappy
--                spark.sql.parquet.compression.codec=snappy
--                spark.sql.readSideCharPadding=true
--                spark.sql.scripting.enabled=true
--                spark.sql.session.timeZone=Etc/UTC
--                spark.sql.shuffleDependency.skipMigration.enabled=true
--                spark.sql.skipIndexForTextSearch.enabled=false
--                spark.sql.skipIndexForTextSearch.ngramIndex.parameterCalculation=true
--                spark.sql.sources.commitProtocolClass=com.databricks.sql.transaction.directory.DirectoryAtomicCommitProtocol
--                spark.sql.sources.default=delta
--                spark.sql.stableDerivedColumnAlias.enabled=true
--                spark.sql.streaming.stateStore.providerClass=com.databricks.sql.streaming.state.RocksDBStateStoreProvider
--                spark.sql.streaming.statefulOperator.stateRebalancing.enabled=false
--                spark.sql.streaming.stopTimeout=15s
--                spark.sql.validateColumnNamesSync.throwException=true
--                spark.sql.variable.substitute=false
-- Owner:         keyegonaws@gmail.com
-- Create Time:   Sun Apr 05 12:02:37 UTC 2026
-- Body:          IF(
    EXISTS(
      SELECT 1 FROM community_health_intelligence.gold.user_access_control 
      WHERE user_email = CURRENT_USER() AND access_level = 'ADMIN'
    ),
    TRUE,
    EXISTS(
      SELECT 1 FROM community_health_intelligence.gold.user_access_control 
      WHERE user_email = CURRENT_USER() AND county_name = county
    )
  )

-- Function: subcounty_access_filter
-- Function:      community_health_intelligence.gold.subcounty_access_filter
-- Type:          SCALAR
-- Input:         county     STRING
--                sub_county STRING
-- Returns:       BOOLEAN
-- Collation:     UTF8_BINARY
-- Deterministic: true
-- Data Access:   READS SQL DATA
-- Configs:       spark.connect.session.connectML.enabled=true
--                spark.connect.session.connectML.mlCache.memoryControl.maxModelSize=268435456
--                spark.connect.session.planCompression.threshold=10485760
--                spark.databricks.ai.adaptiveBatch.initialSize=4
--                spark.databricks.ai.adaptiveBatch.maxRetries=100
--                spark.databricks.ai.adaptiveBatch.partitionJitterEnabled=true
--                spark.databricks.ai.adaptiveBatch.retriableInvokeStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED
--                spark.databricks.ai.adaptiveBatch.retryMaxDelayMs=120000
--                spark.databricks.ai.grpc.sync.retriableStatusCodes=UNAVAILABLE,RESOURCE_EXHAUSTED,DEADLINE_EXCEEDED,ABORTED,CANCELLED
--                spark.databricks.docingest.batchEval.size=8
--                spark.databricks.docingest.contentElementTypes.excluded=
--                spark.databricks.docingest.maxDocumentPages=2000
--                spark.databricks.docingest.pagination.concurrency=1
--                spark.databricks.docingest.stage2.descriptionModel=databricks-gemma-3-12b
--                spark.databricks.docingest.version.default=2.0
--                spark.databricks.docingest.version.supported=2.0
--                spark.databricks.sql.ai.partitionProcessor.aimdLimiter.enabled=true
--                spark.databricks.sql.ai.partitionProcessor.aimdLimiter.max=200
--                spark.databricks.sql.ai.partitionProcessor.globalMaxInFlightElements=10240
--                spark.databricks.sql.ai.partitionProcessor.maxBufferedResults=512
--                spark.databricks.sql.ai.partitionProcessor.maxInFlightElements=200
--                spark.databricks.sql.ai.partitionProcessor.maxWaitTimeSeconds=172800
--                spark.databricks.sql.ai.remoteFunction.grpc.initialRetryInterval=100
--                spark.databricks.sql.ai.remoteFunction.grpc.maxRetryInterval=10000
--                spark.databricks.sql.ai.remoteFunction.grpc.retryIntervalJitterFactor=1.0
--                spark.databricks.sql.expression.aiFunctions.repartition=0
--                spark.databricks.sql.functions.aiForecast.enabled=false
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.clusterSizeBasedGlobalParallelism.scaleFactor=512.0
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.debugLogEnabled=true
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.dynamicPoolSizeEnabled=false
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.enabled=true
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize=2048
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.maxPoolSize.aiParseDocument=64
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdCurrentQpsIncreaseRatio=0.0
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdSuccessRatio=0.95
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.scaleUpThresholdTotalQpsIncreaseRatio=0.0
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.taskWaitTimeInSeconds=1000
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.threadKeepAliveTimeInSeconds=600
--                spark.databricks.sql.functions.aiFunctions.adaptiveThreadPool.useDynamicTaskQueueExecutor=false
--                spark.databricks.sql.functions.aiFunctions.batch.aiQuery.embedding.request.size=40
--                spark.databricks.sql.functions.aiFunctions.batch.execution.size=2048
--                spark.databricks.sql.functions.aiFunctions.batchInferenceApi.enabled=true
--                spark.databricks.sql.functions.aiFunctions.batchSession.enabled=true
--                spark.databricks.sql.functions.aiFunctions.batchSession.terminate.enabled=true
--                spark.databricks.sql.functions.aiFunctions.createBatchSessionOnExecutor=false
--                spark.databricks.sql.functions.aiFunctions.decimal.dataType.enabled=true
--                spark.databricks.sql.functions.aiFunctions.embeddingsEndpointName=databricks-gte-large-en
--                spark.databricks.sql.functions.aiFunctions.extract.tileMetadata.enabled=true
--                spark.databricks.sql.functions.aiFunctions.grpc.enabled=true
--                spark.databricks.sql.functions.aiFunctions.hermesEnabledFunctions=ai_extract,ai_classify
--                spark.databricks.sql.functions.aiFunctions.includeWorkspaceUrl=false
--                spark.databricks.sql.functions.aiFunctions.model.parameters.enabled=true
--                spark.databricks.sql.functions.aiFunctions.modelEndpointTypeParsing.enabled=true
--                spark.databricks.sql.functions.aiFunctions.multiModality.model.list=databricks-llama-4-maverick,databricks-claude-sonnet-4,databricks-claude-3-7-sonnet,databricks-gemma-3-12b,databricks-gpt-5,databricks-gpt-5-mini,databricks-gpt-5-nano,databricks-gpt-5-1,databricks-gpt-5-2,databricks-gemini-2-5-flash,databricks-gemini-2-5-pro,databricks-gemini-3-flash,databricks-gemini-3-pro,databricks-claude-haiku-4-5
--                spark.databricks.sql.functions.aiFunctions.nullInputJsonOutput={"response":null,"error_message":null}
--                spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.batch.execution.enabled=true
--                spark.databricks.sql.functions.aiFunctions.purposeBuiltFunctions.translate.instruction=### Instruction

Translate the provided text to %s, and output only the translated text
in the target language in this format: <DBSQLAI>translated text</DBSQLAI>.

If the text is already in the target language, output the provided text.

### Text

%s
--                spark.databricks.sql.functions.aiFunctions.registry.enabled=false
--                spark.databricks.sql.functions.aiFunctions.remoteFunction.grpc.batch.maxConcurrentRequests=2048
--                spark.databricks.sql.functions.aiFunctions.remoteHttpClient.maxConnections=2048
--                spark.databricks.sql.functions.aiFunctions.remoteHttpClient.timeoutInSeconds=360
--                spark.databricks.sql.functions.aiFunctions.safe.inference.enabled=true
--                spark.databricks.sql.functions.aiFunctions.useDedicatedHttpClient=true
--                spark.databricks.sql.functions.aiFunctions.useGrpcForSessionManagement=true
--                spark.databricks.sql.functions.aiFunctions.vectorSearch.initialRetryIntervalMillis=1000
--                spark.databricks.sql.functions.aiFunctions.vectorSearch.maxRetryInterval=120000
--                spark.databricks.sql.functions.aiFunctions.vectorSearch.retryIntervalMultiplierFactor=2.0
--                spark.databricks.sql.functions.aiGen.endpointName=databricks-meta-llama-3-3-70b-instruct
--                spark.databricks.sql.functions.aiQuery.hybrid.reasoning.foundation.model.list=databricks-claude-3-7-sonnet,databricks-gemini-2-5-pro,databricks-gemini-2-5-flash,databricks-gemini-3-flash
--                spark.databricks.sql.functions.aiQuery.openAI.oSeries.model.list=o1-mini,o1,o3-mini,o3,o4-mini,gpt-5,gpt-5-mini,gpt-5-nano,GPT-5,GPT-5 Mini,GPT-5 Nano
--                spark.databricks.sql.functions.aiQuery.temperature.whitelistMode.enabled=true
--                spark.databricks.sql.functions.vectorSearch.enabled=true
--                spark.databricks.sql.functions.vectorSearch.use.indexIdentifierOnly=true
--                spark.databricks.sql.rowColumnAccess.useEffectivePolicies.enabled=true
--                spark.sql.analyzer.dontDeduplicateExpressionIfExprIdInOutput=false
--                spark.sql.ansi.enabled=true
--                spark.sql.artifact.isolation.enabled=true
--                spark.sql.copyInto.timeout.threadDump.enabled=false
--                spark.sql.cte.recursion.enabled=true
--                spark.sql.defaultStorage.useDefaultStorageForCreateCatalogWithoutLocation.enabled=true
--                spark.sql.excel.streaming.enabled=true
--                spark.sql.functions.remoteHttpClient.retryOn400TimeoutError=true
--                spark.sql.functions.remoteHttpClient.retryOnSocketTimeoutException=true
--                spark.sql.hive.convertCTAS=true
--                spark.sql.insertIntoReplaceUsing.disallowMisalignedColumns.enabled=true
--                spark.sql.insertIntoReplaceUsing.oldDPOCodePath.enabled=true
--                spark.sql.legacy.codingErrorAction=true
--                spark.sql.legacy.createHiveTableByDefault=false
--                spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled=false
--                spark.sql.legacy.execution.pythonUDTF.pandas.conversion.enabled=false
--                spark.sql.legacy.useLegacyXMLParser.migrationValidation.enabled=false
--                spark.sql.orc.compression.codec=snappy
--                spark.sql.parquet.compression.codec=snappy
--                spark.sql.readSideCharPadding=true
--                spark.sql.scripting.enabled=true
--                spark.sql.session.timeZone=Etc/UTC
--                spark.sql.shuffleDependency.skipMigration.enabled=true
--                spark.sql.skipIndexForTextSearch.enabled=false
--                spark.sql.skipIndexForTextSearch.ngramIndex.parameterCalculation=true
--                spark.sql.sources.commitProtocolClass=com.databricks.sql.transaction.directory.DirectoryAtomicCommitProtocol
--                spark.sql.sources.default=delta
--                spark.sql.stableDerivedColumnAlias.enabled=true
--                spark.sql.streaming.stateStore.providerClass=com.databricks.sql.streaming.state.RocksDBStateStoreProvider
--                spark.sql.streaming.statefulOperator.stateRebalancing.enabled=false
--                spark.sql.streaming.stopTimeout=15s
--                spark.sql.validateColumnNamesSync.throwException=true
--                spark.sql.variable.substitute=false
-- Owner:         keyegonaws@gmail.com
-- Create Time:   Sun Apr 05 12:03:47 UTC 2026
-- Body:          IF(
    EXISTS(
      SELECT 1 FROM community_health_intelligence.gold.user_access_control
      WHERE user_email = CURRENT_USER() AND access_level = 'ADMIN'
    ),
    TRUE,
    IF(
      EXISTS(
        SELECT 1 FROM community_health_intelligence.gold.user_access_control
        WHERE user_email = CURRENT_USER() AND access_level = 'COUNTY' AND county_name = county
      ),
      TRUE,
      EXISTS(
        SELECT 1 FROM community_health_intelligence.gold.user_access_control
        WHERE user_email = CURRENT_USER() AND access_level = 'SUBCOUNTY'
          AND county_name = county AND sub_county_name = sub_county
      )
    )
  )


-- ============================================================================
-- Row Filter Application (11 tables)
-- ============================================================================

ALTER TABLE community_health_intelligence.gold.dim_chw
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_name, sub_county_name);

ALTER TABLE community_health_intelligence.gold.dim_facility
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_name, sub_county_name);

ALTER TABLE community_health_intelligence.gold.dim_geography
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_name, sub_county_name);

ALTER TABLE community_health_intelligence.gold.fact_family_planning
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_home_visit
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_immunization
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_pnc
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_population
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_pregnancy
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_pregnancy_journey
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);

ALTER TABLE community_health_intelligence.gold.fact_supervision
  SET ROW FILTER community_health_intelligence.gold.subcounty_access_filter ON (county_clean, sub_county_clean);
