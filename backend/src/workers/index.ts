import { Job } from "bullmq";
import { JobQueue } from "./queue.js";
import { processPriceCollection } from "./priceCollection.job.js";
import { processHealthCalculation } from "./healthCalculation.job.js";
import { processBridgeVerification } from "./bridgeVerification.job.js";
import { processAnalyticsAggregation } from "./analyticsAggregation.worker.js";
import { processMetricsRollup } from "./metricsRollup.worker.js";
import { processDigestScheduler } from "./digestScheduler.worker.js";
import { processMetadataSync } from "./metadataSync.job.js";
import { processExternalDependencyMonitor } from "./externalDependencyMonitor.job.js";
import { logger } from "../utils/logger.js";
import { initSupplyVerificationJob } from "../jobs/supplyVerification.job.js";
import { runAuditRetentionJob } from "../jobs/auditRetention.job.js";
import { runPriceCacheWarmup } from "../jobs/priceCacheWarmup.job.js";

export async function initJobSystem() {
  const jobQueue = JobQueue.getInstance();

  // Run price cache warmup on startup
  try {
    logger.info("Running startup price cache warmup");
    await runPriceCacheWarmup();
  } catch (error) {
    logger.error({ error }, "Startup price cache warmup failed, continuing with job initialization");
  }

  // Initialize worker with processor
  jobQueue.initWorker(async (job: Job) => {
    switch (job.name) {
      case "price-collection":
        await processPriceCollection(job);
        break;
      case "health-calculation":
        await processHealthCalculation(job);
        break;
      case "bridge-verification":
        await processBridgeVerification(job);
        break;
      case "analytics-aggregation":
        await processAnalyticsAggregation(job);
        break;
      case "metrics-rollup":
        await processMetricsRollup(job);
        break;
      case "audit-retention":
        await runAuditRetentionJob(job.data.retentionDays);
        break;
      case "digest-scheduler-daily":
        await processDigestScheduler(job);
        break;
      case "digest-scheduler-weekly":
        await processDigestScheduler(job);
        break;
      case "metadata-sync":
        await processMetadataSync(job);
        break;
      case "external-dependency-monitor":
        await processExternalDependencyMonitor(job);
        break;
      default:
        logger.warn({ jobName: job.name }, "Unknown job name in worker");
    }
  });

  // Initialize supply verification job system (dedicated queue and worker)
  await initSupplyVerificationJob();

  // Schedule repeatable jobs
  // price-collection: every 30 seconds
  await jobQueue.addRepeatableJob("price-collection", {}, "*/30 * * * * *");
  
  // health-calculation: every 5 minutes
  await jobQueue.addRepeatableJob("health-calculation", {}, "*/5 * * * *");
  
  // bridge-verification: every 5 minutes
  await jobQueue.addRepeatableJob("bridge-verification", {}, "*/5 * * * *");

  // Analytics aggregation jobs
  // Protocol stats: every 2 minutes
  await jobQueue.addRepeatableJob("analytics-aggregation", { type: "protocol-stats" }, "*/2 * * * *");
  
  // Bridge comparisons: every 3 minutes
  await jobQueue.addRepeatableJob("analytics-aggregation", { type: "bridge-comparisons" }, "*/3 * * * *");
  
  // Asset rankings: every 3 minutes
  await jobQueue.addRepeatableJob("analytics-aggregation", { type: "asset-rankings" }, "*/3 * * * *");
  
  // Volume aggregations: every 5 minutes
  await jobQueue.addRepeatableJob("analytics-aggregation", { 
    type: "volume-aggregation",
    params: { period: "hourly" }
  }, "*/5 * * * *");
  
  await jobQueue.addRepeatableJob("analytics-aggregation", { 
    type: "volume-aggregation",
    params: { period: "daily" }
  }, "*/5 * * * *");
  
  // Top performers: every 5 minutes
  await jobQueue.addRepeatableJob("analytics-aggregation", { 
    type: "top-performers",
    params: { performerType: "assets", metric: "health", limit: 10 }
  }, "*/5 * * * *");
  
  await jobQueue.addRepeatableJob("analytics-aggregation", { 
    type: "top-performers",
    params: { performerType: "bridges", metric: "tvl", limit: 10 }
  }, "*/5 * * * *");

  // Metrics rollup: every 15 minutes to keep daily stats fresh
  await jobQueue.addRepeatableJob("metrics-rollup", { type: "bridge-volume" }, "*/15 * * * *");

  // Audit log retention: daily at 02:00 UTC, keep 90 days of info-level entries
  await jobQueue.addRepeatableJob("audit-retention", { retentionDays: 90 }, "0 2 * * *");

  // Digest scheduler jobs
  // Daily digest: every hour (service will check user preferences and timezone)
  await jobQueue.addRepeatableJob("digest-scheduler-daily", { digestType: "daily" }, "0 * * * *");
  
  // Weekly digest: every hour on Monday (service will check user preferences)
  await jobQueue.addRepeatableJob("digest-scheduler-weekly", { digestType: "weekly" }, "0 * * * 1");

  // Metadata sync: every 4 hours
  await jobQueue.addRepeatableJob("metadata-sync", {}, "0 */4 * * *");

  // External dependency checks: every 2 minutes
  await jobQueue.addRepeatableJob("external-dependency-monitor", {}, "*/2 * * * *");

  logger.info("Scheduled job system initialized");
}
