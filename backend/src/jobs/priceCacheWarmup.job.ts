import { PriceService } from "../services/price.service.js";
import { SUPPORTED_ASSETS } from "../config/index.js";
import { logger } from "../utils/logger.js";
import { CacheService, CacheTTL } from "../utils/cache.js";
import { getMetricsService } from "../services/metrics.service.js";

/**
 * Price Cache Warmup Job
 * Warms the price cache on startup and on a scheduled basis to ensure
 * the UI has low-latency access to fresh price values.
 */

export interface PriceCacheWarmupMetrics {
  totalAssets: number;
  successfulWarmups: number;
  failedWarmups: number;
  stalePrices: number;
  duration: number;
  timestamp: Date;
}

export interface PriceCacheWarmupConfig {
  enabled: boolean;
  ttl: number; // Cache TTL in seconds
  stalePriceThreshold: number; // Age threshold in seconds to consider a price stale
  maxRetries: number;
  retryDelayMs: number;
  sourceFailoverEnabled: boolean;
}

class PriceCacheWarmupService {
  private config: PriceCacheWarmupConfig;
  private priceService: PriceService;
  private metricsService = getMetricsService();
  private lastWarmupTime: Date | null = null;
  private warmupInProgress = false;

  constructor(config?: Partial<PriceCacheWarmupConfig>) {
    this.config = {
      enabled: true,
      ttl: CacheTTL.PRICES,
      stalePriceThreshold: 300, // 5 minutes
      maxRetries: 3,
      retryDelayMs: 1000,
      sourceFailoverEnabled: true,
      ...config,
    };
    this.priceService = new PriceService();
  }

  /**
   * Warm up the price cache for all supported assets
   */
  async warmupCache(): Promise<PriceCacheWarmupMetrics> {
    if (!this.config.enabled) {
      logger.info("Price cache warmup is disabled");
      return {
        totalAssets: 0,
        successfulWarmups: 0,
        failedWarmups: 0,
        stalePrices: 0,
        duration: 0,
        timestamp: new Date(),
      };
    }

    if (this.warmupInProgress) {
      logger.warn("Price cache warmup already in progress, skipping");
      return {
        totalAssets: 0,
        successfulWarmups: 0,
        failedWarmups: 0,
        stalePrices: 0,
        duration: 0,
        timestamp: new Date(),
      };
    }

    this.warmupInProgress = true;
    const startTime = Date.now();
    const metrics: PriceCacheWarmupMetrics = {
      totalAssets: 0,
      successfulWarmups: 0,
      failedWarmups: 0,
      stalePrices: 0,
      duration: 0,
      timestamp: new Date(),
    };

    try {
      logger.info("Starting price cache warmup");

      // Filter assets to warm up (exclude native and XLM)
      const assetsToWarmup = SUPPORTED_ASSETS.filter(
        (asset) => asset.code !== "native" && asset.code !== "XLM"
      );

      metrics.totalAssets = assetsToWarmup.length;

      // Warm up prices in parallel with concurrency control
      const concurrency = 5;
      for (let i = 0; i < assetsToWarmup.length; i += concurrency) {
        const batch = assetsToWarmup.slice(i, i + concurrency);
        const results = await Promise.allSettled(
          batch.map((asset) => this.warmupAssetPrice(asset.code))
        );

        results.forEach((result) => {
          if (result.status === "fulfilled") {
            if (result.value.success) {
              metrics.successfulWarmups++;
              if (result.value.stale) {
                metrics.stalePrices++;
              }
            } else {
              metrics.failedWarmups++;
            }
          } else {
            metrics.failedWarmups++;
            logger.error(
              { error: result.reason },
              "Price warmup promise rejected"
            );
          }
        });
      }

      metrics.duration = Date.now() - startTime;
      this.lastWarmupTime = new Date();

      logger.info(
        {
          totalAssets: metrics.totalAssets,
          successful: metrics.successfulWarmups,
          failed: metrics.failedWarmups,
          stale: metrics.stalePrices,
          duration: metrics.duration,
        },
        "Price cache warmup completed"
      );

      // Emit metrics
      this.emitWarmupMetrics(metrics);

      return metrics;
    } catch (error) {
      logger.error({ error }, "Price cache warmup failed");
      throw error;
    } finally {
      this.warmupInProgress = false;
    }
  }

  /**
   * Warm up price for a single asset with retry logic
   */
  private async warmupAssetPrice(
    assetCode: string
  ): Promise<{ success: boolean; stale: boolean }> {
    let lastError: Error | null = null;

    for (let attempt = 0; attempt < this.config.maxRetries; attempt++) {
      try {
        const cacheKey = `cache:price:${assetCode}`;

        // Check if price is already cached and fresh
        const cachedPrice = await this.checkCacheFreshness(cacheKey);
        if (cachedPrice && !cachedPrice.stale) {
          logger.debug(
            { asset: assetCode },
            "Price already cached and fresh"
          );
          return { success: true, stale: false };
        }

        // Fetch fresh price with bypass cache flag
        const price = await this.priceService.getAggregatedPrice(
          assetCode,
          true // bypassCache to force refresh
        );

        if (price && price.vwap > 0) {
          logger.debug(
            { asset: assetCode, price: price.vwap },
            "Price cached successfully"
          );
          return { success: true, stale: false };
        }

        throw new Error(`Invalid price received for ${assetCode}`);
      } catch (error) {
        lastError = error as Error;
        logger.warn(
          { asset: assetCode, attempt: attempt + 1, error: lastError.message },
          "Price warmup attempt failed"
        );

        // Wait before retry
        if (attempt < this.config.maxRetries - 1) {
          await this.delay(this.config.retryDelayMs * (attempt + 1));
        }
      }
    }

    logger.error(
      { asset: assetCode, error: lastError?.message },
      "Price warmup failed after all retries"
    );
    return { success: false, stale: false };
  }

  /**
   * Check if a cached price is fresh
   */
  private async checkCacheFreshness(
    cacheKey: string
  ): Promise<{ stale: boolean; age: number } | null> {
    try {
      const cached = await (await import("../utils/redis.js")).redis.get(
        cacheKey
      );
      if (!cached) {
        return null;
      }

      const data = JSON.parse(cached);
      const age = (Date.now() - (data.timestamp || 0)) / 1000;
      const stale = age > this.config.stalePriceThreshold;

      return { stale, age };
    } catch (error) {
      logger.debug(
        { cacheKey, error },
        "Error checking cache freshness"
      );
      return null;
    }
  }

  /**
   * Emit warmup metrics to Prometheus
   */
  private emitWarmupMetrics(metrics: PriceCacheWarmupMetrics): void {
    try {
      // Record successful and failed warmups
      if (this.metricsService.cacheHits) {
        this.metricsService.cacheHits.inc(metrics.successfulWarmups);
      }

      // Record stale prices detected
      if (this.metricsService.cacheEvictions) {
        this.metricsService.cacheEvictions.inc(metrics.stalePrices);
      }

      logger.debug(
        { metrics },
        "Price cache warmup metrics emitted"
      );
    } catch (error) {
      logger.error({ error }, "Failed to emit warmup metrics");
    }
  }

  /**
   * Get the last warmup time
   */
  getLastWarmupTime(): Date | null {
    return this.lastWarmupTime;
  }

  /**
   * Check if warmup is currently in progress
   */
  isWarmupInProgress(): boolean {
    return this.warmupInProgress;
  }

  /**
   * Utility delay function
   */
  private delay(ms: number): Promise<void> {
    return new Promise((resolve) => setTimeout(resolve, ms));
  }
}

// Singleton instance
let warmupService: PriceCacheWarmupService | null = null;

export function getPriceCacheWarmupService(
  config?: Partial<PriceCacheWarmupConfig>
): PriceCacheWarmupService {
  if (!warmupService) {
    warmupService = new PriceCacheWarmupService(config);
  }
  return warmupService;
}

/**
 * Run price cache warmup (for startup and scheduled execution)
 */
export async function runPriceCacheWarmup(
  config?: Partial<PriceCacheWarmupConfig>
): Promise<PriceCacheWarmupMetrics> {
  const service = getPriceCacheWarmupService(config);
  return service.warmupCache();
}

// Automatically runs if executed as script directly
// @ts-expect-error - Required for direct execution script detection
if (import.meta.url === `file://${process.argv[1]}`) {
  runPriceCacheWarmup()
    .then((metrics) => {
      logger.info({ metrics }, "Price cache warmup completed");
      process.exit(0);
    })
    .catch((error) => {
      logger.error({ error }, "Price cache warmup failed");
      process.exit(1);
    });
}
