package com.vbforge.consumerapp.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Service;

/**
 * Dedicated cache service for message deduplication.
 * Uses CacheManager directly to avoid self-invocation issues with @Cacheable.
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class MessageCacheService {

    private final CacheManager cacheManager;
    private static final String CACHE_NAME = "processedMessages";

    /**
     * Check if a message was already processed.
     * Returns true if message ID is in cache (duplicate).
     * Returns false if not in cache (new message).
     */
    public boolean isAlreadyProcessed(String messageId) {
        Cache cache = cacheManager.getCache(CACHE_NAME);
        if (cache == null) {
            log.warn("Cache '{}' not found! Returning false.", CACHE_NAME);
            return false;
        }

        Cache.ValueWrapper existing = cache.get(messageId);

        if (existing != null) {
            log.info("🔄 Cache HIT - Message already processed: {}", messageId);
            return true;
        }

        log.info("🆕 Cache MISS - New message: {}", messageId);
        return false;
    }

    /**
     * Mark a message as processed by adding it to cache.
     * This prevents duplicate processing.
     */
    public void markAsProcessed(String messageId) {
        Cache cache = cacheManager.getCache(CACHE_NAME);
        if (cache == null) {
            log.error("Cache '{}' not found! Cannot mark message as processed.", CACHE_NAME);
            return;
        }

        Cache.ValueWrapper existing = cache.get(messageId);

        if (existing == null) {
            log.info("✅ Caching message ID: {}", messageId);
            cache.put(messageId, true);
        } else {
            log.debug("Message ID already in cache: {}", messageId);
        }
    }

    /**
     * Clear the entire cache (useful for testing).
     */
    public void clearCache() {
        Cache cache = cacheManager.getCache(CACHE_NAME);
        if (cache != null) {
            cache.clear();
            log.info("🧹 Cache cleared: {}", CACHE_NAME);
        }
    }

}
