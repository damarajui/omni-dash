import { v } from "convex/values";
import { mutation, query } from "./_generated/server";

// Confidence decays by 0.1 per 30 days
const DECAY_PER_30_DAYS = 0.1;
const MIN_CONFIDENCE = 0.3;
const MS_PER_30_DAYS = 30 * 24 * 60 * 60 * 1000;

function effectiveConfidence(
  confidence: number,
  createdAt: number,
): number {
  const ageMs = Date.now() - createdAt;
  const decay = (ageMs / MS_PER_30_DAYS) * DECAY_PER_30_DAYS;
  return Math.max(0, confidence - decay);
}

// Add or update a learning (upsert by key+type)
export const upsert = mutation({
  args: {
    skill: v.string(),
    type: v.string(),
    key: v.string(),
    insight: v.string(),
    confidence: v.float64(),
    source: v.string(),
  },
  handler: async (ctx, args) => {
    // Check for existing learning with same key+type
    const existing = await ctx.db
      .query("learnings")
      .withIndex("by_key_type", (q) => q.eq("key", args.key).eq("type", args.type))
      .first();

    if (existing) {
      await ctx.db.patch(existing._id, {
        insight: args.insight,
        confidence: args.confidence,
        source: args.source,
        createdAt: Date.now(),
      });
      return existing._id;
    }

    return await ctx.db.insert("learnings", {
      ...args,
      createdAt: Date.now(),
    });
  },
});

// Full-text search across learnings
export const search = query({
  args: {
    query: v.string(),
    limit: v.optional(v.float64()),
  },
  handler: async (ctx, args) => {
    const limit = args.limit ?? 8;
    const results = await ctx.db
      .query("learnings")
      .withSearchIndex("search_insight", (q) => q.search("insight", args.query))
      .take(20);

    // Apply confidence decay and filter
    return results
      .map((r) => ({
        ...r,
        effectiveConfidence: effectiveConfidence(r.confidence, r.createdAt),
      }))
      .filter((r) => r.effectiveConfidence >= MIN_CONFIDENCE)
      .slice(0, limit);
  },
});

// Get all learnings (for context block injection)
export const listRecent = query({
  args: { limit: v.optional(v.float64()) },
  handler: async (ctx, args) => {
    const limit = args.limit ?? 20;
    const all = await ctx.db.query("learnings").order("desc").take(50);

    return all
      .map((r) => ({
        ...r,
        effectiveConfidence: effectiveConfidence(r.confidence, r.createdAt),
      }))
      .filter((r) => r.effectiveConfidence >= MIN_CONFIDENCE)
      .slice(0, limit);
  },
});
