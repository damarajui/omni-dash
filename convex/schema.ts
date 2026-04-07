import { defineSchema, defineTable } from "convex/server";
import { v } from "convex/values";

export default defineSchema({
  // Agent learnings — corrections, patterns, quirks discovered over time.
  // Confidence decays over time; old unvalidated learnings fade out.
  learnings: defineTable({
    skill: v.string(), // "dashboard_building", "data_discovery", "omni_quirk"
    type: v.string(), // "pattern", "pitfall", "metric_definition", "schema_quirk"
    key: v.string(), // Dedup key: "customer_type_is_plg_slg"
    insight: v.string(), // The actual learning text
    confidence: v.float64(), // 0.0–1.0, decays over time
    source: v.string(), // "user_correction", "agent_discovery", "system"
    createdAt: v.float64(), // Unix timestamp ms
  })
    .index("by_key_type", ["key", "type"])
    .searchIndex("search_insight", {
      searchField: "insight",
      filterFields: ["skill", "type"],
    }),

  // User preferences — per-user settings and remembered context.
  user_preferences: defineTable({
    slackUserId: v.string(),
    slackUserName: v.string(),
    preferredChartTypes: v.optional(v.array(v.string())),
    preferredFolder: v.optional(v.string()),
    notes: v.optional(v.string()), // Free-form agent notes about this user
    lastSeenAt: v.float64(),
  }).index("by_slack_user", ["slackUserId"]),

  // Dashboard creation log — every dashboard build attempt with outcome.
  // Powers analytics: success rate, common failures, retry patterns.
  dashboard_logs: defineTable({
    slackUserId: v.optional(v.string()),
    slackChannel: v.optional(v.string()),
    threadTs: v.optional(v.string()),
    prompt: v.string(), // What the user asked for
    status: v.string(), // "success", "partial", "fail", "blocked"
    dashboardId: v.optional(v.string()),
    dashboardUrl: v.optional(v.string()),
    model: v.string(), // Claude model used
    toolCalls: v.float64(), // Number of tool calls
    durationMs: v.float64(), // Total time
    errorSummary: v.optional(v.string()),
    retryCount: v.float64(), // How many retries before success/failure
    tilesCreated: v.float64(),
    tilesWithData: v.float64(),
    createdAt: v.float64(),
  })
    .index("by_status", ["status"])
    .index("by_user", ["slackUserId"])
    .index("by_created", ["createdAt"]),

  // Feedback — explicit user reactions and corrections.
  feedback: defineTable({
    slackUserId: v.string(),
    slackChannel: v.optional(v.string()),
    threadTs: v.optional(v.string()),
    dashboardId: v.optional(v.string()),
    type: v.string(), // "correction", "praise", "complaint", "suggestion"
    message: v.string(), // Raw user feedback text
    resolved: v.boolean(), // Has the agent acted on this?
    learningId: v.optional(v.id("learnings")), // Link to created learning
    createdAt: v.float64(),
  })
    .index("by_user", ["slackUserId"])
    .index("by_unresolved", ["resolved", "createdAt"]),
});
