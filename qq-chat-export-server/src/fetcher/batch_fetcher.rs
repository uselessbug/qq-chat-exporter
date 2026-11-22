use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::{AcquireError, Mutex, Semaphore, SemaphorePermit, TryAcquireError};

use crate::fetcher::chat_type::is_private_like_chat_type;

/// issue #305 / #316：自适应缩小后的 batchSize 下限。
const MIN_BATCH_SIZE_ON_TIMEOUT: i64 = 200;

/// 缩小后的 batchSize 连续成功该次数后翻倍回升（不超过配置值）。
const BATCH_SIZE_RECOVERY_SUCCESSES: u32 = 3;

/// 分页批次之间的间隔（本地 IPC 调用，仅留出让路空隙）。
const INTER_BATCH_DELAY_MS: u64 = 20;

/// issue #634：群聊序列分页的单页上限。大窗口历史查询会在 QQ 原生
/// Worker（wrapper.node）内积累巨大内存压力，最终导致 0xC0000005 崩溃。
const GROUP_SEQ_PAGE_MAX: i64 = 500;

/// issue #634：超时后的冷却时间。本地超时并不代表 Worker 内的原生查询已经
/// 结束，立即重试会造成重量级查询在 Worker 内叠加。
const TIMEOUT_COOLDOWN_MS: u64 = 15_000;

/// issue #634：bridge / Worker 崩溃后等待恢复（NapCat 重启并重新登录）的上限。
const BRIDGE_RECOVERY_WAIT_MS: u64 = 10 * 60 * 1000;

/// bridge 恢复探测间隔。
const BRIDGE_PROBE_INTERVAL_MS: u64 = 5_000;

/// 单次 API 调用允许的 bridge 崩溃恢复次数（不计入普通重试次数）。
const MAX_BRIDGE_RECOVERIES: u32 = 5;

/// issue #666：锚点二分定位的最大探测次数（覆盖 2^24 ≈ 1600 万条序列空间）。
const SEEK_MAX_PROBES: u32 = 24;

/// issue #716：单页边界 msgSeq 补读的请求上限（含首次包含式重读）。
///
/// `count` 只限制返回记录条数，同一 msgSeq 下可混有系统提示与普通消息；
/// 页尾同一 msgSeq 可能只返回了部分记录。补读窗口跟随自适应 batchSize
/// （100–500 条），上限 8 次请求大约覆盖单个 msgSeq 下 800–4000 条记录；
/// 超过说明数据异常，必须显式失败而不是继续静默跳过。
const SEQ_BOUNDARY_BACKFILL_MAX_REQUESTS: u32 = 8;

/// issue #716：边界补读的最小拉取条数。用户可把 batchSize 调得很小，
/// 补读窗口太小无法判断边界 msgSeq 是否取完。
const SEQ_BOUNDARY_BACKFILL_MIN_COUNT: i64 = 100;

/// 锚点探测单次拉取的消息条数（只需要一条即可读出该处时间）。
const SEEK_PROBE_COUNT: i64 = 1;

/// 锚点探测的超时上限；比正常批次更短，避免定位阶段长时间挂住。
const SEEK_PROBE_TIMEOUT_MS: u64 = 30_000;

/// 定位到的锚点向新方向留出的安全余量（序列号与时间并非严格同序）。
const SEEK_ANCHOR_MARGIN: i64 = GROUP_SEQ_PAGE_MAX;

/// `endTime` 距当前时间在该范围内时视为「到最新消息」，跳过锚点定位。
const SEEK_END_TIME_SLACK_MS: i64 = 6 * 60 * 60 * 1000;

/// 单个二分点探测失败时，向更新方向另找邻近探测点的重试次数。
const SEEK_NEIGHBOR_RETRIES: u32 = 3;

/// 邻近探测点的步长（一个序列页宽度）。
const SEEK_NEIGHBOR_STEP: i64 = GROUP_SEQ_PAGE_MAX;

/// 本地历史 seq=0 尾段收尾：每次重查时 pageLimit 的放大倍数。
const LOCAL_TAIL_LIMIT_GROWTH: i64 = 4;

/// 本地历史 seq=0 尾段收尾的请求上限。pageLimit 依次为页大小的 1 / 4 / 16 倍
/// （默认 500 / 2000 / 8000），仍取不完说明尾段异常庞大，停止并标记未核验，
/// 不把超大窗口压给 QQ 原生 Worker（issue #634）。
const LOCAL_TAIL_MAX_REQUESTS: u32 = 3;

/// 本地历史 seq=0 尾段收尾的总耗时上限；超过后不再发起新的尾段查询。
const LOCAL_TAIL_MAX_ELAPSED_MS: u64 = 3 * 60 * 1000;

/// issue #634：进程级历史查询门控。同一时刻只允许一个重量级历史消息查询
/// 进入 NapCat Worker，防止并发/叠加查询压垮 QQ 原生模块。
fn history_fetch_gate() -> &'static Semaphore {
    static GATE: OnceLock<Semaphore> = OnceLock::new();
    GATE.get_or_init(|| Semaphore::new(1))
}

/// 尝试进入进程级历史查询门控，不等待排队。
pub(crate) fn try_history_query_permit() -> Result<SemaphorePermit<'static>, TryAcquireError> {
    history_fetch_gate().try_acquire()
}

/// 等待进入进程级历史查询门控。
///
/// 后台导出先取得自身的并发许可，再等待此门控，避免排队任务提前占住历史查询
/// 许可并与运行中的普通导出互相等待。
pub(crate) async fn acquire_history_query_permit() -> Result<SemaphorePermit<'static>, AcquireError>
{
    history_fetch_gate().acquire().await
}

/// 聊天对象（对应 NapCat `Peer`）。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Peer {
    /// 会话类型（1 好友 / 2 群聊 / 100 临时会话等）。
    pub chat_type: i64,
    /// 对端 uid。
    pub peer_uid: String,
    /// 冗余 uin（可选）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub guild_id: Option<String>,
}

/// 消息类型筛选项。
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageTypeFilter {
    /// NTMsgType 数值。
    pub r#type: i64,
    /// 子类型列表。
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sub_types: Option<Vec<i64>>,
}

/// 消息筛选条件。
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageFilter {
    /// 起始时间（毫秒时间戳）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_time: Option<i64>,
    /// 结束时间（毫秒时间戳）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_time: Option<i64>,
    /// 发送者 uid 白名单。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sender_uids: Option<Vec<String>>,
    /// 消息类型白名单。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_types: Option<Vec<MessageTypeFilter>>,
    /// 关键词（对 elements JSON 做大小写不敏感包含匹配）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keywords: Option<Vec<String>>,
}

/// 批量获取配置。
#[derive(Debug, Clone)]
pub struct BatchFetchConfig {
    /// 每批次获取数量（建议 1000-10000）。
    pub batch_size: i64,
    /// 超时时间（毫秒）。
    pub timeout_ms: u64,
    /// 重试次数。
    pub retry_count: u32,
    /// 重试间隔（毫秒）。
    pub retry_interval_ms: u64,
    /// 是否启用优化模式。
    pub enable_optimization: bool,
}

impl Default for BatchFetchConfig {
    fn default() -> Self {
        Self {
            batch_size: 5000,
            timeout_ms: 30000,
            retry_count: 3,
            retry_interval_ms: 1000,
            enable_optimization: true,
        }
    }
}

/// 本地历史查询接口使用的精确分页游标。
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LocalHistoryCursor {
    msg_id: String,
    msg_time: String,
    msg_seq: String,
}

impl LocalHistoryCursor {
    /// 从原始消息读取三字段游标。
    fn from_message(msg: &Value) -> Result<Self, FetchError> {
        Ok(Self {
            msg_id: message_msg_id(msg)
                .ok_or_else(|| FetchError::Api("本地消息缺少 msgId".to_string()))?,
            msg_time: loose_msg_time(msg)
                .ok_or_else(|| FetchError::Api("本地消息缺少 msgTime".to_string()))?
                .to_string(),
            msg_seq: loose_msg_seq(msg)
                .ok_or_else(|| FetchError::Api("本地消息缺少 msgSeq".to_string()))?
                .to_string(),
        })
    }
}

/// 一次 `queryMsgsWithFilterEx` 请求的起点。
///
/// 实机日志（群 960251923，1423 个不同请求）还原出的内核行为：
/// - `msgSeq != 0`：返回 `msgSeq < 游标 msgSeq` 的记录，按 msgSeq 降序；
///   全部 `msgSeq = 0` 的记录排在最后，彼此按 msgTime 降序。
/// - `msgSeq = 0` 且带 msgId：内核改按 `msgId < 游标 msgId` 过滤。迁移导入的
///   历史 msgId 与时间无关，结果会跳回已遍历过的区间并周期性重复。
///
/// 因此起点只能是非零 msgSeq 的游标或「从最新开始」的全零游标，
/// 绝不能把 seq=0 的消息当作游标继续翻页。
///
/// 2026-11-22 在同一个群上实机复核：从最后一条非零序号记录重查得到完整的
/// seq=0 尾段（短页），`pageLimit` 大于 500 时按请求值返回。
#[derive(Debug, Clone)]
struct LocalHistoryQuery {
    msg_id: String,
    msg_time: String,
    msg_seq: String,
    include_current: bool,
}

impl LocalHistoryQuery {
    /// 从上一页末尾的游标继续（不含游标自身）。
    fn after(cursor: &LocalHistoryCursor) -> Self {
        Self {
            msg_id: cursor.msg_id.clone(),
            msg_time: cursor.msg_time.clone(),
            msg_seq: cursor.msg_seq.clone(),
            include_current: false,
        }
    }

    /// 从指定序号（含）开始；`"0"` 表示从最新消息开始。
    fn from_seq(msg_seq: String) -> Self {
        Self {
            msg_id: "0".to_string(),
            msg_time: "0".to_string(),
            msg_seq,
            include_current: true,
        }
    }

    /// 只读 seq=0 尾段：不带 msgId 的游标 `msgSeq=1` 只匹配 `msgSeq < 1` 的记录，
    /// 不必先翻完全部非零序号的历史（实机复核，结果与从锚点重查一致）。
    fn zero_seq_tail() -> Self {
        Self {
            msg_id: "0".to_string(),
            msg_time: "0".to_string(),
            msg_seq: "1".to_string(),
            include_current: false,
        }
    }
}

/// 本地历史分页无法核验完整性的原因。
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocalHistoryIssue {
    /// 游标没有前进，或上一页的边界消息被再次返回。
    #[error("分页游标未前进")]
    CursorStalled,
    /// 从非零 msgSeq 游标翻页后出现了更大的 msgSeq。
    #[error("翻页后消息序号反向增大")]
    SeqWentUp,
    /// 之前某一页的页尾边界再次出现。
    #[error("分页结果重复")]
    RepeatedPage,
    /// 放大 pageLimit 后返回条数没有增加，无法判断 seq=0 尾段是否取完。
    #[error("内核限制了单页条数，无法确认序号为 0 的尾段消息已取完")]
    TailPageLimitCapped,
    /// seq=0 尾段在请求数 / 耗时预算内没有取完。
    #[error("序号为 0 的尾段消息过多，未在查询预算内取完")]
    TailBudgetExhausted,
    /// 从非零游标重查尾段时混入了非零序号的记录。
    #[error("序号为 0 的尾段消息重查结果异常")]
    TailNotAtEnd,
    /// 尾段查询失败。
    #[error("序号为 0 的尾段消息查询失败: {0}")]
    TailQueryFailed(String),
}

/// 本地历史回退分页的终止状态。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocalHistoryEnd {
    /// 内核返回空页或短页：本地可枚举的历史已全部读完。
    Complete,
    /// 已回溯到请求的起始时间之前，更早的记录不在导出范围内；
    /// seq=0 尾段已单独读完。
    ReachedStartTime,
    /// 分页异常终止：已抓取的记录全部保留，但完整性未核验。
    Unverified(LocalHistoryIssue),
}

/// 批量获取结果。
#[derive(Debug, Clone, Default)]
pub struct BatchFetchResult {
    /// 本批消息（NapCat RawMessage 原始 JSON）。
    pub messages: Vec<Value>,
    /// 是否还有更多。
    pub has_more: bool,
    /// 下一批起始消息 ID。
    pub next_message_id: Option<String>,
    /// 下一批起始序列号。
    pub next_seq: Option<String>,
    /// 本地数据库分页游标；仅 queryMsgsWithFilterEx 回退模式使用。
    pub local_cursor: Option<LocalHistoryCursor>,
    /// 客户端筛选后的实际条数。
    pub actual_count: usize,
    /// 本次获取耗时（毫秒）。
    pub fetch_time_ms: i64,
    /// 本批最早消息时间（毫秒）。
    pub earliest_msg_time: Option<i64>,
}

/// API 调用统计。
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiCallStats {
    /// 调用总数。
    pub call_count: u64,
    /// 成功次数。
    pub success_count: u64,
    /// 失败次数。
    pub failure_count: u64,
    /// 平均响应时间（毫秒）。
    pub average_response_time: f64,
    /// 连续失败次数。
    pub consecutive_failures: u64,
}

/// 获取器错误。
#[derive(Debug, thiserror::Error)]
pub enum FetchError {
    /// 获取器忙。
    #[error("批量获取器正忙，请稍后再试")]
    Busy,
    /// 操作已被取消。
    #[error("操作已被取消")]
    Cancelled,
    /// API 调用超时。
    #[error("API调用超时 ({0}ms)")]
    Timeout(u64),
    /// 底层 API 错误。
    #[error("{0}")]
    Api(String),
    /// 主动修复未取得进展。
    #[error(
        "MESSAGE_SEQUENCE_REPAIR_NO_PROGRESS: 群聊历史仍有 {gap_count} 个大序列缺口（估算缺失 {missing_positions} 个序号）"
    )]
    SequenceRepairNoProgress {
        gap_count: usize,
        missing_positions: i64,
    },
    /// 主动修复达到请求预算。
    #[error(
        "MESSAGE_SEQUENCE_REPAIR_BUDGET_EXHAUSTED: 群聊历史仍有 {gap_count} 个大序列缺口（估算缺失 {missing_positions} 个序号）"
    )]
    SequenceRepairBudgetExhausted {
        gap_count: usize,
        missing_positions: i64,
    },
    /// 主动修复达到轮次限制后仍未收敛。
    #[error(
        "MESSAGE_SEQUENCE_GAPS_UNRESOLVED: 群聊历史仍有 {gap_count} 个大序列缺口（估算缺失 {missing_positions} 个序号）"
    )]
    SequenceGapsUnresolved {
        gap_count: usize,
        missing_positions: i64,
    },
    /// issue #716：边界 msgSeq 补读超过请求上限，拒绝静默跳过剩余记录。
    #[error(
        "SEQUENCE_BOUNDARY_BACKFILL_EXHAUSTED: 群聊历史边界 msgSeq={msg_seq} 的补读请求数超过上限 {max_requests}，为避免静默丢消息已中止"
    )]
    SequenceBackfillExhausted {
        /// 被截断的边界 msgSeq。
        msg_seq: i64,
        /// 本页允许的最大补读请求数。
        max_requests: u32,
    },
}

impl FetchError {
    /// 判断错误是否属于 API 超时（TS `isTimeoutError`）。
    fn is_timeout(&self) -> bool {
        match self {
            Self::Timeout(_) => true,
            Self::Api(message) => {
                let lower = message.to_lowercase();
                lower.contains("timeout") || message.contains("API调用超时")
            }
            _ => false,
        }
    }

    /// 2004010 表示当前账号已非群成员；继续重试同一原生接口没有意义。
    fn is_not_group_member(&self) -> bool {
        matches!(self, Self::Api(message) if
            message.contains("2004010") || message.contains("is not group member"))
    }

    /// issue #723：判断是否是 QQ Worker 内部重试耗尽。
    ///
    /// NapCat 原生查询返回 `GeneralCallResult.result = -1`，且 `errMsg` 为
    /// `2004007: reach max retry time limit!` 时，说明 Worker 内的原生重试
    /// 已经耗尽预算。立即重试只会再次撞上同一限制，必须和超时一样先冷却。
    fn is_native_retry_exhausted(&self) -> bool {
        match self {
            Self::Api(message) => message
                .to_ascii_lowercase()
                .contains("max retry time limit"),
            _ => false,
        }
    }

    /// 判断错误是否属于 bridge / Worker 不可用（NapCat Worker 崩溃或重启）。
    fn is_bridge_down(&self) -> bool {
        let Self::Api(message) = self else {
            return false;
        };
        if message.contains("传输错误") {
            return true;
        }
        let lower = message.to_lowercase();
        [
            "connection refused",
            "connection reset",
            "connection closed",
            "broken pipe",
            "error sending request",
            "connect error",
            "unexpected eof",
            "channel closed",
            "dns error",
        ]
        .iter()
        .any(|pattern| lower.contains(pattern))
    }
}

/// NapCat 消息获取 API 抽象（由 bridge 客户端实现）。
#[async_trait]
pub trait MessageFetchApi: Send + Sync {
    /// 获取最新消息（对应 `MsgApi.getAioFirstViewLatestMsgs`）。
    /// 返回值需包含 `msgList` 数组。
    async fn get_aio_first_view_latest_msgs(
        &self,
        peer: &Peer,
        count: i64,
    ) -> Result<Value, String>;

    /// 从指定消息向前获取历史消息（对应 `MsgApi.getMsgHistory`，reverse=true）。
    async fn get_msg_history(&self, peer: &Peer, msg_id: &str, count: i64)
        -> Result<Value, String>;

    /// 按序列号范围获取消息（对应 `getMsgService().getMsgsBySeqRange`）。
    async fn get_msgs_by_seq_range(
        &self,
        peer: &Peer,
        start_seq: &str,
        end_seq: &str,
    ) -> Result<Value, String>;

    /// 从指定序列号向前获取固定数量消息。
    async fn get_msgs_by_seq_and_count(
        &self,
        peer: &Peer,
        anchor_seq: i64,
        count: i64,
    ) -> Result<Value, String>;

    /// QQ NT 本地数据库分页查询；默认实现供旧 mock 保持兼容。
    async fn get_local_history(
        &self,
        _peer: &Peer,
        _msg_id: &str,
        _msg_time: &str,
        _msg_seq: &str,
        _include_current: bool,
        _count: i64,
    ) -> Result<Value, String> {
        Err("本地消息查询接口不可用".to_string())
    }

    /// bridge / Worker 是否健康可用（默认视为健康）。
    async fn bridge_healthy(&self) -> bool {
        true
    }
}

/// 获取策略。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchStrategy {
    /// 基于时间范围的顺序获取。
    TimeBasedSequential,
    /// 基于序列号的范围获取。
    SequenceBasedRange,
    /// 混合策略（动态选择）。
    Hybrid,
    /// issue #634：基于序列号的小页分页（群聊安全路径）。
    SequenceBasedPaged,
}

/// 内部可变状态。
#[derive(Debug)]
struct FetcherState {
    batch_size: i64,
    consecutive_successes: u32,
    stats: ApiCallStats,
    current_strategy: FetchStrategy,
    /// 只保留每页末尾记录的 msgId，内存规模按页数而非消息数增长。
    seen_local_page_ends: HashSet<String>,
    /// 本地历史回退分页的终止状态；未进入回退或尚未结束时为 `None`。
    local_history_end: Option<LocalHistoryEnd>,
}

/// 批量消息获取器。
pub struct BatchMessageFetcher {
    api: Arc<dyn MessageFetchApi>,
    config: BatchFetchConfig,
    state: Mutex<FetcherState>,
    is_fetching: AtomicBool,
    cancelled: AtomicBool,
}

impl BatchMessageFetcher {
    /// 创建获取器。
    pub fn new(api: Arc<dyn MessageFetchApi>, config: BatchFetchConfig) -> Self {
        let batch_size = config.batch_size;
        Self {
            api,
            config,
            state: Mutex::new(FetcherState {
                batch_size,
                consecutive_successes: 0,
                stats: ApiCallStats::default(),
                current_strategy: FetchStrategy::Hybrid,
                seen_local_page_ends: HashSet::new(),
                local_history_end: None,
            }),
            is_fetching: AtomicBool::new(false),
            cancelled: AtomicBool::new(false),
        }
    }

    /// 当前使用的获取策略。
    pub async fn current_strategy(&self) -> FetchStrategy {
        self.state.lock().await.current_strategy
    }

    /// 抓取序列起点：重置取消标记（issue #446 —— 取消标记只在序列起点重置，
    /// 避免分页过程中的 cancel() 被下一批次清掉）。
    pub fn reset_cancel_token(&self) {
        self.cancelled.store(false, Ordering::SeqCst);
    }

    /// 取消当前获取操作。
    pub fn cancel(&self) {
        self.cancelled.store(true, Ordering::SeqCst);
    }

    /// 当前抓取是否已被取消（issue #446）。
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::SeqCst)
    }

    /// 是否正在获取中。
    #[must_use]
    pub fn is_busy(&self) -> bool {
        self.is_fetching.load(Ordering::SeqCst)
    }

    /// 获取当前统计信息。
    pub async fn stats(&self) -> ApiCallStats {
        self.state.lock().await.stats.clone()
    }

    /// 重置统计信息。
    pub async fn reset_stats(&self) {
        self.state.lock().await.stats = ApiCallStats::default();
    }

    /// 本地历史回退分页的终止状态。
    ///
    /// 分页循环结束后读取：`None` 表示本次抓取没有进入本地回退（或被取消），
    /// 其余取值区分已核验的完整终止、按时间范围停止与未核验的异常终止。
    pub async fn local_history_end(&self) -> Option<LocalHistoryEnd> {
        self.state.lock().await.local_history_end.clone()
    }

    /// 批量获取消息（主要外部接口，支持筛选与分页）。
    pub async fn fetch_messages(
        &self,
        peer: &Peer,
        filter: &MessageFilter,
        start_message_id: Option<&str>,
        start_seq: Option<&str>,
    ) -> Result<BatchFetchResult, FetchError> {
        if self
            .is_fetching
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(FetchError::Busy);
        }
        // RAII 守卫：任何返回路径都会复位 is_fetching。
        struct FetchGuard<'a>(&'a AtomicBool);
        impl Drop for FetchGuard<'_> {
            fn drop(&mut self) {
                self.0.store(false, Ordering::SeqCst);
            }
        }
        let _guard = FetchGuard(&self.is_fetching);

        let start_time = now_ms();
        let strategy = self.select_optimal_strategy(filter, peer);
        {
            let mut state = self.state.lock().await;
            state.current_strategy = strategy;
        }
        tracing::info!("[BatchMessageFetcher] 选择策略: {strategy:?}, 开始执行获取");

        let result = self
            .execute_strategy(strategy, peer, filter, start_message_id, start_seq)
            .await;
        match result {
            Ok(mut batch) => {
                tracing::info!(
                    "[BatchMessageFetcher] 策略执行完成, 获取{}条消息",
                    batch.messages.len()
                );
                let fetch_time = now_ms() - start_time;
                self.update_stats(true, fetch_time).await;
                batch.fetch_time_ms = fetch_time;
                Ok(batch)
            }
            Err(error) => {
                self.update_stats(false, 0).await;
                Err(error)
            }
        }
    }

    /// 分页拉取：给定上一批结果拉取下一批；首批传 `None`。
    ///
    /// TS 的 `fetchAllMessagesInTimeRange` AsyncGenerator 在 Rust 侧的等价物：
    /// 调用方用 while-let 循环驱动，返回 `Ok(None)` 表示分页结束。
    pub async fn fetch_next_batch(
        &self,
        peer: &Peer,
        filter: &MessageFilter,
        previous: Option<&BatchFetchResult>,
    ) -> Result<Option<BatchFetchResult>, FetchError> {
        match previous {
            None => {
                // 抓取序列起点重置取消标记（issue #446）。
                self.reset_cancel_token();
                let mut state = self.state.lock().await;
                state.seen_local_page_ends.clear();
                state.local_history_end = None;
            }
            Some(prev) => {
                if !prev.has_more || self.is_cancelled() {
                    return Ok(None);
                }
                // 避免过于频繁的 API 调用。
                tokio::time::sleep(Duration::from_millis(INTER_BATCH_DELAY_MS)).await;
            }
        }
        if self.is_cancelled() {
            return Ok(None);
        }

        let (next_message_id, next_seq) = previous.map_or((None, None), |prev| {
            (prev.next_message_id.clone(), prev.next_seq.clone())
        });
        // 一旦触发 2004010，后续页始终使用本地游标，不再反复查询被拒绝的原生接口。
        let result = if peer.chat_type == 2
            && previous.is_some_and(|batch| batch.local_cursor.is_some())
        {
            self.fetch_local_group_page(peer, filter, previous).await?
        } else {
            match self
                .fetch_messages(
                    peer,
                    filter,
                    next_message_id.as_deref(),
                    next_seq.as_deref(),
                )
                .await
            {
                Ok(batch) => batch,
                Err(error) if peer.chat_type == 2 && error.is_not_group_member() => {
                    tracing::warn!(
                        "[BatchMessageFetcher] 2004010 (is not group member)，改用本地数据库分页继续导出"
                    );
                    self.fetch_local_group_page(peer, filter, previous).await?
                }
                Err(error) => return Err(error),
            }
        };

        // 防御性提前停止：客户端筛选后为空且批次最早时间早于开始时间，无需继续回溯。
        if result.messages.is_empty() {
            if let (Some(earliest), Some(start_time)) =
                (result.earliest_msg_time, filter.start_time)
            {
                if earliest < start_time {
                    return Ok(None);
                }
            }
            if !result.has_more {
                return Ok(None);
            }
        }
        Ok(Some(result))
    }

    /// 已退群/封禁群的本地消息分页。原生页完成后使用 next_seq 衔接，
    /// 此后使用 (msgId,msgTime,msgSeq) 精确游标，避免同 seq 记录丢失。
    ///
    /// 翻页与终止规则见 [`LocalHistoryQuery`]：页尾落在 seq=0 尾段时不再把它
    /// 当作游标，而是由 [`Self::complete_zero_seq_tail`] 从非零序号的锚点取完尾段。
    async fn fetch_local_group_page(
        &self,
        peer: &Peer,
        filter: &MessageFilter,
        previous: Option<&BatchFetchResult>,
    ) -> Result<BatchFetchResult, FetchError> {
        let previous_cursor = previous.and_then(|batch| batch.local_cursor.clone());
        let query = previous_cursor.as_ref().map_or_else(
            || {
                LocalHistoryQuery::from_seq(
                    previous
                        .and_then(|batch| batch.next_seq.clone())
                        .unwrap_or_else(|| "0".to_string()),
                )
            },
            LocalHistoryQuery::after,
        );
        let started = now_ms();
        let (mut raw_messages, page_limit) = self.query_local_page(peer, &query, None).await?;

        // 必须从原始消息取游标，不能从筛选后的消息取（否则会跳过记录）。
        let page_end = raw_messages
            .last()
            .map(LocalHistoryCursor::from_message)
            .transpose()?;
        let mut next_cursor = None;
        let mut end = None;
        match page_end {
            None => end = Some(LocalHistoryEnd::Complete),
            Some(page_end) => {
                if let Some(issue) = self
                    .check_local_page(peer, &query, previous_cursor.as_ref(), &raw_messages)
                    .await
                {
                    // 不丢弃当前页，解析阶段已有跨块 msgId 去重，避免漏掉混入的新记录。
                    end = Some(LocalHistoryEnd::Unverified(issue));
                } else if page_end.msg_seq != "0" {
                    next_cursor = Some(page_end);
                } else if (raw_messages.len() as i64) < page_limit {
                    // 短页以 seq=0 结尾：尾段已随本页取完。
                    end = Some(LocalHistoryEnd::Complete);
                } else {
                    end = Some(
                        self.complete_zero_seq_tail(peer, &query, page_limit, &mut raw_messages)
                            .await?,
                    );
                }
            }
        }

        let earliest_msg_time = raw_messages
            .iter()
            .filter_map(loose_msg_time)
            .min()
            .map(to_millis);
        if let (Some(earliest), Some(start_time)) = (earliest_msg_time, filter.start_time) {
            if earliest < start_time && next_cursor.take().is_some() {
                // 更早的非零序号记录都在范围之外，但 seq=0 的记录排在它们之后，
                // 时间却可能落在范围内：单独读取尾段，再交给下面的时间筛选。
                let tail = self
                    .read_zero_seq_tail(
                        peer,
                        &LocalHistoryQuery::zero_seq_tail(),
                        page_limit,
                        None,
                        &mut raw_messages,
                    )
                    .await?;
                end = Some(match tail {
                    LocalHistoryEnd::Complete => LocalHistoryEnd::ReachedStartTime,
                    unverified => unverified,
                });
            }
        }

        {
            let mut state = self.state.lock().await;
            if let Some(next) = &next_cursor {
                state.seen_local_page_ends.insert(next.msg_id.clone());
            }
            if end.is_some() {
                state.local_history_end.clone_from(&end);
            }
        }
        match &end {
            Some(LocalHistoryEnd::Unverified(issue)) => tracing::warn!(
                "[QCE-LOCAL-HISTORY] history_end peer={} status=unverified reason={issue:?}；停止翻页，保留已抓取记录，完整性待核验",
                peer.peer_uid
            ),
            Some(end) => tracing::info!(
                "[QCE-LOCAL-HISTORY] history_end peer={} status={end:?}",
                peer.peer_uid
            ),
            None => {}
        }

        let has_more = next_cursor.is_some();
        let messages = apply_client_side_filter(raw_messages, filter);
        let actual_count = messages.len();
        tracing::info!(
            "[BatchMessageFetcher] 本地历史查询获取 {actual_count} 条消息, nextSeq={:?}, hasMore={has_more}",
            next_cursor.as_ref().map(|c| &c.msg_seq)
        );
        Ok(BatchFetchResult {
            messages,
            has_more,
            next_message_id: next_cursor.as_ref().map(|c| c.msg_id.clone()),
            next_seq: next_cursor.as_ref().map(|c| c.msg_seq.clone()),
            local_cursor: next_cursor,
            actual_count,
            fetch_time_ms: now_ms() - started,
            earliest_msg_time,
        })
    }

    /// 发起一次本地历史查询，返回原始消息与实际使用的 pageLimit。
    ///
    /// `page_limit` 为 `None` 时跟随自适应 batchSize（重试缩小后可能小于上一页），
    /// 判断短页必须用这里返回的实际值。
    async fn query_local_page(
        &self,
        peer: &Peer,
        query: &LocalHistoryQuery,
        page_limit: Option<i64>,
    ) -> Result<(Vec<Value>, i64), FetchError> {
        // 诊断只记录会话标识和分页游标，不输出消息正文或 elements。
        tracing::info!(
            "[QCE-LOCAL-HISTORY] request peer={} msgId={} msgTime={} msgSeq={} includeCurrent={}",
            peer.peer_uid,
            query.msg_id,
            query.msg_time,
            query.msg_seq,
            query.include_current
        );
        let api = Arc::clone(&self.api);
        let call_peer = peer.clone();
        let call_query = query.clone();
        let (messages, used_limit) = self
            .call_with_retry(move |batch_size| {
                let api = Arc::clone(&api);
                let peer = call_peer.clone();
                let query = call_query.clone();
                async move {
                    let limit = page_limit.unwrap_or_else(|| seq_page_size(batch_size));
                    let value = api
                        .get_local_history(
                            &peer,
                            &query.msg_id,
                            &query.msg_time,
                            &query.msg_seq,
                            query.include_current,
                            limit,
                        )
                        .await?;
                    let messages = if is_history_exhausted_result(&value) {
                        Vec::new()
                    } else {
                        extract_msg_page_strict(&value)?
                    };
                    Ok((messages, limit))
                }
            })
            .await?;

        // 简短分页摘要；在默认 info 日志级别可直接看到异常前一页。
        let first = messages.first();
        let last = messages.last();
        tracing::info!(
            "[QCE-LOCAL-HISTORY] response count={} firstSeq={:?} firstTime={:?} lastSeq={:?} lastTime={:?} pageLimit={used_limit}",
            messages.len(),
            first.and_then(loose_msg_seq),
            first.and_then(loose_msg_time),
            last.and_then(loose_msg_seq),
            last.and_then(loose_msg_time),
        );
        Ok((messages, used_limit))
    }

    /// 校验一页本地历史是否符合「从非零游标只会读到更旧序号」的分页模型。
    ///
    /// 返回 `Some` 时调用方停止翻页并把导出标记为未核验；本页记录照常保留。
    /// 正常分页不会触发这里的任何一项，它们只用于拦截未知的内核行为。
    async fn check_local_page(
        &self,
        peer: &Peer,
        query: &LocalHistoryQuery,
        previous: Option<&LocalHistoryCursor>,
        messages: &[Value],
    ) -> Option<LocalHistoryIssue> {
        if let Some(previous) = previous {
            let repeated_positions: Vec<usize> = messages
                .iter()
                .enumerate()
                .filter_map(|(idx, msg)| {
                    (raw_msg_id(msg) == Some(previous.msg_id.as_str())).then_some(idx)
                })
                .collect();
            if !repeated_positions.is_empty() {
                tracing::warn!(
                    "[QCE-LOCAL-HISTORY] cursor_anomaly peer={} prevId={} prevTime={} prevSeq={} count={} repeatedPositions={repeated_positions:?}",
                    peer.peer_uid,
                    previous.msg_id,
                    previous.msg_time,
                    previous.msg_seq,
                    messages.len(),
                );
                return Some(LocalHistoryIssue::CursorStalled);
            }
        }

        let query_seq = query.msg_seq.parse::<i64>().unwrap_or(0);
        if query_seq > 0 {
            let went_up = messages.iter().enumerate().find_map(|(index, msg)| {
                loose_msg_seq(msg)
                    .filter(|seq| *seq > query_seq)
                    .map(|seq| (index, seq))
            });
            if let Some((index, seq)) = went_up {
                tracing::warn!(
                    "[QCE-LOCAL-HISTORY] seq_went_up peer={} cursorSeq={query_seq} returnedSeq={seq} atIndex={index} returnedCount={}",
                    peer.peer_uid,
                    messages.len(),
                );
                return Some(LocalHistoryIssue::SeqWentUp);
            }
        }

        // 同一 msgSeq 下可有多条记录，序号校验拦不住在它们之间打转的游标；
        // 这里追踪「前面任何一页」真实到过的页尾边界记录。
        let state = self.state.lock().await;
        let repeated = messages.iter().enumerate().find(|(_, msg)| {
            raw_msg_id(msg).is_some_and(|id| state.seen_local_page_ends.contains(id))
        });
        if let Some((index, repeated)) = repeated {
            tracing::warn!(
                "[QCE-LOCAL-HISTORY] history_cycle peer={} repeatedPageBoundaryId={:?} repeatedTime={:?} repeatedSeq={:?} repeatedAtIndex={} returnedCount={} visitedPages={}",
                peer.peer_uid,
                raw_msg_id(repeated),
                loose_msg_time(repeated),
                loose_msg_seq(repeated),
                index,
                messages.len(),
                state.seen_local_page_ends.len()
            );
            return Some(LocalHistoryIssue::RepeatedPage);
        }
        None
    }

    /// 取完 seq=0 尾段。
    ///
    /// `page` 是一张以 seq=0 结尾的满页：尾段可能还没取完，而 seq=0 的消息不能
    /// 当游标。改为从页内最后一条非零序号的记录重查，内核会从尾段开头返回。
    async fn complete_zero_seq_tail(
        &self,
        peer: &Peer,
        page_query: &LocalHistoryQuery,
        page_limit: i64,
        page: &mut Vec<Value>,
    ) -> Result<LocalHistoryEnd, FetchError> {
        let anchor_index = page
            .iter()
            .rposition(|msg| loose_msg_seq(msg).is_some_and(|seq| seq != 0));
        match anchor_index {
            Some(index) => {
                let anchor =
                    LocalHistoryQuery::after(&LocalHistoryCursor::from_message(&page[index])?);
                self.read_zero_seq_tail(peer, &anchor, page_limit, None, page)
                    .await
            }
            // 整页都是 seq=0：沿用本页的请求起点，已知该起点下至少有一整页。
            None => {
                let known = page.len();
                self.read_zero_seq_tail(
                    peer,
                    page_query,
                    page_limit * LOCAL_TAIL_LIMIT_GROWTH,
                    Some(known),
                    page,
                )
                .await
            }
        }
    }

    /// 从 `anchor` 读取 seq=0 尾段并追加到 `page`。
    ///
    /// 返回条数小于 pageLimit 即证明尾段完整；尾段超过一页时逐次放大
    /// pageLimit，请求数与耗时都有上限。`previous_count` 是同一起点在更小
    /// pageLimit 下已知的返回条数。新记录按 msgId 去重后追加，已有记录不会被
    /// 替换或丢弃。
    async fn read_zero_seq_tail(
        &self,
        peer: &Peer,
        anchor: &LocalHistoryQuery,
        mut limit: i64,
        mut previous_count: Option<usize>,
        page: &mut Vec<Value>,
    ) -> Result<LocalHistoryEnd, FetchError> {
        let mut seen: HashSet<String> = page.iter().map(message_dedupe_key).collect();
        let started = tokio::time::Instant::now();
        let budget = Duration::from_millis(LOCAL_TAIL_MAX_ELAPSED_MS);

        for request in 1..=LOCAL_TAIL_MAX_REQUESTS {
            if started.elapsed() > budget {
                break;
            }
            let (tail, used_limit) = match self.query_local_page(peer, anchor, Some(limit)).await {
                Ok(tail) => tail,
                Err(FetchError::Cancelled) => return Err(FetchError::Cancelled),
                Err(error) => {
                    return Ok(LocalHistoryEnd::Unverified(
                        LocalHistoryIssue::TailQueryFailed(error.to_string()),
                    ))
                }
            };
            let returned = tail.len();
            let stray = tail.iter().any(|msg| loose_msg_seq(msg) != Some(0));
            let known = page.len();
            for msg in tail {
                if seen.insert(message_dedupe_key(&msg)) {
                    page.push(msg);
                }
            }
            tracing::info!(
                "[QCE-LOCAL-HISTORY] zero_seq_tail peer={} request={request}/{LOCAL_TAIL_MAX_REQUESTS} anchorSeq={} pageLimit={used_limit} returned={returned} newMessages={} elapsedMs={}",
                peer.peer_uid,
                anchor.msg_seq,
                page.len() - known,
                started.elapsed().as_millis(),
            );
            if stray {
                return Ok(LocalHistoryEnd::Unverified(LocalHistoryIssue::TailNotAtEnd));
            }
            // pageLimit 变大而条数没变：内核对单页条数有上限，短页不再可信。
            if previous_count.is_some_and(|previous| returned <= previous) {
                return Ok(LocalHistoryEnd::Unverified(
                    LocalHistoryIssue::TailPageLimitCapped,
                ));
            }
            if (returned as i64) < used_limit {
                return Ok(LocalHistoryEnd::Complete);
            }
            previous_count = Some(returned);
            limit = used_limit * LOCAL_TAIL_LIMIT_GROWTH;
        }
        Ok(LocalHistoryEnd::Unverified(
            LocalHistoryIssue::TailBudgetExhausted,
        ))
    }

    /// 根据筛选条件和性能情况选择最优的获取策略。
    fn select_optimal_strategy(&self, filter: &MessageFilter, peer: &Peer) -> FetchStrategy {
        // 单聊型会话（含好友、临时会话、服务号、频道私聊等，issue #365）
        // 直接使用最简单可靠的方法。
        if is_private_like_chat_type(Some(peer.chat_type)) {
            tracing::debug!(
                "策略选择: 单聊使用基础getMsgHistory方法, 对等体={}, chatType={}",
                peer.peer_uid,
                peer.chat_type
            );
            return FetchStrategy::TimeBasedSequential;
        }
        if !self.config.enable_optimization {
            return FetchStrategy::TimeBasedSequential;
        }
        // issue #634：群聊使用小页序列分页，避免大窗口 getMsgHistory 查询
        // 压垮 QQ 原生 Worker。
        let _ = filter;
        tracing::debug!(
            "策略选择: 群聊使用序列小页分页, 对等体={}, chatType={}",
            peer.peer_uid,
            peer.chat_type
        );
        FetchStrategy::SequenceBasedPaged
    }

    /// 执行指定的获取策略。
    async fn execute_strategy(
        &self,
        strategy: FetchStrategy,
        peer: &Peer,
        filter: &MessageFilter,
        start_message_id: Option<&str>,
        start_seq: Option<&str>,
    ) -> Result<BatchFetchResult, FetchError> {
        match strategy {
            FetchStrategy::TimeBasedSequential => {
                self.fetch_by_time_based_sequential(peer, filter, start_message_id)
                    .await
            }
            FetchStrategy::SequenceBasedRange => {
                self.fetch_by_sequence_range(peer, filter, start_seq).await
            }
            FetchStrategy::Hybrid => {
                self.fetch_by_hybrid_strategy(peer, filter, start_message_id, start_seq)
                    .await
            }
            FetchStrategy::SequenceBasedPaged => {
                self.fetch_by_seq_paged(peer, filter, start_seq).await
            }
        }
    }

    /// issue #666：把首批锚点二分定位到 `filter.end_time` 附近。
    ///
    /// 序列分页只能从新到旧翻页。当时间范围上界远早于最新消息时（例如导出
    /// 2020–2022 的历史），逐页回溯会先把范围之外的上百万条消息全部查一遍，
    /// 在 QQ 原生 Worker 内积累巨量内存并触发连接重置（`recv failed 10054`）。
    ///
    /// 这里用 `O(log n)` 次单条探测把锚点定位到上界附近；无法定位时返回
    /// `None`，回退到原来的从最新消息翻页行为。
    async fn seek_anchor_for_end_time(&self, peer: &Peer, filter: &MessageFilter) -> Option<i64> {
        let end_time = filter.end_time?;
        // 上界接近当前时间：最新消息本身就在范围内，无需定位。
        if end_time >= now_ms() - SEEK_END_TIME_SLACK_MS {
            return None;
        }

        let (latest_seq, latest_time) = self.probe_seq_head(peer, None).await?;
        if latest_time <= end_time {
            return None;
        }

        let mut lo = 1_i64;
        let mut hi = latest_seq;
        let mut best: Option<i64> = None;
        let mut successful_probes = 0_u32;
        let mut probes = 0_u32;
        while lo <= hi && probes < SEEK_MAX_PROBES {
            if self.is_cancelled() {
                return None;
            }
            let mid = lo + (hi - lo) / 2;
            // 探测点可能落在撤回 / 不可见消息上，也可能只是接口抖动。这类
            // 「未知」结果绝不能当成「目标在更高处」的证据（那会整段漏掉历史
            // 消息），只允许在更新方向另找邻近探测点重试。
            let mut probe: Option<(i64, i64)> = None;
            let mut point = mid;
            for retry in 0..=SEEK_NEIGHBOR_RETRIES {
                if probes >= SEEK_MAX_PROBES {
                    break;
                }
                let candidate = mid + i64::from(retry) * SEEK_NEIGHBOR_STEP;
                if candidate > hi {
                    break;
                }
                probes += 1;
                if let Some(result) = self.probe_seq_head(peer, Some(candidate)).await {
                    point = candidate;
                    probe = Some(result);
                    break;
                }
            }
            // 邻近探测点全部失败：方向不可判定。此时任何「猜一个锚点」都可能整段
            // 漏掉历史消息，只能放弃定位，退回从最新消息翻页（慢但绝不丢消息）。
            let Some((probe_seq, probe_time)) = probe else {
                tracing::warn!(
                    "[BatchMessageFetcher] issue #666：锚点探测连续失败（seq≈{mid}），回退到从最新消息翻页"
                );
                return None;
            };
            successful_probes += 1;
            if probe_time <= end_time {
                best = Some(probe_seq.max(best.unwrap_or(probe_seq)));
                lo = point + 1;
            } else {
                hi = probe_seq.min(point) - 1;
            }
        }

        // 探测次数用尽而区间还没收敛：同样不能凭半成品猜锚点。
        if lo <= hi {
            tracing::warn!(
                "[BatchMessageFetcher] issue #666：锚点定位未收敛（probes={probes}），回退到从最新消息翻页"
            );
            return None;
        }
        // best 为空说明整段历史都比 endTime 新：锚在最旧处，首批自然为空。
        let anchor = best
            .map(|seq| (seq + SEEK_ANCHOR_MARGIN).min(latest_seq))
            .or_else(|| (successful_probes > 0).then_some(1))?;
        tracing::info!(
            "[BatchMessageFetcher] issue #666：按 endTime={end_time} 定位首批锚点 anchorSeq={anchor}（latestSeq={latest_seq}, probes={probes}）"
        );
        Some(anchor)
    }

    /// 探测 `seq <= anchor_seq` 区间内最新一条消息的 `(msgSeq, msgTime)`。
    ///
    /// `anchor_seq` 为 `None` 时探测会话最新消息。空结果 / 调用失败返回 `None`。
    async fn probe_seq_head(&self, peer: &Peer, anchor_seq: Option<i64>) -> Option<(i64, i64)> {
        let permit = history_fetch_gate().acquire().await.ok()?;
        let timeout = Duration::from_millis(self.config.timeout_ms.min(SEEK_PROBE_TIMEOUT_MS));
        let call = async {
            match anchor_seq {
                None => {
                    self.api
                        .get_aio_first_view_latest_msgs(peer, SEEK_PROBE_COUNT)
                        .await
                }
                Some(anchor_seq) => {
                    self.api
                        .get_msgs_by_seq_and_count(peer, anchor_seq, SEEK_PROBE_COUNT)
                        .await
                }
            }
        };
        let result = tokio::time::timeout(timeout, call).await;
        drop(permit);
        let value = match result {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => {
                tracing::warn!(
                    "[BatchMessageFetcher] 锚点探测失败 (anchor={anchor_seq:?}): {error}"
                );
                return None;
            }
            Err(_) => {
                tracing::warn!("[BatchMessageFetcher] 锚点探测超时 (anchor={anchor_seq:?})");
                return None;
            }
        };
        extract_msg_list(value)
            .iter()
            .filter_map(|message| {
                Some((loose_msg_seq(message)?, to_millis(loose_msg_time(message)?)))
            })
            .max_by_key(|(seq, _)| *seq)
    }

    /// issue #634：基于序列号的小页分页获取（群聊安全路径）。
    ///
    /// 首批用 `getAioFirstViewLatestMsgs` 的小窗口拿到当前最新序列号，之后
    /// 每批用 `getMsgsBySeqAndCount` 从锚点向前翻小页。空页（历史缺口）按
    /// 页宽线性跳过，序列号单调递减保证终止。
    async fn fetch_by_seq_paged(
        &self,
        peer: &Peer,
        filter: &MessageFilter,
        start_seq: Option<&str>,
    ) -> Result<BatchFetchResult, FetchError> {
        let mut anchor = start_seq.and_then(|seq| seq.parse::<i64>().ok());
        if let Some(anchor) = anchor {
            if anchor <= 0 {
                return Ok(BatchFetchResult::default());
            }
        }

        // issue #666：首批先把锚点定位到 endTime 附近，避免从最新消息一路翻页
        // 回到时间范围上界（活跃大群会因此多做数万次原生历史查询，把 QQ
        // Worker 的内存推到崩溃并出现 recv failed 10054）。
        if anchor.is_none() {
            anchor = self.seek_anchor_for_end_time(peer, filter).await;
        }

        let api = Arc::clone(&self.api);
        let peer_clone = peer.clone();
        // 记录实际发出的分页大小：超时重试可能缩小 batchSize，成功后的
        // 恢复逻辑也可能放大它，事后从 state 读回的值不一定等于本次请求值。
        let requested_page = Arc::new(AtomicI64::new(1));
        let requested_page_in_call = Arc::clone(&requested_page);
        let result = self
            .call_with_retry(move |batch_size| {
                let api = Arc::clone(&api);
                let peer = peer_clone.clone();
                let requested_page = Arc::clone(&requested_page_in_call);
                async move {
                    let page = seq_page_size(batch_size);
                    requested_page.store(page, Ordering::SeqCst);
                    let value = match anchor {
                        None => {
                            tracing::info!(
                                "[BatchMessageFetcher] 调用 getAioFirstViewLatestMsgs API (seq 分页首批), count={page}"
                            );
                            api.get_aio_first_view_latest_msgs(&peer, page).await?
                        }
                        Some(anchor) => {
                            tracing::info!(
                                "[BatchMessageFetcher] 调用 getMsgsBySeqAndCount API, anchorSeq={anchor}, count={page}"
                            );
                            api.get_msgs_by_seq_and_count(&peer, anchor, page).await?
                        }
                    };
                    // issue #716：原生错误（GeneralCallResult.result != 0）与无效
                    // 响应必须显式失败，不能再像空页一样按页宽跳过而静默丢消息。
                    // issue #723：校验必须在 `call_with_retry` 内完成，否则瞬时
                    // 的 2004007（原生重试耗尽）会绕过重试直接终止导出。
                    if is_history_exhausted_result(&value) {
                        Ok(None)
                    } else {
                        extract_msg_page_strict(&value).map(Some)
                    }
                }
            })
            .await?;

        let page = requested_page.load(Ordering::SeqCst).max(1);
        let mut messages = match result {
            Some(messages) => messages,
            None => self.recover_history_start_page(peer, anchor, page).await?,
        };

        // issue #716：满页时页尾边界 msgSeq 可能只返回了部分记录（count 限制
        // 的是记录条数，同一 msgSeq 可混有系统提示与普通消息）。先做包含式
        // 补读，再允许锚点推进到 min(msgSeq) - 1。
        let mut backfilled_boundary = None;
        if messages.len() as i64 >= page {
            if let Some(backfill) = self.backfill_seq_boundary(peer, &messages).await? {
                messages.extend(backfill.messages);
                backfilled_boundary = Some(backfill.boundary_seq);
            }
        }

        let mut batch =
            process_seq_page_messages(messages, filter, anchor, page, backfilled_boundary);
        batch.messages = apply_client_side_filter(batch.messages, filter);
        batch.actual_count = batch.messages.len();
        Ok(batch)
    }

    /// issue #728：收缩跨越历史起点的查询窗口，尽量取回剩余消息。
    async fn recover_history_start_page(
        &self,
        peer: &Peer,
        anchor: Option<i64>,
        page: i64,
    ) -> Result<Vec<Value>, FetchError> {
        let Some(anchor) = anchor else {
            return Ok(Vec::new());
        };
        let mut count = page.max(1) / 2;
        while count >= 1 {
            if self.is_cancelled() {
                return Err(FetchError::Cancelled);
            }
            let api = Arc::clone(&self.api);
            let peer = peer.clone();
            let recovered = self
                .call_with_retry(move |_| {
                    let api = Arc::clone(&api);
                    let peer = peer.clone();
                    async move {
                        let value = api.get_msgs_by_seq_and_count(&peer, anchor, count).await?;
                        if is_history_exhausted_result(&value) {
                            Ok(None)
                        } else {
                            extract_msg_page_strict(&value).map(Some)
                        }
                    }
                })
                .await?;
            if let Some(messages) = recovered {
                return Ok(messages);
            }
            count /= 2;
        }
        Ok(Vec::new())
    }

    /// 基于时间的顺序获取策略。
    async fn fetch_by_time_based_sequential(
        &self,
        peer: &Peer,
        filter: &MessageFilter,
        start_message_id: Option<&str>,
    ) -> Result<BatchFetchResult, FetchError> {
        tracing::info!(
            "[BatchMessageFetcher] 时间筛选参数: 原始={:?}-{:?}",
            filter.start_time,
            filter.end_time
        );

        let api = Arc::clone(&self.api);
        let peer_clone = peer.clone();
        let start_message_id_owned = start_message_id.map(ToString::to_string);
        let result = self
            .call_with_retry(move |batch_size| {
                let api = Arc::clone(&api);
                let peer = peer_clone.clone();
                let start_message_id = start_message_id_owned.clone();
                async move {
                    match start_message_id {
                        None => {
                            tracing::info!(
                                "[BatchMessageFetcher] 调用 getAioFirstViewLatestMsgs API, count={batch_size}"
                            );
                            api.get_aio_first_view_latest_msgs(&peer, batch_size).await
                        }
                        Some(msg_id) => {
                            tracing::info!(
                                "[BatchMessageFetcher] 调用 getMsgHistory API, msgId={msg_id}, count={batch_size}"
                            );
                            api.get_msg_history(&peer, &msg_id, batch_size).await
                        }
                    }
                }
            })
            .await?;

        let mut batch = process_api_result(result, Some(filter), start_message_id);
        batch.messages = apply_client_side_filter(batch.messages, filter);
        batch.actual_count = batch.messages.len();
        Ok(batch)
    }

    /// 基于序列号范围的获取策略。
    async fn fetch_by_sequence_range(
        &self,
        peer: &Peer,
        filter: &MessageFilter,
        start_seq: Option<&str>,
    ) -> Result<BatchFetchResult, FetchError> {
        let start_seq = match start_seq {
            Some(seq) => seq.to_string(),
            None => {
                let mut latest = self
                    .api
                    .get_aio_first_view_latest_msgs(peer, 1)
                    .await
                    .map_err(FetchError::Api)?;
                let first = latest
                    .get_mut("msgList")
                    .and_then(Value::as_array_mut)
                    .and_then(|messages| {
                        if messages.is_empty() {
                            None
                        } else {
                            Some(messages.remove(0))
                        }
                    });
                let Some(first) = first else {
                    return Ok(BatchFetchResult::default());
                };
                first
                    .get("msgSeq")
                    .and_then(Value::as_str)
                    .unwrap_or("0")
                    .to_string()
            }
        };

        let batch_size = { self.state.lock().await.batch_size };
        let start_seq_num = start_seq.parse::<i64>().unwrap_or(0);
        let end_seq = (start_seq_num - batch_size).max(0).to_string();

        let api = Arc::clone(&self.api);
        let peer_clone = peer.clone();
        let start_seq_owned = start_seq.clone();
        let result = self
            .call_with_retry(move |_batch_size| {
                let api = Arc::clone(&api);
                let peer = peer_clone.clone();
                let end_seq = end_seq.clone();
                let start_seq = start_seq_owned.clone();
                async move { api.get_msgs_by_seq_range(&peer, &end_seq, &start_seq).await }
            })
            .await?;

        let mut batch = process_api_result(result, Some(filter), None);
        batch.messages = apply_client_side_filter(batch.messages, filter);
        batch.actual_count = batch.messages.len();
        Ok(batch)
    }

    /// 混合策略：根据筛选复杂度动态选择 API。
    async fn fetch_by_hybrid_strategy(
        &self,
        peer: &Peer,
        filter: &MessageFilter,
        start_message_id: Option<&str>,
        start_seq: Option<&str>,
    ) -> Result<BatchFetchResult, FetchError> {
        let has_complex_filter = filter
            .sender_uids
            .as_ref()
            .is_some_and(|uids| !uids.is_empty())
            || filter
                .message_types
                .as_ref()
                .is_some_and(|types| !types.is_empty())
            || filter
                .keywords
                .as_ref()
                .is_some_and(|keywords| !keywords.is_empty());
        if has_complex_filter {
            self.fetch_by_time_based_sequential(peer, filter, start_message_id)
                .await
        } else {
            self.fetch_by_sequence_range(peer, filter, start_seq).await
        }
    }

    /// 带重试的 API 调用。
    ///
    /// issue #305 / #316：超时类错误下次重试自动折半 batchSize（不低于
    /// [`MIN_BATCH_SIZE_ON_TIMEOUT`]），让 QQ 客户端有机会用更小窗口完成查询。
    async fn call_with_retry<F, Fut, T>(&self, api_call: F) -> Result<T, FetchError>
    where
        F: Fn(i64) -> Fut,
        Fut: std::future::Future<Output = Result<T, String>>,
    {
        let mut bridge_recoveries = 0_u32;
        let mut attempt = 0_u32;

        loop {
            if self.is_cancelled() {
                return Err(FetchError::Cancelled);
            }
            let batch_size = { self.state.lock().await.batch_size };
            tracing::info!(
                "[BatchMessageFetcher] 开始API调用 (尝试 {}/{}) batchSize={batch_size}",
                attempt + 1,
                self.config.retry_count + 1
            );

            // issue #634：进程级串行门控，避免多个重量级历史查询同时进入 Worker。
            let gate_permit = history_fetch_gate()
                .acquire()
                .await
                .map_err(|_| FetchError::Api("历史查询门控已关闭".to_string()))?;
            let call_result = tokio::time::timeout(
                Duration::from_millis(self.config.timeout_ms),
                api_call(batch_size),
            )
            .await;
            drop(gate_permit);
            let result = match call_result {
                Ok(Ok(value)) => {
                    tracing::info!("[BatchMessageFetcher] API调用成功");
                    let mut state = self.state.lock().await;
                    state.stats.consecutive_failures = 0;
                    // issue #305 / #316 的反向路径：缩小后的 batchSize 在连续成功后
                    // 逐步翻倍回升，避免一次超时让整个任务全程使用小批次。
                    if state.batch_size < self.config.batch_size {
                        state.consecutive_successes += 1;
                        if state.consecutive_successes >= BATCH_SIZE_RECOVERY_SUCCESSES {
                            let previous = state.batch_size;
                            let next = (previous * 2).min(self.config.batch_size);
                            state.batch_size = next;
                            state.consecutive_successes = 0;
                            tracing::info!(
                                "[BatchMessageFetcher] 连续成功，batchSize 回升: {previous} -> {next}"
                            );
                        }
                    } else {
                        state.consecutive_successes = 0;
                    }
                    return Ok(value);
                }
                Ok(Err(message)) => FetchError::Api(message),
                Err(_) => FetchError::Timeout(self.config.timeout_ms),
            };

            tracing::warn!(
                "[BatchMessageFetcher] API调用失败 (尝试 {}/{}): {result}",
                attempt + 1,
                self.config.retry_count + 1
            );
            {
                let mut state = self.state.lock().await;
                state.stats.consecutive_failures += 1;
            }

            if result.is_not_group_member() {
                return Err(result);
            }

            // issue #634：bridge / Worker 崩溃属于可恢复状态：等待 NapCat 重启
            // 并重新登录后从当前游标继续，不计入普通重试次数。
            if result.is_bridge_down() {
                if bridge_recoveries >= MAX_BRIDGE_RECOVERIES {
                    return Err(result);
                }
                bridge_recoveries += 1;
                self.shrink_batch_size().await;
                tracing::warn!(
                    "[BatchMessageFetcher] bridge 不可用（Worker 可能崩溃/重启），等待恢复后继续 (第 {bridge_recoveries}/{MAX_BRIDGE_RECOVERIES} 次)"
                );
                if !self.wait_for_bridge_recovery().await {
                    return Err(result);
                }
                continue;
            }

            if attempt >= self.config.retry_count {
                return Err(result);
            }
            attempt += 1;

            // issue #305 / #316：超时类错误下次重试用更小的 batchSize。
            // issue #634：本地超时不代表 Worker 内旧查询已结束，先冷却再等
            // bridge 健康，避免重量级查询在 Worker 内叠加。
            // issue #723：原生 Worker 重试耗尽（2004007）同样需要冷却，
            // 否则三次 1s/2s/3s 的普通重试会再次撞上同一限制。
            if result.is_timeout() || result.is_native_retry_exhausted() {
                self.shrink_batch_size().await;
                let cooldown =
                    TIMEOUT_COOLDOWN_MS.max(self.config.retry_interval_ms * u64::from(attempt));
                let reason = if result.is_timeout() {
                    "超时"
                } else {
                    "原生重试耗尽"
                };
                tracing::info!("[BatchMessageFetcher] {reason}冷却 {cooldown}ms 后重试");
                tokio::time::sleep(Duration::from_millis(cooldown)).await;
                if !self.wait_for_bridge_recovery().await {
                    return Err(result);
                }
            } else {
                let retry_delay = self.config.retry_interval_ms * u64::from(attempt);
                tracing::info!("[BatchMessageFetcher] 等待 {retry_delay}ms 后重试");
                tokio::time::sleep(Duration::from_millis(retry_delay)).await;
            }
        }
    }

    /// 超时 / 崩溃后自适应缩小 batchSize（不低于 [`MIN_BATCH_SIZE_ON_TIMEOUT`]）。
    async fn shrink_batch_size(&self) {
        let mut state = self.state.lock().await;
        state.consecutive_successes = 0;
        if state.batch_size > MIN_BATCH_SIZE_ON_TIMEOUT {
            let previous = state.batch_size;
            let next = (previous / 2).max(MIN_BATCH_SIZE_ON_TIMEOUT);
            state.batch_size = next;
            tracing::warn!("[BatchMessageFetcher] 自适应缩小 batchSize: {previous} -> {next}");
        }
    }

    /// 等待 bridge / Worker 恢复健康；被取消或超出等待上限时返回 `false`。
    async fn wait_for_bridge_recovery(&self) -> bool {
        let deadline = std::time::Instant::now() + Duration::from_millis(BRIDGE_RECOVERY_WAIT_MS);
        loop {
            if self.is_cancelled() {
                return false;
            }
            if self.api.bridge_healthy().await {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                tracing::error!(
                    "[BatchMessageFetcher] bridge 在 {BRIDGE_RECOVERY_WAIT_MS}ms 内未恢复"
                );
                return false;
            }
            tracing::info!("[BatchMessageFetcher] 等待 bridge 恢复...");
            tokio::time::sleep(Duration::from_millis(BRIDGE_PROBE_INTERVAL_MS)).await;
        }
    }

    /// 更新统计信息。
    async fn update_stats(&self, success: bool, response_time_ms: i64) {
        let mut state = self.state.lock().await;
        let stats = &mut state.stats;
        stats.call_count += 1;
        if success {
            stats.success_count += 1;
            stats.average_response_time = (stats.average_response_time
                * (stats.success_count - 1) as f64
                + response_time_ms as f64)
                / stats.success_count as f64;
        } else {
            stats.failure_count += 1;
        }
    }

    /// issue #716：执行一次边界补读请求，返回（消息列表, 实际请求条数）。
    ///
    /// 复用 `call_with_retry` 的取消检查、超时、bridge 恢复与有界重试；
    /// 响应经 `extract_msg_page_strict` 校验，原生错误 / 无效响应显式失败。
    /// 拉取条数跟随自适应 batchSize，但不小于 [`SEQ_BOUNDARY_BACKFILL_MIN_COUNT`]
    /// 以保证足够窗口判断边界 msgSeq 是否取完。
    async fn seq_backfill_fetch(
        &self,
        peer: &Peer,
        query: SeqBackfillQuery<'_>,
    ) -> Result<(Vec<Value>, i64), FetchError> {
        let api = Arc::clone(&self.api);
        let peer = peer.clone();
        self.call_with_retry(move |batch_size| {
            let api = Arc::clone(&api);
            let peer = peer.clone();
            async move {
                let count = seq_page_size(batch_size)
                    .clamp(SEQ_BOUNDARY_BACKFILL_MIN_COUNT, GROUP_SEQ_PAGE_MAX);
                let value = match query {
                    SeqBackfillQuery::Seq(seq) => {
                        api.get_msgs_by_seq_and_count(&peer, seq, count).await?
                    }
                    SeqBackfillQuery::MsgId(msg_id) => {
                        api.get_msg_history(&peer, msg_id, count).await?
                    }
                };
                // issue #723：与分页请求一样，原生 2004007 必须在重试预算内
                // 冷却后重试，而不是把整段补读直接判死。
                let messages = if is_history_exhausted_result(&value) {
                    Vec::new()
                } else {
                    extract_msg_page_strict(&value)?
                };
                Ok((messages, count.max(1)))
            }
        })
        .await
    }

    /// issue #716：满页边界 msgSeq 的包含式补读。
    ///
    /// `count` 只限制返回记录条数，同一 msgSeq 下可混有系统提示与普通消息；
    /// 页尾同一 msgSeq 可能只返回了部分记录。直接把下一锚点设为
    /// `min(msgSeq) - 1` 会永久跳过该 seq 剩余记录（含普通消息），而导出仍
    /// 报告成功。
    ///
    /// 因此满页时先对页内最小 msgSeq 做一次包含式重读，只补入 msgSeq 等于
    /// 边界且 msgId 未见过的记录；若边界 seq 本身超过一个窗口（重读仍是满页
    /// 且全部为该 seq），再用最旧边界记录的 msgId 向旧方向翻页，直到越过该
    /// seq 或到达尾部。补读完成后才允许锚点推进到 `boundary - 1`。
    ///
    /// 返回 `Ok(Some(..))` 表示边界已补齐；无 msgSeq 可读时返回 `Ok(None)`。
    /// 任何原生错误、无效响应、游标不前进或超过
    /// [`SEQ_BOUNDARY_BACKFILL_MAX_REQUESTS`] 都显式失败。
    async fn backfill_seq_boundary(
        &self,
        peer: &Peer,
        page_messages: &[Value],
    ) -> Result<Option<SeqBoundaryBackfill>, FetchError> {
        let Some(boundary_seq) = page_messages.iter().filter_map(loose_msg_seq).min() else {
            return Ok(None);
        };

        // 页内已见记录的去重键（msgId 优先，缺失时退化为整条 JSON）。
        let mut seen: HashSet<String> = page_messages.iter().map(message_dedupe_key).collect();
        // 边界 seq 下已收集的全部记录（页内 + 补读），用于挑选 msgId 游标。
        let mut boundary_msgs: Vec<Value> = page_messages
            .iter()
            .filter(|msg| loose_msg_seq(msg) == Some(boundary_seq))
            .cloned()
            .collect();
        let mut recovered: Vec<Value> = Vec::new();
        let mut requests = 0_u32;

        // 第一步：包含式重读边界序号。重读结果里出现更小的 msgSeq，或未取满，
        // 都说明边界 seq 的记录已全部返回。
        requests += 1;
        let (reread, used) = self
            .seq_backfill_fetch(peer, SeqBackfillQuery::Seq(boundary_seq))
            .await?;
        let passed_boundary = collect_boundary_messages(
            &reread,
            boundary_seq,
            &mut seen,
            &mut boundary_msgs,
            &mut recovered,
        );
        if passed_boundary || (reread.len() as i64) < used {
            tracing::info!(
                "[BatchMessageFetcher] issue #716：边界 msgSeq={boundary_seq} 补读完成，补入 {} 条",
                recovered.len()
            );
            return Ok(Some(SeqBoundaryBackfill {
                boundary_seq,
                messages: recovered,
            }));
        }

        // 第二步：边界 seq 本身超过一个窗口（重读页全是边界 seq 记录）。
        // 不能只重复请求同一个 seq 锚点（会原地打转），改用最旧边界记录的
        // msgId 向旧方向翻页，直到越过该 seq 或到达尾部。
        loop {
            if requests >= SEQ_BOUNDARY_BACKFILL_MAX_REQUESTS {
                return Err(FetchError::SequenceBackfillExhausted {
                    msg_seq: boundary_seq,
                    max_requests: SEQ_BOUNDARY_BACKFILL_MAX_REQUESTS,
                });
            }
            if self.is_cancelled() {
                return Err(FetchError::Cancelled);
            }
            let Some(cursor_id) = oldest_boundary_msg_id(&boundary_msgs) else {
                return Err(FetchError::Api(format!(
                    "边界 msgSeq={boundary_seq} 的补读记录缺少 msgId，无法继续翻页"
                )));
            };
            requests += 1;
            tracing::info!(
                "[BatchMessageFetcher] issue #716：边界 msgSeq={boundary_seq} 占满整页，按 msgId={cursor_id} 向旧方向补读 (第 {requests}/{SEQ_BOUNDARY_BACKFILL_MAX_REQUESTS} 次)"
            );
            let (history, _used) = self
                .seq_backfill_fetch(peer, SeqBackfillQuery::MsgId(cursor_id.as_str()))
                .await?;
            let passed = collect_boundary_messages(
                &history,
                boundary_seq,
                &mut seen,
                &mut boundary_msgs,
                &mut recovered,
            );
            // 越过边界 seq 或到达空页（历史尾部）⇒ 边界已补齐。
            if passed || history.is_empty() {
                break;
            }
            // 游标必须前进：最旧边界记录的 msgId 发生变化，否则会原地打转，
            // 必须显式失败而不是无限重试或静默跳过。
            let Some(next_cursor) = oldest_boundary_msg_id(&boundary_msgs) else {
                return Err(FetchError::Api(format!(
                    "边界 msgSeq={boundary_seq} 的补读记录缺少 msgId，无法继续翻页"
                )));
            };
            if next_cursor == cursor_id {
                return Err(FetchError::Api(format!(
                    "边界 msgSeq={boundary_seq} 补读游标未前进（msgId={cursor_id}），拒绝继续跳过"
                )));
            }
        }

        tracing::info!(
            "[BatchMessageFetcher] issue #716：边界 msgSeq={boundary_seq} msgId 游标补读完成，累计补入 {} 条",
            recovered.len()
        );
        Ok(Some(SeqBoundaryBackfill {
            boundary_seq,
            messages: recovered,
        }))
    }
}

/// 序列分页的每页大小：跟随自适应 batchSize，但不超过 [`GROUP_SEQ_PAGE_MAX`]。
fn seq_page_size(batch_size: i64) -> i64 {
    batch_size.clamp(1, GROUP_SEQ_PAGE_MAX)
}

/// 宽松读取 msgSeq（可能是字符串或数字）。
fn loose_msg_seq(msg: &Value) -> Option<i64> {
    match msg.get("msgSeq") {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.trim().parse::<i64>().ok(),
        _ => None,
    }
}

/// 从 API 响应中提取消息数组（兼容 `result.msgList` / `msgList` 两种包装）。
fn extract_msg_list(api_result: Value) -> Vec<Value> {
    match api_result {
        Value::Object(mut root) => {
            if let Some(Value::Object(result)) = root.get_mut("result") {
                if let Some(Value::Array(messages)) = result.remove("msgList") {
                    return messages;
                }
            }
            match root.remove("msgList") {
                Some(Value::Array(messages)) => messages,
                _ => Vec::new(),
            }
        }
        _ => Vec::new(),
    }
}

/// issue #716 边界补读的查询锚点：按 seq 重读边界，或按 msgId 向旧方向翻页。
#[derive(Debug, Clone, Copy)]
enum SeqBackfillQuery<'a> {
    /// `getMsgsBySeqAndCount`：包含式重读边界序号。
    Seq(i64),
    /// `getMsgHistory` / `getMsgsIncludeSelf`：从最旧边界记录向旧方向翻页。
    MsgId(&'a str),
}

/// issue #716：边界 msgSeq 补读的结果。
struct SeqBoundaryBackfill {
    /// 已补齐的边界 msgSeq（用于把 next_anchor 精确推进到 boundary - 1）。
    boundary_seq: i64,
    /// 补入的消息（全部 msgSeq == boundary_seq 且 msgId 未见过）。
    messages: Vec<Value>,
}

/// 从序列分页响应中提取消息列表，并拒绝把原生错误 / 无效响应当成空页。
///
/// NapCat 原生调用返回 `GeneralCallResult { result: 0 | -1, errMsg }`，
/// 业务错误不抛异常而是 `result = -1`；`msgList` 可能在顶层或包一层
/// `result` 对象内（与 `extract_msg_list` 的两种形状一致）。
fn is_history_exhausted_result(api_result: &Value) -> bool {
    api_result.get("result").and_then(Value::as_i64) == Some(2004000)
}

fn extract_msg_page_strict(api_result: &Value) -> Result<Vec<Value>, String> {
    if let Some(code) = api_result.get("result").and_then(Value::as_i64) {
        if code != 0 {
            let detail = api_result
                .get("errMsg")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            return Err(format!("原生消息查询失败 result={code}: {detail}"));
        }
    }
    let candidates = [
        api_result.get("msgList"),
        api_result.get("result").and_then(|r| r.get("msgList")),
        api_result.get("messages"),
        api_result.get("result").and_then(|r| r.get("messages")),
    ];
    let Some(candidate) = candidates.into_iter().flatten().next() else {
        return Err("响应缺少 msgList".to_string());
    };
    match candidate {
        Value::Array(items) => Ok(items.clone()),
        Value::Null => Ok(Vec::new()),
        _ => Err("msgList 字段不是数组".to_string()),
    }
}

/// msgId 去重键：优先原生 msgId，缺失时退化为整条 JSON（与 sequence_integrity 一致）。
fn message_dedupe_key(msg: &Value) -> String {
    message_msg_id(msg).map_or_else(
        || serde_json::to_string(msg).unwrap_or_else(|_| "null".to_string()),
        |id| format!("id:{id}"),
    )
}

/// 读取原生 msgId（字符串）。
fn message_msg_id(msg: &Value) -> Option<String> {
    raw_msg_id(msg).map(ToString::to_string)
}

/// 读取原生 msgId，不复制字符串。
fn raw_msg_id(msg: &Value) -> Option<&str> {
    msg.get("msgId")
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
}

/// 边界 seq 下最旧记录的 msgId（msgTime 最小者），作为 msgId 游标。
fn oldest_boundary_msg_id(boundary_msgs: &[Value]) -> Option<String> {
    boundary_msgs
        .iter()
        .filter_map(|msg| {
            let id = message_msg_id(msg)?;
            let time = loose_msg_time(msg).map_or(i64::MAX, to_millis);
            Some((time, id))
        })
        .min_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)))
        .map(|(_, id)| id)
}

/// issue #716：把补读响应中的记录并入边界集合。
///
/// 只保留 msgSeq == boundary_seq 且去重键未见过的记录；返回响应中是否出现了
/// msgSeq < boundary_seq（表示已越过边界 seq）。
fn collect_boundary_messages(
    response: &[Value],
    boundary_seq: i64,
    seen: &mut HashSet<String>,
    boundary_msgs: &mut Vec<Value>,
    recovered: &mut Vec<Value>,
) -> bool {
    let mut passed_boundary = false;
    for msg in response {
        let Some(seq) = loose_msg_seq(msg) else {
            continue;
        };
        if seq < boundary_seq {
            passed_boundary = true;
        } else if seq == boundary_seq && seen.insert(message_dedupe_key(msg)) {
            boundary_msgs.push(msg.clone());
            recovered.push(msg.clone());
        }
    }
    passed_boundary
}

/// issue #634 / #716：处理序列分页的消息列表。
///
/// `backfilled_boundary` 为 `Some(seq)` 时表示该页边界 msgSeq 已完成包含式
/// 补读；此时即使 `min(msgSeq) == anchor`（整页同 seq）也只需退到
/// `boundary - 1`，不能按页宽跳过（会漏掉 boundary-page..boundary-1 的消息）。
fn process_seq_page_messages(
    messages: Vec<Value>,
    filter: &MessageFilter,
    anchor: Option<i64>,
    page: i64,
    backfilled_boundary: Option<i64>,
) -> BatchFetchResult {
    let min_seq = messages.iter().filter_map(loose_msg_seq).min();
    let mut earliest_msg_time = messages
        .iter()
        .filter_map(loose_msg_time)
        .min()
        .map(to_millis);
    if messages.is_empty() {
        earliest_msg_time = None;
    }

    let next_anchor = match (min_seq, anchor) {
        (Some(min_seq), Some(anchor)) if min_seq < anchor => min_seq - 1,
        (Some(min_seq), None) => min_seq - 1,
        // issue #716：边界 seq 已补齐时，即使 min_seq == anchor（整页同 seq），
        // 也只需退到 boundary - 1；按页宽跳过会漏掉 boundary-page..boundary-1
        // 区间内的消息。min_seq > anchor 的异常形状仍走缺口跳过兜底。
        (Some(min_seq), Some(anchor))
            if min_seq == anchor && backfilled_boundary == Some(min_seq) =>
        {
            min_seq - 1
        }
        // 空页或未前进：按页宽跳过缺口。首批（无锚点）空结果视为空会话。
        (_, Some(anchor)) => anchor - page.max(1),
        (None, None) => 0,
    };

    let mut has_more = next_anchor > 0 && !(anchor.is_none() && messages.is_empty());

    // 早停：最早时间早于筛选开始时间。
    if let (Some(earliest), Some(start_time)) = (earliest_msg_time, filter.start_time) {
        if earliest < start_time {
            tracing::info!(
                "[BatchMessageFetcher] 早停：earliestMsgTime={earliest} < startTime={start_time}，停止继续获取"
            );
            has_more = false;
        }
    }

    let next_seq = has_more.then(|| next_anchor.to_string());
    tracing::info!(
        "[BatchMessageFetcher] 序列分页结果: {} 条消息, anchor={anchor:?}, nextSeq={next_seq:?}, hasMore={has_more}",
        messages.len()
    );

    let actual_count = messages.len();
    BatchFetchResult {
        messages,
        has_more,
        next_message_id: None,
        next_seq,
        local_cursor: None,
        actual_count,
        fetch_time_ms: 0,
        earliest_msg_time,
    }
}

/// issue #634：处理序列分页的 API 结果（测试用宽松提取包装）。
#[cfg(test)]
fn process_seq_page_result(
    api_result: Value,
    filter: &MessageFilter,
    anchor: Option<i64>,
    page: i64,
) -> BatchFetchResult {
    process_seq_page_messages(extract_msg_list(api_result), filter, anchor, page, None)
}

/// 处理 API 调用结果，统一格式化。
fn process_api_result(
    api_result: Value,
    filter: Option<&MessageFilter>,
    current_message_id: Option<&str>,
) -> BatchFetchResult {
    let messages = extract_msg_list(api_result);

    let mut has_more = !messages.is_empty();
    let mut next_message_id: Option<String> = None;
    let mut next_seq: Option<String> = None;
    let mut earliest_msg_time: Option<i64> = None;

    if !messages.is_empty() {
        // 取时间最早的消息作为下一次查询的起点。
        let mut earliest = &messages[0];
        for msg in &messages {
            let msg_time = loose_msg_time(msg);
            let earliest_time = loose_msg_time(earliest);
            if msg_time.is_some() && (earliest_time.is_none() || msg_time < earliest_time) {
                earliest = msg;
            }
        }
        next_message_id = earliest
            .get("msgId")
            .and_then(Value::as_str)
            .map(ToString::to_string);
        next_seq = loose_msg_seq(earliest).map(|seq| seq.to_string());
        if let Some(raw_time) = loose_msg_time(earliest) {
            earliest_msg_time = Some(to_millis(raw_time));
        }
        // 防止无限循环：返回了与当前查询起点相同的消息。
        if let (Some(current), Some(next)) = (current_message_id, next_message_id.as_deref()) {
            if current == next {
                has_more = false;
                next_message_id = None;
                next_seq = None;
            }
        }
    }

    // 早停：最早时间早于筛选开始时间。
    if let (Some(earliest), Some(start_time)) = (
        earliest_msg_time,
        filter.and_then(|filter| filter.start_time),
    ) {
        if earliest < start_time {
            tracing::info!(
                "[BatchMessageFetcher] 早停：earliestMsgTime={earliest} < startTime={start_time}，停止继续获取"
            );
            has_more = false;
            next_message_id = None;
            next_seq = None;
        }
    }

    tracing::info!(
        "[BatchMessageFetcher] 处理结果: {} 条消息, hasMore={has_more}, nextMessageId={next_message_id:?}, earliestMsgTime={earliest_msg_time:?}",
        messages.len()
    );

    let actual_count = messages.len();
    BatchFetchResult {
        messages,
        has_more,
        next_message_id,
        next_seq,
        local_cursor: None,
        actual_count,
        fetch_time_ms: 0,
        earliest_msg_time,
    }
}

/// 客户端筛选。
fn apply_client_side_filter(messages: Vec<Value>, filter: &MessageFilter) -> Vec<Value> {
    let input_count = messages.len();
    let mut filtered = messages;

    // 时间筛选（秒级时间戳自动转毫秒）。
    if filter.start_time.is_some() || filter.end_time.is_some() {
        filtered.retain(|msg| {
            let Some(raw_time) = loose_msg_time(msg) else {
                return false;
            };
            let msg_time = to_millis(raw_time);
            filter.start_time.is_none_or(|start| msg_time >= start)
                && filter.end_time.is_none_or(|end| msg_time <= end)
        });
    }

    // 发送者筛选。
    if let Some(sender_uids) = filter.sender_uids.as_ref().filter(|uids| !uids.is_empty()) {
        filtered.retain(|msg| {
            let sender = msg
                .get("senderUid")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
                .or_else(|| msg.get("peerUid").and_then(Value::as_str))
                .unwrap_or("");
            sender_uids.iter().any(|uid| uid == sender)
        });
    }

    // 消息类型筛选。
    if let Some(message_types) = filter
        .message_types
        .as_ref()
        .filter(|types| !types.is_empty())
    {
        let allowed: std::collections::HashSet<i64> =
            message_types.iter().map(|mt| mt.r#type).collect();
        filtered.retain(|msg| {
            msg.get("msgType")
                .and_then(Value::as_i64)
                .is_some_and(|t| allowed.contains(&t))
        });
    }

    // 关键词筛选（对 elements JSON 做大小写不敏感包含匹配）。
    if let Some(keywords) = filter.keywords.as_ref().filter(|kw| !kw.is_empty()) {
        let lowered_keywords: Vec<String> = keywords
            .iter()
            .map(|keyword| keyword.to_lowercase())
            .collect();
        filtered.retain(|msg| {
            let content = msg
                .get("elements")
                .map(|elements| elements.to_string().to_lowercase())
                .unwrap_or_default();
            lowered_keywords
                .iter()
                .any(|keyword| content.contains(keyword))
        });
    }

    tracing::info!(
        "[BatchMessageFetcher] 客户端筛选完成，最终输出消息数量: {} (输入: {input_count}, 过滤掉: {})",
        filtered.len(),
        input_count - filtered.len()
    );
    filtered
}

/// 宽松读取 msgTime（可能是字符串或数字）。
fn loose_msg_time(msg: &Value) -> Option<i64> {
    match msg.get("msgTime") {
        Some(Value::Number(n)) => n.as_i64(),
        Some(Value::String(s)) => s.parse::<i64>().ok(),
        _ => None,
    }
}

/// 秒级时间戳（10 位数）自动转毫秒。
fn to_millis(raw_time: i64) -> i64 {
    if raw_time > 1_000_000_000 && raw_time < 10_000_000_000 {
        raw_time * 1000
    } else {
        raw_time
    }
}

/// 当前毫秒时间戳。
fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, VecDeque};
    use std::sync::Mutex;

    use serde_json::json;

    use super::*;

    #[test]
    fn processes_result_wrapped_message_lists() {
        let batch = process_api_result(
            json!({
                "result": {
                    "msgList": [{
                        "msgId": "message-1",
                        "msgSeq": "10",
                        "msgTime": "1783866274"
                    }]
                }
            }),
            None,
            None,
        );

        assert_eq!(batch.actual_count, 1);
        assert_eq!(batch.next_message_id.as_deref(), Some("message-1"));
    }

    struct NoopApi;

    #[async_trait]
    impl MessageFetchApi for NoopApi {
        async fn get_aio_first_view_latest_msgs(
            &self,
            _peer: &Peer,
            _count: i64,
        ) -> Result<Value, String> {
            Ok(json!({ "msgList": [] }))
        }

        async fn get_msg_history(
            &self,
            _peer: &Peer,
            _msg_id: &str,
            _count: i64,
        ) -> Result<Value, String> {
            Ok(json!({ "msgList": [] }))
        }

        async fn get_msgs_by_seq_range(
            &self,
            _peer: &Peer,
            _start_seq: &str,
            _end_seq: &str,
        ) -> Result<Value, String> {
            Ok(json!({ "msgList": [] }))
        }

        async fn get_msgs_by_seq_and_count(
            &self,
            _peer: &Peer,
            _anchor_seq: i64,
            _count: i64,
        ) -> Result<Value, String> {
            Ok(json!({ "msgList": [] }))
        }
    }

    #[tokio::test]
    async fn shrunk_batch_size_recovers_after_consecutive_successes() {
        let fetcher = BatchMessageFetcher::new(Arc::new(NoopApi), BatchFetchConfig::default());
        {
            let mut state = fetcher.state.lock().await;
            state.batch_size = MIN_BATCH_SIZE_ON_TIMEOUT;
        }

        for _ in 0..BATCH_SIZE_RECOVERY_SUCCESSES {
            fetcher
                .call_with_retry(|_batch_size| async { Ok(json!({ "msgList": [] })) })
                .await
                .expect("call succeeds");
        }
        assert_eq!(
            fetcher.state.lock().await.batch_size,
            MIN_BATCH_SIZE_ON_TIMEOUT * 2
        );

        // 回升不超过配置上限。
        {
            let mut state = fetcher.state.lock().await;
            state.batch_size = 4000;
            state.consecutive_successes = 0;
        }
        for _ in 0..BATCH_SIZE_RECOVERY_SUCCESSES {
            fetcher
                .call_with_retry(|_batch_size| async { Ok(json!({ "msgList": [] })) })
                .await
                .expect("call succeeds");
        }
        assert_eq!(fetcher.state.lock().await.batch_size, 5000);
    }

    /// issue #666：合成的连续群聊历史（seq 1..=`latest_seq`，每条相隔 `step_ms`）。
    struct SyntheticHistory {
        latest_seq: i64,
        oldest_time_ms: i64,
        step_ms: i64,
        calls: std::sync::atomic::AtomicU64,
        /// issue #716：锚点探测（count <= SEEK_PROBE_COUNT）的单独计数，
        /// 与正常分页 / 边界补读区分开。
        probe_calls: std::sync::atomic::AtomicU64,
        /// 单条探测落入该闭区间时模拟接口失败（issue #666 现场的超时 / 10054）。
        fail_range: Option<(i64, i64)>,
    }

    impl SyntheticHistory {
        fn new(latest_seq: i64, step_ms: i64) -> Self {
            Self {
                latest_seq,
                // 对齐到整秒：合成消息的 msgTime 是秒级字符串。
                oldest_time_ms: (now_ms() - latest_seq * step_ms) / 1000 * 1000,
                step_ms,
                calls: std::sync::atomic::AtomicU64::new(0),
                probe_calls: std::sync::atomic::AtomicU64::new(0),
                fail_range: None,
            }
        }

        fn with_failing_range(mut self, low: i64, high: i64) -> Self {
            self.fail_range = Some((low, high));
            self
        }

        fn fails_at(&self, anchor_seq: i64) -> bool {
            self.fail_range
                .is_some_and(|(low, high)| (low..=high).contains(&anchor_seq))
        }

        fn time_at(&self, seq: i64) -> i64 {
            self.oldest_time_ms + seq * self.step_ms
        }

        fn page(&self, anchor_seq: i64, count: i64) -> Value {
            self.calls.fetch_add(1, Ordering::SeqCst);
            let messages: Vec<Value> = (1..=anchor_seq.min(self.latest_seq))
                .rev()
                .take(usize::try_from(count.max(1)).unwrap_or(1))
                .map(|seq| {
                    json!({
                        "msgId": format!("m{seq}"),
                        "msgSeq": seq.to_string(),
                        "msgTime": (self.time_at(seq) / 1000).to_string(),
                    })
                })
                .collect();
            json!({ "msgList": messages })
        }

        fn call_count(&self) -> u64 {
            self.calls.load(Ordering::SeqCst)
        }

        /// 锚点探测调用数（issue #716：与分页 / 边界补读区分）。
        fn probe_call_count(&self) -> u64 {
            self.probe_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl MessageFetchApi for SyntheticHistory {
        async fn get_aio_first_view_latest_msgs(
            &self,
            _peer: &Peer,
            count: i64,
        ) -> Result<Value, String> {
            if count <= SEEK_PROBE_COUNT {
                self.probe_calls.fetch_add(1, Ordering::SeqCst);
            }
            Ok(self.page(self.latest_seq, count))
        }

        async fn get_msg_history(
            &self,
            _peer: &Peer,
            _msg_id: &str,
            _count: i64,
        ) -> Result<Value, String> {
            Ok(json!({ "msgList": [] }))
        }

        async fn get_msgs_by_seq_range(
            &self,
            _peer: &Peer,
            _start_seq: &str,
            _end_seq: &str,
        ) -> Result<Value, String> {
            Ok(json!({ "msgList": [] }))
        }

        async fn get_msgs_by_seq_and_count(
            &self,
            _peer: &Peer,
            anchor_seq: i64,
            count: i64,
        ) -> Result<Value, String> {
            if count <= SEEK_PROBE_COUNT {
                self.probe_calls.fetch_add(1, Ordering::SeqCst);
                if self.fails_at(anchor_seq) {
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    return Err("recv failed with error: 10054".to_string());
                }
            }
            Ok(self.page(anchor_seq, count))
        }
    }

    fn group_peer() -> Peer {
        Peer {
            chat_type: 2,
            peer_uid: "group-1".to_string(),
            guild_id: None,
        }
    }

    #[tokio::test]
    async fn first_batch_anchor_seeks_to_end_time_instead_of_walking_from_latest() {
        // 20 万条消息、每条相隔 1 分钟；导出区间只覆盖 seq 1000 附近的一小段。
        let api = Arc::new(SyntheticHistory::new(200_000, 60_000));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            BatchFetchConfig::default(),
        );
        let filter = MessageFilter {
            start_time: Some(api.time_at(900)),
            end_time: Some(api.time_at(1000)),
            ..MessageFilter::default()
        };

        // 像导出流程那样把分页跑完。
        let mut previous = None;
        let mut seqs: Vec<i64> = Vec::new();
        let mut batches = 0;
        while let Some(batch) = fetcher
            .fetch_next_batch(&group_peer(), &filter, previous.as_ref())
            .await
            .expect("batch")
        {
            seqs.extend(batch.messages.iter().filter_map(loose_msg_seq));
            batches += 1;
            assert!(batches < 20, "分页未按预期收敛");
            previous = Some(batch);
        }

        // 区间内消息全部到位，且不包含区间之外的消息。
        seqs.sort_unstable();
        assert_eq!(seqs, (900..=1000).collect::<Vec<i64>>());
        // 定位成本是对数级：远小于线性回溯所需的约 400 页。
        assert!(
            api.call_count() <= u64::from(SEEK_MAX_PROBES) + 5,
            "锚点定位调用次数过多: {}",
            api.call_count()
        );
    }

    /// issue #666：探测失败不得被当成「目标在更高序列」的证据——否则二分
    /// 会把下界推过目标区间，整段历史消息会静默丢失。
    #[tokio::test]
    async fn anchor_seek_does_not_lose_messages_when_probes_fail() {
        // 让目标区间（900..=1000）附近的探测全部失败。
        let api = Arc::new(SyntheticHistory::new(20_000, 60_000).with_failing_range(300, 2_000));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            BatchFetchConfig {
                retry_count: 0,
                retry_interval_ms: 0,
                ..BatchFetchConfig::default()
            },
        );
        let filter = MessageFilter {
            start_time: Some(api.time_at(400)),
            end_time: Some(api.time_at(500)),
            ..MessageFilter::default()
        };

        let mut previous = None;
        let mut seqs: Vec<i64> = Vec::new();
        let mut batches = 0;
        while let Some(batch) = fetcher
            .fetch_next_batch(&group_peer(), &filter, previous.as_ref())
            .await
            .expect("batch")
        {
            seqs.extend(batch.messages.iter().filter_map(loose_msg_seq));
            batches += 1;
            assert!(batches < 4_000, "分页未按预期收敛");
            previous = Some(batch);
        }

        seqs.sort_unstable();
        assert_eq!(seqs, (400..=500).collect::<Vec<i64>>());
    }

    #[tokio::test]
    async fn first_batch_skips_anchor_seek_when_end_time_is_recent() {
        let api = Arc::new(SyntheticHistory::new(5_000, 60_000));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            BatchFetchConfig::default(),
        );
        let filter = MessageFilter {
            start_time: Some(api.time_at(1)),
            end_time: Some(now_ms()),
            ..MessageFilter::default()
        };

        fetcher
            .fetch_messages(&group_peer(), &filter, None, None)
            .await
            .expect("first batch");

        // issue #716：满页边界补读会额外产生一次 getMsgsBySeqAndCount 重读，
        // 但绝不能触发锚点定位探测（count=SEEK_PROBE_COUNT 的单条查询）。
        assert_eq!(api.probe_call_count(), 0, "范围到最新消息时不应做锚点定位");
        assert!(
            api.call_count() <= 2,
            "首批最多 1 次分页 + 1 次边界补读，实际 {} 次",
            api.call_count()
        );
    }

    #[test]
    fn seq_page_result_advances_cursor_monotonically() {
        let filter = MessageFilter::default();
        let batch = process_seq_page_result(
            json!({ "msgList": [
                { "msgId": "m3", "msgSeq": "300", "msgTime": "1783866274" },
                { "msgId": "m2", "msgSeq": 250, "msgTime": "1783866270" },
            ] }),
            &filter,
            Some(320),
            100,
        );
        assert!(batch.has_more);
        assert_eq!(batch.next_seq.as_deref(), Some("249"));
    }

    #[test]
    fn seq_page_result_skips_empty_pages_without_looping() {
        let filter = MessageFilter::default();
        // 空页（历史缺口）：按页宽跳过，游标必须前进。
        let batch = process_seq_page_result(json!({ "msgList": [] }), &filter, Some(1000), 200);
        assert!(batch.has_more);
        assert_eq!(batch.next_seq.as_deref(), Some("800"));

        // 序列号未前进（返回的最小 seq >= 锚点）同样按页宽跳过。
        let batch = process_seq_page_result(
            json!({ "msgList": [{ "msgId": "m1", "msgSeq": "1000" }] }),
            &filter,
            Some(1000),
            200,
        );
        assert_eq!(batch.next_seq.as_deref(), Some("800"));

        // 游标触底后终止。
        let batch = process_seq_page_result(json!({ "msgList": [] }), &filter, Some(150), 200);
        assert!(!batch.has_more);
        assert!(batch.next_seq.is_none());
    }

    #[test]
    fn seq_page_result_terminates_on_empty_first_page() {
        let batch = process_seq_page_result(
            json!({ "msgList": [] }),
            &MessageFilter::default(),
            None,
            200,
        );
        assert!(!batch.has_more);
        assert!(batch.next_seq.is_none());
    }

    #[test]
    fn seq_page_result_stops_early_before_start_time() {
        let filter = MessageFilter {
            start_time: Some(1_800_000_000_000),
            ..MessageFilter::default()
        };
        let batch = process_seq_page_result(
            json!({ "msgList": [{ "msgId": "m1", "msgSeq": "500", "msgTime": "1783866274" }] }),
            &filter,
            Some(600),
            200,
        );
        assert!(!batch.has_more);
        assert!(batch.next_seq.is_none());
    }

    #[test]
    fn seq_page_size_is_capped() {
        assert_eq!(seq_page_size(5000), GROUP_SEQ_PAGE_MAX);
        assert_eq!(seq_page_size(200), 200);
        assert_eq!(seq_page_size(0), 1);
    }

    #[test]
    fn bridge_down_errors_are_detected() {
        assert!(FetchError::Api("传输错误: error sending request".to_string()).is_bridge_down());
        assert!(FetchError::Api("Connection refused (os error 111)".to_string()).is_bridge_down());
        assert!(!FetchError::Api("API调用超时".to_string()).is_bridge_down());
        assert!(!FetchError::Timeout(1000).is_bridge_down());
    }

    /// issue #723：QQ Worker 内部重试耗尽（2004007）必须与超时一样冷却重试。
    #[test]
    fn native_retry_exhausted_errors_are_detected() {
        assert!(FetchError::Api(
            "原生消息查询失败 result=-1: 2004007: reach max retry time limit!".to_string()
        )
        .is_native_retry_exhausted());
        assert!(
            !FetchError::Api("原生消息查询失败 result=-1: native boom".to_string())
                .is_native_retry_exhausted()
        );
        assert!(!FetchError::Timeout(1000).is_native_retry_exhausted());
        assert!(FetchError::Timeout(1000).is_timeout());
    }

    /// issue #723：原生 2004007 在 #716 之后会被严格校验拒绝；校验必须发生在
    /// `call_with_retry` 内，否则单页失败会绕过重试预算直接终止导出。
    #[tokio::test(start_paused = true)]
    async fn native_retry_exhausted_page_is_retried_after_cooldown() {
        let entries = vec![
            ("m3".to_string(), 3, 3_000),
            ("m2".to_string(), 2, 2_000),
            ("m1".to_string(), 1, 1_000),
        ];
        let api = Arc::new(SeqBoundaryMock::new(entries));
        api.script_seq(Ok(json!({
            "result": -1,
            "errMsg": "2004007: reach max retry time limit!",
        })));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            BatchFetchConfig {
                batch_size: 5,
                retry_count: 1,
                retry_interval_ms: 0,
                ..BatchFetchConfig::default()
            },
        );

        let started = tokio::time::Instant::now();
        let batch = fetcher
            .fetch_messages(&group_peer(), &MessageFilter::default(), None, Some("3"))
            .await
            .expect("原生重试耗尽后应冷却重试并成功");
        assert_eq!(batch.messages.len(), 3, "重试后应返回同一页的全部消息");
        let seq_calls = api
            .call_log()
            .iter()
            .filter(|call| call.starts_with("seq:"))
            .count();
        assert_eq!(seq_calls, 2, "第一次 2004007 后必须重试同一页");
        assert!(
            started.elapsed() >= Duration::from_millis(TIMEOUT_COOLDOWN_MS),
            "原生重试耗尽必须等待冷却时间"
        );
    }

    #[tokio::test]
    async fn history_gate_serializes_concurrent_calls() {
        use std::sync::atomic::AtomicI64;

        struct ConcurrencyProbe {
            active: AtomicI64,
            max_active: AtomicI64,
        }

        #[async_trait]
        impl MessageFetchApi for ConcurrencyProbe {
            async fn get_aio_first_view_latest_msgs(
                &self,
                _peer: &Peer,
                _count: i64,
            ) -> Result<Value, String> {
                let now = self.active.fetch_add(1, Ordering::SeqCst) + 1;
                self.max_active.fetch_max(now, Ordering::SeqCst);
                tokio::time::sleep(Duration::from_millis(20)).await;
                self.active.fetch_sub(1, Ordering::SeqCst);
                Ok(json!({ "msgList": [] }))
            }

            async fn get_msg_history(
                &self,
                _peer: &Peer,
                _msg_id: &str,
                _count: i64,
            ) -> Result<Value, String> {
                Ok(json!({ "msgList": [] }))
            }

            async fn get_msgs_by_seq_range(
                &self,
                _peer: &Peer,
                _start_seq: &str,
                _end_seq: &str,
            ) -> Result<Value, String> {
                Ok(json!({ "msgList": [] }))
            }

            async fn get_msgs_by_seq_and_count(
                &self,
                _peer: &Peer,
                _anchor_seq: i64,
                _count: i64,
            ) -> Result<Value, String> {
                Ok(json!({ "msgList": [] }))
            }
        }

        let api = Arc::new(ConcurrencyProbe {
            active: AtomicI64::new(0),
            max_active: AtomicI64::new(0),
        });
        // 两个独立 fetcher 实例并发调用，进程级门控必须保证串行。
        let fetcher_a =
            BatchMessageFetcher::new(Arc::clone(&api) as _, BatchFetchConfig::default());
        let fetcher_b =
            BatchMessageFetcher::new(Arc::clone(&api) as _, BatchFetchConfig::default());
        let api_a = Arc::clone(&api);
        let api_b = Arc::clone(&api);
        let peer = Peer {
            chat_type: 2,
            peer_uid: "group".to_string(),
            guild_id: None,
        };
        let peer_a = peer.clone();
        let peer_b = peer;
        let call_a = fetcher_a.call_with_retry(move |count| {
            let api = Arc::clone(&api_a);
            let peer = peer_a.clone();
            async move { api.get_aio_first_view_latest_msgs(&peer, count).await }
        });
        let call_b = fetcher_b.call_with_retry(move |count| {
            let api = Arc::clone(&api_b);
            let peer = peer_b.clone();
            async move { api.get_aio_first_view_latest_msgs(&peer, count).await }
        });
        let (a, b) = tokio::join!(call_a, call_b);
        a.expect("call a succeeds");
        b.expect("call b succeeds");
        assert_eq!(api.max_active.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn nonblocking_history_permit_uses_the_process_gate() {
        let permit = history_fetch_gate()
            .acquire()
            .await
            .expect("history permit");
        assert!(try_history_query_permit().is_err());
        drop(permit);
    }

    #[test]
    fn processes_root_message_lists() {
        let batch = process_api_result(
            json!({
                "msgList": [{
                    "msgId": "message-2",
                    "msgSeq": "11",
                    "msgTime": "1783866275"
                }]
            }),
            None,
            None,
        );

        assert_eq!(batch.actual_count, 1);
        assert_eq!(batch.next_message_id.as_deref(), Some("message-2"));
    }

    /// issue #716：按真实 NapCat 语义模拟的群历史。
    ///
    /// - `getMsgsBySeqAndCount` / `getAioFirstViewLatestMsgs`：count 限制返回
    ///   记录条数（不是 seq 个数），seq 锚点包含，结果从新到旧。
    /// - `getMsgHistory`（`getMsgsIncludeSelf`）：从 msgId 含自身向旧方向返回。
    /// - 响应形状对齐 `GeneralCallResult`：`{ "result": 0, "msgList": [...] }`。
    struct SeqBoundaryMock {
        /// 全量历史，按 (msgSeq, msgTime, msgId) 从新到旧排序。
        records: Vec<Value>,
        /// msgId → records 下标，供 getMsgHistory 定位。
        positions: HashMap<String, usize>,
        calls: Mutex<Vec<String>>,
        /// 依次覆盖 get_msgs_by_seq_and_count 的返回；取完后回落到真实语义。
        scripted_seq: Mutex<VecDeque<Result<Value, String>>>,
        /// 可模拟分页跨多页后重新返回先前页，覆盖真实 QQ NT 循环。
        scripted_local: Mutex<VecDeque<Vec<Value>>>,
        /// get_msg_history 只返回游标自身（模拟游标不前进）。
        stall_history: bool,
        /// 让本地分页错误地重复返回游标本身。
        stall_local_history: bool,
        /// 把每个接口的 msgList 反转返回。
        ///
        /// NapCat TS 层只是透传 `NodeIKernelMsgService`，源码没有记载
        /// `getMsgsBySeqAndCount` / `getMsgsIncludeSelf` 的返回顺序；issue #716
        /// 的实测样例按从新到旧书写，但 LLOneBot 的用法暗示可能相反。分页与
        /// 补读逻辑按集合语义处理（min / 去重 / 全量扫描），必须与返回顺序无关，
        /// 用该开关验证这一点。
        reverse_output: bool,
    }

    impl SeqBoundaryMock {
        /// entries: (msgId, msgSeq, msgTime_ms)。
        fn new(entries: Vec<(String, i64, i64)>) -> Self {
            let mut sorted = entries;
            sorted.sort_by(|a, b| {
                b.1.cmp(&a.1)
                    .then_with(|| b.2.cmp(&a.2))
                    .then_with(|| b.0.cmp(&a.0))
            });
            let records: Vec<Value> = sorted
                .iter()
                .map(|(id, seq, time_ms)| {
                    json!({
                        "msgId": id,
                        "msgSeq": seq.to_string(),
                        "msgTime": (time_ms / 1000).to_string(),
                        "msgType": 2,
                    })
                })
                .collect();
            let positions = records
                .iter()
                .enumerate()
                .filter_map(|(index, msg)| {
                    msg.get("msgId")
                        .and_then(Value::as_str)
                        .map(|id| (id.to_string(), index))
                })
                .collect();
            Self {
                records,
                positions,
                calls: Mutex::new(Vec::new()),
                scripted_seq: Mutex::new(VecDeque::new()),
                scripted_local: Mutex::new(VecDeque::new()),
                stall_history: false,
                stall_local_history: false,
                reverse_output: false,
            }
        }

        fn with_stall_history(mut self) -> Self {
            self.stall_history = true;
            self
        }

        fn with_stalled_local_history(mut self) -> Self {
            self.stall_local_history = true;
            self
        }

        fn with_reversed_output(mut self) -> Self {
            self.reverse_output = true;
            self
        }

        fn script_seq(&self, result: Result<Value, String>) {
            self.scripted_seq.lock().unwrap().push_back(result);
        }

        fn script_local(&self, messages: Vec<Value>) {
            self.scripted_local.lock().unwrap().push_back(messages);
        }

        fn call_log(&self) -> Vec<String> {
            self.calls.lock().unwrap().clone()
        }
    }

    #[async_trait]
    impl MessageFetchApi for SeqBoundaryMock {
        async fn get_aio_first_view_latest_msgs(
            &self,
            _peer: &Peer,
            count: i64,
        ) -> Result<Value, String> {
            self.calls.lock().unwrap().push(format!("latest:{count}"));
            let mut messages: Vec<Value> = self
                .records
                .iter()
                .take(count.max(0) as usize)
                .cloned()
                .collect();
            if self.reverse_output {
                messages.reverse();
            }
            Ok(json!({ "result": 0, "msgList": messages }))
        }

        async fn get_msg_history(
            &self,
            _peer: &Peer,
            msg_id: &str,
            count: i64,
        ) -> Result<Value, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("history:{msg_id}:{count}"));
            let Some(&index) = self.positions.get(msg_id) else {
                return Ok(json!({ "result": 0, "msgList": [] }));
            };
            if self.stall_history {
                return Ok(json!({ "result": 0, "msgList": [self.records[index].clone()] }));
            }
            let mut messages: Vec<Value> = self
                .records
                .iter()
                .skip(index)
                .take(count.max(0) as usize)
                .cloned()
                .collect();
            if self.reverse_output {
                messages.reverse();
            }
            Ok(json!({ "result": 0, "msgList": messages }))
        }

        async fn get_msgs_by_seq_range(
            &self,
            _peer: &Peer,
            _start_seq: &str,
            _end_seq: &str,
        ) -> Result<Value, String> {
            Ok(json!({ "result": 0, "msgList": [] }))
        }

        async fn get_local_history(
            &self,
            _peer: &Peer,
            msg_id: &str,
            _msg_time: &str,
            msg_seq: &str,
            include_current: bool,
            count: i64,
        ) -> Result<Value, String> {
            self.calls.lock().unwrap().push(format!(
                "local:{msg_id}:{msg_seq}:{include_current}:{count}"
            ));
            if let Some(messages) = self.scripted_local.lock().unwrap().pop_front() {
                return Ok(json!({"result": 0, "msgList": messages}));
            }
            if self.stall_local_history && msg_id != "0" {
                let pos = *self.positions.get(msg_id).ok_or("unknown local msgId")?;
                return Ok(json!({"result": 0, "msgList": [self.records[pos].clone()]}));
            }
            let start = if msg_id != "0" {
                let pos = *self.positions.get(msg_id).ok_or("unknown local msgId")?;
                pos + usize::from(!include_current)
            } else if msg_seq == "0" {
                0
            } else {
                self.records
                    .iter()
                    .position(|msg| {
                        loose_msg_seq(msg)
                            .is_some_and(|seq| seq <= msg_seq.parse::<i64>().unwrap_or(0))
                    })
                    .unwrap_or(self.records.len())
            };
            let messages: Vec<Value> = self
                .records
                .iter()
                .skip(start)
                .take(count.max(0) as usize)
                .cloned()
                .collect();
            Ok(json!({ "result": 0, "msgList": messages }))
        }

        async fn get_msgs_by_seq_and_count(
            &self,
            _peer: &Peer,
            anchor_seq: i64,
            count: i64,
        ) -> Result<Value, String> {
            self.calls
                .lock()
                .unwrap()
                .push(format!("seq:{anchor_seq}:{count}"));
            if let Some(scripted) = self.scripted_seq.lock().unwrap().pop_front() {
                return scripted;
            }
            let mut messages: Vec<Value> = self
                .records
                .iter()
                .filter(|msg| loose_msg_seq(msg).is_some_and(|seq| seq <= anchor_seq))
                .take(count.max(0) as usize)
                .cloned()
                .collect();
            if self.reverse_output {
                messages.reverse();
            }
            Ok(json!({ "result": 0, "msgList": messages }))
        }
    }

    fn boundary_fetcher_config() -> BatchFetchConfig {
        BatchFetchConfig {
            batch_size: 5,
            retry_count: 0,
            retry_interval_ms: 0,
            ..BatchFetchConfig::default()
        }
    }

    /// 跑完整个分页循环，返回所有批次 msgId（含重复）。
    async fn collect_all_ids(
        fetcher: &BatchMessageFetcher,
        peer: &Peer,
        filter: &MessageFilter,
    ) -> Vec<String> {
        let mut previous = None;
        let mut ids = Vec::new();
        let mut batches = 0;
        while let Some(batch) = fetcher
            .fetch_next_batch(peer, filter, previous.as_ref())
            .await
            .expect("batch")
        {
            for msg in &batch.messages {
                if let Some(id) = msg.get("msgId").and_then(Value::as_str) {
                    ids.push(id.to_string());
                }
            }
            batches += 1;
            assert!(batches < 2_000, "分页未按预期收敛");
            previous = Some(batch);
        }
        ids
    }

    /// issue #716：页尾同一 msgSeq 被截断时，包含式补读必须找回 msgId=m99。
    #[tokio::test]
    async fn seq_boundary_backfill_recovers_shared_seq_message() {
        // 复现 issue 的合成示例：seq=9 下有 4 条记录（102/101/100/99），
        // count=5 的首页只返回其中 3 条；旧逻辑 next=min-1=8 会丢掉 99。
        let mut entries = vec![
            ("m104".to_string(), 11, 114_000),
            ("m103".to_string(), 10, 113_000),
            ("m102".to_string(), 9, 112_000),
            ("m101".to_string(), 9, 111_000),
            ("m100".to_string(), 9, 110_000),
            ("m99".to_string(), 9, 109_000),
            ("m98".to_string(), 8, 108_000),
            ("m97".to_string(), 8, 107_000),
        ];
        for seq in (1..=7).rev() {
            entries.push((format!("m{seq:02}"), seq, seq * 1_000));
        }
        let api = Arc::new(SeqBoundaryMock::new(entries));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );

        let ids = collect_all_ids(&fetcher, &group_peer(), &MessageFilter::default()).await;

        let unique: HashSet<String> = ids.iter().cloned().collect();
        assert_eq!(unique.len(), ids.len(), "不应有重复 msgId");
        assert_eq!(unique.len(), 15, "应导出全部 15 条消息");
        assert!(unique.contains("m99"), "边界 seq=9 的 msgId=99 必须被补回");
        // 补读确实发生了：对边界 seq=9 做了一次包含式重读（count=100 下限）。
        assert!(
            api.call_log().iter().any(|call| call == "seq:9:100"),
            "缺少边界 seq=9 的包含式重读: {:?}",
            api.call_log()
        );
    }

    /// issue #716：NapCat TS 层不记录 `getMsgsBySeqAndCount` /
    /// `getMsgsIncludeSelf` 的返回顺序（desc / queryOrder 的确切语义无法从
    /// 源码证实）。分页与边界补读必须按集合语义工作：同一份历史无论内核按
    /// 从新到旧还是从旧到新返回，都必须补回同一批 msgId。
    #[tokio::test]
    async fn seq_boundary_backfill_is_output_order_insensitive() {
        let mut entries = vec![
            ("m104".to_string(), 11, 114_000),
            ("m103".to_string(), 10, 113_000),
            ("m102".to_string(), 9, 112_000),
            ("m101".to_string(), 9, 111_000),
            ("m100".to_string(), 9, 110_000),
            ("m99".to_string(), 9, 109_000),
            ("m98".to_string(), 8, 108_000),
            ("m97".to_string(), 8, 107_000),
        ];
        for seq in (1..=7).rev() {
            entries.push((format!("m{seq:02}"), seq, seq * 1_000));
        }
        let api = Arc::new(SeqBoundaryMock::new(entries).with_reversed_output());
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );

        let ids = collect_all_ids(&fetcher, &group_peer(), &MessageFilter::default()).await;

        let unique: HashSet<String> = ids.iter().cloned().collect();
        assert_eq!(unique.len(), ids.len(), "不应有重复 msgId");
        assert_eq!(unique.len(), 15, "应导出全部 15 条消息");
        assert!(unique.contains("m99"), "边界 seq=9 的 msgId=99 必须被补回");
        assert!(
            api.call_log().iter().any(|call| call == "seq:9:100"),
            "缺少边界 seq=9 的包含式重读: {:?}",
            api.call_log()
        );
    }

    /// issue #716：单个 msgSeq 独占整页时，必须用 msgId 游标继续向旧方向翻页，
    /// 直到越过该 seq，而不是重复请求同一个 seq 锚点。
    #[tokio::test]
    async fn seq_boundary_msgid_cursor_pages_within_single_seq() {
        // seq=500 有 150 条记录，超过补读窗口（100）；seq=499/498 各几条。
        let mut entries = Vec::new();
        for i in 0..150_i64 {
            entries.push((format!("s500-{i:03}"), 500, 500_000 + i * 1_000));
        }
        for i in 0..3_i64 {
            entries.push((format!("s499-{i}"), 499, 499_000 + i * 1_000));
        }
        for i in 0..2_i64 {
            entries.push((format!("s498-{i}"), 498, 498_000 + i * 1_000));
        }
        let api = Arc::new(SeqBoundaryMock::new(entries));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            BatchFetchConfig {
                batch_size: 50,
                retry_count: 0,
                retry_interval_ms: 0,
                ..BatchFetchConfig::default()
            },
        );

        let ids = collect_all_ids(&fetcher, &group_peer(), &MessageFilter::default()).await;

        let seq500: HashSet<&String> = ids.iter().filter(|id| id.starts_with("s500-")).collect();
        assert_eq!(seq500.len(), 150, "seq=500 的全部 150 条记录必须补齐");
        assert!(ids.iter().any(|id| id.starts_with("s499-")));
        assert!(ids.iter().any(|id| id.starts_with("s498-")));
        // msgId 游标确实被使用。
        assert!(
            api.call_log()
                .iter()
                .any(|call| call.starts_with("history:")),
            "缺少 msgId 游标补读: {:?}",
            api.call_log()
        );
    }

    /// issue #716：整页同 seq（min_seq == anchor）补读完成后，下一锚点必须是
    /// boundary - 1，不能按页宽跳到 anchor - page 而漏掉中间 seq。
    #[tokio::test]
    async fn seq_boundary_full_single_seq_page_advances_to_boundary_minus_one() {
        // 从 seq=500 续拉：seq500 有 120 条（>补读窗口），seq496..499 各 1 条。
        // 若错误地 next=500-5=495，seq496..499 会被静默跳过。
        let mut entries = Vec::new();
        for i in 0..120_i64 {
            entries.push((format!("s500-{i:03}"), 500, 500_000 + i * 1_000));
        }
        for seq in 496..=499_i64 {
            entries.push((format!("s{seq}"), seq, seq * 1_000));
        }
        let api = Arc::new(SeqBoundaryMock::new(entries));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );
        let filter = MessageFilter::default();

        let first = fetcher
            .fetch_messages(&group_peer(), &filter, None, Some("500"))
            .await
            .expect("first batch");
        assert_eq!(
            first.next_seq.as_deref(),
            Some("499"),
            "边界补齐后必须推进到 boundary-1=499"
        );

        let mut previous = Some(first);
        let mut ids: Vec<String> = Vec::new();
        let mut batches = 0;
        while let Some(batch) = fetcher
            .fetch_next_batch(&group_peer(), &filter, previous.as_ref())
            .await
            .expect("batch")
        {
            for msg in &batch.messages {
                if let Some(id) = msg.get("msgId").and_then(Value::as_str) {
                    ids.push(id.to_string());
                }
            }
            batches += 1;
            assert!(batches < 500, "分页未按预期收敛");
            previous = Some(batch);
        }
        for seq in 496..=499_i64 {
            assert!(
                ids.iter().any(|id| id == &format!("s{seq}")),
                "seq={seq} 的消息不应被跳过"
            );
        }
    }

    /// issue #716：补读游标不前进必须显式失败，不能原地打转或静默跳过。
    #[tokio::test]
    async fn seq_boundary_stalled_msgid_cursor_fails() {
        // seq=500 有 120 条记录（>补读窗口 100），getMsgHistory 只回游标自身。
        let mut entries = Vec::new();
        for i in 0..120_i64 {
            entries.push((format!("s500-{i:03}"), 500, 500_000 + i * 1_000));
        }
        entries.push(("s499-0".to_string(), 499, 499_000));
        let api = Arc::new(SeqBoundaryMock::new(entries).with_stall_history());
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );

        let result = fetcher
            .fetch_next_batch(&group_peer(), &MessageFilter::default(), None)
            .await;
        let err = result.expect_err("游标不前进必须显式失败");
        assert!(
            err.to_string().contains("补读游标未前进"),
            "unexpected error: {err}"
        );
    }

    /// issue #716：补读期间的 API 错误、原生 result=-1 与无效响应都必须显式失败。
    #[tokio::test]
    async fn seq_boundary_backfill_failures_are_explicit() {
        async fn expect_backfill_error(scripted: Result<Value, String>) {
            let mut entries = Vec::new();
            for i in 0..30_i64 {
                entries.push((format!("s500-{i:03}"), 500, 500_000 + i * 1_000));
            }
            entries.push(("s499-0".to_string(), 499, 499_000));
            let api = Arc::new(SeqBoundaryMock::new(entries));
            api.script_seq(scripted);
            let fetcher = BatchMessageFetcher::new(
                Arc::clone(&api) as Arc<dyn MessageFetchApi>,
                boundary_fetcher_config(),
            );
            let result = fetcher
                .fetch_next_batch(&group_peer(), &MessageFilter::default(), None)
                .await;
            assert!(result.is_err(), "补读失败必须显式返回错误而不是当空页跳过");
        }

        // 底层 API 错误。
        expect_backfill_error(Err("recv failed with error: 10054".to_string())).await;
        // 原生 GeneralCallResult.result = -1（NapCat 不抛异常）。
        expect_backfill_error(Ok(json!({ "result": -1, "errMsg": "native boom" }))).await;
        // 无效响应（缺少 msgList）。
        expect_backfill_error(Ok(json!({ "unexpected": true }))).await;
    }

    /// issue #716：补读请求数超过上限必须显式失败（SEQUENCE_BOUNDARY_BACKFILL_EXHAUSTED）。
    #[tokio::test]
    async fn seq_boundary_backfill_cap_fails_explicitly() {
        // seq=500 有 2000 条记录，远超补读窗口；msgId 游标每次最多前进约
        // 100 条，必然在到达 seq=499 之前耗尽 SEQ_BOUNDARY_BACKFILL_MAX_REQUESTS。
        let mut entries = Vec::new();
        for i in 0..2_000_i64 {
            entries.push((format!("s500-{i:04}"), 500, 500_000 + i * 1_000));
        }
        entries.push(("s499-0".to_string(), 499, 499_000));
        let api = Arc::new(SeqBoundaryMock::new(entries));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );

        let result = fetcher
            .fetch_next_batch(&group_peer(), &MessageFilter::default(), None)
            .await;
        let err = result.expect_err("超过补读上限必须显式失败");
        assert!(
            matches!(err, FetchError::SequenceBackfillExhausted { .. }),
            "unexpected error: {err}"
        );
    }
    #[test]
    fn history_start_code_is_distinguished_from_generic_native_errors() {
        let exhausted = json!({ "result": 2004000, "errMsg": "no any more msg!" });
        assert!(is_history_exhausted_result(&exhausted));
        assert!(!is_history_exhausted_result(&json!({ "result": 0 })));
        assert!(!is_history_exhausted_result(&json!({ "result": 2004007 })));
        assert!(!is_history_exhausted_result(&json!({ "msgList": [] })));

        // 基线守卫：2004000 不走通用严格解析，严格解析对它仍然报错。
        assert!(extract_msg_page_strict(&exhausted).is_err());
    }

    /// issue #728：导出范围覆盖最早消息时，翻页越过历史起点返回
    /// result=2004000，必须按空页收敛终止，导出全部历史消息而不是 HTTP 500。
    #[tokio::test]
    async fn seq_paging_treats_history_start_as_end_of_history() {
        // 6 条消息，页宽 5：首批取回 seq 6..2（满页触发边界 seq=2 包含式
        // 补读），第二批锚点 seq=1 落在历史起点，原生接口返回 2004000。
        let entries: Vec<(String, i64, i64)> = (1..=6)
            .rev()
            .map(|seq| (format!("m0{seq}"), seq, seq * 1_000))
            .collect();
        let api = Arc::new(SeqBoundaryMock::new(entries));
        api.script_seq(Ok(json!({
            "result": 0,
            "msgList": [{
                "msgId": "m02",
                "msgSeq": "2",
                "msgTime": "2000",
                "msgType": 2,
            }],
        })));
        api.script_seq(Ok(json!({
            "result": 2004000,
            "errMsg": "no any more msg!",
        })));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );

        let ids = collect_all_ids(&fetcher, &group_peer(), &MessageFilter::default()).await;

        let unique: HashSet<String> = ids.iter().cloned().collect();
        assert_eq!(
            unique.len(),
            6,
            "历史起点之前的消息必须全部导出: {unique:?}"
        );
        assert!(unique.contains("m01"), "最早消息 m01 不能被 2004000 丢弃");
        assert!(
            api.call_log().iter().any(|call| call == "seq:1:5"),
            "第二批应从锚点 seq=1 翻页并得到 2004000: {:?}",
            api.call_log()
        );
    }
    /// 中途退群后原生接口返回 2004010，接续当前序列页而非重头导出。
    #[tokio::test]
    async fn local_history_fallback_resumes_after_membership_error() {
        let entries: Vec<(String, i64, i64)> = (1..=12)
            .map(|seq| (format!("m{seq}"), seq, seq * 1_000))
            .collect();
        let api = Arc::new(SeqBoundaryMock::new(entries));
        api.script_seq(Ok(json!({
            "result": 2004010,
            "errMsg": "is not group member"
        })));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );
        // 先前原生分页已提交 seq 12..8；下一批应从 seq=7 开始。
        let mut previous = BatchFetchResult {
            has_more: true,
            next_seq: Some("7".to_string()),
            ..BatchFetchResult::default()
        };
        let mut ids = Vec::new();
        for _ in 0..5 {
            let Some(batch) = fetcher
                .fetch_next_batch(&group_peer(), &MessageFilter::default(), Some(&previous))
                .await
                .expect("2004010 应进入本地分页")
            else {
                break;
            };
            ids.extend(batch.messages.iter().filter_map(message_msg_id));
            previous = batch;
        }
        let expected: Vec<String> = (1..=7).rev().map(|seq| format!("m{seq}")).collect();
        assert_eq!(ids, expected);
        let calls = api.call_log();
        assert_eq!(
            calls.iter().filter(|call| call.starts_with("seq:")).count(),
            1
        );
        assert!(calls.iter().any(|call| call == "local:0:7:true:5"));
        assert!(calls.iter().any(|call| call == "local:m3:3:false:5"));
    }

    /// 游标不前进（内核把上一页的边界记录再次返回）时停止翻页，
    /// 已读到的记录保留，导出标记为未核验而不是整体失败。
    #[tokio::test]
    async fn local_history_stalled_cursor_stops_as_unverified() {
        let api = Arc::new(
            SeqBoundaryMock::new(vec![
                ("a".to_string(), 9, 9_000),
                ("b".to_string(), 8, 8_000),
            ])
            .with_stalled_local_history(),
        );
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );
        let previous = BatchFetchResult {
            has_more: true,
            local_cursor: Some(local_cursor("a", 9, 9)),
            ..BatchFetchResult::default()
        };
        let batch = fetcher
            .fetch_next_batch(&group_peer(), &MessageFilter::default(), Some(&previous))
            .await
            .expect("游标异常不应让整个导出失败")
            .expect("当前页仍要交给上层去重");
        assert!(!batch.has_more);
        assert_eq!(batch.messages.len(), 1);
        assert_eq!(
            fetcher.local_history_end().await,
            Some(LocalHistoryEnd::Unverified(
                LocalHistoryIssue::CursorStalled
            ))
        );
        assert_eq!(api.call_log().len(), 1, "不得继续翻页");
    }

    /// 从非零游标翻页后出现更大的序号：不符合内核分页模型，停止并标记未核验。
    #[tokio::test]
    async fn local_history_seq_increase_stops_as_unverified() {
        let api = Arc::new(SeqBoundaryMock::new(Vec::new()));
        api.script_local(vec![
            json!({"msgId":"b","msgSeq":"27299","msgTime":"1640970954"}),
            json!({"msgId":"c","msgSeq":"60000","msgTime":"1652925535"}),
        ]);
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );
        let previous = BatchFetchResult {
            has_more: true,
            local_cursor: Some(local_cursor("start", 1_700_000_000, 50_000)),
            ..BatchFetchResult::default()
        };
        let batch = fetcher
            .fetch_next_batch(&group_peer(), &MessageFilter::default(), Some(&previous))
            .await
            .unwrap()
            .expect("异常页的记录必须保留");
        assert!(!batch.has_more);
        assert_eq!(batch.messages.len(), 2);
        assert_eq!(
            fetcher.local_history_end().await,
            Some(LocalHistoryEnd::Unverified(LocalHistoryIssue::SeqWentUp))
        );
    }

    /// 同一 msgSeq 下的多条记录之间打转：序号没有增大，靠页尾边界集合拦截。
    #[tokio::test]
    async fn local_history_repeated_page_boundary_stops_as_unverified() {
        let api = Arc::new(SeqBoundaryMock::new(Vec::new()));
        let message = |id: &str| json!({"msgId": id, "msgSeq": "7", "msgTime": "1640970954"});
        api.script_local(vec![message("x1"), message("x2")]);
        api.script_local(vec![message("x3"), message("x4")]);
        api.script_local(vec![message("x2"), message("x5")]);
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );
        let start = BatchFetchResult {
            has_more: true,
            local_cursor: Some(local_cursor("start", 1_690_000_000, 9)),
            ..BatchFetchResult::default()
        };
        let (ids, end) = walk_local(&fetcher, Some(start)).await;
        assert_eq!(ids, ["x1", "x2", "x3", "x4", "x2", "x5"]);
        assert_eq!(
            end,
            Some(LocalHistoryEnd::Unverified(LocalHistoryIssue::RepeatedPage))
        );
        assert_eq!(api.call_log().len(), 3, "不得继续读取下一轮重复历史");
    }

    fn local_cursor(msg_id: &str, msg_time: i64, msg_seq: i64) -> LocalHistoryCursor {
        LocalHistoryCursor {
            msg_id: msg_id.to_string(),
            msg_time: msg_time.to_string(),
            msg_seq: msg_seq.to_string(),
        }
    }

    /// 跑完本地回退分页，返回全部批次的 msgId（含重复）与终止状态。
    async fn walk_local(
        fetcher: &BatchMessageFetcher,
        start: Option<BatchFetchResult>,
    ) -> (Vec<String>, Option<LocalHistoryEnd>) {
        let mut previous = start;
        let mut ids = Vec::new();
        let mut batches = 0;
        while let Some(batch) = fetcher
            .fetch_next_batch(&group_peer(), &MessageFilter::default(), previous.as_ref())
            .await
            .expect("batch")
        {
            ids.extend(batch.messages.iter().filter_map(message_msg_id));
            batches += 1;
            assert!(batches < 5_000, "分页未按预期收敛");
            previous = Some(batch);
        }
        (ids, fetcher.local_history_end().await)
    }

    /// 按 2026-11-22 事故日志还原的 `queryMsgsWithFilterEx` 行为模型
    /// （见 [`LocalHistoryQuery`]）。该模型逐一重放了群 960251923 无上限分页
    /// 留下的 1423 个不同请求，除缺失的 10 条 seq=0 记录外全部吻合。
    ///
    /// 原生接口的行为对齐已退群的群聊：最新一屏可读，按序号翻页返回 2004010。
    struct LocalKernelMock {
        /// (msgId, msgSeq, msgTime 秒)，按内核顺序排列：非零 msgSeq 降序，
        /// 随后是全部 msgSeq=0 的记录（msgTime 降序）。
        rows: Vec<(u64, i64, i64)>,
        /// 第一条 msgSeq=0 记录的下标。
        zero_start: usize,
        /// 每次本地查询的 (msgId, msgSeq, includeCurrent, pageLimit)。
        requests: Mutex<Vec<(String, String, bool, i64)>>,
        /// 内核对单页条数的硬上限（超出时静默截断）。
        page_limit_cap: Option<i64>,
        /// pageLimit 超过该值时查询直接失败。
        fail_above_limit: Option<i64>,
    }

    impl LocalKernelMock {
        fn new(mut rows: Vec<(u64, i64, i64)>) -> Self {
            rows.sort_by(|a, b| match (a.1, b.1) {
                (0, 0) => b.2.cmp(&a.2).then_with(|| b.0.cmp(&a.0)),
                (0, _) => std::cmp::Ordering::Greater,
                (_, 0) => std::cmp::Ordering::Less,
                _ => b.1.cmp(&a.1).then_with(|| b.0.cmp(&a.0)),
            });
            let zero_start = rows.iter().position(|row| row.1 == 0).unwrap_or(rows.len());
            Self {
                rows,
                zero_start,
                requests: Mutex::new(Vec::new()),
                page_limit_cap: None,
                fail_above_limit: None,
            }
        }

        fn with_page_limit_cap(mut self, cap: i64) -> Self {
            self.page_limit_cap = Some(cap);
            self
        }

        fn with_failure_above_limit(mut self, limit: i64) -> Self {
            self.fail_above_limit = Some(limit);
            self
        }

        fn requests(&self) -> Vec<(String, String, bool, i64)> {
            self.requests.lock().unwrap().clone()
        }

        fn to_value(row: &(u64, i64, i64)) -> Value {
            json!({
                "msgId": row.0.to_string(),
                "msgSeq": row.1.to_string(),
                "msgTime": row.2.to_string(),
                "msgType": 2,
            })
        }

        fn page(
            &self,
            msg_id: &str,
            msg_seq: &str,
            include_current: bool,
            limit: usize,
        ) -> Vec<Value> {
            let seq: i64 = msg_seq.parse().expect("msgSeq");
            let id: u64 = msg_id.parse().expect("msgId");
            if seq != 0 {
                // 非零游标：只看 msgSeq，seq=0 的记录整体排在最后。
                let start = self.rows[..self.zero_start].partition_point(|row| {
                    if include_current {
                        row.1 > seq
                    } else {
                        row.1 >= seq
                    }
                });
                self.rows[start..]
                    .iter()
                    .take(limit)
                    .map(Self::to_value)
                    .collect()
            } else if id == 0 {
                self.rows.iter().take(limit).map(Self::to_value).collect()
            } else {
                // seq=0 的真实消息当游标：内核改按 msgId 过滤。
                self.rows
                    .iter()
                    .filter(|row| row.0 < id)
                    .take(limit)
                    .map(Self::to_value)
                    .collect()
            }
        }

        fn not_group_member() -> Value {
            json!({ "result": 2_004_010, "errMsg": "is not group member" })
        }
    }

    #[async_trait]
    impl MessageFetchApi for LocalKernelMock {
        async fn get_aio_first_view_latest_msgs(
            &self,
            _peer: &Peer,
            count: i64,
        ) -> Result<Value, String> {
            let messages = self.page("0", "0", true, count.max(0) as usize);
            Ok(json!({ "result": 0, "msgList": messages }))
        }

        async fn get_msg_history(
            &self,
            _peer: &Peer,
            _msg_id: &str,
            _count: i64,
        ) -> Result<Value, String> {
            Ok(Self::not_group_member())
        }

        async fn get_msgs_by_seq_range(
            &self,
            _peer: &Peer,
            _start_seq: &str,
            _end_seq: &str,
        ) -> Result<Value, String> {
            Ok(Self::not_group_member())
        }

        async fn get_msgs_by_seq_and_count(
            &self,
            _peer: &Peer,
            _anchor_seq: i64,
            _count: i64,
        ) -> Result<Value, String> {
            Ok(Self::not_group_member())
        }

        async fn get_local_history(
            &self,
            _peer: &Peer,
            msg_id: &str,
            _msg_time: &str,
            msg_seq: &str,
            include_current: bool,
            count: i64,
        ) -> Result<Value, String> {
            self.requests.lock().unwrap().push((
                msg_id.to_string(),
                msg_seq.to_string(),
                include_current,
                count,
            ));
            if self.fail_above_limit.is_some_and(|limit| count > limit) {
                return Err("worker crashed".to_string());
            }
            let limit = self.page_limit_cap.map_or(count, |cap| count.min(cap));
            let messages = self.page(msg_id, msg_seq, include_current, limit.max(0) as usize);
            Ok(json!({ "result": 0, "msgList": messages }))
        }
    }

    /// 小规模历史：`nonzero` 条非零序号记录（seq 从 101 起）加 `zero` 条
    /// seq=0 记录。seq=0 记录的 msgId 插在非零记录中间，模拟迁移导入的编号。
    fn kernel_rows(nonzero: u64, zero: u64) -> Vec<(u64, i64, i64)> {
        let mut rows: Vec<(u64, i64, i64)> = (0..nonzero)
            .map(|i| {
                (
                    1_000 + i * 10,
                    101 + i as i64,
                    1_600_000_000 + i as i64 * 60,
                )
            })
            .collect();
        rows.extend((0..zero).map(|i| (1_005 + i * 10, 0, 1_650_000_000 + i as i64 * 60)));
        rows
    }

    fn kernel_fetcher(api: &Arc<LocalKernelMock>, batch_size: i64) -> BatchMessageFetcher {
        BatchMessageFetcher::new(
            Arc::clone(api) as Arc<dyn MessageFetchApi>,
            BatchFetchConfig {
                batch_size,
                retry_count: 0,
                retry_interval_ms: 0,
                ..BatchFetchConfig::default()
            },
        )
    }

    /// 没有任何请求把 seq=0 的真实消息当作游标。
    fn assert_no_zero_seq_cursor(requests: &[(String, String, bool, i64)]) {
        assert!(
            requests
                .iter()
                .all(|(id, seq, _, _)| seq != "0" || id == "0"),
            "seq=0 的消息不能作为翻页游标: {requests:?}"
        );
    }

    /// 模型自检：seq=0 的消息当游标时，内核按 msgId 过滤并跳回更新的区间。
    /// 这是事故中无限循环的来源，下面的用例都依赖 mock 保留这一行为。
    #[test]
    fn local_kernel_mock_zero_seq_cursor_jumps_back() {
        let api = LocalKernelMock::new(kernel_rows(13, 9));
        // 最旧的 seq=0 记录（msgId=1005）夹在 seq=101 与 seq=102 之间。
        let page = api.page("1005", "0", false, 5);
        let seqs: Vec<i64> = page.iter().filter_map(loose_msg_seq).collect();
        assert_eq!(seqs, [101], "只剩 msgId 更小的记录，而不是尾段的后续");
        let page = api.page("1085", "0", false, 5);
        let seqs: Vec<i64> = page.iter().filter_map(loose_msg_seq).collect();
        assert_eq!(
            seqs,
            [109, 108, 107, 106, 105],
            "跳回已经读过的非零序号区间"
        );
    }

    /// 复现群 960251923 事故的最后一页（qce-server.log 2026-11-22 11:21:47 UTC，
    /// 第 1157 个请求），游标与边界记录使用日志里的真实取值：
    /// 游标 seq=6166 之后剩 447 条非零序号记录和 63 条 seq=0 记录，500 条的满页
    /// 恰好停在第 53 条 seq=0 记录上。旧实现拿它当游标，内核跳回 seq=162343
    /// 后循环；最旧的 10 条 seq=0 记录没有进入当时的导出文件。
    #[tokio::test(start_paused = true)]
    async fn local_history_incident_page_reads_whole_zero_seq_tail() {
        const Z53: u64 = 7_364_343_445_115_159_676;
        const Z63: u64 = 7_364_343_392_942_836_996;
        let mut rows = vec![
            // 更新区间内的记录：msgId 小于 Z53，seq=0 游标会跳回这里。
            (7_364_343_443_804_498_622, 162_343, 1_652_925_535),
            (7_364_343_443_804_498_359, 162_217, 1_652_889_475),
            (7_364_343_384_774_841_018, 6_166, 1_637_836_282),
            (7_364_343_384_774_841_016, 6_165, 1_637_836_281),
        ];
        // seq 5570..=6015：共 446 条，加上 6165 即日志中该页的 447 条非零记录。
        rows.extend((0..446_u64).map(|i| {
            (
                7_364_343_383_609_136_765 + i * 2,
                5_570 + i as i64,
                1_637_676_912 + i as i64 * 12,
            )
        }));
        // 63 条 seq=0 记录，时间从 2023-08-08 递减到 2021-12-31。
        rows.extend((0..52_u64).map(|i| {
            (
                7_364_343_693_368_147_127 - i * 1_000_003,
                0,
                1_691_505_242 - i as i64 * 600_000,
            )
        }));
        rows.push((Z53, 0, 1_652_500_711));
        rows.extend(
            (1..=9_u64).map(|i| (Z53 - i * 1_000_003, 0, 1_652_500_711 - i as i64 * 1_000_000)),
        );
        rows.push((Z63, 0, 1_640_972_368));
        let total_after_cursor = 447 + 63;

        let api = Arc::new(LocalKernelMock::new(rows));
        let fetcher = kernel_fetcher(&api, 3_000);
        let start = BatchFetchResult {
            has_more: true,
            local_cursor: Some(local_cursor("7364343384774841018", 1_637_836_282, 6_166)),
            ..BatchFetchResult::default()
        };
        let (ids, end) = walk_local(&fetcher, Some(start)).await;

        assert_eq!(end, Some(LocalHistoryEnd::Complete));
        assert_eq!(ids.len(), total_after_cursor, "不重复、不遗漏");
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), total_after_cursor);
        assert!(
            unique.contains(&Z63.to_string()),
            "最旧的 seq=0 记录必须读到"
        );
        assert_eq!(
            api.requests(),
            [
                (
                    "7364343384774841018".to_string(),
                    "6166".to_string(),
                    false,
                    500
                ),
                // 尾段从最后一条非零序号记录（seq=5570）重查，63 条 < 500 即终点。
                (
                    "7364343383609136765".to_string(),
                    "5570".to_string(),
                    false,
                    500
                ),
            ]
        );
    }

    /// 真实数据量级（578,447 条非零序号 + 63 条 seq=0，页大小 500）下的查询
    /// 预算：请求数应为 1157 页 + 1 次尾段查询，全部记录恰好各读到一次。
    /// 事故中同样的历史在无上限分页时发出了 37,462 个请求。
    #[tokio::test(start_paused = true)]
    async fn local_history_real_scale_walk_stays_within_request_budget() {
        const NONZERO: u64 = 578_447;
        const ZERO: u64 = 63;
        let mut rows: Vec<(u64, i64, i64)> = (0..NONZERO)
            .map(|i| {
                (
                    7_364_343_383_609_136_765 + i * 2,
                    5_570 + i as i64,
                    1_637_676_912 + i as i64 * 95,
                )
            })
            .collect();
        // seq=0 记录的 msgId 落在非零记录之间，seq=0 游标会跳回旧区间。
        rows.extend((0..ZERO).map(|i| {
            (
                7_364_343_383_609_136_766 + i * 18_000,
                0,
                1_640_972_368 + i as i64 * 800_000,
            )
        }));
        let api = Arc::new(LocalKernelMock::new(rows));
        let fetcher = kernel_fetcher(&api, 3_000);

        let mut previous: Option<BatchFetchResult> = None;
        let mut unique = HashSet::new();
        let mut fetched = 0_u64;
        while let Some(batch) = fetcher
            .fetch_next_batch(&group_peer(), &MessageFilter::default(), previous.as_ref())
            .await
            .expect("batch")
        {
            for id in batch.messages.iter().filter_map(message_msg_id) {
                fetched += 1;
                unique.insert(id);
            }
            previous = Some(BatchFetchResult {
                messages: Vec::new(),
                ..batch
            });
        }

        assert_eq!(
            fetcher.local_history_end().await,
            Some(LocalHistoryEnd::Complete)
        );
        assert_eq!(unique.len() as u64, NONZERO + ZERO);
        assert_eq!(fetched, NONZERO + ZERO, "没有重复抓取");
        let requests = api.requests();
        assert_eq!(requests.len(), 1_157 + 1);
        assert_no_zero_seq_cursor(&requests);
    }

    /// 带起始时间的导出：回溯到范围之外即停止翻页，但 seq=0 的记录排在全部
    /// 非零序号之后，必须用合成游标单独读取，范围内的那几条才不会丢。
    #[tokio::test(start_paused = true)]
    async fn local_history_time_range_stop_still_reads_zero_seq_tail() {
        const T0: i64 = 1_700_000_000;
        let mut rows: Vec<(u64, i64, i64)> = (0..20_u64)
            .map(|i| (1_000 + i * 10, 101 + i as i64, T0 + i as i64 * 1_000))
            .collect();
        // 三条 seq=0 记录：一条早于起始时间，两条落在范围内。
        rows.push((1_045, 0, T0 + 4_500));
        rows.push((1_155, 0, T0 + 15_500));
        rows.push((1_185, 0, T0 + 18_500));
        let api = Arc::new(LocalKernelMock::new(rows));
        let fetcher = kernel_fetcher(&api, 5);
        let filter = MessageFilter {
            start_time: Some((T0 + 12_500) * 1_000),
            ..MessageFilter::default()
        };

        let mut previous: Option<BatchFetchResult> = None;
        let mut ids = Vec::new();
        while let Some(batch) = fetcher
            .fetch_next_batch(&group_peer(), &filter, previous.as_ref())
            .await
            .expect("batch")
        {
            ids.extend(batch.messages.iter().filter_map(message_msg_id));
            previous = Some(batch);
        }

        // seq 120..=114 共 7 条在范围内，加上两条范围内的 seq=0 记录。
        let expected = [
            "1190", "1180", "1170", "1160", "1150", "1140", "1130", "1185", "1155",
        ];
        assert_eq!(ids, expected);
        assert_eq!(
            fetcher.local_history_end().await,
            Some(LocalHistoryEnd::ReachedStartTime)
        );
        let requests = api.requests();
        assert_no_zero_seq_cursor(&requests);
        assert_eq!(requests.len(), 3, "两页常规分页 + 一次尾段查询");
        assert_eq!(
            requests[2],
            ("0".to_string(), "1".to_string(), false, 5),
            "尾段用不带 msgId 的 msgSeq=1 游标读取，不再向更早的历史翻页"
        );
    }

    /// 尾段超过一页：从非零锚点重查并放大 pageLimit，直到返回短页。
    #[tokio::test(start_paused = true)]
    async fn local_history_zero_seq_tail_longer_than_a_page_is_completed() {
        let api = Arc::new(LocalKernelMock::new(kernel_rows(13, 9)));
        let fetcher = kernel_fetcher(&api, 5);
        let (ids, end) = walk_local(&fetcher, None).await;

        assert_eq!(end, Some(LocalHistoryEnd::Complete));
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!((ids.len(), unique.len()), (22, 22));
        let requests = api.requests();
        assert_no_zero_seq_cursor(&requests);
        let limits: Vec<i64> = requests.iter().map(|request| request.3).collect();
        // 三页常规分页，随后尾段查询 5 条（满页）→ 20 条（返回 9 条，短页）。
        assert_eq!(limits, [5, 5, 5, 5, 20]);
        assert_eq!(requests[3].1, "101", "尾段锚点是最后一条非零序号记录");
        assert_eq!(requests[4].1, "101");
    }

    /// 以 seq=0 结尾的短页本身就是终点，不需要任何额外请求。
    #[tokio::test(start_paused = true)]
    async fn local_history_short_page_ending_in_zero_seq_is_the_end() {
        let api = Arc::new(LocalKernelMock::new(kernel_rows(8, 1)));
        let fetcher = kernel_fetcher(&api, 5);
        let (ids, end) = walk_local(&fetcher, None).await;

        assert_eq!(end, Some(LocalHistoryEnd::Complete));
        assert_eq!(ids.len(), 9);
        // 第二页 3 条非零序号 + 1 条 seq=0，不满 5 条。
        assert_eq!(api.requests().len(), 2);
    }

    /// 整页都是 seq=0（没有非零锚点）：沿用同一起点放大 pageLimit。
    #[tokio::test(start_paused = true)]
    async fn local_history_all_zero_seq_page_grows_from_same_start() {
        let api = Arc::new(LocalKernelMock::new(kernel_rows(0, 7)));
        let fetcher = kernel_fetcher(&api, 5);
        let (ids, end) = walk_local(&fetcher, None).await;

        assert_eq!(end, Some(LocalHistoryEnd::Complete));
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!((ids.len(), unique.len()), (7, 7));
        assert_eq!(
            api.requests(),
            [
                ("0".to_string(), "0".to_string(), true, 5),
                ("0".to_string(), "0".to_string(), true, 20),
            ]
        );
    }

    /// 内核静默截断 pageLimit 时短页不再可信：停止、保留记录、标记未核验。
    #[tokio::test(start_paused = true)]
    async fn local_history_capped_page_limit_is_reported_as_unverified() {
        let api = Arc::new(LocalKernelMock::new(kernel_rows(3, 12)).with_page_limit_cap(5));
        let fetcher = kernel_fetcher(&api, 5);
        let (ids, end) = walk_local(&fetcher, None).await;

        assert_eq!(
            end,
            Some(LocalHistoryEnd::Unverified(
                LocalHistoryIssue::TailPageLimitCapped
            ))
        );
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), 3 + 5, "已读到的记录全部保留");
        let requests = api.requests();
        assert_eq!(requests.len(), 3);
        assert_no_zero_seq_cursor(&requests);
    }

    /// 尾段异常庞大：三次放大后仍是满页则止损，不无限放大窗口。
    #[tokio::test(start_paused = true)]
    async fn local_history_oversized_zero_seq_tail_stops_within_budget() {
        let api = Arc::new(LocalKernelMock::new(kernel_rows(3, 100)));
        let fetcher = kernel_fetcher(&api, 5);
        let (ids, end) = walk_local(&fetcher, None).await;

        assert_eq!(
            end,
            Some(LocalHistoryEnd::Unverified(
                LocalHistoryIssue::TailBudgetExhausted
            ))
        );
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), 3 + 80, "最大窗口内读到的记录全部保留");
        let limits: Vec<i64> = api.requests().iter().map(|request| request.3).collect();
        assert_eq!(limits, [5, 5, 20, 80]);
    }

    /// 尾段查询失败不丢弃已抓取的记录，导出降级为未核验。
    #[tokio::test(start_paused = true)]
    async fn local_history_tail_query_failure_keeps_fetched_messages() {
        let api = Arc::new(LocalKernelMock::new(kernel_rows(3, 12)).with_failure_above_limit(5));
        let fetcher = kernel_fetcher(&api, 5);
        let (ids, end) = walk_local(&fetcher, None).await;

        assert!(matches!(
            end,
            Some(LocalHistoryEnd::Unverified(
                LocalHistoryIssue::TailQueryFailed(_)
            ))
        ));
        let unique: HashSet<&String> = ids.iter().collect();
        assert_eq!(unique.len(), 3 + 5);
    }

    /// 即使 #716 边界补读遭到 2004010，仍能从本地库读取完整记录。
    #[tokio::test]
    async fn local_history_fallback_handles_boundary_query_error() {
        let entries: Vec<(String, i64, i64)> = (1..=9)
            .map(|seq| (format!("m{seq}"), seq, seq * 1_000))
            .collect();
        let api = Arc::new(SeqBoundaryMock::new(entries));
        // 首次原生最新消息正常；接着边界补读失败。
        api.script_seq(Ok(json!({
            "result": 2004010,
            "errMsg": "is not group member"
        })));
        let fetcher = BatchMessageFetcher::new(
            Arc::clone(&api) as Arc<dyn MessageFetchApi>,
            boundary_fetcher_config(),
        );
        let ids = collect_all_ids(&fetcher, &group_peer(), &MessageFilter::default()).await;
        let expected: Vec<String> = (1..=9).rev().map(|seq| format!("m{seq}")).collect();
        assert_eq!(ids, expected);
        assert!(api.call_log().iter().any(|call| call == "local:0:0:true:5"));
    }
}
