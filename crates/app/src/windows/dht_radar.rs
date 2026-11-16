//! 活动栏「DHT 种子雷达」页：展示引擎被动采集到的种子，支持排序、刷新、加入下载与复制磁力。
//!
//! 数据面是 `daemon.dht.radar.*`（见 `native/daemon/src/dht_radar_rpc.rs`）；采集本身跑在引擎线程里，
//! 本页只做轮询与呈现——列表是纯内存查询，3 秒一次刷新对 daemon 无压力。

use std::sync::Arc;
use std::time::Duration;

use fluxdown_protocol::{
    ApplicationErrorCode, CreateTaskRequest, DaemonCreateTaskParams, DhtRadarClearDto,
    DhtRadarConfigDto, DhtRadarExportDto, DhtRadarImportDto, DhtRadarImportParams, DhtRadarItemDto,
    DhtRadarListDto, DhtRadarListParams, DhtRadarSetConfigParams, DhtRadarStatsDto, method,
};
use fluxdown_ui_components::{ButtonVariant, card, loading_button};
use fluxdown_ui_i18n::Translator;
use fluxdown_ui_theme::active_theme;
use gpui::{
    AnyElement, App, ClickEvent, ClipboardItem, Context, Div, FontWeight, InteractiveElement as _,
    IntoElement, ParentElement as _, PathPromptOptions, Render, SharedString,
    StatefulInteractiveElement as _, Styled as _, Window, div, prelude::FluentBuilder as _, px,
};
use gpui_component::{h_flex, scroll::ScrollableElement as _, v_flex};
use serde_json::Value;

use crate::agent_client::AgentClient;
use crate::app::Desktop;

/// 列表一次拉取的条数（雷达侧上限 500）。
const PAGE_LIMIT: u32 = 200;
/// 轮询间隔。
const REFRESH_INTERVAL: Duration = Duration::from_secs(3);
/// 导出文件的建议名。
const EXPORT_FILE_NAME: &str = "fluxdown-dht-radar.json";
/// 导入文件的大小上限（与 daemon 侧一致）。
const MAX_IMPORT_BYTES: u64 = 16 * 1024 * 1024;

/// 排序方式，与 daemon 的 `sort` 参数一一对应。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Sort {
    Recent,
    Peers,
    Size,
}

impl Sort {
    fn as_str(self) -> &'static str {
        match self {
            Self::Recent => "recent",
            Self::Peers => "peers",
            Self::Size => "size",
        }
    }

    fn label_key(self) -> &'static str {
        match self {
            Self::Recent => "dhtRadarSortRecent",
            Self::Peers => "dhtRadarSortPeers",
            Self::Size => "dhtRadarSortSize",
        }
    }

    const ALL: [Self; 3] = [Self::Recent, Self::Peers, Self::Size];
}

/// 雷达页视图。
pub(crate) struct DhtRadarView {
    translator: Translator,
    client: Arc<AgentClient>,
    items: Vec<DhtRadarItemDto>,
    total: u64,
    stats: Option<DhtRadarStatsDto>,
    config: Option<DhtRadarConfigDto>,
    sort: Sort,
    loading: bool,
    failed: bool,
    /// 配置切换 / 下载等写操作进行中，避免重复点击。
    busy: bool,
    /// 操作结果提示（成功或失败都放在这一行）。
    notice: Option<SharedString>,
    /// 最近一次请求的令牌：旧响应直接丢弃。
    request: u64,
    /// 清空存档的二次确认：第一次点击只切到「确认清空」。
    confirm_clear: bool,
    /// 只显示已解析出名字的条目。
    only_named: bool,
}

impl DhtRadarView {
    /// 创建页面并立即拉一次数据。
    pub(crate) fn new(
        translator: Translator,
        client: Arc<AgentClient>,
        cx: &mut Context<Self>,
    ) -> Self {
        let mut view = Self {
            translator,
            client,
            items: Vec::new(),
            total: 0,
            stats: None,
            config: None,
            sort: Sort::Recent,
            loading: false,
            failed: false,
            busy: false,
            notice: None,
            request: 0,
            confirm_clear: false,
            only_named: false,
        };
        view.refresh(cx);
        view.start_polling(cx);
        view
    }

    fn t(&self, key: &str) -> SharedString {
        SharedString::from(self.translator.text(key).to_owned())
    }

    /// 3 秒一次自动刷新；视图释放后循环自行退出。
    fn start_polling(&self, cx: &mut Context<Self>) {
        cx.spawn(async move |this, cx| {
            loop {
                cx.background_executor().timer(REFRESH_INTERVAL).await;
                let alive = this.update(cx, |this, cx| this.refresh(cx));
                if alive.is_err() {
                    return;
                }
            }
        })
        .detach();
    }

    fn refresh(&mut self, cx: &mut Context<Self>) {
        self.request = self.request.wrapping_add(1);
        let token = self.request;
        let client = Arc::clone(&self.client);
        let params = DhtRadarListParams {
            query: None,
            sort: Some(self.sort.as_str().to_owned()),
            offset: 0,
            limit: PAGE_LIMIT,
            named_only: self.only_named,
        };
        self.loading = true;
        cx.notify();
        cx.spawn(async move |this, cx| {
            let list = client
                .call::<DhtRadarListParams, DhtRadarListDto>(
                    method::DAEMON_DHT_RADAR_LIST,
                    Some(params),
                )
                .await;
            let stats = client
                .call::<Value, DhtRadarStatsDto>(method::DAEMON_DHT_RADAR_STATS, None)
                .await;
            let config = client
                .call::<Value, DhtRadarConfigDto>(method::DAEMON_DHT_RADAR_CONFIG, None)
                .await;
            // 视图已释放时丢弃结果。
            drop(this.update(cx, |this, cx| {
                this.finish_refresh(token, list, stats, config, cx);
            }));
        })
        .detach();
    }

    fn finish_refresh(
        &mut self,
        token: u64,
        list: Result<DhtRadarListDto, fluxdown_protocol::RpcErrorData>,
        stats: Result<DhtRadarStatsDto, fluxdown_protocol::RpcErrorData>,
        config: Result<DhtRadarConfigDto, fluxdown_protocol::RpcErrorData>,
        cx: &mut Context<Self>,
    ) {
        if token != self.request {
            return;
        }
        self.loading = false;
        match list {
            Ok(page) => {
                self.failed = false;
                self.total = page.total;
                self.items = page.items;
            }
            Err(_) => self.failed = true,
        }
        if let Ok(stats) = stats {
            self.stats = Some(stats);
        }
        if let Ok(config) = config {
            self.config = Some(config);
        }
        cx.notify();
    }

    fn toggle_only_named(&mut self, cx: &mut Context<Self>) {
        self.only_named = !self.only_named;
        self.refresh(cx);
    }

    fn set_sort(&mut self, sort: Sort, cx: &mut Context<Self>) {
        if self.sort == sort {
            return;
        }
        self.sort = sort;
        self.refresh(cx);
    }

    /// 开 / 关采集：写配置后立即重拉，配置项里包含新状态。
    fn toggle_enabled(&mut self, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let Some(config) = self.config else {
            return;
        };
        self.busy = true;
        self.notice = None;
        cx.notify();
        let client = Arc::clone(&self.client);
        let params = DhtRadarSetConfigParams {
            enabled: Some(!config.enabled),
            port: None,
            capacity: None,
            metadata: None,
            nodes: None,
            archive_threshold: None,
        };
        cx.spawn(async move |this, cx| {
            let result = client
                .call::<DhtRadarSetConfigParams, DhtRadarConfigDto>(
                    method::DAEMON_DHT_RADAR_CONFIG_SET,
                    Some(params),
                )
                .await;
            drop(this.update(cx, |this, cx| {
                this.busy = false;
                match result {
                    Ok(config) => {
                        this.config = Some(config);
                        this.notice = Some(this.t(if config.enabled {
                            "dhtRadarStarted"
                        } else {
                            "dhtRadarStoppedNotice"
                        }));
                        this.refresh(cx);
                    }
                    Err(_) => {
                        this.notice = Some(this.t("dhtRadarActionFailed"));
                        cx.notify();
                    }
                }
            }));
        })
        .detach();
    }

    /// 把磁力链接交给下载引擎。
    fn download(&mut self, item: &DhtRadarItemDto, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        self.busy = true;
        self.notice = None;
        cx.notify();
        let client = Arc::clone(&self.client);
        let params = DaemonCreateTaskParams {
            request: CreateTaskRequest {
                url: item.magnet.clone(),
                file_name: String::new(),
                save_dir: String::new(),
                segments: 0,
                cookies: String::new(),
                referrer: String::new(),
                proxy_url: String::new(),
                user_agent: String::new(),
                queue_id: String::new(),
                checksum: String::new(),
                ignore_tls_errors: false,
                headers: None,
                torrent_b64: None,
                method: None,
                body: None,
                audio_url: None,
                start_paused: false,
                http_user: String::new(),
                http_password: String::new(),
                save_site_auth: false,
            },
            torrent_blob_id: None,
            unattended: false,
            hint_file_size: None,
            name_hint: item.name.clone(),
            category_route: None,
        };
        cx.spawn(async move |this, cx| {
            let result = client
                .call::<DaemonCreateTaskParams, Value>(method::DAEMON_TASK_CREATE, Some(params))
                .await;
            drop(this.update(cx, |this, cx| {
                this.busy = false;
                this.notice = Some(this.t(if result.is_ok() {
                    "dhtRadarDownloadStarted"
                } else {
                    "dhtRadarActionFailed"
                }));
                cx.notify();
            }));
        })
        .detach();
    }

    fn copy_magnet(&mut self, item: &DhtRadarItemDto, cx: &mut Context<Self>) {
        cx.write_to_clipboard(ClipboardItem::new_string(item.magnet.clone()));
        self.notice = Some(self.t("dhtRadarCopied"));
        cx.notify();
    }

    /// 清空已保存的存档与当前列表：第一次点击进入确认态，再点一次才真正删除。
    fn clear_saved(&mut self, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        if !self.confirm_clear {
            self.confirm_clear = true;
            self.notice = Some(self.t("dhtRadarClearHint"));
            cx.notify();
            return;
        }
        self.confirm_clear = false;
        self.busy = true;
        self.notice = None;
        cx.notify();
        let client = Arc::clone(&self.client);
        cx.spawn(async move |this, cx| {
            let result = client
                .call::<Value, DhtRadarClearDto>(method::DAEMON_DHT_RADAR_CLEAR, None)
                .await;
            drop(this.update(cx, |this, cx| {
                this.busy = false;
                match result {
                    Ok(cleared) => {
                        this.items.clear();
                        this.total = 0;
                        this.notice = Some(SharedString::from(format!(
                            "{} ({})",
                            this.t("dhtRadarCleared"),
                            cleared.removed
                        )));
                        this.refresh(cx);
                    }
                    Err(_) => {
                        this.notice = Some(this.t("dhtRadarActionFailed"));
                        cx.notify();
                    }
                }
            }));
        })
        .detach();
    }

    /// 导出全部已保存的种子：先选保存位置，再取 JSON 文本写盘。
    fn export(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let directory = std::env::home_dir().unwrap_or_else(std::env::temp_dir);
        let receiver = cx.prompt_for_new_path(&directory, Some(EXPORT_FILE_NAME));
        let client = Arc::clone(&self.client);
        self.busy = true;
        self.notice = None;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let path = match receiver.await {
                Ok(Ok(Some(path))) => path,
                _ => {
                    // 用户取消：恢复按钮，不报错。
                    drop(this.update_in(cx, |this, _, cx| {
                        this.busy = false;
                        cx.notify();
                    }));
                    return;
                }
            };
            // 旧的后台服务不认识新方法（应用更新后没完全退出重开）时，给出可操作的提示。
            let mut stale_service = false;
            let written = match client
                .call::<Value, DhtRadarExportDto>(method::DAEMON_DHT_RADAR_EXPORT, None)
                .await
            {
                Ok(dto) if dto.count > 0 => {
                    let count = dto.count;
                    let text = dto.text;
                    cx.background_executor()
                        .spawn(async move { std::fs::write(&path, text) })
                        .await
                        .ok()
                        .map(|()| count)
                }
                // 没有存档时也把选择的路径当成一次成功的空导出说清楚。
                Ok(_) => Some(0),
                Err(error) => {
                    stale_service = error.code == ApplicationErrorCode::Unsupported;
                    None
                }
            };
            drop(this.update_in(cx, |this, _, cx| {
                this.busy = false;
                this.notice = Some(match written {
                    None if stale_service => this.t("dhtRadarRestartRequired"),
                    None => this.t("dhtRadarExportFailed"),
                    Some(0) => this.t("dhtRadarExportEmpty"),
                    Some(count) => SharedString::from(
                        this.translator
                            .text_with("dhtRadarExported", &[("n", &count.to_string())]),
                    ),
                });
                cx.notify();
            }));
        })
        .detach();
    }

    /// 导入导出文件：读盘后交给 daemon 去重合并，再刷新列表。
    fn import(&mut self, window: &mut Window, cx: &mut Context<Self>) {
        if self.busy {
            return;
        }
        let receiver = cx.prompt_for_paths(PathPromptOptions {
            files: true,
            directories: false,
            multiple: false,
            prompt: Some(self.t("dhtRadarImport")),
        });
        let client = Arc::clone(&self.client);
        self.busy = true;
        self.notice = None;
        cx.notify();
        cx.spawn_in(window, async move |this, cx| {
            let path = match receiver.await {
                Ok(Ok(Some(mut paths))) => paths.pop(),
                _ => None,
            };
            let Some(path) = path else {
                // 用户取消：恢复按钮，不报错。
                drop(this.update_in(cx, |this, _, cx| {
                    this.busy = false;
                    cx.notify();
                }));
                return;
            };
            let text = cx
                .background_executor()
                .spawn(async move {
                    let size = std::fs::metadata(&path).ok()?.len();
                    if size > MAX_IMPORT_BYTES {
                        return None;
                    }
                    std::fs::read_to_string(&path).ok()
                })
                .await;
            let mut stale_service = false;
            let result = match text {
                None => None,
                Some(text) => match client
                    .call::<DhtRadarImportParams, DhtRadarImportDto>(
                        method::DAEMON_DHT_RADAR_IMPORT,
                        Some(DhtRadarImportParams { text }),
                    )
                    .await
                {
                    Ok(dto) => Some(dto),
                    Err(error) => {
                        stale_service = error.code == ApplicationErrorCode::Unsupported;
                        None
                    }
                },
            };
            drop(this.update_in(cx, |this, _, cx| {
                this.busy = false;
                match result {
                    Some(dto) => {
                        this.notice = Some(SharedString::from(this.translator.text_with(
                            "dhtRadarImported",
                            &[
                                ("n", &dto.imported.to_string()),
                                ("skipped", &dto.skipped.to_string()),
                                ("invalid", &dto.invalid.to_string()),
                            ],
                        )));
                        this.refresh(cx);
                    }
                    None => {
                        this.notice = Some(this.t(if stale_service {
                            "dhtRadarRestartRequired"
                        } else {
                            "dhtRadarImportFailed"
                        }));
                        cx.notify();
                    }
                }
            }));
        })
        .detach();
    }

    /// 顶部统计条：运行状态 + 节点 / 种子 / 报文。
    fn stats_line(&self, cx: &App) -> Div {
        let tokens = active_theme(cx).tokens().clone();
        let extended = active_theme(cx).extended().clone();
        let mut parts: Vec<SharedString> = Vec::new();
        if let Some(stats) = self.stats.clone() {
            parts.push(SharedString::from(format!(
                "{}: {}",
                self.translator.text("dhtRadarNodes"),
                stats.nodes
            )));
            parts.push(SharedString::from(format!(
                "{}: {}",
                self.translator.text("dhtRadarTorrents"),
                stats.torrents
            )));
            parts.push(SharedString::from(format!(
                "{}: {}",
                self.translator.text("dhtRadarPackets"),
                stats.packets_received
            )));
            parts.push(SharedString::from(format!(
                "{}: {}",
                self.translator.text("dhtRadarWithMetadata"),
                stats.with_metadata
            )));
            parts.push(SharedString::from(format!(
                "{}: {}",
                self.translator.text("dhtRadarSaved"),
                stats.saved
            )));
            if stats.port > 0 {
                parts.push(SharedString::from(format!("UDP: {}", stats.port)));
            }
        }
        if let Some(config) = self.config
            && config.archive_threshold > 0
        {
            parts.push(SharedString::from(self.translator.text_with(
                "dhtRadarArchiveLimit",
                &[("n", &config.archive_threshold.to_string())],
            )));
        }
        let running = self.stats.as_ref().is_some_and(|stats| stats.running);
        let state = self.t(if running {
            "dhtRadarRunning"
        } else {
            "dhtRadarIdle"
        });
        h_flex()
            .w_full()
            .flex_wrap()
            .items_center()
            .gap(tokens.spacing.sm)
            .text_size(extended.caption.size)
            .line_height(extended.caption.line_height)
            .text_color(extended.colors.text_tertiary)
            .child(
                div()
                    .px(tokens.spacing.xs)
                    .rounded(tokens.radius.sm)
                    .bg(if running {
                        tokens.colors.primary.opacity(0.14)
                    } else {
                        extended.colors.hairline
                    })
                    .text_color(if running {
                        tokens.colors.primary
                    } else {
                        extended.colors.text_tertiary
                    })
                    .child(state),
            )
            .children(parts.into_iter().map(|part| div().child(part)))
    }

    fn toolbar(&self, cx: &mut Context<Self>) -> AnyElement {
        let tokens = active_theme(cx).tokens().clone();
        let enabled = self.config.is_some_and(|config| config.enabled);
        let toggle_label = self.t(if enabled {
            "dhtRadarDisable"
        } else {
            "dhtRadarEnable"
        });
        let mut bar = h_flex()
            .w_full()
            .flex_wrap()
            .items_center()
            .gap(tokens.spacing.sm)
            .child(
                loading_button(
                    "dht-radar-toggle",
                    toggle_label,
                    if enabled {
                        ButtonVariant::Secondary
                    } else {
                        ButtonVariant::Primary
                    },
                    self.busy,
                    cx,
                )
                .on_click(cx.listener(|this, _: &ClickEvent, _, cx| this.toggle_enabled(cx))),
            )
            .child(
                loading_button(
                    "dht-radar-refresh",
                    self.t("dhtRadarRefresh"),
                    ButtonVariant::Secondary,
                    self.loading,
                    cx,
                )
                .on_click(cx.listener(|this, _: &ClickEvent, _, cx| this.refresh(cx))),
            )
            .child(
                loading_button(
                    "dht-radar-export",
                    self.t("dhtRadarExport"),
                    ButtonVariant::Secondary,
                    self.busy,
                    cx,
                )
                .on_click(cx.listener(|this, _: &ClickEvent, window, cx| this.export(window, cx))),
            )
            .child(
                loading_button(
                    "dht-radar-import",
                    self.t("dhtRadarImport"),
                    ButtonVariant::Secondary,
                    self.busy,
                    cx,
                )
                .on_click(cx.listener(|this, _: &ClickEvent, window, cx| this.import(window, cx))),
            );
        // 有存档时才显示：清空是不可撤销的写操作，放在次要按钮位并要二次确认。
        if self.stats.as_ref().is_some_and(|stats| stats.saved > 0) {
            bar = bar.child(
                loading_button(
                    "dht-radar-clear",
                    self.t(if self.confirm_clear {
                        "dhtRadarClearConfirm"
                    } else {
                        "dhtRadarClear"
                    }),
                    ButtonVariant::Secondary,
                    self.busy,
                    cx,
                )
                .on_click(cx.listener(|this, _: &ClickEvent, _, cx| this.clear_saved(cx))),
            );
        }
        for sort in Sort::ALL {
            let selected = self.sort == sort;
            bar = bar.child(
                div()
                    .id(("dht-radar-sort", sort as usize))
                    .px(tokens.spacing.sm)
                    .py(px(4.))
                    .rounded(tokens.radius.sm)
                    .cursor_pointer()
                    .text_size(tokens.typography.sm.size)
                    .when(selected, |chip| {
                        chip.bg(tokens.colors.primary.opacity(0.14))
                            .text_color(tokens.colors.primary)
                    })
                    .when(!selected, |chip| {
                        chip.text_color(active_theme(cx).extended().colors.text_tertiary)
                    })
                    .child(self.t(sort.label_key()))
                    .on_click(
                        cx.listener(move |this, _: &ClickEvent, _, cx| this.set_sort(sort, cx)),
                    ),
            );
        }
        let named_selected = self.only_named;
        bar = bar.child(
            div()
                .id("dht-radar-only-named")
                .px(tokens.spacing.sm)
                .py(px(4.))
                .rounded(tokens.radius.sm)
                .cursor_pointer()
                .text_size(tokens.typography.sm.size)
                .when(named_selected, |chip| {
                    chip.bg(tokens.colors.primary.opacity(0.14))
                        .text_color(tokens.colors.primary)
                })
                .when(!named_selected, |chip| {
                    chip.text_color(active_theme(cx).extended().colors.text_tertiary)
                })
                .child(self.t("dhtRadarOnlyNamed"))
                .on_click(cx.listener(|this, _: &ClickEvent, _, cx| this.toggle_only_named(cx))),
        );
        if let Some(notice) = self.notice.clone() {
            bar = bar.child(
                div()
                    .text_size(tokens.typography.sm.size)
                    .text_color(active_theme(cx).extended().colors.text_tertiary)
                    .child(notice),
            );
        }
        bar.into_any_element()
    }

    fn header_row(&self, cx: &App) -> Div {
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        let extended = theme.extended();
        let cell = |label: SharedString, width: f32| {
            div()
                .flex_none()
                .w(theme.text_extent(width))
                .truncate()
                .child(label)
        };
        h_flex()
            .w_full()
            .items_center()
            .gap(tokens.spacing.sm)
            .px(tokens.spacing.sm)
            .py(tokens.spacing.xs)
            .border_b_1()
            .border_color(extended.colors.hairline)
            .text_size(extended.caption.size)
            .line_height(extended.caption.line_height)
            .font_weight(FontWeight::MEDIUM)
            .text_color(extended.colors.text_tertiary)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .child(self.t("dhtRadarColName")),
            )
            .child(cell(self.t("dhtRadarColSize"), 96.))
            .child(cell(self.t("dhtRadarColFiles"), 56.))
            .child(cell(self.t("dhtRadarColPeers"), 56.))
            .child(cell(self.t("dhtRadarColSeen"), 88.))
            .child(cell(self.t("dhtRadarColActions"), 132.))
    }

    fn item_row(&self, index: usize, item: &DhtRadarItemDto, cx: &mut Context<Self>) -> AnyElement {
        let theme = active_theme(cx);
        let tokens = theme.tokens();
        let extended = theme.extended();
        let name = item.name.clone().map_or_else(
            || {
                SharedString::from(format!(
                    "{}…",
                    &item.info_hash[..16.min(item.info_hash.len())]
                ))
            },
            SharedString::from,
        );
        let has_name = item.name.is_some();
        let size = item
            .total_size
            .map(format_bytes)
            .unwrap_or_else(|| "—".to_owned());
        let files = item
            .file_count
            .map(|count| count.to_string())
            .unwrap_or_else(|| "—".to_owned());
        let peers = item.peers.to_string();
        let seen = format_age(item.last_seen);
        let cell = |text: String, width: f32| {
            div()
                .flex_none()
                .w(theme.text_extent(width))
                .truncate()
                .text_color(tokens.colors.muted_foreground)
                .child(SharedString::from(text))
        };
        h_flex()
            .id(("dht-radar-row", index))
            .w_full()
            .items_center()
            .gap(tokens.spacing.sm)
            .px(tokens.spacing.sm)
            .py(tokens.spacing.xs)
            .border_b_1()
            .border_color(extended.colors.hairline)
            .hover(|style| style.bg(extended.colors.row_hover))
            .text_size(tokens.typography.sm.size)
            .line_height(tokens.typography.sm.line_height)
            .text_color(tokens.colors.foreground)
            .child(
                div()
                    .flex_1()
                    .min_w_0()
                    .truncate()
                    .when(!has_name, |cell| {
                        cell.font_family(tokens.typography.mono.clone())
                            .text_color(tokens.colors.muted_foreground)
                    })
                    .child(name),
            )
            .child(cell(size, 96.))
            .child(cell(files, 56.))
            .child(cell(peers, 56.))
            .child(cell(seen, 88.))
            .child(
                h_flex()
                    .flex_none()
                    .w(theme.text_extent(132.))
                    .gap(tokens.spacing.xs)
                    .child(
                        loading_button(
                            ("dht-radar-download", index),
                            self.t("dhtRadarDownload"),
                            ButtonVariant::Primary,
                            self.busy,
                            cx,
                        )
                        .on_click(cx.listener({
                            let item = item.clone();
                            move |this, _: &ClickEvent, _, cx| this.download(&item, cx)
                        })),
                    )
                    .child(
                        loading_button(
                            ("dht-radar-copy", index),
                            self.t("dhtRadarCopy"),
                            ButtonVariant::Secondary,
                            false,
                            cx,
                        )
                        .on_click(cx.listener({
                            let item = item.clone();
                            move |this, _: &ClickEvent, _, cx| this.copy_magnet(&item, cx)
                        })),
                    ),
            )
            .into_any_element()
    }

    fn body(&self, cx: &mut Context<Self>) -> AnyElement {
        let tokens = active_theme(cx).tokens().clone();
        if self.items.is_empty() {
            let message = if self.failed {
                self.t("dhtRadarLoadFailed")
            } else if self.loading {
                self.t("dhtRadarLoading")
            } else {
                self.t("dhtRadarEmpty")
            };
            return v_flex()
                .flex_1()
                .w_full()
                .items_center()
                .justify_center()
                .gap(tokens.spacing.sm)
                .text_size(tokens.typography.sm.size)
                .text_color(tokens.colors.muted_foreground)
                .child(message)
                .into_any_element();
        }
        let mut table = card(cx)
            .flex()
            .flex_col()
            .w_full()
            .overflow_hidden()
            .child(self.header_row(cx));
        for (index, item) in self.items.iter().enumerate() {
            table = table.child(self.item_row(index, item, cx));
        }
        v_flex()
            .flex_1()
            .w_full()
            .min_h_0()
            .gap(tokens.spacing.xs)
            .child(div().flex_1().min_h_0().overflow_y_scrollbar().child(table))
            .child(
                div()
                    .text_size(active_theme(cx).extended().caption.size)
                    .text_color(active_theme(cx).extended().colors.text_tertiary)
                    .child(SharedString::from(format!(
                        "{}: {} / {}",
                        self.translator.text("dhtRadarShown"),
                        self.items.len(),
                        self.total
                    ))),
            )
            .into_any_element()
    }
}

impl Render for DhtRadarView {
    fn render(&mut self, _: &mut Window, cx: &mut Context<Self>) -> impl IntoElement {
        let tokens = active_theme(cx).tokens().clone();
        v_flex()
            .size_full()
            .gap(tokens.spacing.sm)
            .p(tokens.spacing.lg)
            .child(
                h_flex()
                    .w_full()
                    .items_center()
                    .justify_between()
                    .gap(tokens.spacing.sm)
                    .child(
                        div()
                            .text_size(tokens.typography.lg.size)
                            .font_weight(FontWeight::SEMIBOLD)
                            .child(self.t("dhtRadarTitle")),
                    )
                    .child(self.toolbar(cx)),
            )
            .child(self.stats_line(cx))
            .child(self.body(cx))
    }
}

/// 由 `crate::app::Desktop` 取客户端，供主窗口装配活动栏路由。
pub(crate) fn client(cx: &App) -> Arc<AgentClient> {
    Arc::clone(&Desktop::global(cx).client)
}

/// 字节数 → 人类可读（1024 进制，保留一位小数）。
fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit + 1 < UNITS.len() {
        value /= 1024.0;
        unit += 1;
    }
    if unit == 0 {
        format!("{bytes} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

/// Unix 秒 → 相对时间（采集页只关心「多久之前」）。
fn format_age(timestamp: i64) -> String {
    if timestamp <= 0 {
        return "—".to_owned();
    }
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| i64::try_from(elapsed.as_secs()).unwrap_or(i64::MAX))
        .unwrap_or(0);
    let seconds = (now - timestamp).max(0);
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        format!("{}m", seconds / 60)
    } else if seconds < 86_400 {
        format!("{}h", seconds / 3600)
    } else {
        format!("{}d", seconds / 86_400)
    }
}
