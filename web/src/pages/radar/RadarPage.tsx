// DHT 种子雷达页（对齐 GPUI crates/app/src/windows/dht_radar.rs 的功能面）。
// 纯前端 + 既有 daemon.dht.radar.* RPC：采集开关 / 统计 / 发现列表 / 排序搜索 / 清空 / 导出导入。
// 功能面与桌面 GPUI 页一致；样式沿用 Web 设计皮肤（--fx-* 语义 token）。
//
// stats / list 非快照字段，本页自行轮询（页面挂载且有连接时），与 RSS / 详情页一致。

import { Copy, Download, FileDown, FileUp, RefreshCw, Search, Trash2, Wifi } from 'lucide-react'
import { useCallback, useEffect, useRef, useState } from 'react'
import type { ChangeEvent, ReactNode } from 'react'
import { useT } from '../../i18n'
import { cn } from '../../lib/cn'
import { RpcError, rpc } from '../../lib/rpc'
import { toastRpcError } from '../../lib/rpcToast'
import type { DhtRadarConfigDto, DhtRadarItemDto, DhtRadarStatsDto } from '../../lib/rpc'
import { TitleBarSlot } from '../../shell'
import { Badge, Button, Checkbox, EmptyState, Icon, Input, SectionHeader, SegmentedTabs, Spinner, Switch, Tooltip, confirmDialog, toast } from '../../ui'
import { formatBytes } from '../downloads/model/task'
import { openNewDownload } from '../downloads/dialogs'

const EMPTY_STATS: DhtRadarStatsDto = {
  running: false, port: 0, packetsReceived: 0, packetsSent: 0, queries: 0, responses: 0,
  nodes: 0, instances: 0, infohashes: 0, peers: 0, metadataOk: 0, metadataFailed: 0,
  lastPacket: 0, saved: 0, torrents: 0, withMetadata: 0,
}
const EMPTY_CONFIG: DhtRadarConfigDto = { enabled: false, port: 0, capacity: 0, metadata: false, nodes: 0, archiveThreshold: 0 }
const PAGE_SIZE = 100
const POLL_MS = 4000

type SortKey = 'recent' | 'peers' | 'size'

function relativeSeen(secs: number, now: number): string {
  if (secs <= 0) return '—'
  const delta = Math.max(0, now - secs)
  if (delta < 60) return '<1m'
  if (delta < 3600) return `${Math.floor(delta / 60)}m`
  if (delta < 86400) return `${Math.floor(delta / 3600)}h`
  return `${Math.floor(delta / 86400)}d`
}

/** 统计小卡：统一命中设计皮肤的表面色与圆角。 */
function Stat({ label, value, success }: { label: string; value: string | number; success?: boolean }) {
  return (
    <div className="flex min-w-0 flex-col gap-0.5 rounded-lg border border-hairline bg-surface px-3 py-2">
      <span className="truncate text-caption text-text-tertiary">{label}</span>
      <span className={cn('tabular truncate text-md font-medium', success ? 'text-success' : 'text-foreground')}>{value}</span>
    </div>
  )
}

function ConfigRow({ label, children }: { label: string; children: ReactNode }) {
  return (
    <div className="flex min-h-control items-center justify-between gap-3 px-1 text-sm">
      <span className="min-w-0 text-muted-foreground">{label}</span>
      {children}
    </div>
  )
}

export function RadarPage() {
  const t = useT()
  // 旧版常驻 daemon 不认识导入导出方法时，给出可操作的提示而不是通用 RPC 失败。
  const toastActionError = (error: unknown) => {
    if (error instanceof RpcError && error.is('unsupported')) {
      toast.error(t('dhtRadarRestartRequired'))
    } else {
      toastRpcError(error)
    }
  }
  const [stats, setStats] = useState<DhtRadarStatsDto>(EMPTY_STATS)
  const [config, setConfig] = useState<DhtRadarConfigDto>(EMPTY_CONFIG)
  const [draft, setDraft] = useState<DhtRadarConfigDto>(EMPTY_CONFIG)
  const [items, setItems] = useState<readonly DhtRadarItemDto[]>([])
  const [total, setTotal] = useState(0)
  const [loading, setLoading] = useState(true)
  const [statsFailed, setStatsFailed] = useState(false)
  const [query, setQuery] = useState('')
  const [sort, setSort] = useState<SortKey>('recent')
  const [namedOnly, setNamedOnly] = useState(false)
  const [offset, setOffset] = useState(0)
  const [busy, setBusy] = useState(false)
  const [clearArmed, setClearArmed] = useState(false)
  const [showConfig, setShowConfig] = useState(false)
  const fileRef = useRef<HTMLInputElement>(null)
  const mounted = useRef(true)

  useEffect(() => {
    mounted.current = true
    return () => { mounted.current = false }
  }, [])

  const refresh = useCallback(async () => {
    try {
      const [nextStats, list] = await Promise.all([
        rpc.daemon.dhtRadar.stats(),
        rpc.daemon.dhtRadar.list({ query: query || undefined, sort, offset, limit: PAGE_SIZE, namedOnly }),
      ])
      if (!mounted.current) return
      setStats(nextStats)
      setItems(list.items)
      setTotal(list.total)
      setStatsFailed(false)
    } catch {
      if (mounted.current) setStatsFailed(true)
    } finally {
      if (mounted.current) setLoading(false)
    }
  }, [query, sort, offset, namedOnly])

  const loadConfig = useCallback(async () => {
    try {
      const cfg = await rpc.daemon.dhtRadar.config()
      if (mounted.current) { setConfig(cfg); setDraft(cfg) }
    } catch (error) {
      toastRpcError(error)
    }
  }, [])

  useEffect(() => { void loadConfig() }, [loadConfig])
  useEffect(() => { setLoading(true); void refresh() }, [refresh])
  useEffect(() => {
    const timer = window.setInterval(() => { void refresh() }, POLL_MS)
    return () => window.clearInterval(timer)
  }, [refresh])

  const runBusy = async (action: () => Promise<void>) => {
    if (busy) return
    setBusy(true)
    try { await action() } finally { if (mounted.current) setBusy(false) }
  }

  const toggleCollection = () => void runBusy(async () => {
    try {
      const next = await rpc.daemon.dhtRadar.setConfig({ enabled: !config.enabled })
      setConfig(next); setDraft(next)
      toast.key(next.enabled ? 'dhtRadarStarted' : 'dhtRadarStoppedNotice', 'success')
      await refresh()
    } catch (error) { toastActionError(error) }
  })

  const saveConfig = () => void runBusy(async () => {
    try {
      const next = await rpc.daemon.dhtRadar.setConfig({
        port: draft.port, capacity: draft.capacity, metadata: draft.metadata,
        nodes: draft.nodes, archiveThreshold: draft.archiveThreshold,
      })
      setConfig(next); setDraft(next); setShowConfig(false)
      toast.success(t('queueSaveAction'))
      await refresh()
    } catch (error) { toastActionError(error) }
  })

  const clearSaved = () => void runBusy(async () => {
    if (!clearArmed) { setClearArmed(true); return }
    const ok = await confirmDialog({
      title: t('dhtRadarClear'),
      description: t('dhtRadarClearHint'),
      intent: 'destructive',
      okLabel: t('dhtRadarClearConfirm'),
    })
    if (!ok) { setClearArmed(false); return }
    try {
      await rpc.daemon.dhtRadar.clear()
      toast.success(t('dhtRadarCleared'))
      setClearArmed(false); setOffset(0); await refresh()
    } catch (error) { toastActionError(error) }
  })

  const exportSaved = () => void runBusy(async () => {
    try {
      const result = await rpc.daemon.dhtRadar.export()
      if (result.count === 0) { toast.info(t('dhtRadarExportEmpty')); return }
      const url = URL.createObjectURL(new Blob([result.text], { type: 'application/json;charset=utf-8' }))
      const anchor = document.createElement('a')
      anchor.href = url
      anchor.download = `fluxdown-dht-radar-${new Date().toISOString().slice(0, 10)}.json`
      // Firefox / Safari 会在 detached <a> 或过早 revoke 时直接放弃下载。
      document.body.append(anchor)
      anchor.click()
      anchor.remove()
      window.setTimeout(() => URL.revokeObjectURL(url), 60_000)
      toast.success(t('dhtRadarExported', { n: result.count }))
    } catch (error) { toastActionError(error) }
  })

  const importFile = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file) return
    void runBusy(async () => {
      try {
        const result = await rpc.daemon.dhtRadar.import({ text: await file.text() })
        toast.success(t('dhtRadarImported', { imported: result.imported, skipped: result.skipped, invalid: result.invalid }))
        setOffset(0); await refresh()
      } catch (error) { toastActionError(error) }
    })
  }

  const copyMagnet = (item: DhtRadarItemDto) => {
    void navigator.clipboard.writeText(item.magnet).then(
      () => toast.success(t('dhtRadarCopied')),
      () => toast.error(t('dhtRadarActionFailed')),
    )
  }

  const download = (item: DhtRadarItemDto) => {
    openNewDownload({ urls: [item.magnet] })
    toast.success(t('dhtRadarDownloadStarted'))
  }

  const now = Math.floor(Date.now() / 1000)
  const pageCount = Math.max(1, Math.ceil(total / PAGE_SIZE))
  const page = Math.floor(offset / PAGE_SIZE) + 1
  const sortItems: readonly { value: SortKey; label: string }[] = [
    { value: 'recent', label: t('dhtRadarSortRecent') },
    { value: 'peers', label: t('dhtRadarSortPeers') },
    { value: 'size', label: t('dhtRadarSortSize') },
  ]

  return (
    <div className="flex h-full min-h-0 w-full flex-col">
      <TitleBarSlot>
        <div className="flex min-w-0 flex-1 items-center justify-end gap-2">
          <Tooltip content={t('dhtRadarRefresh')}>
            <Button variant="ghost" iconOnly icon={RefreshCw} loading={loading} aria-label={t('dhtRadarRefresh')} onClick={() => void refresh()} />
          </Tooltip>
          <Button
            variant={stats.running ? 'outline' : 'primary'}
            className={stats.running ? undefined : 'fx-gradient-cta'}
            icon={stats.running ? undefined : Download}
            disabled={busy}
            onClick={toggleCollection}
          >
            {t(stats.running ? 'dhtRadarDisable' : 'dhtRadarEnable')}
          </Button>
        </div>
      </TitleBarSlot>

      <div className="flex min-h-0 flex-1 flex-col gap-3 overflow-y-auto p-3">
        {/* 状态概览 */}
        <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 lg:grid-cols-6">
          <Stat label={t('dhtRadarRunning')} value={stats.running ? t('dhtRadarRunning') : t('dhtRadarIdle')} success={stats.running} />
          <Stat label={t('dhtRadarNodes')} value={stats.nodes} />
          <Stat label={t('dhtRadarTorrents')} value={stats.torrents} />
          <Stat label={t('dhtRadarWithMetadata')} value={stats.withMetadata} />
          <Stat label={t('dhtRadarPackets')} value={stats.packetsReceived} />
          <Stat label={t('dhtRadarSaved')} value={stats.saved} />
        </div>

        {statsFailed ? (
          <div role="alert" className="rounded-md border border-destructive/40 bg-destructive/10 px-3 py-2 text-xs text-destructive">
            {t('dhtRadarLoadFailed')}
          </div>
        ) : null}

        {/* 配置面板（折叠） */}
        <section className="rounded-xl border border-hairline bg-chrome">
          <SectionHeader
            trailing={
              <Button variant="ghost" onClick={() => { setShowConfig((value) => !value); void loadConfig() }}>
                {t(showConfig ? 'collapseSidebar' : 'expandSidebar')}
              </Button>
            }
          >
            {t('settings')}
          </SectionHeader>
          {showConfig ? (
            <div className="grid gap-2 px-3 pb-3 sm:grid-cols-2">
              <ConfigRow label={t('dhtRadarEnable')}>
                <Switch checked={draft.enabled} onCheckedChange={(value) => setDraft((prev) => ({ ...prev, enabled: value }))} />
              </ConfigRow>
              <ConfigRow label="UDP">
                <Input className="w-24" type="number" min={0} value={draft.port} onChange={(e) => setDraft((prev) => ({ ...prev, port: Number(e.target.value) || 0 }))} />
              </ConfigRow>
              <ConfigRow label={t('dhtRadarNodes')}>
                <Input className="w-24" type="number" min={1} value={draft.nodes} onChange={(e) => setDraft((prev) => ({ ...prev, nodes: Number(e.target.value) || 1 }))} />
              </ConfigRow>
              <ConfigRow label={t('dhtRadarShown')}>
                <Input className="w-24" type="number" min={16} value={draft.capacity} onChange={(e) => setDraft((prev) => ({ ...prev, capacity: Number(e.target.value) || 16 }))} />
              </ConfigRow>
              <ConfigRow label={t('dhtRadarArchiveLimit', { n: draft.archiveThreshold })}>
                <Input className="w-24" type="number" min={0} value={draft.archiveThreshold} onChange={(e) => setDraft((prev) => ({ ...prev, archiveThreshold: Number(e.target.value) || 0 }))} />
              </ConfigRow>
              <ConfigRow label={t('dhtRadarWithMetadata')}>
                <Switch checked={draft.metadata} onCheckedChange={(value) => setDraft((prev) => ({ ...prev, metadata: value }))} />
              </ConfigRow>
              <div className="flex justify-end gap-2 pt-1 sm:col-span-2">
                <Button variant="outline" onClick={saveConfig} disabled={busy}>{t('queueSaveAction')}</Button>
              </div>
            </div>
          ) : null}
        </section>

        {/* 工具栏 */}
        <div className="flex flex-wrap items-center gap-2">
          <div className="relative min-w-[180px] flex-1">
            <Icon icon={Search} size="md" className="pointer-events-none absolute left-2 top-1/2 -translate-y-1/2 text-text-tertiary" />
            <Input className="w-full pl-8" placeholder={t('searchTasksPlaceholder')} value={query} onChange={(event) => { setQuery(event.target.value); setOffset(0) }} />
          </div>
          <div className="w-[240px] max-w-full">
            <SegmentedTabs items={sortItems} value={sort} onValueChange={(value) => { setSort(value); setOffset(0) }} aria-label={t('dhtRadarSortRecent')} />
          </div>
          <label className="flex cursor-pointer items-center gap-1.5 text-xs text-muted-foreground">
            <Checkbox checked={namedOnly} onCheckedChange={(value) => { setNamedOnly(value); setOffset(0) }} />
            {t('dhtRadarOnlyNamed')}
          </label>
          <div className="flex items-center gap-1">
            <Tooltip content={t('dhtRadarExport')}>
              <Button variant="ghost" iconOnly icon={FileDown} aria-label={t('dhtRadarExport')} loading={busy} onClick={exportSaved} />
            </Tooltip>
            <Tooltip content={t('dhtRadarImport')}>
              <Button variant="ghost" iconOnly icon={FileUp} aria-label={t('dhtRadarImport')} disabled={busy} onClick={() => fileRef.current?.click()} />
            </Tooltip>
            <input ref={fileRef} type="file" accept="application/json,.json" className="hidden" onChange={importFile} />
            <Tooltip content={t('dhtRadarClear')}>
              <Button variant={clearArmed ? 'danger' : 'ghost'} iconOnly icon={Trash2} aria-label={t('dhtRadarClear')} disabled={busy} onClick={clearSaved} />
            </Tooltip>
          </div>
        </div>
        {clearArmed ? <p className="text-xs text-warning">{t('dhtRadarClearHint')}</p> : null}

        {/* 发现列表 */}
        <div className="min-h-0 flex-1 overflow-hidden rounded-xl border border-hairline bg-surface">
          {loading && items.length === 0 ? (
            <div className="flex h-full items-center justify-center gap-2 text-sm text-muted-foreground">
              <Spinner /> {t('dhtRadarLoading')}
            </div>
          ) : items.length === 0 ? (
            <EmptyState className="h-full" icon={Wifi} title={t('dhtRadarTitle')} description={t('dhtRadarEmpty')} />
          ) : (
            <div className="h-full overflow-auto">
              <table className="w-full text-left text-sm">
                <thead className="sticky top-0 z-10 bg-chrome text-caption text-text-tertiary">
                  <tr>
                    <th className="px-3 py-2 font-medium">{t('dhtRadarColName')}</th>
                    <th className="px-3 py-2 text-right font-medium">{t('dhtRadarColSize')}</th>
                    <th className="px-3 py-2 text-right font-medium">{t('dhtRadarColFiles')}</th>
                    <th className="px-3 py-2 text-right font-medium">{t('dhtRadarColPeers')}</th>
                    <th className="px-3 py-2 text-right font-medium">{t('dhtRadarColSeen')}</th>
                    <th className="px-3 py-2 text-right font-medium">{t('dhtRadarColActions')}</th>
                  </tr>
                </thead>
                <tbody>
                  {items.map((item) => (
                    <tr key={item.infoHash} className="border-t border-hairline hover:bg-row-hover">
                      <td className="max-w-[320px] px-3 py-2">
                        <div className="flex min-w-0 items-center gap-2">
                          <span className="truncate text-foreground" title={item.name ?? item.infoHash}>{item.name ?? item.infoHash.slice(0, 12)}</span>
                          {item.hasMetadata ? <Badge tone="success">{t('dhtRadarWithMetadata')}</Badge> : null}
                        </div>
                        <div className="truncate font-mono text-caption text-text-tertiary" title={item.infoHash}>{item.infoHash.slice(0, 16)}…</div>
                      </td>
                      <td className="tabular whitespace-nowrap px-3 py-2 text-right text-muted-foreground">{item.totalSize ? formatBytes(item.totalSize) : '—'}</td>
                      <td className="tabular px-3 py-2 text-right text-muted-foreground">{item.fileCount ?? '—'}</td>
                      <td className="tabular px-3 py-2 text-right text-muted-foreground">{item.peers}</td>
                      <td className="tabular whitespace-nowrap px-3 py-2 text-right text-text-tertiary">{relativeSeen(item.lastSeen, now)}</td>
                      <td className="px-3 py-2">
                        <div className="flex items-center justify-end gap-1">
                          <Tooltip content={t('dhtRadarDownload')}>
                            <Button variant="ghost" iconOnly icon={Download} aria-label={t('dhtRadarDownload')} onClick={() => download(item)} />
                          </Tooltip>
                          <Tooltip content={t('dhtRadarCopy')}>
                            <Button variant="ghost" iconOnly icon={Copy} aria-label={t('dhtRadarCopy')} onClick={() => copyMagnet(item)} />
                          </Tooltip>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* 分页 */}
        <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">
          <span className="tabular">{t('dhtRadarShown')}: {items.length} / {total}</span>
          <div className="flex items-center gap-2">
            <Button variant="outline" disabled={offset === 0 || busy} onClick={() => setOffset((value) => Math.max(0, value - PAGE_SIZE))}>{t('progressWindowPrevious')}</Button>
            <span className="tabular">{page} / {pageCount}</span>
            <Button variant="outline" disabled={offset + PAGE_SIZE >= total || busy} onClick={() => setOffset((value) => value + PAGE_SIZE)}>{t('progressWindowNext')}</Button>
          </div>
        </div>
      </div>
    </div>
  )
}
