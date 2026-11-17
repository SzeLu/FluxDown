// DHT 种子雷达 DTO（手写镜像 native/protocol/src/daemon.rs 的 DhtRadar*，wire 为 camelCase JSON）。
// 方法名见 method.ts；后端实现 native/daemon/src/dht_radar_rpc.rs。

/** `daemon.dht.radar.list` 参数。 */
export interface DhtRadarListParams {
  /** 匹配种子名（不区分大小写）或 infohash 前缀。 */
  query?: string
  /** `recent`（默认）/ `peers` / `size`。 */
  sort?: string
  offset?: number
  /** 默认 100，上限 500。 */
  limit?: number
  namedOnly?: boolean
}

/** 雷达列表中的一条种子。 */
export interface DhtRadarItemDto {
  infoHash: string
  name?: string
  totalSize?: number
  fileCount?: number
  peers: number
  /** Unix 秒。 */
  firstSeen: number
  /** Unix 秒。 */
  lastSeen: number
  hasMetadata: boolean
  magnet: string
}

export interface DhtRadarListDto {
  total: number
  items: DhtRadarItemDto[]
}

/** `daemon.dht.radar.stats` 结果。 */
export interface DhtRadarStatsDto {
  running: boolean
  port: number
  packetsReceived: number
  packetsSent: number
  queries: number
  responses: number
  nodes: number
  instances: number
  infohashes: number
  peers: number
  metadataOk: number
  metadataFailed: number
  /** 最近一次报文的 Unix 秒；0 = 还没收到。 */
  lastPacket: number
  saved: number
  torrents: number
  withMetadata: number
}

/** 雷达配置。 */
export interface DhtRadarConfigDto {
  enabled: boolean
  /** 0 = 系统分配。 */
  port: number
  capacity: number
  metadata: boolean
  nodes: number
  /** 0 = 关闭自动归档。 */
  archiveThreshold: number
}

/** `daemon.dht.radar.config.set` 参数；只提交需要修改的字段。 */
export interface DhtRadarSetConfigParams {
  enabled?: boolean
  port?: number
  capacity?: number
  metadata?: boolean
  nodes?: number
  archiveThreshold?: number
}

export interface DhtRadarClearDto {
  removed: number
}

export interface DhtRadarExportDto {
  count: number
  text: string
}

export interface DhtRadarImportParams {
  text: string
}

export interface DhtRadarImportDto {
  imported: number
  skipped: number
  invalid: number
}
