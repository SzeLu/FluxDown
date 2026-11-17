// 「IDM 7.0 视觉重设计」皮肤（design/ 设计稿）——纯前端样式数据，无功能逻辑。
//
// 落点：在 themeVariables() 解析完 GPUI 主题 token 之后，仅当当前使用【默认浅色 / 默认深色】
// 主题时，把中性结构色与状态色覆盖为设计稿配色。这样做的好处：
//   1. 默认打开即是新设计，深浅两套齐备（对应 design/浅色模式.jpeg、深色模式主窗口和下载窗口.jpeg）；
//   2. 用户主动切换到 midnight-blue / nord / graphite 等其他内置主题时原样保留 —— 不破坏主题切换功能；
//   3. 强调色（primary / accent / ring）由 appearance 系统继续控制 —— 不破坏强调色切换功能；
//   4. 不修改共享的 @gpui-theme registry（GPUI 桌面端、官网主题编辑器不受影响）。
//
// 配色取自 design/配色标准.jpeg：
//   Apple Green #60CD38 / Bleu De France #2D96F6 / Lightning Yellow #FFC225 /
//   Dark Orchid #9542D6 / Light Gray #F2F2F2 / Quill Grey #A8A8A8 /
//   Vampire Grey #4D4D4D / Charcoal #333333。

import type { AppearancePreferences, ThemeModeName } from './appearance'

type ColorMap = Readonly<Record<string, string>>

/** 浅色：浅灰蓝窗口 + 白卡片 + 绿色主色 + 蓝色强调。 */
const LIGHT: ColorMap = {
  '--fx-colors-background': '#e9eff1ff',
  '--fx-colors-surface': '#ffffffff',
  '--fx-colors-surface-foreground': '#333333ff',
  '--fx-colors-foreground': '#333333ff',
  '--fx-colors-chrome': '#f5f7f8ff',
  '--fx-colors-muted': '#edf1f2ff',
  '--fx-colors-muted-foreground': '#6b7780ff',
  '--fx-colors-text-tertiary': '#a8a8a8ff',
  '--fx-colors-border': '#e2e8eaff',
  '--fx-colors-hairline': '#e6ebedff',
  '--fx-colors-input': '#ffffffff',
  '--fx-colors-row-hover': '#f0f7efff',
  '--fx-colors-nav-hover': '#eaf3e6ff',
  '--fx-colors-nav-selected': '#e7f4e1ff',
  '--fx-colors-nav-selected-foreground': '#2f7a1aff',
  '--fx-colors-nav-selected-icon': '#60cd38ff',
  '--fx-colors-accent-text': '#2d96f6ff',
  '--fx-colors-status-downloading': '#4bb628ff',
  '--fx-colors-status-completed': '#60cd38ff',
  '--fx-colors-status-failed': '#e5484dff',
  '--fx-colors-status-paused': '#a8a8a8ff',
  '--fx-colors-status-queued': '#d99100ff',
  '--fx-colors-progress-track': '#e6ebedff',
  '--fx-colors-progress-fill': '#60cd38ff',
  '--fx-colors-success': '#4bb628ff',
  '--fx-colors-warning': '#d99100ff',
  '--fx-colors-info': '#2d96f6ff',
  '--fx-colors-caret': '#60cd38ff',
  // 设计稿的圆角语言：窗口/卡片/控件更圆润。
  '--fx-radius-md': '10px',
  '--fx-radius-lg': '14px',
  '--fx-radius-xl': '18px',
  '--fx-components-dialog-radius': '18px',
  '--fx-components-task-row-radius': '12px',
}

/** 深色：深蓝灰窗口 + 深卡片 + 绿色主色 + 亮蓝强调。 */
const DARK: ColorMap = {
  '--fx-colors-background': '#1f272dff',
  '--fx-colors-surface': '#2b343bff',
  '--fx-colors-surface-foreground': '#e8eef1ff',
  '--fx-colors-foreground': '#e8eef1ff',
  '--fx-colors-chrome': '#262f36ff',
  '--fx-colors-muted': '#333d45ff',
  '--fx-colors-muted-foreground': '#9aa8b0ff',
  '--fx-colors-text-tertiary': '#7d8a92ff',
  '--fx-colors-border': '#3b464eff',
  '--fx-colors-hairline': '#38434aff',
  '--fx-colors-input': '#2b343bff',
  '--fx-colors-row-hover': '#35423aff',
  '--fx-colors-nav-hover': '#333e37ff',
  '--fx-colors-nav-selected': '#30432fff',
  '--fx-colors-nav-selected-foreground': '#bfe8adff',
  '--fx-colors-nav-selected-icon': '#60cd38ff',
  '--fx-colors-accent-text': '#4fa8f7ff',
  '--fx-colors-status-downloading': '#4fa8f7ff',
  '--fx-colors-status-completed': '#6fd445ff',
  '--fx-colors-status-failed': '#f07575ff',
  '--fx-colors-status-paused': '#8b979eff',
  '--fx-colors-status-queued': '#ffc225ff',
  '--fx-colors-progress-track': '#3b464eff',
  '--fx-colors-progress-fill': '#60cd38ff',
  '--fx-colors-success': '#6fd445ff',
  '--fx-colors-warning': '#ffc225ff',
  '--fx-colors-info': '#4fa8f7ff',
  '--fx-colors-caret': '#60cd38ff',
  '--fx-radius-md': '10px',
  '--fx-radius-lg': '14px',
  '--fx-radius-xl': '18px',
  '--fx-components-dialog-radius': '18px',
  '--fx-components-task-row-radius': '12px',
}

/**
 * 把设计稿配色叠加到已解析的 token 变量表上。
 * 仅当当前模式使用的是默认浅/深主题时生效；切换到其他内置主题则保持原样。
 */
export function applyDesignSkin(vars: Record<string, string>, prefs: AppearancePreferences, mode: ThemeModeName): Record<string, string> {
  const onDefaultTheme = mode === 'dark' ? prefs.darkTheme === 'defaultDark' : prefs.lightTheme === 'defaultLight'
  if (!onDefaultTheme) return vars

  const skin = { ...vars, ...(mode === 'dark' ? DARK : LIGHT) }

  // 品牌主色：设计稿以 Apple Green 为主。仅在强调色仍是出厂默认（blue）时套用，
  // 让用户主动选择的任意强调色（含自定义色）完全由原 appearance 系统控制，功能不受影响。
  if (prefs.colorScheme === 'blue') {
    skin['--fx-colors-primary'] = mode === 'dark' ? '#5fcf38ff' : '#4bb628ff'
    skin['--fx-colors-primary-foreground'] = mode === 'dark' ? '#10240bff' : '#ffffffff'
    skin['--fx-colors-accent'] = mode === 'dark' ? '#4aa829ff' : '#d8f0caff'
    skin['--fx-colors-accent-foreground'] = mode === 'dark' ? '#5fcf38ff' : '#3a9721ff'
    skin['--fx-colors-ring'] = '#60cd38ff'
  }
  return skin
}
