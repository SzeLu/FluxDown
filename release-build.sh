#!/usr/bin/env bash
#
# release-build.sh — FluxDown 桌面端发布构建脚本
#
# 一次完成：cargo release 构建（desktop / agent / daemon / nmh）→ 平台打包 → 产物清单。
# 打包逻辑仍由 scripts/package_gpui_{linux,macos}.sh 与 installer/windows/setup.iss 承担，
# 本脚本只做平台 / 架构 / 格式选择、依赖检查与命令编排（与 CI 的 release.yml 同一套命令；
# Windows 的暂存 / Inno Setup / 便携 zip 步骤与 release.yml 的 pwsh 步骤等价，经 PowerShell 内联执行）。
#
# 用法:
#   ./release-build.sh [平台] [选项]
#
# 平台:
#   linux | windows | macos   （缺省 = 当前平台；也接受 gnu/win/darwin 等别名）
#
# 格式:
#   linux    tar（便携 tar.gz） / appimage / deb / arch（.pkg.tar.zst）
#   windows  setup（Inno Setup 安装程序） / portable（便携 zip）
#   macos    dmg / tar（便携 tar.gz，内含 FluxDown.app）
#
# 选项:
#   -b, --bundles <列表>  直接指定包格式，跳过交互选择（逗号分隔，如 deb,arch）
#   -a, --arch <架构>     x64 | arm64 | universal（universal 仅 macOS）
#   -V, --version <版本>  发布版本；缺省取 v* tag，再缺省取 Cargo.toml 版本
#   -y, --yes             非交互模式，构建该平台支持的全部格式
#   -n, --dry-run         只打印将要执行的命令，不实际构建
#   -v, --verbose         显示详细构建日志
#   -j, --jobs <N>        并行编译任务数（缺省 = cargo 默认 = 主机核数）
#       --low-memory      内存受限时使用：-j1、thin LTO、16 个 codegen 单元、O2、
#                         关闭增量构建；牺牲编译时间换取更低内存峰值
#       --skip-build      复用 target/<triple>/release 下已有产物，只做打包
#   -h, --help            显示本帮助
#
# 示例:
#   ./release-build.sh                       # 当前平台，交互选择格式
#   ./release-build.sh linux -b deb,arch     # 只出 deb 与 Arch 包
#   ./release-build.sh macos -a universal    # macOS 通用二进制（DMG + tar.gz）
#   ./release-build.sh windows -y            # Windows 安装程序 + 便携 zip
#   ./release-build.sh -V 0.7.0 -n           # 预览发布版本 0.7.0 的构建命令
#
# 说明:
#   * 产物统一写到 build/installer，命名与 CI 发布资产一致
#     （FluxDown-<版本>-<平台>-<架构>.*）。
#   * 只支持在目标操作系统上构建（Windows 需要 Windows 主机、macOS 需要 macOS 主机、
#     Linux 需要 Linux 主机）；同一系统内的跨架构支持 macOS universal 与
#     Windows x64 ↔ arm64。Linux 目前只有 x64 发行包。
#   * 版本来源按 --version → 环境变量 VERSION → 环境变量 FLUXDOWN_APP_VERSION →
#     最近的 v* tag 顺序解析；都取不到时回退 crates/app/Cargo.toml 的版本，且
#     不注入 FLUXDOWN_APP_VERSION（应用按非官方构建处理，不参与自动更新）。
#   * macOS 签名 / 公证环境变量原样透传给 packaging 脚本：
#     MACOS_CERT_P12_PATH + MACOS_CERT_PASSWORD 或 MACOS_SIGN_IDENTITY；
#     公证另需 APPLE_API_KEY_PATH + APPLE_API_KEY_ID + APPLE_API_ISSUER_ID。
#     都没有时做 ad-hoc 签名（不可分发）。Windows 产物与 CI 一样不签名。
#   * Linux AppImage 需要 appimagetool（PATH 中，或 APPIMAGETOOL 指定路径）；
#     deb / Arch 分别需要 dpkg-deb / zstd。
#   * --low-memory 的产物与正式发布包优化参数不同（thin LTO / 多 codegen 单元 / O2），
#     只用于本机验证与出包，不要作为发布资产分发。日常 cargo 构建（check / build /
#     nextest）请用 scripts/cargo-low-mem.sh，它带同一套低内存参数。
#   * 进一步压低链接内存：安装 lld（sudo apt-get install lld）后，先
#     export RUSTFLAGS='-C link-arg=-fuse-ld=lld' 再运行本脚本。
#   * 兼容 macOS 自带的 bash 3.2：不使用 bash 4 才有的关联数组。
set -euo pipefail

REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO"

OUT_DIR="$REPO/build/installer"
POWERSHELL=""
ISCC_PATH=""

# ---------------------------------------------------------------- 输出辅助
if [[ -t 1 ]]; then
  BOLD=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GREEN=$'\033[32m'
  YELLOW=$'\033[33m'; CYAN=$'\033[36m'; RESET=$'\033[0m'
else
  BOLD=''; DIM=''; RED=''; GREEN=''; YELLOW=''; CYAN=''; RESET=''
fi

info()  { printf '%s==>%s %s\n' "$CYAN" "$RESET" "$*"; }
warn()  { printf '%s[警告]%s %s\n' "$YELLOW" "$RESET" "$*" >&2; }
error() { printf '%s[错误]%s %s\n' "$RED" "$RESET" "$*" >&2; }
die()   { error "$*"; exit 1; }

usage() {
  cat <<'USAGE'

release-build.sh — FluxDown 桌面端发布构建脚本

用法:
  ./release-build.sh [平台] [选项]

平台:
  linux | windows | macos   （缺省 = 当前平台；也接受 gnu/win/darwin 等别名）

格式:
  linux    tar（便携 tar.gz） / appimage / deb / arch（.pkg.tar.zst）
  windows  setup（Inno Setup 安装程序） / portable（便携 zip）
  macos    dmg / tar（便携 tar.gz，内含 FluxDown.app）

选项:
  -b, --bundles <列表>  直接指定包格式，跳过交互选择（逗号分隔，如 deb,arch）
  -a, --arch <架构>     x64 | arm64 | universal（universal 仅 macOS）
  -V, --version <版本>  发布版本；缺省取 v* tag，再缺省取 Cargo.toml 版本
  -y, --yes             非交互模式，构建该平台支持的全部格式
  -n, --dry-run         只打印将要执行的命令，不实际构建
  -v, --verbose         显示详细构建日志
  -j, --jobs <N>        并行编译任务数（缺省 = cargo 默认 = 主机核数）
      --low-memory      内存受限时使用：-j1、thin LTO、16 个 codegen 单元、O2、
                        关闭增量构建；牺牲编译时间换取更低内存峰值
      --skip-build      复用 target/<triple>/release 下已有产物，只做打包
  -h, --help            显示本帮助

示例:
  ./release-build.sh                       # 当前平台，交互选择格式
  ./release-build.sh linux -b deb,arch     # 只出 deb 与 Arch 包
  ./release-build.sh macos -a universal    # macOS 通用二进制（DMG + tar.gz）
  ./release-build.sh windows -y            # Windows 安装程序 + 便携 zip
  ./release-build.sh -V 0.7.0 -n           # 预览发布版本 0.7.0 的构建命令
  ./release-build.sh --low-memory -y       # 小内存机器（8GB 级）串行低峰构建

产物:
  build/installer/FluxDown-<版本>-<平台>-<架构>.*（与 CI 发布资产同名）

USAGE
}

# ---------------------------------------------------------------- 参数解析
PLATFORM_ARG=""
FORMATS_ARG=""
ARCH_ARG=""
VERSION_ARG=""
JOBS_ARG=""
ASSUME_YES=0
DRY_RUN=0
VERBOSE=0
SKIP_BUILD=0
LOW_MEMORY=0

need_value() {
  [[ $# -ge 2 && -n "${2:-}" ]] || die "选项 $1 需要一个参数值"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)            usage; exit 0 ;;
    -b|--bundles|--format|-f)
      need_value "$@"; FORMATS_ARG="$2"; shift 2 ;;
    --bundles=*|--format=*)
      FORMATS_ARG="${1#*=}"; [[ -n "$FORMATS_ARG" ]] || die "选项 ${1%%=*} 需要一个参数值"; shift ;;
    -a|--arch)
      need_value "$@"; ARCH_ARG="$2"; shift 2 ;;
    --arch=*)
      ARCH_ARG="${1#*=}"; [[ -n "$ARCH_ARG" ]] || die "选项 --arch 需要一个参数值"; shift ;;
    -V|--version)
      need_value "$@"; VERSION_ARG="$2"; shift 2 ;;
    --version=*)
      VERSION_ARG="${1#*=}"; [[ -n "$VERSION_ARG" ]] || die "选项 --version 需要一个参数值"; shift ;;
    --skip-build)         SKIP_BUILD=1; shift ;;
    -y|--yes)             ASSUME_YES=1; shift ;;
    -n|--dry-run)         DRY_RUN=1; shift ;;
    -v|--verbose)         VERBOSE=1; shift ;;
    -j|--jobs)
      need_value "$@"; JOBS_ARG="$2"; shift 2 ;;
    --jobs=*)
      JOBS_ARG="${1#*=}"; [[ "$JOBS_ARG" =~ ^[1-9][0-9]*$ ]] || die "选项 --jobs 需要正整数"; shift ;;
    --low-memory)         LOW_MEMORY=1; shift ;;
    --)                   shift; break ;;
    -*)                   die "未知选项：$1（使用 --help 查看用法）" ;;
    *)
      [[ -z "$PLATFORM_ARG" ]] || die "只能指定一个目标平台（已指定：$PLATFORM_ARG，重复：$1）"
      PLATFORM_ARG="$1"; shift ;;
  esac
done

if [[ -n "$JOBS_ARG" && ! "$JOBS_ARG" =~ ^[1-9][0-9]*$ ]]; then
  die "选项 --jobs 需要正整数（收到：$JOBS_ARG）"
fi

# ---------------------------------------------------------------- 平台与格式定义
lower() { printf '%s' "$1" | tr '[:upper:]' '[:lower:]'; }

normalize_platform() {
  case "$(lower "$1")" in
    linux|gnu)                           echo linux ;;
    windows|win|win32|msvc|windows-msvc) echo windows ;;
    macos|mac|osx|darwin|apple)          echo macos ;;
    *) return 1 ;;
  esac
}

platform_display() {
  case "$1" in
    linux)   echo "Linux" ;;
    windows) echo "Windows" ;;
    macos)   echo "macOS" ;;
  esac
}

# 各平台可用的打包格式（顺序即交互菜单顺序）
platform_formats() {
  case "$1" in
    linux)   echo "tar appimage deb arch" ;;
    windows) echo "setup portable" ;;
    macos)   echo "dmg tar" ;;
  esac
}

format_label() {
  case "$1" in
    tar)      echo "便携 tar.gz (.tar.gz)" ;;
    appimage) echo "AppImage 便携包 (.AppImage)" ;;
    deb)      echo "Debian 包 (.deb)" ;;
    arch)     echo "Arch 包 (.pkg.tar.zst)" ;;
    setup)    echo "Inno Setup 安装程序 (.exe)" ;;
    portable) echo "便携 zip (.zip)" ;;
    dmg)      echo "macOS 磁盘映像 (.dmg)" ;;
    *)        echo "$1" ;;
  esac
}

normalize_format() {
  case "$(lower "$1")" in
    tar|targz|tar.gz|tgz)          echo tar ;;
    appimage)                      echo appimage ;;
    deb|debian)                    echo deb ;;
    arch|archlinux|pkg|pkg.tar.zst) echo arch ;;
    setup|exe|installer|inno|iss)  echo setup ;;
    portable|zip|portable.zip)     echo portable ;;
    dmg|diskimage)                 echo dmg ;;
    *) return 1 ;;
  esac
}

normalize_arch() {
  case "$(lower "$1")" in
    x64|x86_64|amd64)     echo x64 ;;
    arm64|aarch64)        echo arm64 ;;
    universal|universal2) echo universal ;;
    *) return 1 ;;
  esac
}

# ---------------------------------------------------------------- 环境探测
need_cmd() {
  # $1 命令；$2 安装提示
  if command -v "$1" >/dev/null 2>&1; then
    return 0
  fi
  if (( DRY_RUN )); then
    warn "缺少 $1（实际构建前需要）：$2"
    return 0
  fi
  die "缺少 $1：$2"
}

need_cmd rustc "请先安装 Rust 工具链（rustup）"
if (( ! SKIP_BUILD )); then
  need_cmd cargo "请先安装 Rust 工具链（rustup）"
fi

HOST_TRIPLE="$(rustc -vV | awk '/^host: /{print $2}')"
[[ -n "$HOST_TRIPLE" ]] || die "无法确定 Rust 主机三元组（rustc -vV 输出异常）"

case "$HOST_TRIPLE" in
  *-linux-*)        HOST_PLATFORM=linux ;;
  *-windows-*)      HOST_PLATFORM=windows ;;
  *-apple-darwin*)  HOST_PLATFORM=macos ;;
  *)
    case "$(uname -s 2>/dev/null || true)" in
      Linux*)  HOST_PLATFORM=linux ;;
      Darwin*) HOST_PLATFORM=macos ;;
      MINGW*|MSYS*|CYGWIN*) HOST_PLATFORM=windows ;;
      *)       die "无法识别当前平台（$HOST_TRIPLE）" ;;
    esac ;;
esac

case "$(lower "$(uname -m 2>/dev/null || echo "")")" in
  x86_64|amd64)  HOST_ARCH=x64 ;;
  aarch64|arm64) HOST_ARCH=arm64 ;;
  *)             HOST_ARCH="$(normalize_arch "${HOST_TRIPLE%%-*}" 2>/dev/null || echo x64)" ;;
esac

# 目标平台：未传参 → 当前平台
if [[ -n "$PLATFORM_ARG" ]]; then
  TARGET_PLATFORM="$(normalize_platform "$PLATFORM_ARG")" \
    || die "未知平台：$PLATFORM_ARG（可选：linux / windows / macos）"
else
  TARGET_PLATFORM="$HOST_PLATFORM"
fi
PLATFORM_DISPLAY="$(platform_display "$TARGET_PLATFORM")"

# 目标架构：未传参 → 主机架构
ARCH="$HOST_ARCH"
if [[ -n "$ARCH_ARG" ]]; then
  ARCH="$(normalize_arch "$ARCH_ARG")" \
    || die "未知架构：$ARCH_ARG（可选：x64 / arm64 / universal）"
fi

if [[ "$ARCH" == universal && "$TARGET_PLATFORM" != macos ]]; then
  die "--arch universal 仅适用于 macOS"
fi

# Linux 发行包目前只有 x64（scripts/package_gpui_linux.sh 的产物名、deb / Arch 元数据
# 都按 x64 固定；更新检查也按 linux-x64 资产匹配）。
if [[ "$TARGET_PLATFORM" == linux && "$ARCH" != x64 ]]; then
  die "Linux 发布包目前只有 x64：scripts/package_gpui_linux.sh 的产物名与 deb / Arch
元数据均按 x64 固定。交叉架构请在目标架构主机上另行打包。"
fi

# 三元组
triple_of() {
  case "$1-$2" in
    linux-x64)       echo x86_64-unknown-linux-gnu ;;
    linux-arm64)     echo aarch64-unknown-linux-gnu ;;
    windows-x64)     echo x86_64-pc-windows-msvc ;;
    windows-arm64)   echo aarch64-pc-windows-msvc ;;
    macos-x64)       echo x86_64-apple-darwin ;;
    macos-arm64)     echo aarch64-apple-darwin ;;
    *) return 1 ;;
  esac
}

TARGET_TRIPLES=()
if [[ "$ARCH" == universal ]]; then
  TARGET_TRIPLES=(aarch64-apple-darwin x86_64-apple-darwin)
  TARGET_TRIPLE=universal-apple-darwin   # 仅用于展示
else
  TARGET_TRIPLE="$(triple_of "$TARGET_PLATFORM" "$ARCH")" \
    || die "不支持的平台 / 架构组合：$TARGET_PLATFORM / $ARCH"
  TARGET_TRIPLES=("$TARGET_TRIPLE")
fi
ARCH_LABEL="$ARCH"

# ---------------------------------------------------------------- 跨平台可行性
if [[ "$TARGET_PLATFORM" != "$HOST_PLATFORM" ]]; then
  case "$TARGET_PLATFORM" in
    windows)
      die "不支持从 $(platform_display "$HOST_PLATFORM") 交叉构建 Windows 包：
GPUI 的 Windows 构建需要 MSVC 工具链与 Windows SDK，且 Inno Setup 只能在 Windows 上运行。
建议：在 Windows 主机上运行本脚本，或使用 CI（.github/workflows/release.yml）。" ;;
    macos)
      die "不支持从 $(platform_display "$HOST_PLATFORM") 交叉构建 macOS 包：
需要 Xcode 工具链（iconutil / codesign / hdiutil）与签名环境。
建议：在 macOS 主机或 CI（macos-15）上构建。" ;;
    linux)
      die "不支持从 $(platform_display "$HOST_PLATFORM") 交叉构建 Linux 包：
需要 Linux 链接器与 deb / Arch / AppImage 打包工具。
建议：使用 Linux 主机 / 容器 / CI（ubuntu-22.04）。" ;;
  esac
fi

# 同一系统内的跨架构提示
if [[ "$ARCH" != universal && "$ARCH" != "$HOST_ARCH" ]]; then
  case "$TARGET_PLATFORM" in
    windows)
      warn "Windows 跨架构构建（$HOST_ARCH → $ARCH）：需要 Visual Studio 的 $ARCH 生成工具与
rustup 目标；链接阶段缺工具链时请安装对应组件。" ;;
    macos)
      info "macOS 跨架构构建（$HOST_ARCH → $ARCH）：需先 rustup target add $(triple_of macos "$ARCH")" ;;
  esac
fi

# ---------------------------------------------------------------- 版本
VERSION_FROM=""
OFFICIAL=1
if [[ -n "$VERSION_ARG" ]]; then
  VERSION_VALUE="$VERSION_ARG"; VERSION_FROM="--version"
elif [[ -n "${VERSION:-}" ]]; then
  VERSION_VALUE="$VERSION"; VERSION_FROM="环境变量 VERSION"
elif [[ -n "${FLUXDOWN_APP_VERSION:-}" ]]; then
  VERSION_VALUE="$FLUXDOWN_APP_VERSION"; VERSION_FROM="环境变量 FLUXDOWN_APP_VERSION"
else
  TAG="$(git describe --tags --abbrev=0 --match 'v[0-9]*' 2>/dev/null || true)"
  if [[ -n "$TAG" ]]; then
    VERSION_VALUE="${TAG#v}"; VERSION_FROM="git tag $TAG"
  else
    VERSION_VALUE="$(sed -n 's/^version *= *"\([0-9][^"]*\)".*/\1/p' crates/app/Cargo.toml | head -1)"
    VERSION_FROM="crates/app/Cargo.toml（非发布版本）"
    OFFICIAL=0
  fi
fi
[[ -n "$VERSION_VALUE" ]] || die "无法确定发布版本，请用 --version 或环境变量 VERSION 指定"

# ---------------------------------------------------------------- 可用格式
read -r -a CANDIDATES <<< "$(platform_formats "$TARGET_PLATFORM")"

candidate_labels() {
  local f out=""
  for f in "${CANDIDATES[@]}"; do
    out+="$(format_label "$f")、"
  done
  echo "${out%'、'}"
}

# ---------------------------------------------------------------- 格式选择
SELECTED=()

selection_contains() {
  local needle="$1" item
  for item in "${SELECTED[@]+"${SELECTED[@]}"}"; do
    [[ -n "$item" && "$item" == "$needle" ]] && return 0
  done
  return 1
}

parse_selection() {
  local input="$1" token key idx c found
  local -a tokens
  local IFS=', '
  read -r -a tokens <<< "$input"
  SELECTED=()
  for token in "${tokens[@]}"; do
    [[ -z "$token" ]] && continue
    if [[ "$(lower "$token")" == all || "$token" == "*" ]]; then
      SELECTED=("${CANDIDATES[@]}")
      return 0
    fi
    if [[ "$token" =~ ^[0-9]+$ ]]; then
      idx=$((10#$token - 1))
      if (( idx < 0 || idx >= ${#CANDIDATES[@]} )); then
        warn "无效编号：$token"
        return 1
      fi
      key="${CANDIDATES[$idx]}"
    else
      key="$(normalize_format "$token")" || { warn "未知格式：$token"; return 1; }
      found=0
      for c in "${CANDIDATES[@]}"; do [[ "$c" == "$key" ]] && found=1; done
      if (( ! found )); then
        warn "该平台不支持格式：$token（支持：$(candidate_labels)）"
        return 1
      fi
    fi
    if ! selection_contains "$key"; then
      SELECTED+=("$key")
    fi
  done
  (( ${#SELECTED[@]} > 0 ))
}

prompt_selection() {
  local reply i
  info "请选择构建的目标包格式（可多选，如 1 3 或 deb,arch；直接回车 = 全部）："
  for i in "${!CANDIDATES[@]}"; do
    printf '  %2d) %s\n' "$((i + 1))" "$(format_label "${CANDIDATES[$i]}")"
  done
  while true; do
    printf '选择 > '
    if ! IFS= read -r reply; then
      reply=""
    fi
    if [[ -z "${reply//[[:space:],]/}" ]]; then
      SELECTED=("${CANDIDATES[@]}")
      info "未做出选择，构建全部支持的格式"
      return 0
    fi
    if parse_selection "$reply"; then
      return 0
    fi
    printf '请重新输入（直接回车 = 全部）：\n'
  done
}

if [[ -n "$FORMATS_ARG" ]]; then
  parse_selection "$FORMATS_ARG" \
    || die "无法解析 --bundles \"$FORMATS_ARG\"（$(platform_display "$TARGET_PLATFORM") 支持：$(candidate_labels)）"
  info "使用指定格式：${SELECTED[*]}"
elif (( ${#CANDIDATES[@]} == 1 )); then
  SELECTED=("${CANDIDATES[0]}")
  info "该平台仅一种可用格式，自动选择：$(format_label "${SELECTED[0]}")"
elif (( ASSUME_YES )) || [[ ! -t 0 ]]; then
  SELECTED=("${CANDIDATES[@]}")
  info "非交互模式，构建全部支持的格式：${SELECTED[*]}"
else
  prompt_selection
fi

FORMATS_CSV="$(IFS=,; echo "${SELECTED[*]}")"

# ---------------------------------------------------------------- Rust 目标
ensure_rust_target() {
  local t="$1"
  if ! command -v rustup >/dev/null 2>&1; then
    warn "未找到 rustup，跳过 Rust 目标检查（请确认已安装 $t）"
    return 0
  fi
  if rustup target list --installed 2>/dev/null | grep -qx "$t"; then
    return 0
  fi
  info "安装 Rust 目标：$t"
  if (( DRY_RUN )); then
    echo "  rustup target add $t"
    return 0
  fi
  rustup target add "$t"
}

if (( ! SKIP_BUILD )); then
  for t in "${TARGET_TRIPLES[@]}"; do
    ensure_rust_target "$t"
  done
fi

# ---------------------------------------------------------------- 依赖检查
if [[ "$TARGET_PLATFORM" == linux ]]; then
  need_cmd tar "请安装 tar"
  if selection_contains appimage; then
    APPIMAGETOOL_BIN="${APPIMAGETOOL:-appimagetool}"
    if command -v "$APPIMAGETOOL_BIN" >/dev/null 2>&1 || [[ -x "$APPIMAGETOOL_BIN" ]]; then
      :
    elif (( DRY_RUN )); then
      warn "缺少 appimagetool（实际构建前需要）：从 https://github.com/AppImage/appimagetool 下载，或设 APPIMAGETOOL=<路径>"
    else
      die "缺少 appimagetool：从 https://github.com/AppImage/appimagetool 下载，或设 APPIMAGETOOL=<路径>"
    fi
  fi
  if selection_contains deb; then
    need_cmd dpkg-deb "请安装 dpkg-dev（sudo apt-get install dpkg-dev）"
  fi
  if selection_contains arch; then
    need_cmd zstd "请安装 zstd（sudo apt-get install zstd）"
  fi
elif [[ "$TARGET_PLATFORM" == macos ]]; then
  need_cmd iconutil "需要 Xcode Command Line Tools（xcode-select --install）"
  need_cmd plutil "需要 Xcode Command Line Tools（xcode-select --install）"
  need_cmd codesign "需要 Xcode Command Line Tools（xcode-select --install）"
  need_cmd ditto "需要 Xcode Command Line Tools（xcode-select --install）"
  if [[ "$ARCH" == universal ]]; then
    need_cmd lipo "需要 Xcode Command Line Tools（xcode-select --install）"
  fi
  if selection_contains dmg; then
    need_cmd hdiutil "需要 macOS 主机"
  fi
elif [[ "$TARGET_PLATFORM" == windows ]]; then
  for c in powershell.exe pwsh pwsh.exe; do
    if command -v "$c" >/dev/null 2>&1; then POWERSHELL="$c"; break; fi
  done
  [[ -n "$POWERSHELL" ]] || die "找不到 PowerShell（需要 powershell.exe 或 pwsh）"
  if [[ -n "${ISCC:-}" && -f "$ISCC" ]]; then
    ISCC_PATH="$ISCC"
  elif command -v iscc.exe >/dev/null 2>&1; then
    ISCC_PATH="$(command -v iscc.exe)"
  elif [[ -f "/c/Program Files (x86)/Inno Setup 6/ISCC.exe" ]]; then
    ISCC_PATH="/c/Program Files (x86)/Inno Setup 6/ISCC.exe"
  elif [[ -f "/c/Program Files/Inno Setup 6/ISCC.exe" ]]; then
    ISCC_PATH="/c/Program Files/Inno Setup 6/ISCC.exe"
  fi
  if selection_contains setup && [[ -z "$ISCC_PATH" ]]; then
    if (( DRY_RUN )); then
      warn "缺少 Inno Setup 6 的 ISCC.exe（实际构建前需要）：choco install innosetup，或设 ISCC=<ISCC.exe 路径>"
    else
      die "缺少 Inno Setup 6 的 ISCC.exe：choco install innosetup，或设 ISCC=<ISCC.exe 路径>"
    fi
  fi
  if ! command -v cygpath >/dev/null 2>&1; then
    warn "未找到 cygpath（非 Git Bash / MSYS 环境），Windows 打包可能不可用"
  fi
fi

# ---------------------------------------------------------------- 低内存模式
# 正式 release profile 是 opt-level=3 + fat LTO + codegen-units=1，链接期峰值很高；
# 8GB 级机器上并行 rustc 很容易被 OOM killer 杀掉。这里只覆盖本次 cargo 进程的环境：
# 串行编译、thin LTO（跨 crate 内联保留，但按模块并行/分块）、16 个 codegen 单元、
# O2，并关闭增量与调试信息。产物不是发布等价物，只用于本机验证 / 出包。
LOW_MEM_ENV=()
if (( LOW_MEMORY )); then
  LOW_MEM_ENV=(
    CARGO_BUILD_JOBS="${JOBS_ARG:-1}"
    CARGO_INCREMENTAL=0
    CARGO_PROFILE_RELEASE_LTO=thin
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
    CARGO_PROFILE_RELEASE_OPT_LEVEL=2
    CARGO_PROFILE_RELEASE_DEBUG=false
  )
elif [[ -n "$JOBS_ARG" ]]; then
  LOW_MEM_ENV=(CARGO_BUILD_JOBS="$JOBS_ARG")
fi

if (( ! LOW_MEMORY && ! SKIP_BUILD )); then
  TOTAL_KIB=""
  if [[ -r /proc/meminfo ]]; then
    TOTAL_KIB="$(awk '/^MemTotal:/{print $2}' /proc/meminfo || true)"
  elif command -v sysctl >/dev/null 2>&1; then
    MEM_BYTES="$(sysctl -n hw.memsize 2>/dev/null || true)"
    if [[ -n "$MEM_BYTES" ]]; then
      TOTAL_KIB=$((MEM_BYTES / 1024))
    fi
  fi
  if [[ -n "$TOTAL_KIB" ]] && (( TOTAL_KIB < 12 * 1024 * 1024 )); then
    warn "检测到约 $((TOTAL_KIB / 1024 / 1024)) GiB 内存：release profile 的 fat LTO 容易 OOM，建议加 --low-memory（或至少 -j1）。"
  fi
fi

# ---------------------------------------------------------------- 构建
# 产物目录：macOS 打包脚本固定从 target/<triple>/release 取二进制（即使 triple 等于
# 主机三元组，也必须有 --target 构建）；其它平台在主机架构下沿用 target/release。
bin_dir_for() {
  local triple="$1"
  if [[ "$triple" == "$HOST_TRIPLE" ]]; then
    echo "target/release"
  else
    echo "target/$triple/release"
  fi
}

build_triple() {
  local triple="$1"
  local -a cmd=(cargo build --locked --release
    -p fluxdown_ui_app -p fluxdown_agent -p fluxdown_daemon -p fluxdown_nmh
    --features fluxdown_agent/desktop --bins)
  local need_target=1
  if [[ "$triple" == "$HOST_TRIPLE" && "$TARGET_PLATFORM" != macos ]]; then
    need_target=0
  fi
  if (( need_target )); then
    cmd+=(--target "$triple")
  fi
  if (( VERBOSE )); then
    cmd+=(--verbose)
  fi

  local -a env_args=()
  if (( ${#LOW_MEM_ENV[@]} > 0 )); then
    env_args+=("${LOW_MEM_ENV[@]}")
  fi
  if (( OFFICIAL )); then
    env_args+=(FLUXDOWN_APP_VERSION="$VERSION_VALUE")
  fi
  case "$triple" in
    x86_64-apple-darwin)  env_args+=(MACOSX_DEPLOYMENT_TARGET=10.15.7) ;;
    aarch64-apple-darwin) env_args+=(MACOSX_DEPLOYMENT_TARGET=11.0) ;;
  esac

  if (( DRY_RUN )); then
    printf '  '
    if (( ${#env_args[@]} > 0 )); then
      printf '%s ' "${env_args[@]}"
    fi
    printf '%s\n' "${cmd[*]}"
    return 0
  fi
  info "构建 $triple ..."
  env "${env_args[@]+"${env_args[@]}"}" "${cmd[@]}"
}

# ---------------------------------------------------------------- Windows 打包
win_path() {
  if command -v cygpath >/dev/null 2>&1; then
    cygpath -w "$1"
  else
    printf '%s' "$1"
  fi
}

run_powershell_stdin() {
  # PowerShell 脚本经 stdin 传入，路径 / 版本等一律走环境变量，避免 MSYS 路径转换。
  "$POWERSHELL" -NoProfile -NonInteractive -ExecutionPolicy Bypass -Command -
}

stage_windows_bins() {
  local arch="$1" triple="$2"
  local stage="$REPO/build/gpui/windows-$arch"
  local rel="$REPO/$(bin_dir_for "$triple")"

  if (( DRY_RUN )); then
    echo "  PowerShell: 暂存 4 个 .exe + MSVC CRT → build/gpui/windows-$arch"
    return 0
  fi
  info "暂存 Windows 产物：build/gpui/windows-$arch"
  FLUXDOWN_STAGE="$(win_path "$stage")" \
  FLUXDOWN_RELEASE="$(win_path "$rel")" \
  FLUXDOWN_ARCH="$arch" \
  run_powershell_stdin <<'PS'
$ErrorActionPreference = 'Stop'
$stage = $env:FLUXDOWN_STAGE
$rel = $env:FLUXDOWN_RELEASE
if (-not (Test-Path (Join-Path $rel 'fluxdown-desktop.exe'))) {
  throw "missing build output: $rel\fluxdown-desktop.exe"
}
Remove-Item -Recurse -Force $stage -ErrorAction SilentlyContinue
New-Item -ItemType Directory -Force -Path $stage | Out-Null
foreach ($bin in 'fluxdown-desktop', 'fluxdown-agent', 'fluxdownd', 'fluxdown_nmh') {
  Copy-Item (Join-Path $rel "$bin.exe") $stage
}
$vswhere = Join-Path ${env:ProgramFiles(x86)} 'Microsoft Visual Studio\Installer\vswhere.exe'
if (-not (Test-Path $vswhere)) {
  throw "vswhere not found: $vswhere (install Visual Studio / Build Tools)"
}
$vsRoot = & $vswhere -latest -products * -property installationPath
if (-not $vsRoot) { throw 'Visual Studio installation not found via vswhere' }
$archName = $env:FLUXDOWN_ARCH
$crtDir = Get-ChildItem "$vsRoot\VC\Redist\MSVC\*\$archName\Microsoft.VC1*.CRT" -Directory |
  Sort-Object FullName | Select-Object -Last 1
if (-not $crtDir) { throw "MSVC CRT redist for $archName not found under $vsRoot" }
Copy-Item "$($crtDir.FullName)\msvcp140*.dll", "$($crtDir.FullName)\vcruntime140*.dll" $stage -Force
foreach ($required in 'msvcp140.dll', 'vcruntime140.dll') {
  if (-not (Test-Path (Join-Path $stage $required))) { throw "$required missing after CRT copy" }
}
PS
}

package_windows_setup() {
  local arch="$1"
  local stage="$REPO/build/gpui/windows-$arch"
  local iss="$REPO/installer/windows/setup.iss"
  local lang="$REPO/installer/windows/ChineseSimplified.isl"
  local iscc_win=""
  if [[ -n "$ISCC_PATH" ]]; then
    iscc_win="$(win_path "$ISCC_PATH")"
  fi

  if (( DRY_RUN )); then
    echo "  PowerShell: iscc /DMyAppVersion=$VERSION_VALUE /DMyAppArch=$arch /DMySourceDir=<stage> installer/windows/setup.iss"
    return 0
  fi
  info "生成 Inno Setup 安装程序 ..."
  FLUXDOWN_ISCC="$iscc_win" \
  FLUXDOWN_ISS="$(win_path "$iss")" \
  FLUXDOWN_LANG_SRC="$(win_path "$lang")" \
  FLUXDOWN_STAGE="$(win_path "$stage")" \
  FLUXDOWN_ARCH="$arch" \
  FLUXDOWN_VERSION="$VERSION_VALUE" \
  run_powershell_stdin <<'PS'
$ErrorActionPreference = 'Stop'
$iscc = $env:FLUXDOWN_ISCC
if (-not $iscc) { throw 'ISCC.exe not found (install Inno Setup 6 or set ISCC=<path>)' }
$lang = Join-Path (Split-Path $iscc) 'Languages\ChineseSimplified.isl'
if (-not (Test-Path $lang)) {
  New-Item -ItemType Directory -Force -Path (Split-Path $lang) | Out-Null
  try {
    Copy-Item $env:FLUXDOWN_LANG_SRC $lang -Force
  } catch {
    throw "cannot write $lang (needs administrator). Copy $($env:FLUXDOWN_LANG_SRC) there manually, or run this script elevated."
  }
}
& $iscc "/DMyAppVersion=$($env:FLUXDOWN_VERSION)" "/DMyAppArch=$($env:FLUXDOWN_ARCH)" "/DMySourceDir=$($env:FLUXDOWN_STAGE)" $env:FLUXDOWN_ISS
if ($LASTEXITCODE -ne 0) { throw "iscc failed with exit code $LASTEXITCODE" }
PS
}

package_windows_portable() {
  local arch="$1" triple="$2"
  local stage="$REPO/build/gpui/windows-$arch"
  local portable="$REPO/build/gpui/portable-$arch"
  local rel="$REPO/$(bin_dir_for "$triple")"
  local zip="$OUT_DIR/FluxDown-$VERSION_VALUE-windows-$arch-portable.zip"

  if (( DRY_RUN )); then
    echo "  PowerShell: 便携 zip → build/installer/FluxDown-$VERSION_VALUE-windows-$arch-portable.zip"
    return 0
  fi
  info "生成便携 zip ..."
  FLUXDOWN_STAGE="$(win_path "$stage")" \
  FLUXDOWN_PORTABLE="$(win_path "$portable")" \
  FLUXDOWN_RELEASE="$(win_path "$rel")" \
  FLUXDOWN_ZIP="$(win_path "$zip")" \
  run_powershell_stdin <<'PS'
$ErrorActionPreference = 'Stop'
$stage = $env:FLUXDOWN_STAGE
$portable = $env:FLUXDOWN_PORTABLE
$rel = $env:FLUXDOWN_RELEASE
$zip = $env:FLUXDOWN_ZIP
Remove-Item -Recurse -Force $portable -ErrorAction SilentlyContinue
Copy-Item $stage $portable -Recurse
New-Item -ItemType File -Force -Path (Join-Path $portable 'portable') | Out-Null
Copy-Item (Join-Path $rel 'flux_down.exe') $portable
New-Item -ItemType Directory -Force -Path (Split-Path $zip) | Out-Null
Compress-Archive -Path (Join-Path $portable '*') -DestinationPath $zip -Force
PS
}

# ---------------------------------------------------------------- 打包分发
package_linux() {
  local bin_dir
  bin_dir="$(bin_dir_for "$TARGET_TRIPLE")"
  if (( DRY_RUN )); then
    echo "  VERSION=$VERSION_VALUE BIN_DIR=$bin_dir OUT_DIR=$OUT_DIR FORMATS=$FORMATS_CSV scripts/package_gpui_linux.sh"
    return 0
  fi
  info "打包 Linux（${SELECTED[*]}）..."
  VERSION="$VERSION_VALUE" BIN_DIR="$bin_dir" OUT_DIR="$OUT_DIR" FORMATS="$FORMATS_CSV" \
    scripts/package_gpui_linux.sh
}

package_macos() {
  local archs prefix
  case "$ARCH" in
    universal) archs="arm64 x86_64" ;;
    arm64)     archs="arm64" ;;
    *)         archs="x86_64" ;;
  esac
  prefix="FluxDown-$VERSION_VALUE-macos-$ARCH_LABEL"
  if (( DRY_RUN )); then
    echo "  ARCHS='$archs' VERSION=$VERSION_VALUE OUT_DIR=$OUT_DIR ARTIFACT_PREFIX=$prefix SKIP_BUILD=1 FORMATS=$FORMATS_CSV scripts/package_gpui_macos.sh"
    return 0
  fi
  info "打包 macOS（${SELECTED[*]}）..."
  ARCHS="$archs" VERSION="$VERSION_VALUE" OUT_DIR="$OUT_DIR" ARTIFACT_PREFIX="$prefix" \
    SKIP_BUILD=1 FORMATS="$FORMATS_CSV" scripts/package_gpui_macos.sh
}

package_windows() {
  local triple="$TARGET_TRIPLE"
  stage_windows_bins "$ARCH" "$triple"
  if selection_contains setup; then
    package_windows_setup "$ARCH"
  fi
  if selection_contains portable; then
    package_windows_portable "$ARCH" "$triple"
  fi
}

# ---------------------------------------------------------------- 汇总
echo
info "目标平台 : ${BOLD}${PLATFORM_DISPLAY}${RESET}  (${TARGET_TRIPLE})"
info "主机平台 : $(platform_display "$HOST_PLATFORM")  (${HOST_TRIPLE})"
info "包格式   : ${SELECTED[*]}"
info "版本     : ${VERSION_VALUE}  (${VERSION_FROM})"
info "输出目录 : build/installer"
if (( LOW_MEMORY )); then
  info "编译模式 : 低内存（-j${JOBS_ARG:-1}，thin LTO，CGU 16，O2，无增量）"
elif [[ -n "$JOBS_ARG" ]]; then
  info "编译模式 : -j${JOBS_ARG}"
fi
if (( SKIP_BUILD )); then
  info "版本注入 : 跳过（--skip-build，复用已有产物）"
elif (( OFFICIAL )); then
  info "版本注入 : FLUXDOWN_APP_VERSION=${VERSION_VALUE}（按正式版本构建）"
else
  warn "未指定发布版本，使用 ${VERSION_VALUE} 且不注入 FLUXDOWN_APP_VERSION（应用视为非官方构建，不参与自动更新）"
fi
echo

if (( DRY_RUN )); then
  info "dry-run：将执行以下命令"
fi

START=$SECONDS
if (( SKIP_BUILD )); then
  info "--skip-build：复用已有产物，跳过 cargo build"
else
  info "开始构建 ..."
  for t in "${TARGET_TRIPLES[@]}"; do
    build_triple "$t"
  done
fi
if (( ! DRY_RUN )); then
  mkdir -p "$OUT_DIR"
fi

case "$TARGET_PLATFORM" in
  linux)   package_linux ;;
  windows) package_windows ;;
  macos)   package_macos ;;
esac

if (( DRY_RUN )); then
  info "dry-run 模式，未执行构建"
  exit 0
fi

info "完成，耗时 $((SECONDS - START)) 秒"

# ---------------------------------------------------------------- 产物清单
echo
if [[ -d "$OUT_DIR" ]]; then
  shopt -s nullglob
  ARTIFACTS=("$OUT_DIR"/FluxDown-"$VERSION_VALUE"-"$TARGET_PLATFORM"-"$ARCH_LABEL"*)
  shopt -u nullglob
  if (( ${#ARTIFACTS[@]} > 0 )); then
    info "构建产物："
    printf '  %s\n' "${ARTIFACTS[@]}"
    if [[ "$TARGET_PLATFORM" == macos ]]; then
      info "macOS 配置了公证凭据时 DMG 已公证并 staple；无凭据时为 ad-hoc 签名（不可分发）。"
    fi
  else
    warn "未在 $OUT_DIR 找到 FluxDown-${VERSION_VALUE}-${TARGET_PLATFORM}-${ARCH_LABEL}* 产物"
  fi
else
  warn "未找到产物目录：$OUT_DIR"
fi
