#!/usr/bin/env bash
# 低内存 cargo 包装：在 8GB 级机器上避免 rustc 被 OOM killer 杀掉。
#
# 用法：
#   scripts/cargo-low-mem.sh build -p fluxdown_daemon
#   scripts/cargo-low-mem.sh check -p fluxdown_engine --lib
#   scripts/cargo-low-mem.sh nextest run -p fluxdown_engine <filter>
#   scripts/cargo-low-mem.sh build --release -p fluxdown_ui_app -p fluxdown_agent ...
#
# 相对默认设置的变化（都是进程级环境变量，不改 Cargo.toml）：
#   CARGO_BUILD_JOBS=1                并行 rustc 数量降为 1（峰值 = 单个 crate + 一次链接）
#   CARGO_INCREMENTAL=0               关闭增量编译（省内存与磁盘）
#   CARGO_PROFILE_RELEASE_LTO=thin    替代 fat LTO：保留跨 crate 内联，但按模块分块，
#   CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16
#   CARGO_PROFILE_RELEASE_OPT_LEVEL=2 替代 O3。链接期内存峰值大幅下降
#   CARGO_PROFILE_DEV_DEBUG=0         dev 构建不生成调试信息（显著降低编译内存）
#
# 代价：编译更慢；release 产物的优化参数与正式发布包不同（thin LTO / O2 / 多 CGU），
# 只用于本机开发与验证，不要作为发布资产分发。发布构建请用 ./release-build.sh
# （内存受限时加 --low-memory，或直接 `./release-build.sh --low-memory`）。
# 注意：profile 参数不同会让 cargo 判定缓存失效，本包装与直接 `cargo` 交替使用会触发重编；
# 固定在一种方式上即可。
#
# 可选：安装 lld 后链接期内存更低、速度更快，可再加 RUSTFLAGS='-C link-arg=-fuse-ld=lld'
# 前缀（sudo apt-get install lld）；该变量会改变缓存指纹，首次使用会全量重编。
#
# 可用环境变量覆盖：MEM_JOBS（默认 1）、CARGO_PROFILE_RELEASE_LTO / _CODEGEN_UNITS /
# _OPT_LEVEL、CARGO_PROFILE_DEV_DEBUG。cargo 命令行上的 -j 优先于 MEM_JOBS。
set -euo pipefail

REPO="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO"

if [[ $# -eq 0 ]]; then
  cat >&2 <<'USAGE'
用法：scripts/cargo-low-mem.sh <cargo 子命令> [参数...]

示例：
  scripts/cargo-low-mem.sh build -p fluxdown_daemon
  scripts/cargo-low-mem.sh check -p fluxdown_engine --lib
  scripts/cargo-low-mem.sh nextest run -p fluxdown_engine <filter>
USAGE
  exit 2
fi

export CARGO_BUILD_JOBS="${MEM_JOBS:-${CARGO_BUILD_JOBS:-1}}"
export CARGO_INCREMENTAL=0
export CARGO_PROFILE_RELEASE_LTO="${CARGO_PROFILE_RELEASE_LTO:-thin}"
export CARGO_PROFILE_RELEASE_CODEGEN_UNITS="${CARGO_PROFILE_RELEASE_CODEGEN_UNITS:-16}"
export CARGO_PROFILE_RELEASE_OPT_LEVEL="${CARGO_PROFILE_RELEASE_OPT_LEVEL:-2}"
export CARGO_PROFILE_DEV_DEBUG="${CARGO_PROFILE_DEV_DEBUG:-0}"

exec cargo "$@"
