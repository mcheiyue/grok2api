# 上游同步说明（mcheiyue/grok2api）

本仓库是 [chenyme/grok2api](https://github.com/chenyme/grok2api) 的 **fork**，用作「Build + Console 并集」改造主线。

## 钉死信息

| 项 | 值 |
|----|-----|
| 本仓 | `https://github.com/mcheiyue/grok2api` |
| 上游 | `https://github.com/chenyme/grok2api` |
| 本地路径 | `D:\OpenCode\VPS\Grok改造\chen-fork\` |
| W0 基线 tip | **`90ec921`**（同步 upstream/main，含 #636 一带） |
| 本地 tip（含改造） | **`d301104`**（docs tip；业务 W2.5 `b40ad1a` WARP profile） |
| 上游版本文件 | `VERSION` = `v3.0.0` |
| 镜像（fork 推 main 后） | `ghcr.io/mcheiyue/grok2api:latest`（workflow 按 `GITHUB_REPOSITORY` 命名） |

能力源（非上游）：`../Gork/`（mcheiyue/Gork，Console/防封/选号）。

## Remote 约定

```text
origin    → mcheiyue/grok2api   （日常 push）
upstream  → chenyme/grok2api    （只读同步）
```

初始化（已完成时可跳过）：

```powershell
cd D:\OpenCode\VPS\Grok改造\chen-fork
git remote -v
# 若缺 upstream：
git remote add upstream https://github.com/chenyme/grok2api.git
```

## 同步命令（推荐）

```powershell
cd D:\OpenCode\VPS\Grok改造\chen-fork
git fetch upstream
git checkout main
# 尚未有本地改造分支分叉时：
git merge --ff-only upstream/main
git push origin main

# 或用 GitHub 侧强制对齐（慎用，会覆盖 origin/main 上未上游的提交）：
# gh repo sync mcheiyue/grok2api --source chenyme/grok2api --force
```

改造开发请用分支，例如：

```powershell
git checkout -b w1/bypass-compose
# ... 改完后 PR 合入 main
```

## 冲突策略

1. **默认**：`upstream/main` 快进合并到 `main`；有本地 commit 时用 merge commit，不用 rebase 已推送历史。
2. **我们改过的文件与上游冲突**：优先保留 fork 业务意图（Console 迁入、模型别名、防封 profile），再手工合上游 bugfix。
3. **纯上游目录未改动**：接受上游。
4. **密钥 / 数据卷 / 生产 compose**：永不进入本仓库；只放 example 与文档。

## 同步频率

| 场景 | 频率 |
|------|------|
| W0–W1 建基 | 每个工作切片开始前 `git fetch upstream` 并看 `cli/**` / console / admin 变更 |
| W2+ 改造期 | 至少每周一次；上游 security/hot-fix 随时 cherry-pick |
| 切流前 | 必须再同步一次并跑 W1 最小验收 |

## 验证（本地）

```powershell
cd D:\OpenCode\VPS\Grok改造\chen-fork\backend
go test ./...
```

前端（可选，需 pnpm）：

```powershell
cd D:\OpenCode\VPS\Grok改造\chen-fork\frontend
pnpm install --frozen-lockfile
pnpm build
```

## 与工作区文档

- 现行规划：`../plans/chen-fork-改造规划.md`（v1.4）
- W3 运营：`../plans/W3-进度.md` 及 W3.1–W3.4 文档
- 工作区入口：`../README.md`
- 旧 Gork 吸收史料：`../archive/gork-absorb-legacy/`（只读）

---

*W0 建立 · W2/W3 文档同步 · 2026-07-15*
