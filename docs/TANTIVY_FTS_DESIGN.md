# DuckDB Tantivy 全文检索设计

扩展只负责 feature/build wiring 和 provider 注册。核心能力与 SQL 实现位于固定版本的 `AstroVela/vortex`：`vortex-index` 定义 FullTextIndex/FullTextHit，`vortex-fts` 实现 `TantivyProvider`，`vortex-duckdb` 实现 SQL bind、执行、缓存、验证和按排名回表。

`VORTEX_ENABLE_INDEX_TANTIVY` 将 `index-tantivy` 传给 Corrosion，普通和 Vane Rust manifest 都启用 `vortex-duckdb/index` 与 `vortex-fts`。扩展加载前注册 `tantivy.static` factory，每次打开 generation 使用独立受生命周期约束的 scratch lease。不存在注册时，SQL build 明确报未知后端，不会转用其他检索算法。

DuckDB 1.5 的动态加载入口接收 `ExtensionLoader &`，通过 `GetDatabaseInstance()` 调用 Rust 初始化；legacy `vortex_init(DatabaseInstance &)` 继续用于对应入口。这一点通过实际 LOAD 验证，避免仅内部单测成功但动态库不可加载。

两个 SQL table functions 复用现有索引基础设施。build 只在执行阶段构造，完成 seal/reopen 与源复验后原子发布；search 在 bind 阶段固定返回 schema 和 reference identity，执行时校验访问权限与 identity，并使用严格验证或显式 prepared snapshot。通用缓存不再强制向量 capability，具体入口分别检查 full text 或 squared L2，向量行为保持原约束。

一个索引覆盖所有输入文件，Tantivy Searcher 提供统一 BM25 统计。先应用 RowFilter 再维护 top-k 堆，按 score DESC/RowAddress ASC 确定排序；回表保留排名。SQL 外层谓词不下推到索引，用户不能把外层 WHERE 当作候选集合限制。

完整存储格式、资源上限、故障路径与 Iceberg 快照物化设计见核心仓库 `vortex-fts/docs/design.md` 和 Iceberg 配套设计文档。依赖升级必须同时更新两个 Rust manifest 与锁文件，保持 provider 和 native SQL ABI 一致。
