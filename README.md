# 知识图谱在民营经济政策薄弱环节识别与量化评估中的应用

本项目是2025年春季课程《知识图谱及其应用》的课程大作业的代码，旨在探索运用知识图谱（Knowledge Graph）技术，并结合大型语言模型（LLM），对现有民营经济促进政策进行系统性梳理与分析，以实现对政策体系中潜在空白点或薄弱环节的精准识别和量化评估。项目实现了一套完整的技术方案，从多源异构数据的自动化收集与深度清洗，到基于大语言模型的多阶段、多任务知识抽取与融合，再到构建具有严谨Schema的Neo4j知识图谱，最终通过一个集成了复杂图查询与高级AI分析的前后端应用，实现了对政策支持度的量化评估与智能化报告生成。
![demo.png](https://img.picui.cn/free/2025/07/09/686e3a4126ae9.png)

## 涉及技术

- **知识图谱数据库**: `Neo4j`
- **后端服务**: `Python`, `Flask`
- **语言模型交互**: `DeepSeek`
- **前端界面**: `HTML`, `JavaScript`, `CSS`
- **数据处理**: `Pandas`

## 系统架构与工作流

本项目的核心是构建一个从原始政策文本到深度分析报告的端到端自动化处理系统。整个流程分为以下几个关键模块：

1. **数据收集与预处理 (`data_clean.py`)**
   - 数据来源于“北大法宝V6”及“国家法律法规数据库”，格式涵盖PDF、Word、HTML等，首先被统一转换为纯文本（.txt）格式。
   - 通过`data_clean.py`脚本对文本内容进行深度清洗，包括移除元数据行、重复标题，并利用正则表达式提取官方发文字号。
   - 脚本将政策目录（Excel）与全文文本进行匹配，并对缺失的元数据（如发文字号）进行回填，最终生成一个整合了元数据与全文的干净CSV文件。
2. **基于LLM的知识抽取**
   - 利用大型语言模型（LLM）进行多阶段、多任务的知识抽取。
   - **机构实体标准化 (`disambiguation.py`)**: 解决“制定机关”名称不一致的问题。通过精心设计的Prompt模板，为LLM设定“中国政府机构结构专家”的角色，并内置标准化规则，同时结合预定义的机构名录进行优化匹配。
   - **核心要素抽取 (`core_entity_types.py`)**: 从政策文本中识别出`PolicyTopic` (政策主题), `PolicyTool` (政策工具)等五大核心实体。为每种实体类型定义了详尽的描述、候选列表和抽取指令，并要求LLM以JSON格式返回结果。
   - **量化信息提取 (`quantitative_info.py`)**: 将非结构化的量化描述（如“补贴50万元”）转化为结构化数据。该脚本为90多种政策工具预设了精细的量化提取模板，并采用异步并发架构高效处理API调用，提升了处理速度。
3. **知识图谱建模与存储 (`schema_v2.py`)**
   - **Schema设计**: 设计了包含`Policy`, `IssuingBody`等七种核心实体及它们之间关系（如 `ISSUED_BY`, `APPLIES_TOOL`）的图模型。一个关键设计是将量化信息作为`:APPLIES_TOOL`关系的属性，因为它描述的是特定政策应用特定工具时的具体体现。
   - **数据库初始化**: 通过`setup_database_schema`函数为每种实体的主要标识符创建唯一性约束，这保证了数据唯一性并能加速查询。
   - **事务化数据加载**: 数据加载逻辑被封装在独立的事务函数中（如`create_policy_tx`, `link_policy_to_tool_tx`），保证了操作的原子性和数据库的一致性。
4. **应用层 (前后端)**
   - **后端服务 (`app_with_llm.py`)**: 基于Flask构建，负责数据查询、逻辑处理和与LLM的交互。
     - 核心的`get_policy_metrics_for_scope`函数使用动态构建的Cypher查询，在图数据库中进行复杂的层级化政策聚合与分析。
     - `call_deepseek_llm_for_analysis`函数则负责将图谱查询出的量化指标与用户设定的阈值发送给LLM，生成深度分析报告。
   - **前端交互 (`index_withLLM.html`)**:
     - 提供用户友好的界面，允许用户输入分析维度（如区域、主题）和评估阈值。
     - 通过异步`fetch`请求与后端通信，并使用`marked.js`库将后端返回的Markdown格式报告动态渲染为富文本HTML，实现了前后端的关注点分离。

## 主要功能

- **政策强度量化评估**: 用户可以根据特定维度（如区域、政策主题、受益对象等）查询知识图谱，获取量化的政策覆盖指标，如政策数量、平均政策年龄、政策工具多样性等。
- **政策薄弱环节识别**: 系统结合用户设定的评估阈值，对查询结果进行分析，自动识别并指出在特定范围内政策支持的潜在空白点或薄弱环节。
- **智能化分析报告生成**: 集成大型语言模型（LLM），对量化的图谱查询结果进行深度解读和对比分析（如与全国基准对比），最终生成结构化、易于理解的分析报告。

## 安装与使用

1. **环境配置**

   - **Neo4j数据库**:

     - 确保您的Neo4j数据库正在运行。
     - 在以下文件中更新您的数据库连接信息（URI, 用户名, 密码）：
       - `KG_policy/schema_v2.py`
       - `task1_withLLM/app_withllm.py`

   - **API密钥**:

     - 本项目使用DeepSeek LLM。请在以下文件中填入您的API密钥：
       - `KG_policy/disambiguation.py`
       - `KG_policy/core_entity_types.py`
       - `KG_policy/quantitative_info.py`
       - `task1_withLLM/app_withllm.py`

   - **Python依赖**:

     - 安装所有必要的Python库，例如 `pandas`, `neo4j`, `flask`, `flask-cors`, `openai`。

       Bash

       ```
       pip install pandas neo4j flask flask-cors openai
       ```

2. **数据处理与图谱构建**

   - **数据清洗**: 运行 `KG_policy/data_clean.py` 脚本，它将处理原始数据并生成 `combined_policy_data_adjusted_v2.csv`。
   - **知识抽取**: 依次运行以下脚本，利用LLM进行实体和信息的抽取：
     1. `KG_policy/disambiguation.py` (机构实体标准化)
     2. `KG_policy/core_entity_types.py` (核心要素抽取)
     3. `KG_policy/quantitative_info.py` (量化信息提取)
   - **图谱导入**: 运行 `KG_policy/schema_v2.py`，此脚本会连接到Neo4j，创建约束和索引，并将所有处理好的CSV数据导入到知识图谱中。

3. **启动应用**

   - **启动后端服务**:

     Bash

     ```
     python task1_withLLM/app_withllm.py
     ```

     服务将在 `http://127.0.0.1:5001` 上运行。

   - **访问前端**: 在您的网络浏览器中直接打开 `task1_withLLM/index_withLLM.html` 文件。

4. **开始分析**

   - 在前端页面上，配置您感兴趣的分析参数，如分析区域、政策主题和各项评估阈值。
   - 点击“开始分析”按钮，系统将向后端发送请求，后端查询图谱并将结果交由LLM进行分析，最终将生成的报告返回并展示在页面上。

## 文件说明

- `知识图谱在民营经济政策薄弱环节识别与量化评估中的应用.md`: 项目的详细技术报告，阐述了研究背景、目标、技术方案和实现细节。
- `/KG_policy/`: 包含数据处理和知识图谱构建流程的所有核心脚本。
  - `data_clean.py`: 原始数据清洗与整合。
  - `disambiguation.py`, `core_entity_types.py`, `quantitative_info.py`: 基于LLM的知识抽取脚本。
  - `schema_v2.py`: 定义图谱模式，并将处理后的数据导入Neo4j。
  - `policy_tool.xlsx`, `area_code.xlsx`: 用于丰富图谱节点属性的外部数据映射表。
- `/task1_withLLM/`: 包含LLM增强版应用的前后端代码。
  - `app_withllm.py`: Flask后端应用，处理API请求，查询图谱并调用LLM进行分析。
  - `index_withLLM.html`: 应用的前端用户界面。
